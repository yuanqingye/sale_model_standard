#低频消费可能要更加使用统计知识以减少偶然性
#initialization can't be done if already have value in var
# sale_data_list = list()
# contract_list = list()
# sale_data_stall_sum_list = list()
# sale_perarea_in_period = list(list(list()))
# sale_perarea_by_brand = list(list(list()))

#get sale data by function
source("~/Rfile/R_hive.R")
sale_data_list[["shanghaizhenbei"]] = get_sale_data_by_mall_name("上海真北商场")
#get on sale date
on_sale_data = get_on_sale_date_and_clean()
#get contract data
source('~/Rfile/R_hana.R', encoding = 'UTF-8')
contract_list[["shanghaizhenbei"]] = get_contract_data("上海真北商场")
#get combined data(with contract and clean the data)
sale_data_picked_list[["shanghaizhenbei"]] = join_clean_sale_and_contract_data(sale_data_list[["shanghaizhenbei"]],contract_list[["shanghaizhenbei"]])
#get each stall's data(over the whole period)
sale_data_stall_sum_list[["shanghaizhenbei"]] = setup_stall_metrics(sale_data_picked_list[["shanghaizhenbei"]])

source("~/Rfile/R_hive.R")
# sale_data_list[["copy"]] = sale_data_list[["shanghaijinqiao"]]
sale_data_list[["shanghaijinqiao"]] = get_sale_data_by_mall_name("上海金桥商场")
#get on sale date
on_sale_data = get_on_sale_date_and_clean()
#get contract data
source('~/Rfile/R_hana.R', encoding = 'UTF-8')
contract_list[["shanghaijinqiao"]] = get_contract_data("上海金桥商场")

# dict_CAT2 = unique(sale_data_picked_list[["shanghaijinqiao"]][,c("cont_cat2_name","CATEGORY_NAME_2")])
# this step can be updated to an automated process
# fix(dict_CAT2)
# dict_CAT2 = dict_CAT2[,c("CATEGORY_NAME_2","CATEGORY_2_EDIT")]
# dict_CAT2_copy = dict_CAT2
dict_CAT2 = unique(dict_CAT2)
dict_CAT2 = data.table(dict_CAT2)
dict_CAT2$CATEGORY_2_EDIT = enc2utf8(dict_CAT2$CATEGORY_2_EDIT)
Sys.setlocale(category = "LC_ALL",locale = "English_United States.1252")
temp = merge(contract_list[["shanghaijinqiao"]],dict_CAT2,by = "CATEGORY_NAME_2",all.x = TRUE)
Sys.setlocale(category = "LC_ALL",locale = "")
temp[str_detect(CATEGORY_NAME_2,"青少年家具/儿童家具/儿童家具/儿童家具/儿童家具/儿童家具"),"CATEGORY_2_EDIT"] = "青少年家具"
temp[str_detect(CATEGORY_NAME_2,"儿童家具/儿童家具/青少年家具/儿童家具/儿童家具/青少年家具/儿童家具/青少年家具"),"CATEGORY_2_EDIT"] = "青少年家具"
temp[str_detect(CATEGORY_NAME_2,"全屋定制/板木/实木/实木"),"CATEGORY_2_EDIT"] = "板木"
temp[is.na(CATEGORY_2_EDIT),CATEGORY_2_EDIT:=CATEGORY_NAME_2]
contract_list[["shanghaijinqiao"]] = temp

#get combined data(with contract and clean the data)
sale_data_picked_list[["shanghaijinqiao"]] = join_clean_sale_and_contract_data(sale_data_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]])
#get each stall's data(over the whole period)
sale_data_stall_sum_list[["shanghaijinqiao"]] = setup_stall_metrics(sale_data_picked_list[["shanghaijinqiao"]])

#品类分布
#Calculate and demonstrate metrics on category
#plot the sale per area by some classifier(like category 2) on specific month, here it is 2018-03
plot_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-03-01",finish_date = "2018-03-31",catvar = "CATEGORY_2_EDIT")
plot_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-04-01",finish_date = "2018-04-30",catvar = "CATEGORY_2_EDIT")
#get the sale per area by some classifier(like category 2) on specific month, here it is 2018-03
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_2_EDIT"]][["2018-03"]] = generate_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-03-01",finish_date = "2018-03-31",catvar = "CATEGORY_2_EDIT")
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_2_EDIT"]][["2018-04"]] = generate_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-04-01",finish_date = "2018-04-30",catvar = "CATEGORY_2_EDIT")
#品类系数
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_2_EDIT"]][["2018-03"]] = sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_2_EDIT"]][["2018-03"]][,saleperarea_median := median(saleperarea,na.rm = TRUE)]
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_2_EDIT"]][["2018-03"]] = sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_2_EDIT"]][["2018-03"]][,cat2_index := saleperarea/saleperarea_median]

#品牌分布
#plot CATEGORY on diff panel
#Default setting is brandlist = c("实木","卫浴","瓷砖")
plot_sale_perarea_by_cat2_brand(sale_data_picked_list[["shanghaijinqiao"]]) #result not good
#品牌系数
sale_perarea_by_brand = sale_data_picked_list[["shanghaijinqiao"]][,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("CATEGORY_2_EDIT","SERIES_NAME")]
sale_perarea_by_brand[,saleperarea_median := median(saleperarea,na.rm = TRUE),by = "CATEGORY_2_EDIT"]
sale_perarea_by_brand[,brand_index := saleperarea/saleperarea_median]
#plot CATEGORY on the same panel
cat_list = enc2utf8(c("实木","卫浴","瓷砖"))
cat_list = unique(sale_perarea_by_brand$CATEGORY_2_EDIT)[1:3]
plot_multiple_factor(data = sale_perarea_by_brand[CATEGORY_2_EDIT %in% cat_list,],"CATEGORY_2_EDIT","SERIES_NAME","saleperarea")

#品类年份，季节，工作日/周末单位面积销量分布
#品类单位面积销量（当下限定在某个月）工作日/周末,本函数特别针对七月
sale_perarea_by_weekend_dcast = plot_sale_perarea_by_weekend(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],"CATEGORY_2_EDIT")
#品类单位面积销量（当下限定在某一年）不同季节比较
sale_perarea_by_season = plot_sale_perarea_by_season(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],year = "2017",catvar = "CATEGORY_2_EDIT")
#品类单位面积销量（当下限定在某个月）在促销和非促销情形下的比较
sale_perarea_by_onsale = plot_sale_perarea_by_onsale(sale_data_picked_list[["shanghaijinqiao"]],on_sale_data,contract_list[["shanghaijinqiao"]],catvar = "CATEGORY_2_EDIT",year = '2017')
#品类单位面积销量（当下限定在某个月）不同年的比较  
sale_perarea_by_year = plot_sale_perarea_by_year(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],catvar = "CATEGORY_2_EDIT")

#find the intersection of these 2 malls
intersected_brand = data.frame(brand_name = intersect(unique(contract_list[["shanghaizhenbei"]]$BRAND_NAME),unique(contract_list[["shanghaijinqiao"]]$BRAND_NAME)))
