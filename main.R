#低频消费可能要更加使用统计知识以减少偶然性
#initialization can't be done if already have value in var
# sale_data_list = list()
# contract_list = list()
# sale_data_stall_sum_list = list()
# sale_perarea_in_period = list(list(list()))
sale_perarea_by_brand = list(list(list()))

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
sale_data_list[["shanghaijinqiao"]] = get_sale_data_by_mall_name("上海金桥商场")
#get on sale date
on_sale_data = get_on_sale_date_and_clean()
#get contract data
source('~/Rfile/R_hana.R', encoding = 'UTF-8')
contract_list[["shanghaijinqiao"]] = get_contract_data("上海金桥商场")
#get combined data(with contract and clean the data)
sale_data_picked_list[["shanghaijinqiao"]] = join_clean_sale_and_contract_data(sale_data_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]])
#get each stall's data(over the whole period)
sale_data_stall_sum_list[["shanghaijinqiao"]] = setup_stall_metrics(sale_data_picked_list[["shanghaijinqiao"]])

#品类分布
#Calculate and demonstrate metrics on category
#plot the sale per area by some classifier(like category 2) on specific month, here it is 2018-03
plot_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-03-01",finish_date = "2018-03-31",catvar = "CATEGORY_NAME_2")
plot_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-04-01",finish_date = "2018-04-30",catvar = "CATEGORY_NAME_2")
#get the sale per area by some classifier(like category 2) on specific month, here it is 2018-03
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_NAME_2"]][["2018-03"]] = generate_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-03-01",finish_date = "2018-03-31",catvar = "CATEGORY_NAME_2")
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_NAME_2"]][["2018-04"]] = generate_sale_perarea_by_catvar_in_period(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],begin_date = "2018-04-01",finish_date = "2018-04-30",catvar = "CATEGORY_NAME_2")
#品类系数
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_NAME_2"]][["2018-03"]] = sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_NAME_2"]][["2018-03"]][,saleperarea_median := median(saleperarea,na.rm = TRUE)]
sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_NAME_2"]][["2018-03"]] = sale_perarea_in_period[["shanghaijinqiao"]][["CATEGORY_NAME_2"]][["2018-03"]][,cat2_index := saleperarea/saleperarea_median]

#品牌分布
#plot CATEGORY on diff panel
#Default setting is brandlist = c("实木","卫浴","瓷砖")
plot_sale_perarea_by_cat2_brand(sale_data_picked_list[["shanghaijinqiao"]])
#品牌系数
sale_perarea_by_brand = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("CATEGORY_NAME_2","SERIES_NAME")]
sale_perarea_by_brand[,saleperarea_median := median(saleperarea,na.rm = TRUE),by = "CATEGORY_NAME_2"]
sale_perarea_by_brand[,brand_index := saleperarea/saleperarea_median]
#plot CATEGORY on the same panel
cat_list = enc2utf8(c("实木","卫浴","瓷砖"))
cat_list = unique(sale_perarea_by_brand$CATEGORY_NAME_2)[1:3]
plot_multiple_factor(data = sale_perarea_by_brand[CATEGORY_NAME_2 %in% cat_list,],"CATEGORY_NAME_2","SERIES_NAME","saleperarea")

#品类年份，季节，工作日/周末单位面积销量分布
#品类单位面积销量（当下限定在某个月）工作日/周末
sale_perarea_by_weekend_dcast = plot_sale_perarea_by_weekend(sale_data_picked_list[["shanghaijinqiao"]],contract_list[["shanghaijinqiao"]],"CATEGORY_NAME_2")
#品类单位面积销量（当下限定在某个月）不同季节比较
