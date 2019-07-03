library(ggplot2)
library(treemap)
library(plotrix)
library(tidyr)
library(plyr)
library(scales)
library(stringr)
library(easyGgplot2)
source("~/Rfile/R_hive.R")
#contract related table
#ods.ods_hana_bigbi_dim_contract_booth_detail_dt
#hana BIGBI.dim_contract_detail

#function to get sale data by mall name, could put open_id,custom_id in future
get_sale_data_by_mall_name = function(mall_name){
  #old logic ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0)) is not used anymore
  sql = paste0("select ordr_id,date_id,ordr_date,prod_name,mall_name,shop_id,shop_name,contract_code,house_no,booth_id,booth_desc,cnt_cat1_num,cnt_cat2_num,cnt_cat3_num,is_coupon,partner_name,cont_cat1_name,cont_cat2_name,cont_cat3_name,cont_main_brand_name,act_amt from dl.fct_ordr where mall_name like '%",mall_name,"%' and 
  act_amt > 0 and ordr_status not in ('1','7','19','Z','X')")
  result = read_data_hive_general(sql)
  result$month_id = str_sub(result$ordr_date,1,7)
  result = data.table(result)
  return(result)
}

#function to get sale data by mall name
get_sale_data_by_mall_name_with_time_range = function(mall_name,time_start,time_end){
  #old logic ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0)) is not used anymore
  sql = paste0("select date_id,ordr_date,prod_name,mall_name,shop_id,shop_name,contract_code,house_no,booth_id,booth_desc,cnt_cat1_num,cnt_cat2_num,cnt_cat3_num,is_coupon,partner_name,cont_cat1_name,cont_cat2_name,cont_cat3_name,act_amt from dl.fct_ordr where mall_name like '%",mall_name,"%' and 
               act_amt > 0 and ordr_status not in ('1','7','19','Z','X') and date_id between '",time_start,"' and '",time_end,"'")
  result = read_data_hive_general(sql)
  result$month_id = str_sub(result$ordr_date,1,7)
  result = data.table(result)
  return(result)
}

#using this function to get a clean on sale data list
get_on_sale_date_and_clean = function(){
  on_sale_data_sql = "select distinct prom_begin_time,`date` from dm.dm_weixin_ticket_schedule_dt"
  on_sale_data = read_data_hive_general(on_sale_data_sql)
  on_sale_data = data.table(on_sale_data)
  on_sale_data = on_sale_data[str_detect(prom_begin_time,"[\\d]{4}-[\\d]{2}-[\\d]{2}"),]
  on_sale_data = on_sale_data[as.numeric(difftime(as.Date(`date`,'%Y-%m-%d'),as.Date(prom_begin_time,'%Y-%m-%d')))<15,]
  return(on_sale_data)
}

#funtion to get contract data by mall_name
get_contract_data = function(mall_name){
  source('~/Rfile/R_hana.R', encoding = 'UTF-8')
  contract_data_sql = paste0("select * from BIGBI.dim_contract_detail where mall_name like '%",mall_name,"%'")
  contract_raw = read_data_from_hana(contract_data_sql)
  contract_raw = data.table(contract_raw)
  contract_duration = difftime(contract_raw$FINISH_DATE,contract_raw$BEGIN_DATE,units = "days")
  contract_raw$CONTRACT_DURATION = as.numeric(contract_duration)
  contract = contract_raw[,c("CONTRACT_CODE","BRAND_NAME","SERIES_NAME","BOOTH_CODE","BOOTH_GRADE","BOOTH_NAME","CATEGORY_NAME_1","CATEGORY_NAME_2","CATEGORY_NAME_3","FLOOR_NAME","RENTABLE_AREA","ACTUAL_AREA","ZX_PRICE","MONTH_AMOUNT","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]
  dbDisconnect(con)
  return(contract)
}

#join the sale data and contract data to gather info together,the contract will contains fixed data on category2
join_clean_sale_and_contract_data = function(sale_data_raw,contract){
  sale_data = merge(sale_data_raw,contract,by.x = "contract_code",by.y = "CONTRACT_CODE",all.x = TRUE)
  sale_data = sale_data[str_trim(CATEGORY_NAME_1) == ""|is.na(CATEGORY_NAME_1),c("CATEGORY_NAME_1","CATEGORY_NAME_2","CATEGORY_NAME_3") := list(cont_cat1_name,cont_cat2_name,cont_cat3_name)]
  sale_data = sale_data[str_trim(cont_cat1_name) == ""|is.na(cont_cat1_name),c("cont_cat1_name","cont_cat2_name","cont_cat3_name") := list(CATEGORY_NAME_1,CATEGORY_NAME_2,CATEGORY_NAME_3)]
  if("CATEGORY_2_EDIT" %in% colnames(contract)){
     sale_data_picked = sale_data[,c("date_id","ordr_date","month_id","prod_name","mall_name","shop_id","shop_name","house_no","act_amt","contract_code","BRAND_NAME","SERIES_NAME","BOOTH_CODE","BOOTH_GRADE","BOOTH_NAME","cont_cat1_name","cont_cat2_name","cont_cat3_name","CATEGORY_NAME_1",
                                  "CATEGORY_NAME_2","CATEGORY_NAME_3","CATEGORY_2_EDIT","FLOOR_NAME","RENTABLE_AREA","ACTUAL_AREA","ZX_PRICE","MONTH_AMOUNT","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]}
  else{
     sale_data_picked = sale_data[,c("date_id","ordr_date","month_id","prod_name","mall_name","shop_id","shop_name","house_no","act_amt","contract_code","BRAND_NAME","SERIES_NAME","BOOTH_CODE","BOOTH_GRADE","BOOTH_NAME","cont_cat1_name","cont_cat2_name","cont_cat3_name","CATEGORY_NAME_1",
                                    "CATEGORY_NAME_2","CATEGORY_NAME_3","FLOOR_NAME","RENTABLE_AREA","ACTUAL_AREA","ZX_PRICE","MONTH_AMOUNT","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]}
  sale_data_picked = sale_data_picked[,avg_amt := act_amt/ACTUAL_AREA,]
  return(sale_data_picked)
}

#set up stall's metrics for ploting and further analysis
setup_stall_metrics = function(sale_data_picked,start_month = '2017-04'){
  #get the sale,sale per area,and order number for each stall and each month
  sale_data_month_booth_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),order_num = .N),by = c("month_id","CATEGORY_NAME_3","FLOOR_NAME","BOOTH_CODE","BOOTH_NAME","BOOTH_GRADE","house_no","shop_name","shop_id","BRAND_NAME","SERIES_NAME","contract_code","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]
  #sale data booth sum with series name and category name and brand name in it
  sale_data_booth_sum_recent = sale_data_month_booth_sum[month_id>=start_month,.(saleperarea = sum(saleperarea),series_name = SERIES_NAME[sort(table(SERIES_NAME),decreasing = TRUE)[1]],category_name_3 = CATEGORY_NAME_3[sort(table(CATEGORY_NAME_3),decreasing = TRUE)[1]],brand_name = BRAND_NAME[sort(table(BRAND_NAME),decreasing = TRUE)[1]]),by = c("BOOTH_CODE","BOOTH_GRADE","contract_code","BEGIN_DATE","FINISH_DATE")]
  colnames(sale_data_booth_sum_recent) = c("BOOTH_CODE","BOOTH_GRADE","contract_code","BEGIN_DATE","FINISH_DATE","saleperarea","series_name","category_name_3","brand_name")
  #sale per area in categorical form
  sale_data_booth_sum_recent = sale_data_booth_sum_recent[!is.na(saleperarea),]
  saleperareaquantile = quantile(sale_data_booth_sum_recent$saleperarea,c(0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1))
  sale_data_booth_sum_recent[,saleperareainterval := cut(saleperarea,saleperareaquantile)]
  # sale per area per time interval in categorical form
  sale_data_booth_sum_recent[,TIME_SPAN := as.numeric(difftime(pmin(as.character(Sys.Date()),FINISH_DATE),BEGIN_DATE,units = "days"))]
  sale_data_booth_sum_recent[,saleperareaperduration := saleperarea/TIME_SPAN]
  saleperareaperdurationquantile = quantile(sale_data_booth_sum_recent$saleperareaperduration,
                                            c(0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1))
  sale_data_booth_sum_recent[,saleperareaperdurationinterval := cut(saleperareaperduration,saleperareaperdurationquantile)]
  return(sale_data_booth_sum_recent)
}

#A method make average sale data in some period per area by some classifier
#will miss a little contract due to eliminate in-complete contract, so only used in short period
calculate_sale_perarea_by_catvar_in_period = function(sale_data_picked,contract_raw,begin_date,finish_date,catvar = "CATEGORY_2_EDIT",join_var = "CATEGORY_2_EDIT"){
  #this step is the most fragile because the time span for 
  time_contract = contract_raw[BEGIN_DATE<=begin_date & FINISH_DATE>=finish_date,]
  area = time_contract[,.(area = sum(ACTUAL_AREA)),by = join_var]
  sale = sale_data_picked[month_id >= str_sub(begin_date,1,7) & month_id <= str_sub(finish_date,1,7),.(sale = sum(act_amt)),by = catvar]
  #join sale data and contract data togather
  sale_area = merge(sale,area,by = join_var,all.x = TRUE)
  sale_area = sale_area[,saleperarea := sale/area]
  sale_area = sale_area[order(saleperarea,decreasing = TRUE),]
  sale_area$id = 1:nrow(sale_area)
  #if you don't ggplot on this dim,your plot won't order this way
  sale_area$id = factor(sale_area$id,levels = sale_area$id)
  return(sale_area)
}

#according to the adjusted categorical area for contracts,
#calculated each season's average sale
calculate_seasonal_sale_perarea_by_catvar = function(sale_data_picked,contract_raw,year = "2017",catvar = "CATEGORY_2_EDIT",join_var = "CATEGORY_2_EDIT"){
  contract_adjusted_area = calculate_weighted_area_on_quarter(contract_raw,year)
  area = contract_adjusted_area[,.(Q1_area_sum = sum(Q1_area),Q2_area_sum = sum(Q2_area),Q3_area_sum = sum(Q3_area),Q4_area_sum = sum(Q4_area)),by = join_var]
  area = area[Q1_area_sum+Q2_area_sum+Q3_area_sum+Q4_area_sum>0,]
  sale1 = sale_data_picked[month_id >= paste0(year,"-01") & month_id <= paste0(year,"-03"),.(sale = sum(act_amt),quarter = "Q1"),by = catvar]
  sale2 = sale_data_picked[month_id >= paste0(year,"-04") & month_id <= paste0(year,"-06"),.(sale = sum(act_amt),quarter = "Q2"),by = catvar]
  sale3 = sale_data_picked[month_id >= paste0(year,"-07") & month_id <= paste0(year,"-09"),.(sale = sum(act_amt),quarter = "Q3"),by = catvar]
  sale4 = sale_data_picked[month_id >= paste0(year,"-10") & month_id <= paste0(year,"-12"),.(sale = sum(act_amt),quarter = "Q4"),by = catvar]
  sale = rbindlist(list(sale1,sale2,sale3,sale4))
  sale_dcast = dcast(sale,eval(as.name(catvar))~quarter,value.var = "sale")
  setnames(sale_dcast,"catvar",catvar)
  sale_area = merge(sale_dcast,area,by = join_var,all.x = TRUE)
  sale_area = sale_area[,c("sale_perarea_Q1","sale_perarea_Q2","sale_perarea_Q3","sale_perarea_Q4") := list(Q1/Q1_area_sum,Q2/Q2_area_sum,Q3/Q3_area_sum,Q4/Q4_area_sum)]
  sale_area = melt(sale_area,id.vars = join_var,measure.vars = c("sale_perarea_Q1","sale_perarea_Q2","sale_perarea_Q3","sale_perarea_Q4"),variable.name = "quarter",value.name = "saleperarea")
  return(sale_area)
}

#according to the adjusted categorical area for contracts,
#calculated each year's average sale
calculate_yearly_sale_perarea_by_catvar = function(sale_data_picked,contract_raw,year = "2017",catvar = "CATEGORY_2_EDIT",join_var = "CATEGORY_2_EDIT",groupvar = NULL){
  contract_adjusted_area = calculate_weighted_area_on_year(contract_raw,year)
  area = contract_adjusted_area[,.(area_sum = sum(area)),by = join_var]
  area = area[area_sum>0,]
  sale = sale_data_picked[month_id >= paste0(year,"-01") & month_id <= paste0(year,"-12"),.(sale = sum(act_amt)),by = c(catvar,groupvar)]
# setnames(sale,"catvar",catvar)
  Sys.setlocale(category = "LC_ALL",locale = "English_United States.1252")
  sale_area = merge(sale,area,by = join_var,all.x = TRUE)
  Sys.setlocale(category = "LC_ALL",locale = "")
  sale_area = sale_area[,c("sale_perarea_year") := list(sale/area_sum)]
  return(sale_area)
}

palette <- c("dodgerblue1", "skyblue4", "chocolate1", "seagreen4",
             "bisque3", "red4", "purple4", "mediumpurple3",
             "maroon", "dodgerblue4", "skyblue2", "darkcyan",
             "darkslategray3", "lightgreen", "bisque",
             "palevioletred1", "black", "gray79", "lightsalmon4",
             "darkgoldenrod1")

plot_single_booth = function(boothname = "A8125",dataset = sale_data_month_booth_sum,timefiltermin = NULL,timefiltermax = NULL,ifdodge = FALSE){
  if(!is.null(timefiltermin)&is.null(timefiltermax)){
    dataset = dataset[month_id>=timefiltermin,]
  }
  else if(!is.null(timefiltermax)&is.null(timefiltermin)){
    dataset = dataset[month_id<=timefiltermax,]
  }
  else if(!is.null(timefiltermin)&!is.null(timefiltermax)){
    dataset = dataset[(month_id>=timefiltermin&month_id<=timefiltermax),]
  }
  shop_pattern = paste0(boothname,collapse = "|")
  shop = dataset[str_detect(BOOTH_NAME,shop_pattern)|str_detect(house_no,shop_pattern),][order(month_id),]
  shop_sale = ggplot(data = shop,aes(x = month_id,y = sale))
  if(ifdodge){
    shop_sale + geom_bar(stat = "identity",aes(fill = SERIES_NAME),position = "dodge")
  }
  else{
    shop_sale + geom_bar(stat = "identity",aes(fill = SERIES_NAME))
  }
}

#brandlist needed sometimes in filterexpression 
plot_single_factor = function(dataset,
                              catvar,
                              valvar,
                              filterexpression = NULL,
                              brandlist,
                              withoutlegend = TRUE) {
  # dataset = dataset[order(saleperarea,decreasing = TRUE),]
  setorderv(dataset, cols = c(valvar), order = -1)
  dataset[[catvar]] <-
    factor(dataset[[catvar]], levels = dataset[[catvar]])
  if (!is.null(filterexpression)) {
    dataset = dataset[eval(filterexpression), ]
  }
  p = ggplot(data = dataset, aes(
    x = eval(as.name(catvar)),
    y = eval(as.name(valvar)),
    fill = eval(as.name(catvar))
  )) + geom_bar(stat = "identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))
  if (withoutlegend) {
    p + theme(legend.position = "none")
  }
  else{
    p
  }
}

plot_multiple_factor = function(dataset,
                              catvar,
                              itemvar,
                              valvar,
                              filterexpression = NULL,
                              brandlist,
                              withoutlegend = TRUE,
                              catname = "") {
  # dataset = dataset[order(saleperarea,decreasing = TRUE),]
  setorderv(dataset, cols = c(catvar,valvar),order = c(1,-1))
  dataset[[itemvar]] <-
    factor(dataset[[itemvar]], levels = dataset[[itemvar]])
  if (!is.null(filterexpression)) {
    dataset = dataset[eval(filterexpression), ]
  }
  p = ggplot(data = dataset, aes(
    x = eval(as.name(itemvar)),
    y = eval(as.name(valvar)),
    fill = eval(as.name(catvar))
  )) + geom_bar(stat = "identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))
  if (withoutlegend) {
    p + theme(legend.position = "none") + 
        xlab("类内品牌") + 
        ylab("单位面积销售额") + 
        ggtitle(paste0(catname,"下品牌销售图"))
  }
  else{
    p + xlab("类内品牌") + ylab("单位面积销售额") + ggtitle(paste0(catname,"下品牌销售图"))
  }
}

#Plot heat plot for each floor with heat value sale etc. And the label may be series or category
#most variable is caculated when testing
plot_floor_heat = function(f_str_m,label_name = "series_name",value_name = "saleperareainterval",filter_col = "BOOTH_CODE",sale_data_booth_sum_recent = sale_data_booth_sum_recent){
  if(filter_col == "BOOTH_CODE"){
    f_value_m = apply(f_str_m,c(1,2),function(m){return(sale_data_booth_sum_recent[BOOTH_CODE == m,.SD[contract_code==max(contract_code),],by = "BOOTH_CODE"][[value_name]])})
  } 
  else if(filter_col == "contract_code"){
    f_value_m = apply(f_str_m,c(1,2),function(m){return(sale_data_booth_sum_recent[contract_code == m,.SD[1,],by = "contract_code"][[value_name]])})
  }
  f_value_m[is.na(!(f_value_m > 0))] = 0
  f_value_m = as.data.frame(f_value_m)
  f_value_m[] = sapply(f_value_m,unlist)
  f_value_m$rownum = 1:nrow(f_value_m)
  f_value_m$rownum = -f_value_m$rownum
  f_value_m_melt = melt(f_value_m,id.vars = "rownum")
  #name each grid in the matrix
  f_cat_list = list()
  f_cat_str = matrix(list(character(0)),nrow = nrow(f_str_m),ncol = ncol(f_str_m))
  for(name in label_name){
    if (filter_col == "BOOTH_CODE") {
      f_cat_list[[name]] = apply(f_str_m, c(1, 2), function(m) {
        return(sale_data_booth_sum_recent[BOOTH_CODE == m,.SD[contract_code==max(contract_code),],by = "BOOTH_CODE"][[name]])
      })
      # f_cat_temp = convert_list_matrix_to_character_matrix(f_cat_list[[name]])
      f_cat_str[] = paste_matrix2(f_cat_str, f_cat_list[[name]])
    }
    if (filter_col == "contract_code") {
      f_cat_list[[name]] = apply(f_str_m, c(1, 2), function(m) {return(sale_data_booth_sum_recent[contract_code == m,.SD[1,],by = "contract_code"][[name]])})
      # f_cat_temp = convert_list_matrix_to_character_matrix(f_cat_list[[name]])
      f_cat_str[] = paste_matrix2(f_cat_str, f_cat_list[[name]])
    }
  }
  f_cat_m = f_cat_list[[1]]
  f_cat_m[] = f_cat_str
  f_cat_length_m = apply(f_cat_m,c(1,2),function(m){str_length(str_trim(m[[1]]))})
  f_cat_m[is.na(f_cat_length_m>0)] = ""
  f_cat_m = as.data.frame(f_cat_m)
  f_cat_m[] = sapply(f_cat_m,unlist)
  f_cat_m$rownum = -(1:nrow(f_cat_m))
  f_cat_m_melt = melt(f_cat_m,id.vars = "rownum")
  #Add BOOTH_GRADE information
  if (filter_col == "BOOTH_CODE") {
    f_grade_m = apply(f_str_m, c(1, 2), function(m) {
      return(sale_data_booth_sum_recent[BOOTH_CODE == m,.SD[contract_code==max(contract_code),],by = "BOOTH_CODE"][["BOOTH_GRADE"]])
    })
  }
  else if(filter_col == "contract_code"){
    f_grade_m = apply(f_str_m, c(1, 2), function(m) {
      return(sale_data_booth_sum_recent[contract_code == m, .SD[1, ], by = "contract_code"][["BOOTH_GRADE"]])
    })
  }
  f_grade_m[is.na(!(f_grade_m > 0))] = 0
  f_grade_m = as.data.frame(f_grade_m)
  f_grade_m[] = sapply(f_grade_m,unlist)
  f_grade_m$rownum = 1:nrow(f_grade_m)
  f_grade_m$rownum = -f_grade_m$rownum
  f_grade_m_melt = melt(f_grade_m,id.vars = "rownum")
  f_grade_m_melt$value = ifelse((f_grade_m_melt$value == "0"),"",f_grade_m_melt$value)
  p <- ggplot(f_value_m_melt, aes(variable,rownum)) + geom_tile(aes(fill = value),colour = "white") + scale_fill_gradient(low = "white",high = "purple")+geom_text(aes(label=f_cat_m_melt$value),angle = 45)+geom_text(aes(label = f_grade_m_melt$value),color = "red")
  p
}

plot_floor_heat_uv = function(f_str_m,label_name = "SHOP_NAME",value_name = "saleperareainterval",filter_col = "BOOTH_CODE",filter_col2 = "BOOTH_CODE",data = month_daily_uv_with_contract_code,data2 = sale_data_stall_sum_list[["shanghaijinqiao"]],mix_order = TRUE){
  #pick first row as value is not always correct
  f_value_m = apply(f_str_m,c(1,2),function(m){return(data[eval(as.name(filter_col)) == m,.SD[1,],by = filter_col][[value_name]])})
  f_value_m[is.na(!(f_value_m > 0))] = 0
  f_value_m = as.data.frame(f_value_m)
  f_value_m[] = sapply(f_value_m,unlist)
  f_value_m$rownum = 1:nrow(f_value_m)
  f_value_m$rownum = -f_value_m$rownum
  f_value_m_melt = melt(f_value_m,id.vars = "rownum")
  f_cat_list = list()
  f_cat_str = matrix(list(character(0)),nrow = nrow(f_str_m),ncol = ncol(f_str_m))
  data2$saleperareaperduration = round(data2$saleperareaperduration,3)
  for(name in label_name){
    if (!mix_order) {
      f_cat_list[[name]] = apply(f_str_m, c(1, 2), function(m) {return(data[eval(as.name(filter_col)) == m,.SD[1,],by = filter_col][[name]])})
      # f_cat_temp = convert_list_matrix_to_character_matrix(f_cat_list[[name]])
      f_cat_str[] = paste_matrix2(f_cat_str, f_cat_list[[name]])
    }
    if (mix_order) {
      f_cat_list[[name]] = apply(f_str_m, c(1, 2), function(m) {return(data2[eval(as.name(filter_col2)) == m,.SD[1,],by = filter_col2][[name]])})
      # f_cat_temp = convert_list_matrix_to_character_matrix(f_cat_list[[name]])
      f_cat_str[] = paste_matrix2(f_cat_str, f_cat_list[[name]])
    }
  }
  f_cat_m = f_cat_list[[1]]
  f_cat_m[] = f_cat_str
  f_cat_length_m = apply(f_cat_m,c(1,2),function(m){str_length(str_trim(m[[1]]))})
  f_cat_m[is.na(f_cat_length_m>0)] = ""
  f_cat_m = as.data.frame(f_cat_m)
  f_cat_m[] = sapply(f_cat_m,unlist)
  f_cat_m$rownum = -(1:nrow(f_cat_m))
  f_cat_m_melt = melt(f_cat_m,id.vars = "rownum")
  p <- ggplot(f_value_m_melt, aes(variable,rownum)) + geom_tile(aes(fill = value),colour = "white") + scale_fill_gradient(low = "white",high = "purple")+geom_text(aes(label=f_cat_m_melt$value),angle = 45)
  p
}

#mainly focus on one month per area,you can also devided by time span if you wish
#using calculate_sale_perarea_by_catvar_in_period generate data and plot it
plot_sale_perarea_by_catvar_in_period = function(sale_data_picked,contract_raw,begin_date,finish_date,catvar = "CATEGORY_2_EDIT"){
  sale_area = calculate_sale_perarea_by_catvar_in_period(sale_data_picked,contract_raw,begin_date,finish_date,catvar)
  sp = ggplot(data = sale_area,mapping = aes(x = reorder(eval(as.name(catvar)),-saleperarea),y = saleperarea,fill = "good")) + geom_bar(stat = "identity") + scale_fill_manual(values=c("#9999CC"))+theme(axis.text.x = element_text(angle = 90, hjust = 1)) + 
       xlab("销售大类") + 
       ylab("单位面积销售额") + 
       ggtitle("销售分类图")
  sp
  # sp + facet_grid(facets=. ~ month_id)
}

plot_sale_perarea_by_cat2_brand = function(sale_data_picked,brandlist = c("实木","卫浴","瓷砖")){
  brandlist = enc2utf8(brandlist)
  sale_data_brand_saleperarea_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("CATEGORY_2_EDIT","SERIES_NAME")]
  brand_plot_under_cat = list()
  brand_plot_under_cat[[brandlist[[1]]]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(CATEGORY_2_EDIT==brandlist[[1]]),brandlist = brandlist)
  brand_plot_under_cat[[brandlist[[2]]]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(CATEGORY_2_EDIT==brandlist[[2]]),brandlist = brandlist)
  brand_plot_under_cat[[brandlist[[3]]]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(CATEGORY_2_EDIT==brandlist[[3]]),brandlist = brandlist)
  ggplot2.multiplot(brand_plot_under_cat[[brandlist[[1]]]],
                    brand_plot_under_cat[[brandlist[[2]]]],
                    brand_plot_under_cat[[brandlist[[3]]]], cols=3)
}

#plot sale data perarea by some classifier and weekday and weekend
plot_sale_perarea_by_weekend = function(sale_data_picked,contract_raw,catvar,begin_date = "2018-07-01",finish_date="2018-07-31")
{
  Sys.setlocale(category = "LC_ALL", locale = "English_United States.1252")
  sale_data_picked[,dayofweek := weekdays(as.Date(date_id,'%Y-%m-%d'))]
  sale_data_picked[,ifweekend := ifelse(dayofweek %in% c("Saturday","Sunday"),"weekend","weekday")]
  Sys.setlocale(category = "LC_ALL", locale = "")
  #old name sale_data_cat2_week_saleperarea_sum
  sale_perarea_by_weekend = calculate_sale_perarea_by_catvar_in_period(sale_data_picked,contract_raw,begin_date,finish_date,c(catvar,"ifweekend"))
  sale_perarea_by_weekend[,saleperarea_mirror := ifelse(ifweekend == "weekend",saleperarea,-saleperarea)]
  p = ggplot(sale_perarea_by_weekend, aes(x=reorder(eval(parse(text = catvar)),-saleperarea,min), y=saleperarea_mirror, fill=ifweekend)) + geom_bar(stat="identity", position="identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
      xlab("销售分类") +
      ylab("单位面积销售额") +
      ggtitle("各品类销售周末/非周末情况对比图")
  print(p)
  #calculate weekday/weekend ratio for further using
  sale_perarea_by_weekend_dcast = dcast(sale_perarea_by_weekend,CATEGORY_2_EDIT~ifweekend,value.var = "saleperarea")
  sale_perarea_by_weekend_dcast[,weekend_index := weekend/weekday]
  return(sale_perarea_by_weekend_dcast)
}

#plot sale data perarea by some classifier and dodged by different season(quarter)
plot_sale_perarea_by_season = function(sale_data_picked,contract_raw,catvar,year = '2017'){
  sale_data_picked[,season := quarters(as.Date(date_id,'%Y-%m-%d'))]
  #old var:sale_data_cat2_season_saleperarea_sum
  sale_perarea_by_season = calculate_seasonal_sale_perarea_by_catvar(sale_data_picked,contract_raw,year,catvar,catvar)
  p = ggplot(data = sale_perarea_by_season,aes(reorder(x = eval(parse(text = catvar)),-saleperarea,sum),y = saleperarea,group = quarter)) + geom_bar(stat = "identity", position = "dodge", aes(fill = quarter)) + theme(axis.text.x = element_text(angle = 90,hjust = 1)) +
      xlab("销售分类") +
      ylab("按季节单位面积销售额") +
      ggtitle("各品类销售按季节对比图")
  print(p)
# sale_perarea_by_season_dcast = dcast(sale_perarea_by_season,eval(parse(text = catvar))~season,value.var = "saleperarea")
# sale_perarea_by_season_dcast = sale_perarea_by_season_dcast[,year]
  return(sale_perarea_by_season)
}

plot_sale_perarea_by_onsale = function(sale_data_picked,on_sale_data,contract_raw,catvar,year = '2017',groupvar = "onsale"){
  sale_data_picked_for_spec_year = sale_data_picked[month_id>=paste0(year,'-01') & month_id<=paste0(year,'-12'),]
  sale_data_picked_for_spec_year$onsale = sapply(sale_data_picked_for_spec_year$date_id,function(dot_date,start_dates,end_dates){any(data.table::between(dot_date,start_dates,end_dates))},on_sale_data$prom_begin_time,on_sale_data$date)
  sale_perarea_by_cat2_for_spec_year = calculate_yearly_sale_perarea_by_catvar(sale_data_picked_for_spec_year,contract_raw,year,catvar,catvar,groupvar)
  p = ggplot(data = sale_perarea_by_cat2_for_spec_year,aes(x = reorder(eval(parse(text = catvar)),-sale_perarea_year,min),y = sale_perarea_year,group = onsale)) + geom_bar(stat = "identity", position = "dodge", aes(fill = onsale)) + theme(axis.text.x = element_text(angle = 90,hjust = 1)) +
      xlab("销售分类") +
      ylab("按促销状态单位面积销售额") +
      ggtitle("各品类销售按是否促销对比图")
  print(p)
  sale_perarea_by_cat2_for_spec_year_dcast = dcast(sale_perarea_by_cat2_for_spec_year,eval(parse(text = catvar))~onsale,value.var = "sale_perarea_year")
  sale_perarea_by_cat2_for_spec_year_dcast[,onsale_index := `TRUE`/`FALSE`]
  return(sale_perarea_by_cat2_for_spec_year_dcast)
}

#want to get each quater's adjusted area according to contract time span
calculate_weighted_area_on_quarter = function(contract_raw,year = '2017'){
  # ifinQ1 = contract_raw$BEGIN_DATE>"2017-03-31"|contract_raw$FINISH_DATE<"2017-01-01"
  # calculate the time span inside each dedicated quarter
  contract_raw[,c("Q1_area","Q2_area","Q3_area","Q4_area") := list(as.numeric(difftime(as.Date(pmin(paste0(year,'-03-31'),FINISH_DATE),'%Y-%m-%d'),as.Date(pmax(paste0(year,'-01-01'),BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date(paste0(year,'-03-31'),'%Y-%m-%d'),as.Date(paste0(year,'-01-01'),'%Y-%m-%d'))),
                                                                   as.numeric(difftime(as.Date(pmin(paste0(year,'-06-30'),FINISH_DATE),'%Y-%m-%d'),as.Date(pmax(paste0(year,'-04-01'),BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date(paste0(year,'-06-30'),'%Y-%m-%d'),as.Date(paste0(year,'-04-01'),'%Y-%m-%d'))),
                                                                   as.numeric(difftime(as.Date(pmin(paste0(year,'-09-30'),FINISH_DATE),'%Y-%m-%d'),as.Date(pmax(paste0(year,'-07-01'),BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date(paste0(year,'-09-30'),'%Y-%m-%d'),as.Date(paste0(year,'-07-01'),'%Y-%m-%d'))),
                                                                   as.numeric(difftime(as.Date(pmin(paste0(year,'-12-31'),FINISH_DATE),'%Y-%m-%d'),as.Date(pmax(paste0(year,'-10-01'),BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date(paste0(year,'-12-31'),'%Y-%m-%d'),as.Date(paste0(year,'-10-01'),'%Y-%m-%d'))))]
  #change the negative part to zero
  contract_raw[,c("Q1_area","Q2_area","Q3_area","Q4_area") := list(pmax(0,Q1_area),pmax(0,Q2_area),pmax(0,Q3_area),pmax(0,Q4_area))]
  #multiple area by percentage
  contract_raw[,c("Q1_area","Q2_area","Q3_area","Q4_area") := list(ACTUAL_AREA*Q1_area,ACTUAL_AREA*Q2_area,ACTUAL_AREA*Q3_area,ACTUAL_AREA*Q4_area)]
}

#want to get each quater's adjusted area according to 
calculate_weighted_area_on_year = function(contract_raw,year = '2017'){
  # ifinQ1 = contract_raw$BEGIN_DATE>"2017-03-31"|contract_raw$FINISH_DATE<"2017-01-01"
  # calculate the time span inside each dedicated quarter
  contract_raw[,c("area") := list(as.numeric(difftime(as.Date(pmin(paste0(year,'-12-31'),FINISH_DATE),'%Y-%m-%d'),as.Date(pmax(paste0(year,'-01-01'),BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date(paste0(year,'-12-31'),'%Y-%m-%d'),as.Date(paste0(year,'-01-01'),'%Y-%m-%d'))))]
                                                          
  #change the negative part to zero
  contract_raw[,c("area") := list(pmax(0,area))]
  #multiple area by percentage
  contract_raw[,c("area") := list(ACTUAL_AREA*area)]
}

#plot in different chart
convert_list_matrix_to_character_matrix = function(m){
  m2 = matrix(character(0),nrow = nrow(m),ncol = ncol(m))
  m2[] = sapply(m,function(x){ifelse(length(x)==1,x,NA)})
  return(m2)
}

#m1,m2 need to be a matrix,since matrix is a list, we need to unlist it.
paste_matrix = function(m1,m2){
  result = m1
  for(i in 1:nrow(m1)){
    for(j in 1:nrow(m2)){
      result[i,j] = paste(unlist(m1[i,j]),unlist(m2[i,j]),sep = "\n")
    }
  }
  return(result)
}

#m1,m2 need to be a matrix,since matrix is a list, we need to unlist it.
paste_matrix2_base = function(e1,e2){
  result = paste(unlist(e1),unlist(e2),sep = "\n")
}

paste_matrix2 = Vectorize(paste_matrix2_base)

paste_omit_na = function(x,y){
  result = ifelse((is.na(x)|is.na(y)),NA,paste0(x,y))
  return(result)
}

paste_omit_na_special = function(x,y){
  result = ifelse((is.na(x)|is.na(y)),NA,ifelse((nchar(y) == 5),paste0(x,y),paste0(x,0,y)))
  return(result)
}

test_function = function(){
  l = list(character(0))
  result = paste0(unlist(l),"\n",unlist(l))
  print(result)
}

compare_onpaper_estimation = function(data){
  library(ggplot2)
  data = data.table(data)
  data_points = data[,.(sum_act_amt = log10(sum_act_amt),sum_act_amt_redstar_estimated = log10(sum_act_amt_redstar_estimated),sale_vs_claim = ifelse(sum_act_amt>sum_act_amt_redstar_estimated,"sale_is_bigger","claim_is_bigger"))]
  ggplot(data = data_points,mapping = aes(x = sum_act_amt_redstar_estimated,y = sum_act_amt,colour = sale_vs_claim)) + 
    geom_point() + coord_fixed(ratio = 1, xlim = c(0,10), ylim = c(0,10)) + 
    geom_abline(slope = 1)
}

#获取最大连击数
get_max_cont_hit = function(v,cond = expression(v[i]<300)){
  k = 0
  max = 0 
  for(i in 1:length(v)){
    if(eval(cond)){
      k = k + 1
      if(max<k){
        max = k
      }
    }
    else{
      k = 0
    }
  }
  return(max)
}

#获取孤立点个数
get_num_isolated_points = function(v){
  num = 0
  n = length(v)
  for(i in 1:(n+1)){
    if(i == 1){
      if(v[i] >300){
        num = num + 1
      }
    }
    else if(i == (n+1)){
      if(v[i-1] > 300){
        num = num + 1
      }
    }
    else{
      if((v[i-1] >300) & (v[i] > 300)){
        num = num + 1
      }
    }
  }
  return(num)
}

#satisfy_cond_perc

# filter_data_set = merged_feature_result_set[!is.na(relation_record_estimate_real), ]
# filter_data_set = filter_data_set[sum_act_amt/redstar_sale_estimated<1.5&sum_act_amt/redstar_sale_estimated>0.75,]
# filter_data_set is the data with respect to detailer's sale data
get_real_value_using_data = function(baseData,filterData = filter_data_set){
  #we need to get only 2017 data from original data set
  baseData = baseData[date_id>='2017-01-01'&date_id<='2017-12-31',]
  resultData = merge(baseData,filterData[,"partner_name"],by = "partner_name")
}

check_if_in_period = function(dot_date,start_dates,end_dates){
  any(between(dot_date,start_dates,end_dates))
}

generate_sale_perarea_by_catvar_in_period = function(sale_data_picked,contract_raw,begin_date,finish_date,catvar = "CATEGORY_2_EDIT",join_var = "CATEGORY_2_EDIT"){
  #this step is the most fragile because the time span for 
  time_contract = contract_raw[BEGIN_DATE<=begin_date & FINISH_DATE>=finish_date,]
  area = time_contract[,.(area = sum(ACTUAL_AREA)),by = join_var]
  sale = sale_data_picked[month_id >= str_sub(begin_date,1,7) & month_id <= str_sub(finish_date,1,7),.(sale = sum(act_amt)),by = catvar]
  #join sale data and contract data togather
  sale_area = merge(sale,area,by = join_var,all.x = TRUE)
  sale_area = sale_area[,saleperarea := sale/area]
  sale_area = sale_area[order(saleperarea,decreasing = TRUE),]
  sale_area$id = 1:nrow(sale_area)
  #if you don't ggplot on this dim,your plot won't order this way
  sale_area$id = factor(sale_area$id,levels = sale_area$id)
  return(sale_area)
  }

function(){
  #品牌系数
  sale_perarea_by_brand = sale_data_picked_list[["shanghaijinqiao"]][,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("CATEGORY_2_EDIT","SERIES_NAME")]
  sale_perarea_by_brand[,saleperarea_median := median(saleperarea,na.rm = TRUE),by = "CATEGORY_2_EDIT"]
  sale_perarea_by_brand[,brand_index := saleperarea/saleperarea_median]
  #plot CATEGORY on the same panel
  cat_list = enc2utf8(c("实木","卫浴","瓷砖"))
  cat_list = unique(sale_perarea_by_brand$CATEGORY_2_EDIT)[1:3]
  plot_multiple_factor(data = sale_perarea_by_brand[CATEGORY_2_EDIT %in% cat_list,],"CATEGORY_2_EDIT","SERIES_NAME","saleperarea")
}

#based on all contract code corresponding to 1 booth code
replace_matrix_with_dict = function(raw_matrix,dict,con_column = "CONTRACT_CODE",target_column = "BOOTH_CODE"){
  library(plyr)
  vector = as.vector(raw_matrix)
  df = data.frame(CONTRACT_CODE = vector)
  transformed_matrix = join(df,dict,by = con_column,type = "left")
  result_matrix = raw_matrix
  result_matrix[] = transformed_matrix[[target_column]]
  return(result_matrix)
}

# check_whether_DT_updated = function(DT){
#   DT[,new_col := 1]
# }

get_tracking_data = function(mon){
  load(paste0("~/R_Projects/sale_model_standard/smart_mall_",mon,"_list.RData"))
  smart_mall_track_data = rbindlist(eval(as.name(paste0("smart_mall_",mon,"_list"))))
  #need to be filled
  return(smart_mall_track_data)
}