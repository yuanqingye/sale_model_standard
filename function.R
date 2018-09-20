library(ggplot2)
library(treemap)
library(plotrix)
library(tidyr)
library(plyr)
library(scales)
library(stringr)
library(easyGgplot2)
#contract related table
#ods.ods_hana_bigbi_dim_contract_booth_detail_dt
#hana BIGBI.dim_contract_detail

#function to get sale data by mall name
get_sale_data_by_mall_name = function(mall_name){
  sql = paste0("select date_id,ordr_date,prod_name,mall_name,shop_id,shop_name,contract_code,house_no,booth_id,booth_desc,cnt_cat1_num,cnt_cat2_num,cnt_cat3_num,is_coupon,partner_name,cont_cat1_name,cont_cat2_name,cont_cat3_name,act_amt from dl.fct_ordr where mall_name like '%",mall_name,"%' and 
 ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0))")
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

#join the sale data and contract data to gather info together
join_clean_sale_and_contract_data = function(sale_data_raw,contract){
  sale_data = merge(sale_data_raw,contract,by.x = "contract_code",by.y = "CONTRACT_CODE",all.x = TRUE)
  sale_data = sale_data[str_trim(CATEGORY_NAME_1) == ""|is.na(CATEGORY_NAME_1),c("CATEGORY_NAME_1","CATEGORY_NAME_2","CATEGORY_NAME_3") := list(cont_cat1_name,cont_cat2_name,cont_cat3_name)]
  sale_data = sale_data[str_trim(cont_cat1_name) == ""|is.na(cont_cat1_name),c("cont_cat1_name","cont_cat2_name","cont_cat3_name") := list(CATEGORY_NAME_1,CATEGORY_NAME_2,CATEGORY_NAME_3)]
  sale_data_picked = sale_data[,c("date_id","ordr_date","month_id","prod_name","mall_name","shop_id","shop_name","house_no","act_amt","contract_code","BRAND_NAME","SERIES_NAME","BOOTH_CODE","BOOTH_GRADE","BOOTH_NAME","cont_cat1_name","cont_cat2_name","cont_cat3_name","CATEGORY_NAME_1",
                                  "CATEGORY_NAME_2","CATEGORY_NAME_3","FLOOR_NAME","RENTABLE_AREA","ACTUAL_AREA","ZX_PRICE","MONTH_AMOUNT","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]
  sale_data_picked = sale_data_picked[,avg_amt := act_amt/ACTUAL_AREA,]
  return(sale_data_picked)
}

#set up stall's metrics for ploting and further analysis
setup_stall_metrics = function(sale_data_picked,start_month = '2017-04'){
  #get the sale,sale per area,and order number for each stall and each month
  sale_data_month_booth_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),order_num = .N),by = c("month_id","CATEGORY_NAME_3","FLOOR_NAME","BOOTH_CODE","BOOTH_NAME","BOOTH_GRADE","house_no","shop_name","shop_id","BRAND_NAME","SERIES_NAME","contract_code","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]
  #sale data booth sum with series name and category name in it
  sale_data_booth_sum_recent = sale_data_month_booth_sum[month_id>=start_month,.(saleperarea = sum(saleperarea),series_name = SERIES_NAME[sort(table(SERIES_NAME),decreasing = TRUE)[1]],category_name_3 = CATEGORY_NAME_3[sort(table(CATEGORY_NAME_3),decreasing = TRUE)[1]]),by = c("BOOTH_CODE","BOOTH_GRADE","contract_code","BEGIN_DATE","FINISH_DATE")]
  colnames(sale_data_booth_sum_recent) = c("BOOTH_CODE","BOOTH_GRADE","contract_code","BEGIN_DATE","FINISH_DATE","saleperarea","series_name","category_name_3")
  #sale per area in categorical form
  sale_data_booth_sum_recent = sale_data_booth_sum_recent[!is.na(saleperarea),]
  saleperareaquantile = quantile(sale_data_booth_sum_recent$saleperarea,c(0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1))
  sale_data_booth_sum_recent[,saleperareainterval := cut(saleperarea,saleperareaquantile)]
  # sale per area per time interval in categorical form
  sale_data_booth_sum_recent[,TIME_SPAN := as.numeric(difftime(pmin(as.character(Sys.Date()),FINISH_DATE),BEGIN_DATE))]
  sale_data_booth_sum_recent[,saleperareaperduration := saleperarea/TIME_SPAN]
  saleperareaperdurationquantile = quantile(sale_data_booth_sum_recent$saleperareaperduration,
                                            c(0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1))
  sale_data_booth_sum_recent[,saleperareaperdurationinterval := cut(saleperareaperduration,saleperareaperdurationquantile)]
  return(sale_data_booth_sum_recent)
}

#A method make average sale data in some period per area by some classifier
generate_sale_perarea_by_catvar_in_period = function(sale_data_picked,contract_raw,begin_date,finish_date,catvar = "CATEGORY_NAME_2",join_var = "CATEGORY_NAME_2"){
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

function(sale_data_picked,contract_adjusted_area,catvar = "CATEGORY_NAME_2",join_var = "CATEGORY_NAME_2"){
  area = time_contract[,.(Q1_area_sum = sum(Q1_area)),by = join_var]
  
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
                              withoutlegend = TRUE) {
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
    p + theme(legend.position = "none")
  }
  else{
    p
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

#mainly focus on one month per area,you can also devided by time span if you wish
#using generate_sale_perarea_by_catvar_in_period generate data and plot it
plot_sale_perarea_by_catvar_in_period = function(sale_data_picked,contract_raw,begin_date,finish_date,catvar = "CATEGORY_NAME_2"){
  sale_area = generate_sale_perarea_by_catvar_in_period(sale_data_picked,contract_raw,begin_date,finish_date,catvar = "CATEGORY_NAME_2")
  sp = ggplot(data = sale_area,mapping = aes(x = eval(as.name(catvar)),y = avg_sale,fill = "good")) + geom_bar(stat = "identity") + scale_fill_manual(values=c("#9999CC"))+theme(axis.text.x = element_text(angle = 90, hjust = 1))
  sp
  # sp + facet_grid(facets=. ~ month_id)
}

plot_sale_perarea_by_cat2_brand = function(sale_data_picked,brandlist = c("实木","卫浴","瓷砖")){
  brandlist = enc2utf8(brandlist)
  sale_data_brand_saleperarea_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("CATEGORY_NAME_2","SERIES_NAME")]
  brand_plot_under_cat = list()
  brand_plot_under_cat[[brandlist[[1]]]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(CATEGORY_NAME_2==brandlist[[1]]),brandlist = brandlist)
  brand_plot_under_cat[[brandlist[[2]]]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(CATEGORY_NAME_2==brandlist[[2]]),brandlist = brandlist)
  brand_plot_under_cat[[brandlist[[3]]]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(CATEGORY_NAME_2==brandlist[[3]]),brandlist = brandlist)
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
  sale_perarea_by_weekend = generate_sale_perarea_by_catvar_in_period(sale_data_picked,contract_raw,begin_date,finish_date,c(catvar,"ifweekend"))
  sale_perarea_by_weekend[,saleperarea_mirror := ifelse(ifweekend == "weekend",saleperarea,-saleperarea)]
  p = ggplot(sale_perarea_by_weekend, aes(x=eval(parse(text = catvar)), y=saleperarea_mirror, fill=ifweekend)) + geom_bar(stat="identity", position="identity") + theme(axis.text.x = element_text(angle = 90, hjust = 1))
  print(p)
  #calculate weekday/weekend ratio for further using
  sale_perarea_by_weekend_dcast = dcast(sale_perarea_by_weekend,CATEGORY_NAME_2~ifweekend,value.var = "saleperarea")
  sale_perarea_by_weekend_dcast[,weekend_index := weekend/weekday]
  return(sale_perarea_by_weekend_dcast)
}

#plot sale data perarea by some classifier and dodged by different season(quarter)
plot_sale_perarea_by_season = function(sale_data_picked,contract_raw,catvar,begin_date = "2018-07-01",finish_date="2018-07-31"){
  sale_data_picked[,season := quarters(as.Date(date_id,'%Y-%m-%d'))]
  #old var:sale_data_cat2_season_saleperarea_sum
  sale_perarea_by_season = generate_sale_perarea_by_catvar_in_period(sale_data_picked,contract_raw,begin_date,finish_date,c(catvar,"season"))
  p = ggplot(data = sale_perarea_by_season,aes(x = eval(parse(text = catvar)),y = saleperarea,group =season)) 
  + geom_bar(stat = "identity", position = "dodge", aes(fill = season)) + theme(axis.text.x = element_text(angle = 90,hjust = 1))
  print(p)
  sale_perarea_by_season_dcast = dcast(sale_perarea_by_season,eval(parse(text = catvar))~season,value.var = "saleperarea")
# sale_perarea_by_season_dcast = sale_perarea_by_season_dcast[,year]
  return(sale_perarea_by_season_dcast)
}

#want to get each quater's adjusted area according to 
calculate_weighted_area_on_quarter = function(contract_raw){
  # ifinQ1 = contract_raw$BEGIN_DATE>"2017-03-31"|contract_raw$FINISH_DATE<"2017-01-01"
  # calculate the time span inside each dedicated quarter
  contract_raw[,c("Q1_area","Q2_area","Q3_area","Q4_area") := list(as.numeric(difftime(as.Date(pmin('2017-03-31',FINISH_DATE),'%Y-%m-%d'),as.Date(pmax('2017-01-01',BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date('2017-03-31','%Y-%m-%d'),as.Date('2017-01-01','%Y-%m-%d'))),
                                                                   as.numeric(difftime(as.Date(pmin('2017-06-30',FINISH_DATE),'%Y-%m-%d'),as.Date(pmax('2017-04-01',BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date('2017-06-30','%Y-%m-%d'),as.Date('2017-04-01','%Y-%m-%d'))),
                                                                   as.numeric(difftime(as.Date(pmin('2017-09-30',FINISH_DATE),'%Y-%m-%d'),as.Date(pmax('2017-07-01',BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date('2017-09-30','%Y-%m-%d'),as.Date('2017-07-01','%Y-%m-%d'))),
                                                                   as.numeric(difftime(as.Date(pmin('2017-12-31',FINISH_DATE),'%Y-%m-%d'),as.Date(pmax('2017-10-01',BEGIN_DATE),'%Y-%m-%d')))/as.numeric(difftime(as.Date('2017-12-31','%Y-%m-%d'),as.Date('2017-10-01','%Y-%m-%d'))))]
  #change the negative part to zero
  contract_raw[,c("Q1_area","Q2_area","Q3_area","Q4_area") := list(pmax(0,Q1_area),pmax(0,Q2_area),pmax(0,Q3_area),pmax(0,Q4_area))]
  #multiple area by percentage
  contract_raw[,c("Q1_area","Q2_area","Q3_area","Q4_area") := list(ACTUAL_AREA*Q1_area,ACTUAL_AREA*Q2_area,ACTUAL_AREA*Q3_area,ACTUAL_AREA*Q4_area)]
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