#contract related table
#ods.ods_hana_bigbi_dim_contract_booth_detail_dt
#hana BIGBI.dim_contract_detail

# sale SQL
# sale_data_sql = "select date_id,ordr_date,prod_name,mall_name,shop_id,shop_name,house_no,booth_id,booth_desc,cnt_cat1_num,cnt_cat2_num,cnt_cat3_num,is_coupon,partner_name,cont_cat1_name,cont_cat2_name,cont_cat3_name,act_amt from dl.fct_ordr where mall_name like '%上海金桥商场%' and 
# ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0))"
# dl.fct_payment

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

plot_avg_sale_on_month = function(begin_date,finish_date,catvar = "CATEGORY_NAME_2"){
  time_contract = contract_raw[BEGIN_DATE<=begin_date & FINISH_DATE>=finish_date,]
  area = time_contract[,.(area = sum(ACTUAL_AREA)),by = catvar]
  sale = sale_data_picked[month_id >= str_sub(begin_date,1,7) & month_id <= str_sub(finish_date,1,7),.(sale = sum(act_amt)),by = catvar]
  sale_area = merge(sale,area,by = catvar,all.x = TRUE)
  sale_area = sale_area[,avg_sale := sale/area]
  sale_area = sale_area[order(avg_sale,decreasing = TRUE),]
  sale_area[[catvar]] = factor(sale_area[[catvar]],levels = sale_area[[catvar]])
  sp = ggplot(data = sale_area,mapping = aes(x = eval(as.name(catvar)),y = avg_sale,fill = "good")) + geom_bar(stat = "identity") + scale_fill_manual(values=c("#9999CC"))+theme(axis.text.x = element_text(angle = 90, hjust = 1))
  sp + facet_grid(facets=. ~ month_id)
  # facet_grid(facets=. ~ gender)
  # ggplot(data = sale_area,mapping = aes(x = "",y = avg_sale,fill = eval(as.name(catvar))))+geom_bar(stat = "identity")+coord_polar('y',start = 0)+scale_fill_brewer(palette="Dark2")+theme_minimal()
  }

plot_single_factor = function(dataset,
                              catvar,
                              valvar,
                              filterexpression = NULL,
                              withoutlegend = TRUE) {
  # dataset = dataset[order(saleperarea,decreasing = TRUE),]
  setorderv(dataset, cols = c(valvar), order = -1)
  facet_formula = as.formula(paste(catvar,"~ ."))
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

plot_floor_heat = function(f_str_m){
  f_value_m = apply(f_str_m,c(1,2),function(m){return(sale_data_booth_sum_recent[BOOTH_CODE == m]$saleperarea)}) 
  f_value_m[is.na(!(f_value_m > 0))] = 0
  f_value_m = as.data.frame(f_value_m)
  f_value_m[] = sapply(f_value_m,unlist)
  f_value_m$rownum = 1:nrow(f_value_m)
  f_value_m$rownum = -f_value_m$rownum
  f_value_m_melt = melt(f_value_m,id.vars = "rownum")
  f_value_m_melt <- ddply(f_value_m_melt, .(variable,rownum), transform,rescale = rescale(value))
  p <- ggplot(f_value_m_melt, aes(variable,rownum)) + geom_tile(aes(fill = (log(value))),colour = "white") + scale_fill_gradient(low = "white",high = "purple")
  p
}

#plot in different chart

test_function = function(){
  print("this is a test function")
}