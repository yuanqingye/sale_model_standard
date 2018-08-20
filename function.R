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

#Plot heat plot for each floor with heat value sale etc. And the label may be series or category
#most variable is caculated when testing
plot_floor_heat = function(f_str_m,label_name = "series_name",value_name = "saleperareainterval",filter_col = "BOOTH_CODE"){
  if(filter_col == "BOOTH_CODE"){
    f_value_m = apply(f_str_m,c(1,2),function(m){return(sale_data_booth_sum_recent[BOOTH_CODE == m,][[value_name]])})
  } 
  else if(filter_col == "contract_code"){
    f_value_m = apply(f_str_m,c(1,2),function(m){return(sale_data_booth_sum_recent[contract_code == m,][[value_name]])})
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
    f_cat_list[[name]] = apply(f_str_m,c(1,2),function(m){return(sale_data_booth_sum_recent[BOOTH_CODE == m,][[name]])})
    # f_cat_temp = convert_list_matrix_to_character_matrix(f_cat_list[[name]])
    f_cat_str[] = paste_matrix2(f_cat_str,f_cat_list[[name]])
    }
  f_cat_m = f_cat_list[[1]]
  f_cat_m[] = f_cat_str
  f_cat_length_m = apply(f_cat_m,c(1,2),function(m){str_length(str_trim(m[[1]]))})
  f_cat_m[is.na(f_cat_length_m>0)] = ""
  f_cat_m = as.data.frame(f_cat_m)
  f_cat_m[] = sapply(f_cat_m,unlist)
  f_cat_m$rownum = -(1:nrow(f_cat_m))
  f_cat_m_melt = melt(f_cat_m,id.vars = "rownum")
  p <- ggplot(f_value_m_melt, aes(variable,rownum)) + geom_tile(aes(fill = value),colour = "white") + scale_fill_gradient(low = "white",high = "purple")+geom_text(aes(label=f_cat_m_melt$value))
  p
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

test_function = function(){
  l = list(character(0))
  result = paste0(unlist(l),"\n",unlist(l))
  print(result)
}