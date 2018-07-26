source("~/Rfile/R_hive.R") ##!!need to include that file
sale_data_sql = "select * from dl.fct_ordr where mall_name like '%上海金桥商场%' and trade_amt > 0"
sale_data = read_data_hive_general(sale_data_sql)


jinqiao_sale_july = temp_hive
names(jinqiao_sale_july) = str_replace(names(jinqiao_sale_july),'fct_ordr.','')
jinqiao_sale_july = data.table(jinqiao_sale_july)
View(jinqiao_sale_july[,.(trade_amount = sum(trade_amt),trade_num = .N),by = cont_cat3_name])
View(jinqiao_sale_july[,.(trade_amount = sum(trade_amt),trade_num = .N),by = cont_cat2_name])
View(jinqiao_sale_july[,.(trade_amount = sum(trade_amt),trade_num = .N),by = cont_cat_name])

