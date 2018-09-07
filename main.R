#get sale data by function
source("~/Rfile/R_hive.R")
sale_data_list = list()
sale_data_list[["shanghaizhenbei"]] = get_sale_data_by_mall_name("上海真北商场")
#get on sale date
on_sale_data = get_on_sale_date_and_clean()
#get contract data
source('~/Rfile/R_hana.R', encoding = 'UTF-8')
contract_list = list()
contract_list[["shanghaizhenbei"]] = get_contract_data("上海真北商场")
#get combined data(with contract and clean the data)
sale_data_picked_list = list()
sale_data_picked_list[["shanghaizhenbei"]] = join_clean_sale_and_contract_data(sale_data_list[["shanghaizhenbei"]],contract_list[["shanghaizhenbei"]])
#get each stall's data(over the whole period)
sale_data_stall_sum_list = list()
sale_data_stall_sum_list[["shanghaizhenbei"]] = setup_stall_metrics(sale_data_picked_list[["shanghaizhenbei"]])
