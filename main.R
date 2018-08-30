#get sale data by function
sale_data_list = list()
sale_data_list[["shanghaizhenbei"]] = makeSQL("上海真北商场")
#get on sale date
on_sale_data = get_on_sale_date_and_clean()
#get contract data
contract_list = list()
contract_list[["shanghaizhenbei"]] = get_contract_data("上海真北商场")
#get combined data(with contract and clean the data)
sale_data_picked_list = list()
sale_data_picked_list[["shanghaizhenbei"]] = join_clean_sale_and_contract_data(sale_data_list[["shanghaizhenbei"]],contract_list[["shanghaizhenbei"]])
