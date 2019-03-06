source("~/Rfile/R_hive.R")
source('~/Rfile/R_hana.R', encoding = 'UTF-8')
#if exists, then using exists value, else using method to fetch.
remain = function(mallName = "上海金桥商场",saleData,contractData){
  if(exists("saleData")){
    sale_data = saleData
  }
  else{
    sale_data = get_sale_data_by_mall_name(mallName)
  }
  on_sale_data = get_on_sale_date_and_clean()
  if(exists("contractData")){
    contract_data = contractData
  }
  else {
    contract_data = get_contract_data(mallName)
  }
  filter_data_set = prepare_filter_data()
  real_value_data = get_real_value_using_data(sale_data,filter_data_set)
  prepared_sale_data = join_clean_sale_and_contract_data(real_value_data,contract_data)
  
  #品类分布
  #Calculate and demonstrate metrics on category
  #need 
  plot_sale_perarea_by_catvar_in_period(prepared_sale_data,contract_data,begin_date = "2017-03-01",finish_date = "2017-5-31",catvar = "CATEGORY_2_EDIT")
  
}