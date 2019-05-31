source('~/Rfile/R_hana.R', encoding = 'UTF-8')
source("~/Rfile/R_hive.R")
source("~/Rfile/R_impala.R")
source('~/R_Projects/sale_model_standard/Rfile/function.R', encoding = 'UTF-8')
source('~/R_Projects/sale_model_standard/R_scripts_functions/sale_passenger_flow_model_functions.R')
# This line not regularly used
# fetch_passenger_flow_data("May")
mall_name =  "上海金桥商场"
threshold = 5
smart_mall_track_data = list()
smart_mall_stay_data = list()
smart_mall_stay_data_without_staff = list()
# smart_mall_stay_data_customer = list()
month_list = c("January","February","March","April","May","June","July","August","September","October","November","December")

is_staff_data_sql = "select distinct rs_profile_id from ods.ods_db_aimallbasic_customer_info_dt where is_valid = 1 and is_staff = 1"
is_staff_data = read_data_impala_general(is_staff_data_sql)
is_staff_data$is_staff = 1
is_staff_data = data.table(is_staff_data)

for(Month in month_list[1:5]){
  smart_mall_track_data[[Month]] = get_tracking_data(Month)
}

for(Month in month_list[1:5]){
  smart_mall_stay_data_without_staff[[Month]] = get_flow_without_staff(smart_mall_track_data,Month,is_staff_data)
}

