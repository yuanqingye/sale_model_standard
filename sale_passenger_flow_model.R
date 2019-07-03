library(dplyr)
source('~/Rfile/R_hana.R', encoding = 'UTF-8')
source("~/Rfile/R_hive.R")
source("~/Rfile/R_impala.R")
source('~/R_Projects/sale_model_standard/Rfile/function.R', encoding = 'UTF-8')
source('~/R_Projects/sale_model_standard/R_scripts_functions/sale_passenger_flow_model_functions.R')
source('~/R_Projects/sale_model_standard/R_scripts_functions/supportingfunctions.R', encoding = 'UTF-8')

# This line not regularly used
# fetch_passenger_flow_data("May")
# fetch_passenger_flow_data("November",2018)
# fetch_passenger_flow_data("December",2018)

mall_name =  "上海金桥商场"
# threshold = 5 only need to seperate the customer stay time
smart_mall_track_data = list()
# smart_mall_stay_data = list()
smart_mall_stay_data_without_staff = list()
# smart_mall_stay_data_customer = list()
month_list = c("January","February","March","April","May","June","July","August","September","October","November","December")

#get flag for if the person is a staff
is_staff_data_sql = "select distinct rs_profile_id from ods.ods_db_aimallbasic_customer_info_dt where is_valid = 1 and is_staff = 1"
is_staff_data = read_data_impala_general(is_staff_data_sql)
is_staff_data$is_staff = 1
is_staff_data = data.table(is_staff_data)

#get people flow for each month with existing(saved )
#the november and december data is in 2018's
for(Month in month_list[c(5)]){
  smart_mall_track_data[[Month]] = get_tracking_data(Month)
}
for(Month in month_list[c(5)]){
  smart_mall_stay_data_without_staff[[Month]] = get_flow_without_staff(smart_mall_track_data,Month,is_staff_data)
}

sale_data_list = list()
sale_data_list_jinqiao = list()
#should get updated at the begining of each month
sale_data_list[["shanghaijinqiao"]] = get_sale_data_by_mall_name("上海金桥商场")
for(Month in month_list[c(5)]){
  sale_data_list_jinqiao[[Month]] = get_sale_data_by_month(sale_data_list[["shanghaijinqiao"]],Month,2019)
}

#merge sale data and flow data together
sale_traffic_merge = list()
sale_traffic_merge_groupby_order = list()
# sale_traffic_merge_with_time_filter = list()
sale_unwanted_columns = c("mall_name","house_no","booth_id","booth_desc","cnt_cat1_num","cnt_cat2_num","cnt_cat3_num","is_coupon","partner_name","cont_cat1_name","cont_cat2_name","cont_cat3_name","month_id")
traffic_unwanted_columns = c("id","create_time","update_time","mall_id","event_type","dt","is_staff")
for(Month in month_list[c(1:5,11:12)]){
sale_traffic_merge[[Month]] = merge(sale_data_list_jinqiao[[Month]][,-c("mall_name","house_no","booth_id","booth_desc","cnt_cat1_num","cnt_cat2_num","cnt_cat3_num","is_coupon","partner_name","cont_cat1_name","cont_cat2_name","month_id")],smart_mall_stay_data_without_staff[[Month]][,-c("id","create_time","update_time","mall_id","event_type","dt","is_staff")],by.x = c("shop_id","date_id"),by.y = c("store_id","enter_date"),allow.cartesian = TRUE)
sale_traffic_merge[[Month]]$ordr_date_xct = lubridate::ymd_hms(sale_traffic_merge[[Month]]$ordr_date)
sale_traffic_merge[[Month]][ordr_date >= enter_time & ordr_date <= exit_time,person_related := 1]
sale_traffic_merge[[Month]][is.na(person_related),person_related := 0]
}

traffic_groupby_shop = list()
order_num_groupby_shop = list()
for(Month in month_list[c(1:5,11:12)]){
traffic_groupby_shop[[Month]] = smart_mall_stay_data_without_staff[[Month]][,.(total_uv = nrow(unique(.SD[,c("enter_date","profile_id")])),total_pv = uniqueN(enter_time),median_duration = median(duration,na.rm = TRUE),avg_duration = mean(duration,na.rm = TRUE)),by = c("store_id")]
# "cont_cat3_name","cont_main_brand_name" need to be added later according to sale data
order_num_groupby_shop[[Month]] = sale_data_list_jinqiao[[Month]][,.(order_num = .N),by = c("shop_id","cont_cat3_name","cont_main_brand_name")]
}

#get related traffic part
traffic_sale_related = list()
traffic_sale_related_groupby_shop = list()
for(Month in month_list[c(1:5,11:12)]){
traffic_sale_related[[Month]] = sale_traffic_merge[[Month]][person_related == 1 & !duplicated(enter_time),]
traffic_sale_related_groupby_shop[[Month]] = traffic_sale_related[[Month]][,.(total_related_uv = nrow(unique(.SD[,c("profile_id")])),total_related_pv = .N,median_related_duration = median(duration,na.rm = TRUE),avg_related_duration = mean(duration,na.rm = TRUE)),,by = c("shop_id")]
}
#combine related traffic part and total part together
library(dplyr)
shop_analysis_data = list()
for(Month in month_list[c(1:5,11:12)]){
  shop_analysis_data[[Month]] = order_num_groupby_shop[[Month]] %>% 
    inner_join(traffic_groupby_shop[[Month]],by = c("shop_id" = "store_id")) %>%
    inner_join(traffic_sale_related_groupby_shop[[Month]], by = c("shop_id"))
}


model_kept_column = c("order_num","total_uv","total_pv","median_duration","avg_duration","total_related_uv",       
                      "total_related_pv","median_related_duration","avg_related_duration")
flow_model_train_set = rbindlist(shop_analysis_data[c(1:3,6,7)])
flow_model_train_set = flow_model_train_set[,model_kept_column,with=FALSE]
flow_model_test_set = shop_analysis_data[[5]]
shop_id = flow_model_test_set$shop_id
setDT(flow_model_test_set)
flow_model_test_set = flow_model_test_set[,model_kept_column,with=FALSE]
library(randomForest)
flow_model_random_forest = randomForest(order_num~.,flow_model_train_set)
result = predict(flow_model_random_forest,flow_model_test_set)
comparison = cbind(shop_id = shop_id,predict = result,real = flow_model_test_set$order_num)
Metrics::rmse(flow_model_test_set$order_num,result)
library(openxlsx)
write.xlsx(comparison,file = "./Rresult/sale_model_result_may.xlsx")
# sale_traffic_merge_groupby_shop_related = list()
# sale_traffic_merge_groupby_shop_related[[Month]]
train_method = c("svmRadial","rpart","rf")
list_model = list()
list_result = list()
model_and_result = list()
model_and_result = train_with_different_model(trainSet = flow_model_train_set,flow_model_test_set,trainMethod = train_method,Formula = order_num~.)
list_model = model_and_result[[1]]
list_result =  model_and_result[[2]]
comparison = cbind(shop_id = shop_id,svm_predict = list_result[[1]],rpart_predict = list_result[[2]],rf_predict = list_result[[3]],real = flow_model_test_set$order_num)
svm_error = Metrics::rmse(flow_model_test_set$order_num,list_result[[1]])
rpart_error = Metrics::rmse(flow_model_test_set$order_num,list_result[[2]])
rf_error = Metrics::rmse(flow_model_test_set$order_num,list_result[[3]])

Control = trainControl(method = "repeatedcv",number = 10,repeats = 3)
preProc = c("center","scale")
Metric = "RMSE"
rpart_model = train(order_num~.,data = flow_model_train_set,method = "rpart",metric = Metric,trControl = Control,preProc = preProc,na.action = na.pass)
rpart_result = predict(rpart_model,flow_model_test_set[,-"order_num"],na.action = na.pass)
rpart_comparison = cbind(shop_id = shop_id,predict = rpart_result,real = flow_model_test_set$order_num)

#create/generate features for future model
for(Month in month_list[1:5]){
sale_traffic_merge[[Month]][ordr_date > enter_time & ordr_date < exit_time,person_related := 1]
sale_traffic_merge[[Month]][is.na(person_related),person_related := 0]
sale_traffic_merge_groupby_shop_and_date = list()
sale_traffic_merge_groupby_shop_and_date[[Month]] = sale_traffic_merge[[Month]][,.(order_num_perday = uniqueN(ordr_id)),by = c("date_id","shop_id")]
sale_traffic_merge[[Month]] = inner_join(sale_traffic_merge[[Month]],sale_traffic_merge_groupby_shop_and_date[[Month]],by = c("shop_id" = "shop_id","date_id" = "date_id"))
setDT(sale_traffic_merge[[Month]])
sale_traffic_merge_groupby_shop_and_date_and_person = list() 
sale_traffic_merge_groupby_shop_and_date_and_person[[Month]] = sale_traffic_merge[[Month]][,.(related_sum = sum(person_related)),by = .(date_id,profile_id,shop_id)]
sale_traffic_merge[[Month]] = inner_join(sale_traffic_merge[[Month]],sale_traffic_merge_groupby_shop_and_date_and_person[[Month]],by = c("shop_id" = "shop_id","date_id" = "date_id","profile_id" = "profile_id"))
setDT(sale_traffic_merge[[Month]])
#sum with some condition:
sale_traffic_merge_groupby_order[[Month]] = sale_traffic_merge[[Month]][,.(visit_times = .N,related_visit_times = sum(person_related),max_duration = max(duration),mean_duration = mean(duration),related_mean_duration = mean(.SD[person_related == 1,][["duration"]]),related_max_duration = max(.SD[person_related == 1,][["duration"]]),time_distance = distance_between_point_to_interval_sets(.SD$ordr_date_xct,.SD$enter_time_xct,.SD$exit_time_xct,.SD$duration,5),related_person_duration_sum = get_related_person_duration_sum(.SD[["related_sum"]],.SD[["duration"]])),by = .(ordr_id,order_num_perday,prod_name,cont_cat3_name,act_amt)]
sale_traffic_merge_groupby_order[[Month]][,visit_per_order:=visit_times/order_num_perday] 
}