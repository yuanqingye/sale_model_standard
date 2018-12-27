# View(shanghai_sale_data_process_total)
library(lubridate)
main = function(model_type = "sl",predict_type = "classification"){
   # prepare the original result/feature data
   source("~/Rfile/R_hive.R",encoding = 'UTF-8')
   origin_feature_data = prepare_feature_data()
   origin_result_data = prepare_merged_estimate_record_data()
   # combine them together and make the train/semi train/test set
   merged_feature_result_set = merge(origin_feature_data,origin_result_data,by = "partner_code",all.x = TRUE)
   origin_data_set = merged_feature_result_set
   origin_data_set = final_process_data(origin_data_set)
   if(predict_type == "classification"){
     origin_data_set$relation_record_estimate_value = NULL
   }
   else{
     origin_data_set$relation_record_estimate = NULL
   }
   # deal with the missing value
   # model training
   if(model_type == "sl"){
     data_set = origin_data_set[!is.na(relation_record_estimate),]
     #this will remove the remain na's
     data_set = data_set[complete.cases(data_set),]
     # data_set$relation_record_estimate = as.factor(data_set$relation_record_estimate)
     set.seed(666)
     train_row_randnum = sample(nrow(data_set),0.9*nrow(data_set))
     train_set = data_set[train_row_randnum,]
     test_set = data_set[-train_row_randnum,]
     dest_model = traing_supervised_model()
   }
   else if(model_type == "ssl"){
     
   }
   # get the test value and plot the test data
   predict_test_value = predict(dest_model,test_set[,-"relation_record_estimate"],na.action = na.pass)
}

prepare_merged_estimate_record_data = function(){
  shanghai_sale_data_sql = "select min(date_id) as min_date_id,partner_code,partner_name,mall_name,mall_city_name,contract_code,shop_id,shop_name,house_no,booth_id,booth_desc,sum(act_amt) as sum_act_amt,count(act_amt) as freq
from
  (select date_id,partner_name,l2.partner_code,mall_name,mall_city_name,contract_code,shop_id,shop_name,house_no,booth_id,booth_desc,act_amt from dl.fct_ordr l1,(select distinct partner_code from dl.fct_ordr where mall_city_name like '上海市' and date_id >= '2017-01-01' and date_id <= '2017-12-31' and ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0))) l2
  where
  ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0)) 
  and 
  (date_id >= '2017-01-01' and date_id <= '2017-12-31')
  and l1.partner_code = l2.partner_code) l3
  group by contract_code,mall_name,shop_id,shop_name,mall_city_name,house_no,booth_id,booth_desc,partner_code,partner_name"
  shanghai_sale_data = read_data_hive_general(shanghai_sale_data_sql)
  shanghai_sale_data = data.table(shanghai_sale_data)
  shanghai_sale_data_partner = shanghai_sale_data[,.(freq = sum(freq),sum_act_amt = sum(sum_act_amt),partner_name = max(partner_name)),by = c("partner_code")]
  shanghai_sale_data_partner$sum_act_amt = shanghai_sale_data_partner$sum_act_amt/10000
  partner_sale_census = readxl::read_xls("~/data/partner_info_1129.xls")
  partner_sale_census$partner_code = paste0("00",partner_sale_census$商户号)
  partner_sale_census$partner_name = partner_sale_census$经销商名称;
  partner_sale_census$contact_name = partner_sale_census$`联系人-姓名`
  partner_sale_census_part = partner_sale_census[,c("去年红星销售摸底","去年总销售规模","partner_name","partner_code","contact_name")]
  colnames(partner_sale_census_part)[1:2] = c("redstar_sale_estimated","general_sale_estimated")
  compare_sale_census = merge(shanghai_sale_data_partner,partner_sale_census_part[,c("redstar_sale_estimated","general_sale_estimated","partner_code")],by = "partner_code")
  #this way may not be proper,may be over 1.2 a better choice
  compare_sale_census[,c("relation_record_estimate"):=ifelse(redstar_sale_estimated/sum_act_amt>1,1,ifelse(redstar_sale_estimated/sum_act_amt==1,0,-1))]
  compare_sale_census[,relation_record_estimate_value := redstar_sale_estimated/sum_act_amt]
  return(compare_sale_census)
  }

#using decision tree to get the difference,偏差过大,远远低于可能都需要训练
#订单时间规律:最短非零时间间隔,单位时间内最大订单数,N连比例(或者孤立点比例) OK
#订单各类型比例	ordr_status semi OK
#订单所属品牌最多大类,二类(如没有,需要做关联)(缺失的少,暂时不用关联) OK
#订单中大(小)订单占的比例(以500元做划分) OK
#覆盖铺位数 OK
#购买人姓名电话异同 OK
#订单某些关键字段的缺失率 OK
prepare_feature_data = function(){
  shanghai_sale_data_detail_sql = "select date_id,ordr_date,partner_code,partner_name,mall_name,mall_city_name,contract_code,shop_id,shop_name,house_no,booth_id,booth_desc,act_amt,ordr_type,cust_name,is_promotion,ordr_status,sale_type,cont_unit_price,prod_avg_discnt_rate,cont_main_brand_name,cont_cat1_name,cont_cat1_code,cont_cat2_name,cont_cat2_code,cont_cat3_name,cont_cat3_code
from
  (select date_id,ordr_date,partner_name,l2.partner_code,mall_name,mall_city_name,contract_code,shop_id,shop_name,house_no,booth_id,booth_desc,act_amt,ordr_type,cust_name,is_promotion,ordr_status,sale_type,cont_unit_price,prod_avg_discnt_rate,cont_main_brand_name,cont_cat1_name,cont_cat1_code,cont_cat2_name,cont_cat2_code,cont_cat3_name,cont_cat3_code from dl.fct_ordr l1,(select distinct partner_code from dl.fct_ordr where mall_city_name like '上海市' and date_id >= '2017-01-01' and date_id <= '2017-12-31' and ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0))) l2
  where
  ((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0)) 
  and 
  (date_id >= '2017-01-01' and date_id <= '2017-12-31')
  and l1.partner_code = l2.partner_code) l3"
  # helpful debug injection
  # read_data_hive_general = function(sql){return(shanghai_sale_data_detail)}
  shanghai_sale_data_detail = read_data_hive_general(shanghai_sale_data_detail_sql)
  shanghai_sale_data_detail$new_order_date = ymd_hms(shanghai_sale_data_detail$ordr_date,tz=Sys.timezone())
  setDT(shanghai_sale_data_detail)
  shanghai_sale_data_detail = shanghai_sale_data_detail[order(partner_code,new_order_date),]
  #time related calculation
  #get time gap 
  shanghai_sale_data_process = shanghai_sale_data_detail[,.(time_diff = diff(as.numeric(new_order_date))),by = "partner_code"]
  #0040002006
  shanghai_sale_data_process_freq = shanghai_sale_data_detail[,.(freq_per_day = .N),by = c("partner_code","date_id")][,.(max_freq_in_day = max(freq_per_day)),by = "partner_code"]
  shanghai_sale_data_process_time = shanghai_sale_data_process[,.(reg_time_perc = sum(time_diff%%86400 == 0)/.N,min_time_space = min(time_diff),min_time_space_nonzero = min(time_diff[time_diff>0]),max_cont_hit = get_max_cont_hit(time_diff),max_cont_hit_nozero = get_max_cont_hit(time_diff,cond = expression(v[i]<300 && v[i]!=0)),isolated_perc = get_num_isolated_points(time_diff)/(.N+1)),by = "partner_code"]
  shanghai_sale_data_process_time[,min_time_space_nonzero := ifelse(min_time_space_nonzero == Inf,86400,min_time_space_nonzero)]
  #percentage between big sale and small sale
  shanghai_sale_data_process_bigsaleperc = shanghai_sale_data_detail[,.(over_500_sum = sum(act_amt>500),below_500_sum = sum(act_amt<=500)),by = "partner_code"]
  shanghai_sale_data_process_bigsaleperc[,big_sale_percentage := over_500_sum/(below_500_sum + over_500_sum)]
  #percentage for ordr_type
  # shanghai_sale_data_process = shanghai_sale_data_detail[,.(diff_ordr_status = unique(ordr_status)),by = "partner_code"]
  #num of booth
  shanghai_sale_data_process_boothnum = shanghai_sale_data_detail[,.(num_booth = uniqueN(booth_id)),by = "partner_code"]
  #num of average num in each cust over total number
  # shanghai_sale_data_detail[,.(num_in_category = .N),by = c("partner_code","cust_name")][,.(avg_in_category = sqrt(sum(num_in_category))/.N),by = "partner_code"]
  shanghai_sale_data_process_custentropy = shanghai_sale_data_detail[,.(num_in_category = .N),by = c("partner_code","cust_name")][,.(p = num_in_category/sum(num_in_category),category_num = .N),by = c("partner_code")][,.(cust_entropy = sum(-p*log(p))/category_num),by = c("partner_code","category_num")]
  #category of the max num in each partner_code and its percentage
  shanghai_sale_data_process_cat3percname = shanghai_sale_data_detail[,.(num_in_cat3 = .N),by = c("partner_code","cont_cat3_name")][,.SD[num_in_cat3==max(num_in_cat3),"cont_cat3_name"][1,],by = "partner_code"]
  shanghai_sale_data_process_cat3percvalue = shanghai_sale_data_detail[,.(num_in_cat3 = .N),by = c("partner_code","cont_cat3_name")][,.(cat3_perc_value = max(num_in_cat3)/sum(num_in_cat3)),by = "partner_code"]
  shanghai_sale_data_process_cat3perc = merge(shanghai_sale_data_process_cat3percname,shanghai_sale_data_process_cat3percvalue,all.x = TRUE,by = "partner_code")
  #missing value pattern
  shanghai_sale_data_process_missingpattern = shanghai_sale_data_detail[,.(missing_house_no = sum(is.na(house_no))/.N,missing_cont_unit_price = sum(is.na(cont_unit_price))/.N),by = "partner_code"]
  #order status pattern
  shanghai_sale_data_process_statuspatterntemp = shanghai_sale_data_detail[,.(num_of_status = .N),by = c("partner_code","ordr_status")][,.(num_of_status/sum(num_of_status),ordr_status),by = "partner_code"]
  #3,15 means already paid
  shanghai_sale_data_process_statuspattern = dcast.data.table(shanghai_sale_data_process_statuspatterntemp,partner_code~ordr_status,value.var = "V1")
  #combine all together
  shanghai_sale_data_process_total = merge(shanghai_sale_data_process_freq,shanghai_sale_data_process_time,all.x = TRUE,by = "partner_code")
  shanghai_sale_data_process_total = merge(shanghai_sale_data_process_total,shanghai_sale_data_process_bigsaleperc,all.x = TRUE,by = "partner_code")
  shanghai_sale_data_process_total = merge(shanghai_sale_data_process_total,shanghai_sale_data_process_boothnum,all.x = TRUE,by = "partner_code")
  shanghai_sale_data_process_total = merge(shanghai_sale_data_process_total,shanghai_sale_data_process_custentropy,all.x = TRUE,by = "partner_code")
  shanghai_sale_data_process_total = merge(shanghai_sale_data_process_total,shanghai_sale_data_process_cat3perc,all.x = TRUE,by = "partner_code")
  shanghai_sale_data_process_total = merge(shanghai_sale_data_process_total,shanghai_sale_data_process_missingpattern,all.x = TRUE,by = "partner_code")
  shanghai_sale_data_process_total = merge(shanghai_sale_data_process_total,shanghai_sale_data_process_statuspattern,all.x = TRUE,by = "partner_code")
  return(shanghai_sale_data_process_total)
}

#deal with all necessary data process,some data change may be modified later
final_process_data = function(dataSet){
  dataSet$redstar_sale_estimated = NULL
  dataSet$general_sale_estimated = NULL
  #model training can't use category var so remove cont_cat3_name
  dataSet$relation_record_estimate = as.factor(dataSet$relation_record_estimate)
  #following steps simplify the case and remove the most na part
  pickedSumDf = dataSet[,c("15","3","Y")]
  sumResult = rowSums(pickedSumDf,na.rm = TRUE)
  dataSet[,agree_degree := sumResult]
  dataSet = dataSet[,-c("1","12","14","15","17","19","3","7","Y")]
  dataSet = dataSet[,c(sapply(dataSet,class) != "character"),with = FALSE]
  return(dataSet)
}


traing_supervised_model = function(trainSet,trainingMethod = "C5.0"){
  library(caret)
  Control = trainControl(method = "repeatedcv",number = 10,repeats = 3)
  preProcess = c("center","scale")
  Metric = "Accuracy"
  model = train(relation_record_estimate~.,data = trainSet,method = trainingMethod,metric = Metric,trControl = Control,preProc = preProcess,na.action = na.pass)
  return(model)
}

train_method = c("lda","svmRadial","nb","rpart","rf","gbm","C5.0")
train_with_different_model = function(trainSet = train_set,trainMethod = train_method){
  varias_model <<- list()
  Control = trainControl(method = "repeatedcv",number = 10,repeats = 3)
  preProcess = c("center","scale")
  Metric = "Accuracy"
  for(modelName in trainMethod){
    tryCatch(
    varias_model[[modelName]] <<- train(relation_record_estimate~.,data = trainSet,method = modelName,metric = Metric,trControl = Control,preProc = preProcess,na.action = na.pass),
    error = function(e){print(paste0("The error is related to ",modelName))})
  }
}

# test_method = c("gbm","C5.0")
evaluation_with_different_model = function(testSet = test_set,testMethod = test_method){
  varias_prediction <<- list()
  varias_prediction_accuracy <<- list()
  for(modelName in testMethod){
    varias_prediction[[modelName]] <<- predict(varias_model[[modelName]],testSet[,-"relation_record_estimate"],na.action = na.pass)
    varias_prediction_accuracy[[modelName]] <<- sum(varias_prediction[[modelName]]==testSet$relation_record_estimate)/nrow(testSet)
  }
}