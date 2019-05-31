# smart_mall_track_data_list = list()
library(lubridate)
source("~/Rfile/R_hive.R") ##!!need to include that file
source('~/Rfile/R_impala.R',encoding = 'UTF-8')
source('~/R_Projects/sale_model_standard/R_scripts_functions/supportingfunctions.R', encoding = 'UTF-8')
needed_col = c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum","calinsky_prop")

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190219' limit 60000"
smart_mall_track_data_list[["20190219"]] = read_data_impala_general(smart_mall_track_data_sql)
smart_mall_April_list = list()
smart_mall_Febrary_list = list()
smart_mall_January_list = list()
file.create("./smart_mall_track_data_list_second.RData")
for(dt in as.character(20190126:20190131))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_wisdowmall_store_track_dt where dt = '",dt,"'")
  smart_mall_track_data_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_track_data_list,file = "./smart_mall_track_data_list_second.RData")
  jgc()
}
library(lubridate)
source("~/Rfile/R_hive.R") ##!!need to include that file
source('~/Rfile/R_impala.R',encoding = 'UTF-8')
source('~/R_Projects/sale_model_standard/R_scripts_functions/supportingfunctions.R', encoding = 'UTF-8')

file.create("./smart_mall_January_list.RData")
for(dt in as.character(ymd(20190101:20190131)))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_db_aimalldigital_store_traffic_dt where enter_time like '",dt,"%'")
  smart_mall_January_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_January_list,file = "~/R_Projects/sale_model_standard/smart_mall_January_list.RData")
  jgc()
}

file.create("./smart_mall_Febrary_list.RData")
for(dt in as.character(ymd(20190201:20190228)))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_db_aimalldigital_store_traffic_dt where enter_time like '",dt,"%'")
  smart_mall_Febrary_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_Febrary_list,file = "~/R_Projects/sale_model_standard/smart_mall_Febrary_list.RData")
  jgc()
}

#smart_mall_track_data stores value for January,Febuary,March
smart_mall_track_data[["Febuary"]]

#check who are those duplicated store name under the same store id
#This step is logically complicated so need to be improved
store_name_count = smart_mall_tracking_record[,.(diff_store = uniqueN(store_name)),by = c("store_id")]
duplicated_name_list = smart_mall_tracking_record[store_id %in% store_name_count[diff_store>1,]$store_id,c("store_id","store_name")][!duplicated(store_name),]

store_id_count = smart_mall_tracking_record[,.(diff_id = uniqueN(store_id)),by = c("store_name")]
duplicated_id_list = smart_mall_tracking_record[store_name %in% store_id_count[diff_id>1,]$store_name,c("store_id","store_name")][!duplicated(store_id),]

#store sale etc. from here you can get all the data needed including sale/tracking
#smart_mall_track_data stores value for January
period = "January"
get_sale_data()
process_uv_pv_data(smart_mall_track_data[[period]])
generate_data_for_cluster()
#clustering,plot and filling mix_uv_sale_normalization[[period]] for comparison
dbscan_cluster = cluster_test(type = "dbscan")
mclust_semi_cluster = cluster_test(type = "EM_gaussian_semiauto")
mclust_manu_cluster = cluster_test(type = "EM_gaussian_manu")
# too big data set
# kmeans_cluster = cluster_test(type = "kmeans")
kmeans_sse_cluster = cluster_test(type = "kmeans_SSE")
kmedoids_cluster = cluster_test(type = "K_Medoids")
calinsky_cluster = cluster_test(type = "Calinsky")
ap_cluster = cluster_test(type = "AP")
#Has only one cluster:
# gap_cluster = cluster_test(type = "GAP")

#remove identical column for this specific case(In other case may not be true)
#mix_uv_sale_normalization[[period]]$sse_kmeans_prop = NULL


#store sale etc. from here you can get all the data needed including sale/tracking
#smart_mall_list stores value listed regarding Febuary
period = "Febuary"
get_sale_data("Febuary","2019-02-01","2019-02-28")
process_uv_pv_data(data = smart_mall_track_data[["Febuary"]],period = "Febuary")
generate_data_for_cluster(period)
dbscan_cluster = cluster_test(period,type = "dbscan")
mclust_semi_cluster = cluster_test(period,type = "EM_gaussian_semiauto")
mclust_manu_cluster = cluster_test(period,type = "EM_gaussian_manu")
# kmeans_cluster = cluster_test(period,type = "kmeans")
kmeans_sse_cluster = cluster_test(period,type = "kmeans_SSE")
kmedoids_cluster = cluster_test(period,type = "K_Medoids")
calinsky_cluster = cluster_test(period,type = "Calinsky")
ap_cluster = cluster_test(period,type = "AP")

mix_uv_sale_normalization[[period]]$calinsky_prop = calinsky_cluster$partition[,4]

#plot to check different clusters:
library(factoextra)
library(ggplot2)

fviz_cluster(kmeans_sse_cluster, data = mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
fviz_cluster(dbscan_cluster, data = mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
fviz_cluster(mclust_manu_cluster, data = mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
fviz_cluster(mclust_semi_cluster, data = mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
fviz_cluster(kmedoids_cluster$pamobject, data = mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
library(cluster)
clusplot(kmedoids_cluster$pamobject)
calinsky_formal_cluster = list(data = mclust_manu_cluster$data,clustering = calinsky_cluster$partition[,4])
fviz_cluster(calinsky_formal_cluster, data = mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
#Once a list has a clustering part and a data part, it can be ploted by fviz_cluster
#here pick calinsky for it shows clear clustering boundary

#ap_cluster's cluster has different structure, need to be processed before plot
ap_cluster = apcluster(negDistMat(r=2), mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")],q = 0.05)
ap_cluster_reunited = reorg_ap_cluster(ap_cluster)
ap_formal_cluster = list(data = mclust_manu_cluster$data,clustering = ap_cluster_reunited)
fviz_cluster(ap_formal_cluster, data = mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
clusplot(ap_formal_cluster)

View(mix_uv_sale_normalization[[period]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum","dbscan_prop","nb_kmeans_prop","kmedoids_prop","calinsky_prop","mclust_semi_prop")])
#switch the value since the cluster is grouped by distinct(contra) number than others
mix_uv_sale_normalization[[period]]$kmedoids_prop[mix_uv_sale_normalization[[period]]$kmedoids_prop == 1] = 3
mix_uv_sale_normalization[[period]]$kmedoids_prop[mix_uv_sale_normalization[[period]]$kmedoids_prop == 2] = 1
mix_uv_sale_normalization[[period]]$kmedoids_prop[mix_uv_sale_normalization[[period]]$kmedoids_prop == 3] = 2


#plot the relationship between sale and track
mix_uv_sale_melt = list()
mix_uv_sale_melt[["February"]] = melt.data.table(mix_uv_sale_normalization[["Febuary"]],id.vars = c("store_id","store_name"))
ggplot(mix_uv_sale_melt[["February"]],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggplot(mix_uv_sale_melt[["February"]][variable %in% c("uv","sale_count"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggplot(mix_uv_sale_melt[["February"]][variable %in% c("pv","sale_count"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggplot(mix_uv_sale_melt[["February"]][variable %in% c("uv","sale_sum"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggplot(mix_uv_sale_melt[["February"]][variable %in% c("pv","sale_sum"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))

#using quantile to check outlier
quantile(mix_uv_sale_normalization[["February"]]$uv_over_sale_count, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$pv_over_sale_count, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$uv_over_sale_sum, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$pv_over_sale_sum, c(.25,.75))
#using boxplot to check outlier
boxplot(mix_uv_sale_normalization[["February"]]$uv_over_sale_count)
boxplot(mix_uv_sale_normalization[["February"]]$pv_over_sale_count)
boxplot(mix_uv_sale_normalization[["February"]]$uv_over_sale_sum)
boxplot(mix_uv_sale_normalization[["February"]]$pv_over_sale_sum)


source("~/R_projects/ensemble_method/Rfile/Go_around_model_fun.R")
temp = go_around_model(temp_data_set,"calinsky_prop~.")
metric <- "Accuracy"
preProcess=c("center", "scale")
control <- trainControl(method="repeatedcv", number=10, repeats=3)
temp_data_set = mix_uv_sale_normalization[["January"]][,needed_col,with = FALSE]
temp_data_set$calinsky_prop = as.character(temp_data_set$calinsky_prop)
test_formula = as.formula("calinsky_prop~.")

#Using Caret package to 
library(abnormTestOnlineFunc)
train_test_smscluster = model_preprocess_smscluster(mix_uv_sale_normalization[[period]],0.2)
train_set_smscluster = train_test_smscluster[[1]]
test_set_smscluster = train_test_smscluster[[2]]
train_set_smscluster$calinsky_prop = as.character(train_set_smscluster$calinsky_prop)
test_set_smscluster$calinsky_prop = as.character(test_set_smscluster$calinsky_prop)
model_df_smscluster = data.frame(model_name = c("lda","rpart","C5.0","treebag","rf","gbm","svmRadial","knn"))
trial_result_smscluster = go_around_model_with_test_simple_version(train_set_smscluster,test_set_smscluster,"calinsky_prop",model_df_smscluster,calAR)
summary(trial_result_smscluster[[1]])
bwplot(trial_result_smscluster[[1]])
densityplot(trial_result_smscluster[[1]], metric = "Accuracy")

#cross validation
library(abnormTestOnlineFunc)
test_ratio = 0.2
source('~/R_Projects/Statistics/Rfile/sample_function.R')
train_test_smscluster = model_preprocess_smscluster_multipart(mix_uv_sale_normalization[[period]],test_ratio)
# "uv","pv","sale_sum","sale_count","uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum","calinsky_prop"
train_set_smscluster = train_test_smscluster[[1]]
test_set_smscluster = train_test_smscluster[[2]]
# random_number = train_test_smscluster[[3]]
# test_random_number = random_number[(floor(length(random_number)*(1-test_ratio))+1):length(random_number)]
test_random_number = train_test_smscluster[[2]][["index"]]
#need get from mix_uv_sale_normalization else no store_id
test_set_smscluster_mix = merge(mix_uv_sale_normalization[[period]][test_random_number,c("store_id","calinsky_prop")],mix_uv_sale_normalization[["Febuary"]],all.x = TRUE,by = "store_id")
test_set_smscluster_mix = test_set_smscluster_mix[!is.na(store_name),]
train_set_smscluster$calinsky_prop = as.character(train_set_smscluster$calinsky_prop)
test_set_smscluster_mix$calinsky_prop = as.character(test_set_smscluster_mix$calinsky_prop)
test_set_smscluster_mix = test_set_smscluster_mix[,c("uv","pv","sale_sum","sale_count","uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum","calinsky_prop")]
model_df_smscluster = data.frame(model_name = c("lda","rpart","C5.0","treebag","rf","gbm","svmRadial","knn"))
trial_result_smscluster = go_around_model_with_test_simple_version(train_set_smscluster,test_set_smscluster_mix,"calinsky_prop",model_df_smscluster,calAR,"accuracy")
summary(trial_result_smscluster[[1]])
bwplot(trial_result_smscluster[[1]])
densityplot(trial_result_smscluster[[1]], metric = "Accuracy")
result = trial_result_smscluster[[2]]
