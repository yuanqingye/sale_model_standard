# smart_mall_track_data_list = list()
source("~/Rfile/R_hive.R") ##!!need to include that file
source('~/Rfile/R_impala.R',encoding = 'UTF-8')
smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190219' limit 60000"
smart_mall_track_data_list[["20190219"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190218' limit 60000"
smart_mall_track_data_list[["20190218"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190215'"
smart_mall_track_data_list[["20190215"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190213'"
smart_mall_track_data_list[["20190213"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190212'"
smart_mall_track_data_list[["20190212"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190211'"
smart_mall_track_data_list[["20190211"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190210'"
smart_mall_track_data_list[["20190210"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190214'"
smart_mall_track_data_list[["20190214"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190216'"
smart_mall_track_data_list[["20190216"]] = read_data_impala_general(smart_mall_track_data_sql)

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190217'"
smart_mall_track_data_list[["20190217"]] = read_data_impala_general(smart_mall_track_data_sql)

file.create("./smart_mall_track_data_list_second.RData")
for(dt in as.character(20190201:20190209))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_wisdowmall_store_track_dt where dt = '",dt,"'")
  smart_mall_track_data_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_track_data_list,file = "./smart_mall_track_data_list_second.RData")
}

#options(java.parameters = "-Xmx1024m")
source('~/Rfile/R_impala.R',encoding = 'UTF-8')
jgc <- function()
{
  gc()
  .jcall("java/lang/System", method = "gc")
}

for(dt in as.character(20190126:20190131))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_wisdowmall_store_track_dt where dt = '",dt,"'")
  smart_mall_track_data_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_track_data_list,file = "./smart_mall_track_data_list_second.RData")
  jgc()
}

for(dt in as.character(20190220:20190228))
{
  smart_mall_track_data_sql = paste0("select * from ods.ods_wisdowmall_store_track_dt where dt = '",dt,"'")
  smart_mall_list[[dt]] = read_data_impala_general(smart_mall_track_data_sql)
  save(smart_mall_list,file = "./smart_mall_list.RData")
  jgc()
}

# missing 0216 smart_mall_list 

View(smart_mall_track_data_list[["20190210"]])

smart_mall_list = list()
for(dt in as.character('20190210':'20190220')){
  smart_mall_list[[dt]] = smart_mall_track_data_list[[dt]]
}

for(dt in 20190201:20190209){
  smart_mall_list[[as.character(dt)]] = smart_mall_track_data_list[[dt]]
}

smart_mall_tracking_record = rbindlist(smart_mall_list)
smart_mall_tracking_record$store_id = as.character(smart_mall_tracking_record$store_id)
# temp = data.matrix(smart_mall_tracking_record)
smart_mall_tracking_uv = smart_mall_tracking_record[event_type == 0,.(uv = uniqueN(pid)),by = c("store_id")]
sum(is.na(smart_mall_tracking_record$store_id))

store_name_count = smart_mall_tracking_record[,.(diff_store = uniqueN(store_name)),by = c("store_id")]
duplicated_name_list = smart_mall_tracking_record[store_id %in% store_name_count[diff_store>1,]$store_id,c("store_id","store_name")][!duplicated(store_name),]
#check who are those duplicated store name under the same store id

store_id_count = smart_mall_tracking_record[,.(diff_id = uniqueN(store_id)),by = c("store_name")]
duplicated_id_list = smart_mall_tracking_record[store_name %in% store_id_count[diff_id>1,]$store_name,c("store_id","store_name")][!duplicated(store_id),]

smart_mall_tracking_summary_by_store = list()
smart_mall_tracking_summary_by_store[["February"]] = smart_mall_tracking_record[event_type == 0,.(uv = uniqueN(pid),pv = .N),by = c("store_id","store_name")]

#store sale etc. from here you can get all the data needed including sale/tracking
period = "January"
get_sale_data()
process_uv_pv_data()
generate_data_for_cluster()
#clustering,plot and filling mix_uv_sale_normalization[[period]] for comparison
dbscan_cluster = cluster_test(type = "dbscan")
mclust_semi_cluster = cluster_test(type = "EM_gaussian_semiauto")
mclust_manu_cluster = cluster_test(type = "EM_gaussian_manu")
kmeans_cluster = cluster_test(type = "kmeans")
kmeans_sse_cluster = cluster_test(type = "kmeans_SSE")
kmedoids_cluster = cluster_test(type = "K_Medoids")
calinsky_cluster = cluster_test(type = "Calinsky")
ap_cluster = cluster_test(type = "AP")
#Has only one cluster:
# gap_cluster = cluster_test(type = "GAP")

#remove identical column for this specific case(In other case may not be true)
mix_uv_sale_normalization[[period]]$sse_kmeans_prop = NULL

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
mix_uv_sale_melt[["February"]] = melt.data.table(mix_uv_sale_normalization[["February"]],id.vars = c("store_id","store_name"))
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



