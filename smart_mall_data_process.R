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

#store sale
source("~/Rfile/R_hive.R")
sale_data_list_with_time = list()
sale_data_list_with_time[["February"]] = get_sale_data_by_mall_name_with_time_range("上海金桥商场","2019-02-01","2019-02-20")

sale_data_period_summary_by_store = list()
sale_data_period_summary_by_store[["February"]] = sale_data_list_with_time[["February"]][,.(sale_sum = sum(act_amt),sale_count = .N),by = c("shop_id","shop_name")]

mix_uv_sale = list()
mix_uv_sale[["February"]] = merge(smart_mall_tracking_summary_by_store[["February"]],sale_data_period_summary_by_store[["February"]],
                                  by.x = c("store_id","store_name"),by.y = c("shop_id","shop_name"))
library(clusterSim)
mix_uv_sale_normalization = list()
mix_uv_sale_normalization[["February"]] = data.Normalization(mix_uv_sale[["February"]],type="n4",normalization="column")
mix_uv_sale_normalization[["February"]] = data.table(mix_uv_sale_normalization[["February"]])
mix_uv_sale_normalization[["February"]]$uv = as.numeric(as.character(mix_uv_sale_normalization[["February"]]$uv))
mix_uv_sale_normalization[["February"]]$pv = as.numeric(as.character(mix_uv_sale_normalization[["February"]]$pv))
mix_uv_sale_normalization[["February"]]$sale_sum = as.numeric(as.character(mix_uv_sale_normalization[["February"]]$sale_sum))
mix_uv_sale_normalization[["February"]]$sale_count = as.numeric(as.character(mix_uv_sale_normalization[["February"]]$sale_count))
mix_uv_sale_normalization[["February"]] = data.table(mix_uv_sale_normalization[["February"]])

mix_uv_sale_melt = list()
mix_uv_sale_melt[["February"]] = melt.data.table(mix_uv_sale_normalization[["February"]],id.vars = c("store_id","store_name"))
ggplot(mix_uv_sale_melt[["February"]],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))

ggplot(mix_uv_sale_melt[["February"]][variable %in% c("uv","sale_count"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggplot(mix_uv_sale_melt[["February"]][variable %in% c("pv","sale_count"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggplot(mix_uv_sale_melt[["February"]][variable %in% c("uv","sale_sum"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))
ggplot(mix_uv_sale_melt[["February"]][variable %in% c("pv","sale_sum"),],aes(x = store_name,y = value,fill = variable,group = variable)) + geom_bar(stat = "identity",position = "dodge")+theme(axis.text.x = element_text(angle = 90, hjust = 1))

mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum"):= .((uv+0.1)/(sale_count+0.1),(pv+0.1)/(sale_count+0.1),(uv+0.1)/(sale_sum+0.1),(pv+0.1)/(sale_sum+0.1))]
mix_uv_sale_normalization[["January"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum"):= .((uv+0.1)/(sale_count+0.1),(pv+0.1)/(sale_count+0.1),(uv+0.1)/(sale_sum+0.1),(pv+0.1)/(sale_sum+0.1))]



quantile(mix_uv_sale_normalization[["February"]]$uv_over_sale_count, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$pv_over_sale_count, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$uv_over_sale_sum, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$pv_over_sale_sum, c(.25,.75))

boxplot(mix_uv_sale_normalization[["February"]]$uv_over_sale_count)
boxplot(mix_uv_sale_normalization[["February"]]$pv_over_sale_count)
boxplot(mix_uv_sale_normalization[["February"]]$uv_over_sale_sum)
boxplot(mix_uv_sale_normalization[["February"]]$pv_over_sale_sum)


library(dbscan)
kNNdist(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")],k = 5)
kNNdistplot(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")], k = 5)
abline(h=1.3, col = "red", lty=2)

dbscan_clust = dbscan(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")], eps = 1.3, minPts = 5)
mix_uv_sale_normalization[["February"]]$dbscan_prop = dbscan_clust$cluster
#in this method we got cluster number 2 and the cluster result

#1.mclust way
library(mclust)
m_clust <- Mclust(as.matrix(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")]), G=1:20) #聚类数目从1一直试到20
summary(m_clust)

plot(m_clust, "BIC")

#2.Using kmeans and using inner method/index to tell which cluster is best
library(NbClust)
set.seed(1234) #因为method选择的是kmeans，所以如果不设定种子，每次跑得结果可能不同
nb_clust <- NbClust(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")],  distance = "euclidean",
                    min.nc=2, max.nc=15, method = "kmeans",
                    index = "alllong", alphaBeale = 0.1)
#best cluster 2

wssplot <- function(data, nc=15, seed=1234){
  wss <- (nrow(data)-1)*sum(apply(data,2,var))
  for (i in 2:nc){
    set.seed(seed)
    wss[i] <- sum(kmeans(data, centers=i)$withinss)
  }
  plot(1:nc, wss, type="b", xlab="Number of Clusters",
       ylab="Within groups sum of squares")}

wssplot(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
#best cluster 2

#3.another kmeans
library(factoextra)
library(ggplot2)
set.seed(1234)
fviz_nbclust(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")], kmeans, method = "wss") +
  geom_vline(xintercept = 2, linetype = 2)

km.res <- kmeans(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")], 2)
fviz_cluster(km.res, data = mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
#best cluster 2

#4.K Medoids clustering
library(fpc)
pamk.best <- pamk(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
pamk.best$nc

library(cluster)
clusplot(pam(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")], pamk.best$nc))
#best cluster 2

#5.Clinsky criterion
library(vegan)
ca_clust <- cascadeKM(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")],1, 10, iter = 1000)
ca_clust$results

#6.Affinity propagation (AP) clustering
library(apcluster)
ap_clust <- apcluster(negDistMat(r=2), mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")])
length(ap_clust@clusters)

#best cluster 13

#7. 轮廓系数Average silhouette method
require(cluster)
library(factoextra)
fviz_nbclust(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")], kmeans, method = "silhouette")

#best cluster 3

#8. Gap Statistic
library(cluster)
set.seed(123)
gap_clust <- clusGap(mix_uv_sale_normalization[["February"]][,c("uv_over_sale_count","pv_over_sale_count","uv_over_sale_sum","pv_over_sale_sum")], kmeans, 10, B = 500, verbose = interactive())
gap_clust
#best cluster 3

