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

View(smart_mall_track_data_list[["20190210"]])

smart_mall_list = list()
for(dt in as.character('20190210':'20190219')){
  smart_mall_list[[dt]] = smart_mall_track_data_list[[dt]]
}

for(dt in 20190201:20190209){
  smart_mall_list[[as.character(dt)]] = smart_mall_track_data_list[[dt]]
}

smart_mall_track_data_sql = "select * from ods.ods_wisdowmall_store_track_dt where dt = '20190220'"
smart_mall_list[["20190220"]] = read_data_impala_general(smart_mall_track_data_sql)

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

quantile(mix_uv_sale_normalization[["February"]]$uv_over_sale_count, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$pv_over_sale_count, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$uv_over_sale_sum, c(.25,.75))
quantile(mix_uv_sale_normalization[["February"]]$pv_over_sale_sum, c(.25,.75))

boxplot(mix_uv_sale_normalization[["February"]]$uv_over_sale_count)
boxplot(mix_uv_sale_normalization[["February"]]$pv_over_sale_count)
boxplot(mix_uv_sale_normalization[["February"]]$uv_over_sale_sum)
boxplot(mix_uv_sale_normalization[["February"]]$pv_over_sale_sum)


