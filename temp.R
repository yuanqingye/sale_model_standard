source("~/Rfile/R_hive.R") ##!!need to include that file
sale_data_sql = "select date_id,ordr_date,prod_name,mall_name,shop_id,shop_name,contract_code,house_no,booth_id,booth_desc,cnt_cat1_num,cnt_cat2_num,cnt_cat3_num,is_coupon,partner_name,cont_cat1_name,cont_cat2_name,cont_cat3_name,act_amt from dl.fct_ordr where mall_name like '%上海金桥商场%' and 
((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0))"
sale_data_raw = read_data_hive_general(sale_data_sql)
# sale_data_raw$floor = str_extract(sale_data_raw$booth_desc,'([\u4e00-\u9fa5]|\\d)楼')
sale_data_raw$month_id = str_sub(sale_data_raw$ordr_date,1,7)
sale_data_raw = data.table(sale_data_raw)
sale_data_raw = sale_data_raw[!is.na(contract_code),]
sale_data_raw[str_trim(contract_code)== '' & str_detect(shop_name,'波比'),]$contract_code = "0203010009980"
sale_data_raw[str_trim(contract_code)== '' & str_detect(shop_name,'方太'),]$contract_code = "0203010009892"
sale_data_raw[str_trim(contract_code)== '' & str_detect(shop_name,'Kids&Teen'),]$contract_code = "0203010010490"
sale_data_raw[str_trim(contract_code)== '' & str_detect(shop_name,'玫瑰佳缘'),]$contract_code = "0203010010225"
sale_data_raw[str_trim(contract_code)== '' & str_detect(shop_name,'圣象强化地板'),]$contract_code = "0203010010534"
sale_data_raw[str_trim(contract_code)== '' & str_detect(shop_name,'法恩莎卫浴'),]$contract_code = "0203010010375"
#Can't figure out is of the contract on the 6th floor or on the 1st floor
sale_data_raw[str_trim(contract_code)== '' & str_detect(shop_name,'家倍得'),]$contract_code = "0203010010149"

on_sale_data_sql = "select distinct prom_begin_time,`date` from dm.dm_weixin_ticket_schedule_dt"
on_sale_data = read_data_hive_general(on_sale_data_sql)
on_sale_data = data.table(on_sale_data)
on_sale_data = on_sale_data[str_detect(prom_begin_time,"[\\d]{4}-[\\d]{2}-[\\d]{2}"),]
# difftime(as.Date('2011-09-09','%Y-%m-%d'),as.Date('2011-09-18','%Y-%m-%d'), units = c("days"))
on_sale_data = on_sale_data[as.numeric(difftime(as.Date(`date`,'%Y-%m-%d'),as.Date(prom_begin_time,'%Y-%m-%d')))<15,]


source('~/Rfile/R_hana.R', encoding = 'UTF-8')
contract_data_sql = "select * from BIGBI.dim_contract_detail where mall_name like '%上海金桥商场%'"
contract_raw = read_data_from_hana(contract_data_sql)
contract_raw = data.table(contract_raw)
contract_duration = difftime(contract_raw$FINISH_DATE,contract_raw$BEGIN_DATE,units = "days")
contract_raw$CONTRACT_DURATION = as.numeric(contract_duration)
contract = contract_raw[,c("CONTRACT_CODE","BRAND_NAME","SERIES_NAME","BOOTH_CODE","BOOTH_GRADE","BOOTH_NAME","CATEGORY_NAME_1","CATEGORY_NAME_2","CATEGORY_NAME_3","FLOOR_NAME","RENTABLE_AREA","ACTUAL_AREA","ZX_PRICE","MONTH_AMOUNT","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]
# limit_time_contract = contract_raw[BEGIN_DATE<="2018-01-01" & FINISH_DATE>="2018-01-01",]
time_contract_201801 = contract_raw[BEGIN_DATE<="2018-01-01" & FINISH_DATE>="2018-01-31",]
area_cat2_201801 = time_contract_201801[,.(area = sum(ACTUAL_AREA)),by = "CATEGORY_NAME_2"]
sale_cat2_201801 = sale_data_picked[month_id == '2018-01',.(sale = sum(act_amt)),by = "CATEGORY_NAME_2"]
sale_area_cat2_201801 = merge(sale_cat2_201801,area_cat2_201801,by = "CATEGORY_NAME_2",all.x = TRUE)
sale_area_cat2_201801 = sale_area_cat2_201801[,avg_sale := sale/area]
sale_area_cat2_201801 = sale_area_cat2_201801[order(avg_sale,decreasing = TRUE),]
sale_area_cat2_201801$CATEGORY_NAME_2 = factor(sale_area_cat2_201801$CATEGORY_NAME_2,levels = sale_area_cat2_201801$CATEGORY_NAME_2)
ggplot(data = sale_area_cat2_201801,mapping = aes(x = CATEGORY_NAME_2,y = avg_sale,fill = "good")) + geom_bar(stat = "identity") + scale_fill_manual(values=c("#9999CC"))+theme(axis.text.x = element_text(angle = 90, hjust = 1))


sale_data = merge(sale_data_raw,contract,by.x = "contract_code",by.y = "CONTRACT_CODE",all.x = TRUE)
View(sale_data[((str_trim(cont_cat1_name) == ""|is.na(cont_cat1_name)) & (str_trim(CATEGORY_NAME_1) == ""|is.na(CATEGORY_NAME_1))),])
sale_data = sale_data[str_trim(CATEGORY_NAME_1) == ""|is.na(CATEGORY_NAME_1),c("CATEGORY_NAME_1","CATEGORY_NAME_2","CATEGORY_NAME_3") := list(cont_cat1_name,cont_cat2_name,cont_cat3_name)]
sale_data = sale_data[str_trim(cont_cat1_name) == ""|is.na(cont_cat1_name),c("cont_cat1_name","cont_cat2_name","cont_cat3_name") := list(CATEGORY_NAME_1,CATEGORY_NAME_2,CATEGORY_NAME_3)]
sale_data_picked = sale_data[,c("date_id","ordr_date","month_id","prod_name","mall_name","shop_id","shop_name","house_no","act_amt","contract_code","BRAND_NAME","SERIES_NAME","BOOTH_CODE","BOOTH_GRADE","BOOTH_NAME","cont_cat1_name","cont_cat2_name","cont_cat3_name","CATEGORY_NAME_1",
                                "CATEGORY_NAME_2","CATEGORY_NAME_3","FLOOR_NAME","RENTABLE_AREA","ACTUAL_AREA","ZX_PRICE","MONTH_AMOUNT","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]
sale_data_picked = sale_data_picked[,avg_amt := act_amt/ACTUAL_AREA,]
sale_data_picked_after_2016 = sale_data_picked[str_sub(month_id,1,4)%in%c(2016,2017,2018),]
sale_data_picked_after_2016$onsale = sapply(sale_data_picked_after_2016$date_id,function(dot_date,start_dates,end_dates){any(between(dot_date,start_dates,end_dates))},on_sale_data$prom_begin_time,on_sale_data$date)

#品类分布
sale_data_month_cat1 = sale_data_picked[,.(sale = sum(act_amt)),by = c("month_id","cont_cat1_name")]
sale_data_wide_cat1 = dcast(sale_data_month_cat1,month_id~cont_cat1_name,value.var = "sale")

sale_data_month_cat2 = sale_data_picked[,.(sale = sum(act_amt)),by = c("month_id","cont_cat2_name")]
sale_data_wide_cat2 = dcast(sale_data_month_cat2,month_id~cont_cat2_name,value.var = "sale")

sale_data_month_cat3 = sale_data_picked[,.(sale = sum(act_amt)),by = c("month_id","cont_cat3_name")]
sale_data_wide_cat3 = dcast(sale_data_month_cat3,month_id~cont_cat3_name,value.var = "sale")

sale_data_cat1_sum = sale_data_picked[,.(sale = sum(act_amt),record_num = .N),by = c("cont_cat1_name")]
sale_data_cat2_sum = sale_data_picked[,.(sale = sum(act_amt),record_num = .N),by = c("cont_cat2_name")]
sale_data_cat3_sum = sale_data_picked[,.(sale = sum(act_amt),record_num = .N),by = c("cont_cat3_name")]

ggplot(data = sale_data_cat1_sum, mapping = aes(x = cont_cat1_name,y = sale)) + geom_bar(stat = "identity",aes(fill = cont_cat1_name))
ggplot(data = sale_data_cat2_sum, mapping = aes(x = cont_cat2_name,y = sale)) + geom_bar(stat = "identity",aes(fill = cont_cat2_name))
ggplot(data = sale_data_cat3_sum, mapping = aes(x = cont_cat3_name,y = sale)) + geom_bar(stat = "identity",aes(fill = cont_cat3_name))


sale_data_cat2_saleperarea_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("cont_cat2_name")]
sale_data_cat2_saleperarea_sum_2017 = sale_data_picked[month_id>='2017-01'&month_id<='2017-12',.(saleperarea = sum(avg_amt),record_num = .N),by = c("cont_cat2_name")]
sale_data_cat2_saleperarea_sum_2018 = sale_data_picked[month_id>='2018-01'&month_id<='2018-12',.(saleperarea = sum(avg_amt),record_num = .N),by = c("cont_cat2_name")]
plot_single_factor(sale_data_cat2_saleperarea_sum,"cont_cat2_name","saleperarea")
plot_single_factor(sale_data_cat2_saleperarea_sum_2017,"cont_cat2_name","saleperarea")
plot_single_factor(sale_data_cat2_saleperarea_sum_2018,"cont_cat2_name","saleperarea")

#品类系数
sale_data_cat2_saleperarea_sum[,saleperarea_median := median(saleperarea)]
sale_data_cat2_saleperarea_sum[,cat2_index := saleperarea/saleperarea_median]

#品牌分布
sale_data_brand_saleperarea_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("cont_cat2_name","SERIES_NAME")]
brand_plot_under_cat = list()
brand_plot_under_cat[["wood"]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(cont_cat2_name=='实木'))
brand_plot_under_cat[["bath"]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(cont_cat2_name=='卫浴'))
brand_plot_under_cat[["tile"]] = plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","saleperarea",expression(cont_cat2_name=='瓷砖'))
ggplot2.multiplot(brand_plot_under_cat[["wood"]],
                  brand_plot_under_cat[["bath"]],
                  brand_plot_under_cat[["tile"]], cols=3)

plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","sale",expression(cont_cat2_name=='瓷砖'))
cat_list = sale_data_cat2_saleperarea_sum$cont_cat2_name[1:3]
plot_single_factor(sale_data_brand_saleperarea_sum,"SERIES_NAME","sale")
treemap(sale_data_brand_saleperarea_sum[cont_cat2_name %in% cat_list,],index = c("cont_cat2_name","SERIES_NAME"),vSize = "saleperarea")

g = ggplot(data = sale_data_brand_saleperarea_sum[cont_cat2_name %in% cat_list,],mapping = aes(x = SERIES_NAME,
                                                                                           y = saleperarea,fill = cont_cat2_name))
g = g + geom_bar(stat = "identity")
g + facet_grid(cont_cat2_name ~ .)
g + facet_wrap(cont_cat2_name ~ .)

#品牌系数
sale_data_brand_saleperarea_sum[,saleperarea_median := median(saleperarea,na.rm = TRUE),by = "cont_cat2_name"]
sale_data_brand_saleperarea_sum[,brand_index := saleperarea/saleperarea_median]

#楼层分布
sale_data_floor_sum = sale_data_picked[,.(sale = sum(act_amt),record_num = .N),by = c("FLOOR_NAME")]
sale_data_floor_sum = sale_data_floor_sum[order(sale,decreasing = TRUE),]
sale_data_floor_sum = within(sale_data_floor_sum,
                             FLOOR_NAME <- factor(FLOOR_NAME,levels = FLOOR_NAME))
ggplot(data = sale_data_floor_sum,aes(FLOOR_NAME))+geom_bar(aes(weight = sale,fill = "good"))+scale_fill_manual(values=c("#9999CC"))+guides(row = guide_legend(reverse = TRUE))+theme(axis.text.x = element_text(angle = 90, hjust = 1))

#品类年份，季节，工作日/周末单位面积销量分布
#工作日/周末单位面积
Sys.setlocale(category = "LC_ALL", locale = "English_United States.1252")
sale_data_picked[,dayofweek := weekdays(as.Date(date_id,'%Y-%m-%d'))]
sale_data_picked[,ifweekend := ifelse(dayofweek %in% c("Saturday","Sunday"),"weekend","weekday")]
# sale_data_picked[,mirrorsaleperarea := ifelse(dayofweek %in% c("Saturday","Sunday"),saleperarea,-saleperarea)]
sale_data_cat2_week_saleperarea_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("cont_cat2_name","ifweekend")]

sale_data_cat2_week_saleperarea_sum[,saleperarea_mirror := ifelse(ifweekend == "weekend",saleperarea,-saleperarea)]
p <- ggplot(sale_data_cat2_week_saleperarea_sum, aes(x=cont_cat2_name, y=saleperarea_mirror, fill=ifweekend)) + geom_bar(stat="identity", position="identity")
p + theme(axis.text.x = element_text(angle = 90, hjust = 1))

sale_data_cat2_week_saleperarea_sum_dcast = dcast(sale_data_cat2_week_saleperarea_sum,cont_cat2_name~ifweekend,value.var = "saleperarea")
sale_data_cat2_week_saleperarea_sum_dcast[,weekend_index := weekend/weekday]
Sys.setlocale(category = "LC_ALL", locale = "")

#季节性变动
sale_data_picked[,season := quarters(as.Date(date_id,'%Y-%m-%d'))]
sale_data_cat2_season_saleperarea_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("cont_cat2_name","season")]
p = ggplot(data = sale_data_cat2_season_saleperarea_sum,aes(x = cont_cat2_name,y = saleperarea,group =season))
p + geom_bar(stat = "identity", position = "dodge", aes(fill = season)) + theme(axis.text.x = element_text(angle = 90,hjust = 1))

#促销性变动
sale_data_cat2_onsale_saleperarea_sum = sale_data_picked_after_2016[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),record_num = .N),by = c("cont_cat2_name","onsale")]
p = ggplot(data = sale_data_cat2_onsale_saleperarea_sum,aes(x = cont_cat2_name,y = saleperarea,group = onsale))
p + geom_bar(stat = "identity", position = "dodge", aes(fill = onsale)) + theme(axis.text.x = element_text(angle = 90,hjust = 1))
sale_data_cat2_onsale_saleperarea_sum_dcast = dcast(sale_data_cat2_onsale_saleperarea_sum,cont_cat2_name~onsale,value.var = "saleperarea")
sale_data_cat2_onsale_saleperarea_sum_dcast[,onsale_index := `TRUE`/`FALSE`]

#店铺热力图


####店铺逐一分析
# sale_data_month_booth_sum = sale_data_raw[,.(sale = sum(trade_amt),order_num = .N),by = c("month_id","cont_cat3_name","booth_id","booth_desc","house_no","shop_name")]
sale_data_month_booth_sum = sale_data_picked[,.(saleperarea = sum(avg_amt),sale = sum(act_amt),order_num = .N),by = c("month_id","CATEGORY_NAME_3","FLOOR_NAME","BOOTH_CODE","BOOTH_NAME","BOOTH_GRADE","house_no","shop_name","shop_id","BRAND_NAME","SERIES_NAME","contract_code","CONTRACT_DURATION","BEGIN_DATE","FINISH_DATE")]
sale_data_month_booth_sum_recent = sale_data_month_booth_sum[month_id>='2017-04',]
# `[.data.frame`(dict,dict$name == 'a',"value")
# apply(m,c(1,2),sum)
Booth_code_name_dict = sale_data_month_booth_sum[,c("BOOTH_CODE","BOOTH_NAME")]
Booth_code_name_dict2 = separate_rows(Booth_code_name_dict,BOOTH_CODE,BOOTH_NAME,sep = "/")
Booth_code_name_dict2 = unique(Booth_code_name_dict2)
# Booth_code_name_dict3 = Booth_code_name_dict2[str_detect(Booth_code_name_dict2$BOOTH_CODE,"^A(\\w)*"),]
#initially we split by BOOTH_CODE,BOOTH_NAME and then we filter by more
sale_data_month_booth_sum2 = separate_rows(sale_data_month_booth_sum,BOOTH_CODE,sep = "/")
sale_data_month_booth_sum3 = merge(sale_data_month_booth_sum2,Booth_code_name_dict2,by = "BOOTH_NAME",all.x = TRUE,allow.cartesian=TRUE) 
#sale data booth sum with series name and category name in it
sale_data_booth_sum_recent = sale_data_month_booth_sum3[month_id>='2017-04',.(saleperarea = sum(saleperarea),series_name = SERIES_NAME[sort(table(SERIES_NAME),decreasing = TRUE)[1]],category_name_3 = CATEGORY_NAME_3[sort(table(CATEGORY_NAME_3),decreasing = TRUE)[1]]),by = c("BOOTH_CODE.y","BOOTH_GRADE","contract_code","BEGIN_DATE")]
colnames(sale_data_booth_sum_recent) = c("BOOTH_CODE","BOOTH_GRADE","contract_code","BEGIN_DATE","saleperarea","series_name","category_name_3")
# sale_data_booth_sum_recent[is.na(BOOTH_CODE),"saleperarea"] = 0
saleperareaquantile = quantile(sale_data_booth_sum_recent$saleperarea,c(0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1))
# saleperareainterval = cut(sale_data_booth_sum_recent$saleperarea,saleperareaquantile)
sale_data_booth_sum_recent[,saleperareainterval := cut(saleperarea,saleperareaquantile)]
# sale per area and per interval
sale_data_booth_sum_recent[,TIME_SPAN := as.numeric(difftime(as.character(Sys.Date()),BEGIN_DATE))]
sale_data_booth_sum_recent[,saleperareaperduration := saleperarea/TIME_SPAN]
saleperareaperdurationquantile = quantile(sale_data_booth_sum_recent$saleperareaperduration,
                                          c(0,0.125,0.25,0.375,0.5,0.625,0.75,0.875,1))
sale_data_booth_sum_recent[,saleperareaperdurationinterval := cut(saleperareaperduration,saleperareaperdurationquantile)]


#一楼楼层平面结构
m1 = c(NA,NA,NA,0404,NA,NA,0525,NA,0526,NA,NA,0255,NA,0577,NA,NA,0484,0484,0349,0350)
m2 = c(rep(NA,19),0350)
m3 = c(NA,0216,NA,0348,0348,rep(NA,4),0457,0445,0444,0339,0413,rep(NA,6))
m4 = c(rep(NA,9),0457,0457,0443,0339,0413,rep(NA,6))
m5 = NA
m6 = c(rep(NA,3),0672,0672,NA,0439,0376,NA,0398,0398,0398,0375,0441,rep(NA,6))
m7 = c(0516,rep(NA,2),0672,0672,NA,0439,0376,NA,0639,0639,0398,0375,0441,rep(NA,5),0579)
m8 = c(0516,rep(NA,19))
m9 = c(rep(NA,3),0366,0248,0248,rep(NA,3),0604,0604,0438,0381,NA,0437,rep(NA,5))
m10 = c(rep(NA,3),0394,0616,0616,rep(NA,3),0605,0605,0571,0381,0474,0437,rep(NA,4),0342)
m11 = NA
m12 = c(0377,NA,NA,0341,0341,NA,NA,0448,NA,NA,0488,0488,0488,NA,NA,0618,0609,rep(NA,3))
m13 = c(rep(NA,3),0341,rep(NA,16))
m14 = c(0624,rep(NA,19))
m15 = c(0624,0320,0320,rep(NA,17))

f1_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14,m15)
f1_str_m = as.matrix(f1_str_m)
f1_str_m[] = as.character(f1_str_m)
f1_str_m[] = paste_omit_na("0203010010",f1_str_m)
plot_floor_heat(f1_str_m,label_name = c("series_name","category_name_3"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code")


#F2平面结构
m1 = c(NA,NA,NA,0471,0386,0470,0513,0622,NA,0533,0511,0379,0621,NA,NA,0539,0459,0602,0602,0493,0493)
m2 = m1
m3 = NA
m4 = c(0227,NA,0559,0389,NA,NA,NA,0502,0460,0405,0536,0551,0228,0503,0475,0370,0504,rep(NA,4))
m5 = c(0515,NA,0559,0389,NA,NA,NA,0401,0401,0578,0578,0464,0464,0464,0464,0464,0464,rep(NA,4))
m6 = c(NA,NA,NA,NA,NA,0338,0355,0353,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA)
m7 = c(0469,NA,0442,0356,NA,0338,0355,0353,NA,0548,0548,0548,0546,0546,0546,0547,0547,0547,0547,NA,NA)
m8 = c(0415,NA,0354,0354,NA,0338,0355,0353,NA,0548,0548,0548,0546,0546,0546,0547,0547,0547,0547,NA,NA)
m9 = c(rep(NA,19),0509,0509)
m10 = c(0229,NA,0231,0361,0361,NA,NA,NA,0508,0508,0538,0234,0234,NA,0500,NA,NA,NA,NA,0509,0509)
m11 = c(0402,NA,0458,0593,0362,NA,NA,NA,0630,0630,0532,0680,0680,0500,0500,NA,NA,NA,NA,NA,NA)
m12 = c(0599,rep(NA,20))
m13 = c(0403,NA,0506,0465,NA,0523,0549,NA,NA,0400,0400,0518,0416,0241,0241,NA,NA,NA,0477,0211,NA)
m14 = c(0352,NA,0506,0465,NA,0523,0549,NA,NA,0400,0400,0518,0416,0241,0241,NA,NA,NA,0477,0211,NA)
f2_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14)
f2_str_m = as.matrix(f2_str_m)
f2_str_m[] = as.character(f2_str_m)
f2_str_m[] = paste_omit_na("0203010010",f2_str_m)
plot_floor_heat(f2_str_m,label_name = c("series_name","category_name_3"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code")

#F3展位情况
m1 = c(NA,NA,0670,0670,NA,NA,NA,0393,0247,0487,0540,NA,NA,NA,0358,0452,0337,NA,NA,NA,NA,0480,0426,0479,0383)
m2 = c(NA,NA,0670,0670,NA,0671,NA,0393,0247,0487,0540,NA,NA,NA,0358,0452,0337,NA,NA,NA,NA,0480,0426,0479,0383)
m3 = c(rep(NA,24),0383)
m4 = c(0368,0368,NA,0434,0434,0434,NA,NA,NA,NA,NA,NA,0662,0412,0512,0667,0380,0440,0363,0449,0449,NA,NA,NA,NA)
m5 = c(0368,0368,NA,0434,0434,0434,NA,NA,NA,NA,NA,NA,0676,0676,0676,0676,0676,0510,0510,0510,0510,NA,NA,0529,0529)
m6 = c(0368,0368,rep(NA,21),0529,0529)
m7 = c(NA,NA,NA,0623,0623,0626,NA,NA,0655,0655,0655,NA,rep(0651,5),rep(0425,4),NA,NA,0529,0529)
m8 = c(0411,0411,NA,0623,0625,0626,NA,NA,0652,0652,0652,NA,rep(0654,5),rep(0554,4),NA,NA,NA,NA)
m9 = c(0411,0411,rep(NA,21),0544,0544)
m10 = c(NA,NA,0396,0396,0620,0620,0620,rep(NA,5),0392,0399,0399,0391,0391,0555,0555,rep(0378,3),NA,0544,0544)
m11 = c(0531,NA,0396,0396,0629,0629,0629,rep(NA,5),0392,0399,0399,0391,0391,0555,0555,rep(0378,3),NA,NA,NA)
m12 = c(0531,rep(NA,22),0521,0521)
m13 = c(0531,0530,0530,0530,NA,0567,0567,0567,NA,NA,0454,0454,0454,NA,NA,NA,0653,0658,0395,NA,NA,NA,0522,0522,NA)
f3_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13)
f3_str_m = as.matrix(f3_str_m)
f3_str_m[] = as.character(f3_str_m)
f3_str_m[] = paste_omit_na("0203010010",f3_str_m)
plot_floor_heat(f3_str_m,label_name = c("series_name","category_name_3"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code")

#四楼楼层平面结构(mainly focus on the position)
m1 = c(NA,NA,-66,rep(NA,4),0423,0245,rep(NA,3),0481,0535,0617,0281,rep(NA,3),0485,0497,0249,0677) #due to lack of data,I using 9934(-66) instead of 0595
m2 = c(NA,NA,-66,NA,0594,NA,0574,0423,0245,0528,NA,0257,0481,0535,0617,0281,NA,0197,NA,0485,0497,0249,0677)
m3 = NA
m4 = c(rep(NA,10),0520,0520,0619,0501,0462,0462,rep(NA,7))
m5 = c(0387,rep(NA,21),0306)
m6 = c(0387,NA,NA,0221,0221,rep(NA,5),0664,0664,0545,0424,rep(NA,9))
m7 = c(0262,NA,NA,0583,0436,rep(NA,2),0505,0505,NA,0585,0565,0545,0424,rep(NA,8),0406)
m8 = c(rep(NA,22),0428)
m9 = c(0279,NA,0584,0675,0328,NA,NA,0591,0591,NA,0213,0562,0410,0498,0498,0271,rep(NA,6),0280)
m10 = c(0279,NA,0584,0675,0328,rep(NA,5),0213,0562,0410,0498,NA,0271,rep(NA,6),0576)
m11 = c(NA,NA,0210,0558,0558,rep(NA,5),0499,0612,0496,0429,0336,0270,rep(NA,7))
m12 = c(rep(NA,22),0575)
m13 = c(rep(NA,22),0637)
m14 = c(rep(NA,22),0427)
m15 = c(0369,rep(NA,3),0422,NA,0586,0586,0640,NA,0421,0473,0507,0543,0433,rep(NA,4),0567,0397,0435,NA) #凤铝没有交易纪录,故做了改变0421
m16 = c(0632,rep(NA,3),0537,rep(NA,18))
m17 = c(0632,0632,0633,0633,rep(NA,19))

f4_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14,m15,m16,m17)
f4_str_m = f4_str_m+10000
f4_str_m = as.matrix(f4_str_m)
f4_str_m[] = as.character(f4_str_m)
# f4_str_m[] = paste_omit_na("0203010010",f4_str_m)
f4_str_m[] = paste_omit_na_special("02030100",f4_str_m)
plot_floor_heat(f4_str_m,label_name = c("series_name","category_name_3"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code")
#plot with value:saleperarea and label:series
#plot_floor_heat(f4_str_m)
#plot with value:saleperarea and label:category_name_3

#F5
m1 = c(NA,0611,rep(NA,4),0580,0335,0365,NA,NA,0346,0542,NA,NA,0360,0360,0408,0408)
m2 = c(NA,0611,rep(NA,4),0580,0335,0365,0367,NA,0346,0542,NA,NA,0360,0360,0408,0408)
m3 = NA
m4 = c(0364,NA,NA,0382,0382,rep(NA,5),0678,0344,0385,0385,rep(NA,5))
m5 = c(0364,NA,NA,0382,0382,rep(NA,5),NA,NA,NA,NA,rep(NA,4),0359)
m6 = c(rep(NA,3),0420,0420,rep(NA,5),rep(0587,4),rep(NA,4),0359)
m7 = c(rep(NA,3),0420,0420,NA,rep(0589,3),NA,rep(0587,4),rep(NA,4),0359)
m8 = NA
m9 = c(0258,NA,0409,0230,0209,NA,rep(0467,3),NA,0347,0347,0431,0345,0345,rep(NA,4))
m10 = c(0258,NA,0409,0230,0209,rep(NA,5),0456,0456,0431,0345,0345,rep(NA,3),0351)
m11 = c(NA,NA,0409,0371,0371,rep(NA,13),0351)
m12 = c(0236,0236,rep(NA,17))
m13 = c(0236,0236,rep(NA,17))
m14 = c(0238,0238,rep(NA,17))
m15 = c(0238,0238,rep(NA,17))
m16 = c(0489,0489,0303,NA,NA,0466,0330,0330,NA,0407,0407,NA,0343,0432,0666,NA,NA,0556,0556)
m17 = c(0489,0489,0303,0303,NA,0466,0330,NA,NA,NA,NA,NA,0343,0432,0666,NA,NA,0556,0556)
f5_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14,m15,m16,m17)
f5_str_m = as.matrix(f5_str_m)
f5_str_m[] = as.character(f5_str_m)
f5_str_m[] = paste_omit_na("0203010010",f5_str_m)
plot_floor_heat(f5_str_m,label_name = c("series_name","category_name_3"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code")

#F6
m1 = c(NA,NA,NA,0614,NA,NA,NA,0687,0687,NA,NA,0418,0418,NA,NA,0372,0372,0645,0645)
m2 = c(NA,NA,NA,0614,NA,NA,NA,0687,0687,0552,NA,0418,0418,NA,NA,0372,0372,0645,0645)
m3 = NA
m4 = c(NA,0613,NA,0573,0573,0573,rep(NA,5),0491,0491,0650,0650,NA,NA,0592,0592)
m5 = c(NA,NA,NA,0573,0572,0572,NA,0661,0661,0661,NA,0553,0553,0414,0414,NA,NA,0592,0592)
m6 = NA
m7 = c(0656,NA,NA,0417,0648,0648,NA,0649,0188,0188,NA,0627,0627,0482,0492,0492,NA,NA,NA)
m8 = c(NA,NA,NA,0417,0472,0472,rep(NA,5),0627,0627,0482,0492,0492,NA,0250,0250)
m9 = NA
m10 = c(0519,NA,0430,0430,rep(NA,7),0201,0494,0494,NA,0193,0193,NA,NA)
m11 = c(0519,NA,0430,rep(NA,4),0222,0222,NA,NA,0201,0494,0494,rep(NA,5))
m12 = c(0582,0582,0581,0581,rep(NA,15))
f6_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12)
f6_str_m = as.matrix(f6_str_m)
f6_str_m[] = as.character(f6_str_m)
f6_str_m[] = paste_omit_na("0203010010",f6_str_m)
plot_floor_heat(f6_str_m,label_name = c("series_name","category_name_3"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code")

#F7
m1 = c(NA,NA,0390,rep(NA,7),0307,rep(NA,4))
m2 = c(rep(NA,4),0388,rep(NA,10))
m3 = c(NA,0557,NA,0490,0490,rep(NA,4),0568,0568,rep(NA,4))
m4 = c(rep(NA,3),0317,0490,NA,0319,0319,NA,0514,0202,rep(NA,4))
m5 = NA
m6 = c(0315,NA,NA,0603,0603,NA,0357,0316,NA,0668,0284,0284,0451,NA,0308)
m7 = c(NA,NA,NA,0603,0603,NA,NA,NA,NA,0668,0659,0451,0451,NA,NA)
m8 = NA
m9 = c(rep(NA,6),0560,0560,NA,0644,0282,0311,NA,0312,0660)
f7_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9)
f7_str_m = as.matrix(f7_str_m)
f7_str_m[] = as.character(f7_str_m)
f7_str_m[] = paste_omit_na("0203010010",f7_str_m)
plot_floor_heat(f7_str_m,label_name = c("series_name","category_name_3"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code")

#上海真北商场
#F1
m1 = c(NA,4026,NA,3816,4030,rep(3841,4),rep(NA,19))
m2 = c(NA,4028,rep(NA,26))
m3 = c(NA,4028,NA,4029,4029,4025,4025,rep(NA,21))
m4 = c(NA,NA,NA,4029,4029,4025,4025,NA,NA,NA,4435,4435,NA,NA,rep(4047,5),4143,4143,NA,4531,4531,4539,4539,NA,NA)
m5 = c(3899,3899,NA,NA,4029,4038,4038,rep(NA,3),4435,4435,4204,NA,4047,rep(NA,5),4143,NA,4531,4531,NA,NA,4437,NA)
m6 = c(rep(NA,26),4437,NA)
m7 = c(3810,3810,NA,NA,3812,4189,NA,NA,4489,4489,NA,4512,4512,4486,4486,rep(NA,5),4398,NA,NA,4508,NA,NA,4437,NA)
m8 = c(NA,3810,NA,3866,3812,4255,NA,NA,4489,NA,NA,4512,4512,4486,4486,4371,4371,4341,4341,NA,4398,NA,NA,4508,4508,NA,3770,NA)
m9 = c(NA,3973,NA,3866,3866,4072,4072,NA,4413,3821,NA,4342,4342,3820,3820,4371,4371,4341,4341,NA,4398,rep(4040,4),NA,3770,3770)
m10= c(3973,3973,NA,3928,3928,3928,3929,3929,NA,4413,3821,4342,4342,3820,3820,4371,4371,4341,4341,4341,NA,rep(4040,4),rep(NA,3))
m11= c(4027,4027,NA,4306,4306,4306,NA,3929,rep(NA,18),4421,NA)
m12= c(4027,4027,NA,4306,4306,4306,4052,4052,NA,NA,3953,4381,4381,3819,3819,4537,4537,3879,3879,NA,4373,4373,4373,4502,4502,NA,NA,NA)
m13= c(NA,NA,NA,3889,3886,3886,4052,4052,NA,3953,3953,3953,4381,NA,3819,4537,4537,3879,3879,NA,NA,4373,NA,NA,4502,NA,4321,NA)
m14= c(3884,3884,NA,NA,3886,rep(NA,4),4343,4343,4343,NA,NA,4344,4392,4392,3879,3879,NA,NA,4340,NA,NA,4384,NA,4321,4321)
m15= c(3884,3884,rep(NA,10),4343,4344,4344,4392,4392,3879,3879,NA,4340,4340,4340,4384,4384,NA,4322,NA)
m16= c(3884,3884,rep(NA,6),4236,rep(NA,17),4322,NA)
m17= c(3884,3884,3884,rep(NA,5),4236,4236,NA,NA,4490,4490,4490,4425,4425,4461,4461,NA,4522,4522,4522,4534,4534,NA,4346,4346)
m18= c(rep(NA,8),3325,4236,4236,NA,NA,4490,4490,4425,4425,4461,4461,NA,4522,4522,4522,4534,4534,NA,3880,NA)
m19= c(rep(NA,8),3325,3325,4236,4236,NA,NA,4490,4425,4425,4461,4461,NA,4345,4345,4345,4529,4529,NA,3880,3880)
m20= c(rep(NA,7),rep(3325,4),4236,4236,NA,NA,4425,4425,4461,4461,NA,4345,4345,4345,4529,4529,NA,3881,NA)
m21= c(rep(NA,5),4227,3324,rep(NA,21))
m22= c(rep(NA,4),4227,NA,3324,3324,NA,4037,4254,4254,4078,NA,4288,4288,4069,4069,NA,NA,3849,3849,NA,NA,NA,3848,3848,NA)
m23= c(rep(NA,6),3324,NA,NA,4037,4254,4254,NA,NA,NA,NA,4069,4069,NA,NA,3849,3849,NA,NA,NA,NA,3848,NA)
m24= c(rep(NA,10),4254,4254,rep(NA,4),4069,NA,NA,NA,3849,3849,NA,NA,NA,NA,3848,NA)
zbf1_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14,m15,m16,m17,m18,m19,m20,m21,m22,m23,m24)
zbf1_str_m = as.matrix(zbf1_str_m)
zbf1_str_m[] = as.character(zbf1_str_m)
zbf1_str_m[] = paste_omit_na("020041000",zbf1_str_m)
plot_floor_heat(zbf1_str_m,label_name = c("series_name"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code",sale_data_stall_sum_list[["shanghaizhenbei"]])

#上海真北商场
#F2
m1 = c(NA,4031,NA,4039,4039,rep(NA,23))
m2 = c(rep(3926,2),NA,rep(3847,2),rep(NA,23))
m3 = c(NA,NA,NA,3915,3915,rep(NA,23))
m4 = c(3011,3011,NA,4033,4034,rep(NA,23))
m5 = c(2988,2988,NA,4036,4035,NA,NA,4335,NA,NA,4524,4524,NA,NA,4222,4222,4222,4511,4511,4511,NA,4393,4393,NA,NA,4367,NA,NA)
m6 = c(4032,4032,NA,4036,4035,NA,NA,4335,4335,NA,4524,4524,4524,NA,4222,rep(NA,4),4511,NA,4393,4393,rep(NA,5))
m7 = c(NA,4032,NA,4036,4035,rep(NA,21),4366,NA)
m8 = c(rep(NA,3),4036,4035,rep(NA,21),4517,NA)
m9 = c(3933,3933,NA,NA,4035,NA,NA,NA,4420,4420,4420,NA,NA,4347,4347,rep(NA,5),4513,NA,4504,NA,NA,NA,4516,NA)
m10= c(3933,3933,NA,NA,NA,NA,NA,NA,4420,4420,NA,NA,NA,4347,4347,4458,4458,4348,4348,NA,4513,NA,4504,4504,NA,NA,3822,NA)
m11= c(3846,3846,NA,NA,3814,4318,NA,NA,4419,4419,NA,4485,4485,4347,4347,4458,4458,4348,4348,NA,rep(4409,4),NA,NA,3822,NA)
m12= c(NA,3846,NA,NA,3814,4055,NA,NA,4419,4419,NA,4485,4485,4347,4347,4458,4458,4348,4348,NA,rep(4409,4),NA,NA,NA,NA)
m13= c(NA,NA,NA,4053,4053,4055,NA,NA,4419,rep(NA,17),3836,NA)
m14= c(NA,NA,NA,3904,3904,4054,rep(NA,4),4443,4443,NA,4528,4528,4510,4510,4436,4436,NA,4453,4453,4467,4467,NA,NA,4423,NA)
m15= c(4507,4507,NA,3904,3904,3904,4054,NA,NA,4443,4443,4443,NA,NA,4528,4510,4510,4436,4436,NA,4453,NA,4467,4467,NA,NA,4423,4423)
m16= c(4100,4100,NA,4317,4317,3927,3902,NA,NA,4443,4443,4535,NA,NA,4528,4510,4510,4436,4436,NA,4453,NA,4451,4451,NA,NA,4525,4525)
m17= c(4100,4100,NA,3890,3890,3927,3927,3903,NA,4535,4535,4535,NA,4528,4528,4510,4510,4436,4436,NA,4453,4453,4451,4451,NA,NA,4525,NA)
m18= c(4100,4100,NA,3890,3890,3927,3927,3974,NA,NA,4535,rep(NA,15),4525,NA)
m19= c(3845,3845,NA,3890,rep(NA,11),4349,4349,4394,4394,NA,rep(4220,4),NA,NA,4527,NA)
m20= c(3914,3914,rep(NA,13),4349,4349,4394,4394,NA,rep(4220,4),NA,NA,3813,3813)
m21= c(3914,3914,3914,rep(NA,11),4349,4349,4349,4394,4394,NA,rep(4220,3),NA,NA,NA,3813,NA)
m22= c(rep(NA,26),3972,NA)
m23= c(rep(NA,7),3889,NA,NA,3842,4329,4329,4329,NA,3932,4422,3930,4429,NA,3888,4465,4465,NA,NA,3898,3972,NA)
m24= c(rep(NA,10),3842,4329,4329,4329,NA,3932,4422,3930,4429,NA,3888,4465,NA,NA,NA,3898,3972,NA)
m25= c(rep(NA,11),4329,NA,4329,rep(NA,7),4465,4465,NA,NA,3898,3898,NA)
zbf2_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14,m15,m16,m17,m18,m19,m20,m21,m22,m23,m24,m25)
zbf2_str_m = as.matrix(zbf2_str_m)
zbf2_str_m[] = as.character(zbf2_str_m)
zbf2_str_m[] = paste_omit_na("020041000",zbf2_str_m)
plot_floor_heat(zbf2_str_m,label_name = c("series_name"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code",sale_data_stall_sum_list[["shanghaizhenbei"]])

#上海真北商场
#F2
m1 = c(NA,4031,NA,4039,4039,rep(3841,23))
m2 = c(rep(3926,2),NA,rep(3847,2),rep(NA,23))
m3 = c(NA,NA,NA,3915,3915,rep(NA,23))
m4 = c(3868,3868,NA,4033,4034,rep(NA,23))
m5 = c(3891,3891,NA,4036,4035,NA,NA,4335,NA,NA,4524,4524,NA,NA,4222,4222,4222,4511,4511,4511,NA,4393,4393,NA,NA,4367,NA,NA)
m6 = c(4032,4032,NA,4036,4035,NA,NA,4335,4335,NA,4524,4524,4524,NA,4222,rep(NA,4),4511,NA,4393,4393,rep(NA,5))
m7 = c(NA,4032,NA,4036,4035,rep(NA,21),4366,NA)
m8 = c(rep(NA,3),4036,4035,rep(NA,21),4517,NA)
m9 = c(3933,3933,NA,NA,4035,NA,NA,NA,4420,4420,4420,NA,NA,4347,4347,rep(NA,5),4513,NA,4504,NA,NA,NA,4516,NA)
m10= c(3933,3933,NA,NA,NA,NA,NA,NA,4420,4420,NA,NA,NA,4347,4347,4458,4458,4348,4348,NA,4513,NA,4504,4504,NA,NA,3822,NA)
m11= c(3846,3846,NA,NA,3814,4318,NA,NA,4419,4419,NA,4485,4485,4347,4347,4458,4458,4348,4348,NA,rep(4409,4),NA,NA,3822,NA)
m12= c(NA,3846,NA,NA,3814,4055,NA,NA,4419,4419,NA,4485,4485,4347,4347,4458,4458,4348,4348,NA,rep(4409,4),NA,NA,NA,NA)
m13= c(NA,NA,NA,4053,4053,4055,NA,NA,4419,rep(NA,17),3836,NA)
m14= c(NA,NA,NA,3904,3904,4054,rep(NA,4),4443,4443,NA,4528,4528,4510,4510,4436,4436,NA,4453,4453,4467,4467,NA,NA,4423,NA)
m15= c(4507,4507,NA,3904,3904,3904,4054,NA,NA,4443,4443,4443,NA,NA,4528,4510,4510,4436,4436,NA,4453,NA,4467,4467,NA,NA,4423,4423)
m16= c(4100,4100,NA,4317,4317,3927,3902,NA,NA,4443,4443,4535,NA,NA,4528,4510,4510,4436,4436,NA,4453,NA,4451,4451,NA,NA,4525,4525)
m17= c(4100,4100,NA,3890,3890,3927,3927,3903,NA,4535,4535,4535,NA,4528,4528,4510,4510,4436,4436,NA,4453,4453,4451,4451,NA,NA,4525,NA)
m18= c(4100,4100,NA,3890,3890,3927,3927,3974,NA,NA,4535,rep(NA,15),4525,NA)
m19= c(3845,3845,NA,3890,rep(NA,11),4349,4349,4394,4394,NA,rep(4220,4),NA,NA,4527,NA)
m20= c(3914,3914,rep(NA,13),4349,4349,4394,4394,NA,rep(4220,4),NA,NA,3813,3813)
m21= c(3914,3914,3914,rep(NA,11),4349,4349,4349,4394,4394,NA,rep(4220,3),NA,NA,NA,3813,NA)
m22= c(rep(NA,26),3972,NA)
m23= c(rep(NA,7),3889,NA,NA,3842,4329,4329,4329,NA,3932,4422,3930,4429,NA,3888,4465,4465,NA,NA,3898,3972,NA)
m24= c(rep(NA,10),3842,4329,4329,4329,NA,3932,4422,3930,4429,NA,3888,4465,NA,NA,NA,3898,3972,NA)
m25= c(rep(NA,11),4329,NA,4329,rep(NA,7),4465,4465,NA,NA,3898,3898,NA)
zbf2_str_m = rbind(m1,m2,m3,m4,m5,m6,m7,m8,m9,m10,m11,m12,m13,m14,m15,m16,m17,m18,m19,m20,m21,m22,m23,m24,m25)
zbf2_str_m = as.matrix(zbf2_str_m)
zbf2_str_m[] = as.character(zbf2_str_m)
zbf2_str_m[] = paste_omit_na("020041000",zbf2_str_m)
plot_floor_heat(zbf2_str_m,label_name = c("series_name"),value_name = "saleperareaperdurationinterval",filter_col = "contract_code",sale_data_stall_sum_list[["shanghaizhenbei"]])

# f1_value_m_melt <- ddply(f1_value_m_melt, .(variable), .fun = transform,rescale = rescale(value))
p <- ggplot(f1_value_m_melt, aes(variable,rownum)) + geom_tile(aes(fill = (value)),colour = "white") + scale_fill_gradient(low = "white",high = "purple")+geom_text(aes(label=f1_cat_m_melt$value),angle = 45)
p

#plot specific shop trend in different category 
#floor 1
shop_A8125 = sale_data_month_booth_sum_recent[str_detect(BOOTH_NAME,"A8125"),][order(month_id),]
shop_A8125_sale = ggplot(data = shop_A8125,aes(x = month_id,y = sale))
shop_A8125_sale + geom_bar(stat = "identity",aes(fill = str_sub(shop_name,1,4))) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = str_sub(shop_name,1,4)))

shop_A8169 = sale_data_month_booth_sum_recent[str_detect(house_no,"A8169"),][order(month_id),]
shop_A8169_sale = ggplot(data = shop_A8169,aes(x = month_id,y = sale))
shop_A8169_sale + geom_bar(stat = "identity",aes(fill = str_sub(shop_name,1,2))) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = str_sub(shop_name,1,2)))

shop_A8130 = sale_data_month_booth_sum_recent[str_detect(house_no,"A8130"),][order(month_id),]
shop_A8130_sale = ggplot(data = shop_A8130,aes(x = month_id,y = sale))
shop_A8130_sale + geom_bar(stat = "identity",aes(fill = str_sub(shop_name,1,2))) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = str_sub(shop_name,1,2)))

shop_A8053 = sale_data_month_booth_sum_recent[str_detect(house_no,"A8053"),][order(month_id),]
shop_A8053_sale = ggplot(data = shop_A8053,aes(x = month_id,y = sale))
shop_A8053_sale + geom_bar(stat = "identity",aes(fill = str_sub(shop_name,1,10))) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = str_sub(shop_name,1,10)))

shop_A8015 = sale_data_month_booth_sum_recent[str_detect(house_no,"A8015"),][order(month_id),]
shop_A8015_sale = ggplot(data = shop_A8015,aes(x = month_id,y = sale))
shop_A8015_sale + geom_bar(stat = "identity",aes(fill = str_sub(shop_name,1,4))) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = str_sub(shop_name,1,4)))

shop_name_list = c("A8125","A8130","A8053","A8015","A8169")
first_level = rbindlist(list(shop_A8125,shop_A8169,shop_A8130,shop_A8053,shop_A8015))
first_level_sale = ggplot(data = first_level,aes(x = month_id,y = sale))
first_level_sale + geom_bar(stat = "identity",mapping = aes(fill = shop_name),position = "stack")

#floor 2
shop_B8170 = sale_data_month_booth_sum_recent[str_detect(house_no,"B8170"),][order(month_id),]
shop_B8170_sale = ggplot(data = shop_B8170,aes(x = month_id,y = sale))
shop_B8170_sale + geom_bar(stat = "identity",aes(fill = shop_name)) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = shop_name))

shop_B8139 = sale_data_month_booth_sum_recent[str_detect(house_no,"B8139"),][order(month_id),]
shop_B8139_sale = ggplot(data = shop_B8139,aes(x = month_id,y = sale))
shop_B8139_sale + geom_bar(stat = "identity",aes(fill = shop_name)) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = shop_name))

shop_B8158 = sale_data_month_booth_sum_recent[str_detect(house_no,"B8158"),][order(month_id),]
shop_B8158_sale = ggplot(data = shop_B8158,aes(x = month_id,y = sale))
shop_B8158_sale + geom_bar(stat = "identity",aes(fill = shop_name)) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = shop_name))

shop_B8007 = sale_data_month_booth_sum_recent[str_detect(house_no,"B8007"),][order(month_id),]
shop_B8007_sale = ggplot(data = shop_B8007,aes(x = month_id,y = sale))
shop_B8007_sale + geom_bar(stat = "identity",aes(fill = shop_name)) + geom_text(aes(label = sale, vjust = -0.8, hjust = 0.5, color = shop_name))

second_level = rbindlist(list(shop_B8170,shop_B8139,shop_B8158,shop_B8007))
second_level_sale = ggplot(data = second_level,aes(x = month_id,y = sale))
second_level_sale + geom_bar(stat = "identity",mapping = aes(fill = shop_name),position = "stack")


#ggplot cat2 sums in bar chart using ggplot2
#change the order of the bar
sale_data_month_cat2_sum <- within(sale_data_month_cat2_sum, 
                                      cont_cat2_name <- factor(cont_cat2_name,levels = cont_cat2_name))

ggplot(data = sale_data_month_cat2_sum,aes(cont_cat2_name))+geom_bar(aes(weight = sale))
bar_graph_cat2 = ggplot(data = sale_data_month_cat2_sum,aes(x = cont_cat2_name,y = sale,fill = cont_cat2_name))+geom_bar(stat = "identity",show.legend = FALSE) + geom_text(aes(label = cont_cat2_name, vjust = -0.8, hjust = 0.5, color = cont_cat2_name), show.legend = FALSE)
# g + theme(legend.position="top")
# g + guides(cont_cat2_name=FALSE) 
bar_graph_cat2 + theme(axis.title.x=element_blank(),axis.text.x=element_blank())

g = ggplot(data = sale_data_month_cat1_sum,aes(x = cont_cat1_name,fill = cont_cat1_name)) + geom_bar(aes(weight = sale))
g + scale_x_discrete (limits = sale_data_month_cat1_sum[order(sale_data_month_cat1_sum$sale,decreasing = TRUE)]$cont_cat1_name)

ggplot(data=sale_data_month_cat1,aes(x=as.numeric(as.factor(month_id)), y=sale)) + geom_line(aes(color = cont_cat1_name))    
# ggplot(data=sale_data_month_cat1[complete.cases(sale_data_month_cat1),][1:100,], aes(x=month_id, y=sale, group = cont_cat1_name,colour = as.factor(cont_cat1_name)))    
temp = sale_data_month_cat1[complete.cases(sale_data_month_cat1),]
temp$month_id = as.numeric(as.factor(temp$month_id))
ggplot(data=temp, aes(x=month_id, y=sale)) + geom_line(aes(color = cont_cat1_name)) 

ggplot(data=sale_data_month_cat2,aes(x=as.numeric(as.factor(month_id)), y=sale)) + geom_line(aes(color = cont_cat2_name))

plot(x = sale_data_wide_cat1$month_id,sale_data_wide_cat1$家具)
plot(sale_data_wide_cat1$家具,type = 'b')

sale_data_wide_cat1$month_id = factor(sale_data_wide_cat1$month_id)
sale_data_wide_cat1$新业态 = NULL
sale_data_wide_cat1 = sale_data_wide_cat1[complete.cases(sale_data_wide_cat1),]
plot(0,0,xlim = c(0,41),ylim = c(1000000,50000000),type = "n")
n = length(colnames(sale_data_wide_cat1))-1
cl <- rainbow(n)
for (i in 1:n){
  print(color.id(cl[i]))
  print(colnames(sale_data_wide_cat1)[i+1])
  lines(as.numeric(sale_data_wide_cat1[[1]]),sale_data_wide_cat1[[i+1]],col = cl[i],type = 'b')
}

plot(sale_data_wide_cat1[[1]],sale_data_wide_cat1[[2]])
library(lattice)
xyplot(sale~month_id,type=c('l','p'),groups= cont_cat1_name,data=sale_data_month_cat1,auto.key=T)

x = str_extract(sale_data_raw$booth_desc,'([\u4e00-\u9fa5]|\\d)楼')

plot()
abline(v=(seq(0,20,25)), col="lightgray", lty="dotted")
abline(h=(seq(0,30,25)), col="lightgray", lty="dotted")

m = matrix(c(0,rep(10,958),100),nrow = 24,ncol = 40)
m = as.data.frame(m)
m$rownum = 1:nrow(m)
m_melt = melt(m,id.vars = "rownum")
ggplot(m_melt, aes(variable,rownum)) + geom_tile(aes(fill = value),colour = "white")+ scale_fill_gradient(low = "white",high = "purple")
