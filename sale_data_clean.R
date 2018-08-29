source("~/Rfile/R_hive.R") ##!!need to include that file
#is_coupon,coupon_id,coupon_amt
buy_data_sql = "select date_id,ordr_date,crdt,prod_name,mall_name,shop_id,shop_name,contract_code,house_no,booth_id,booth_desc,cnt_cat1_num,cnt_cat2_num,cnt_cat3_num,is_coupon,partner_name,cont_cat1_name,cont_cat2_name,cont_cat3_name,act_amt,open_id,mobile,name,is_coupon,coupon_id,coupon_amt from dl.fct_ordr where mall_name like '%上海金桥商场%' and 
((type = 'OMS' and ordr_status ='Y') or (type != 'OMS' and trade_amt > 0))"
buy_data_raw = read_data_hive_general(buy_data_sql)

is_emp_sql = "select * from dl.dl_channel_umid_openid_market where isemp = 'Y'"
is_emp_raw = read_data_hive_general(is_emp_sql)

is_emp_sql2 = "select * from test.myusers"
is_emp_raw2 = read_data_hive_general(is_emp_sql2)
colnames(is_emp_raw2)[1] = "mobile"

check_inner_purchase = join(buy_data_raw,is_emp_raw2,by = "mobile")
