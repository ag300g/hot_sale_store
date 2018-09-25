---------------------------------------------
-- table used： 
-- app.app_wil_hot_sale_store_sku_ord_sale
-- app.app_wil_hot_sku_white
-- app.app_wil_hot_sku_ratio_result
-- app.app_wil_hot_sku_selected
---------------------------------------------

---------------------------------------------

hive> desc app.app_wil_hot_sale_store_sku_ord_sale;
OK
sku_id              	string              	sku编码
store_id            	string              	仓库编码
out_wh_tm           	string              	出库时间
item_third_cate_cd  	string              	商品三级分类编码
parent_sale_ord_id  	string              	父订单id
sale_qtty           	string              	商品销量
split_status_cd     	string              	订单是否拆分
dt                  	string
sub_dt              	string

# Partition Information
# col_name            	data_type           	comment

dt                  	string
sub_dt              	string

---------------------------------------------

hive> desc app.app_wil_hot_sku_all;
OK
dc_id               	string              	配送中心编码
wh_group_id         	string              	仓群编码
current_source_store_id	string              	爆品克隆仓来源仓
hot_sku_target_store_id	string              	爆品克隆仓目的仓
hot_sku_out_store_rate	float               	爆品出库比例
sku_id              	string              	商品编码
future_source_store_id	string              	调整后的仓编码

---------------------------------------------

hive> desc app.app_wil_hot_sku_ratio_result;
OK
sku_id              	string              	爆品编码
dc_id               	string              	配送中心编码
hot_sku_target_store_id	string              	爆品仓编码
future_source_store_id	string              	未来的主仓id
hot_sku_ratio       	float               	爆品所占全部仓的比例
dt                  	string

# Partition Information
# col_name            	data_type           	comment

dt                  	string

---------------------------------------------

hive> desc app.app_wil_hot_sku_selected;
OK
sku_id              	string              	爆品编码
dc_id               	string              	配送中心编码
hot_sku_target_store_id	string              	爆品仓编码
future_source_store_id	string              	未来来源主仓id

---------------------------------------------


set hive.cli.print.header=true;

"
hive> desc dev.dev_ipc_ioa_hot_sale_store_table12;
OK
sku_id              	string              	SKU代码
store_id            	int                 	现行仓代码
future_store_id     	int                 	未来仓代码
is_selected         	int                 	是否被选入爆品仓
dt                  	string

# Partition Information
# col_name            	data_type           	comment

dt                  	string
"



drop table if exists dev.dev_ipc_ioa_hot_sale_store_all_sku;
create table dev.dev_ipc_ioa_hot_sale_store_all_sku stored as orc
as
select distinct t.sku_id
	   ,current_source_store_id
	   ,future_source_store_id
	   ,case when b_sku_id is not null then 1 else 0 end as is_selected
from
	(select a.sku_id as sku_id
	       ,a.current_source_store_id as current_source_store_id
	       ,a.future_source_store_id as future_source_store_id
	       ,b.sku_id as b_sku_id
	from app.app_wil_hot_sku_all a
	left join
	app.app_wil_hot_sku_selected b
	on a.sku_id = b.sku_id
	) t


drop table if exists dev.dev_ipc_ioa_hot_sale_store_all_data;
create table dev.dev_ipc_ioa_hot_sale_store_all_data stored as orc
as
select a.sku_id
       ,a.future_store_id
       ,a.is_selected
       ,b.out_wh_tm
       ,b.ord_id
       ,b.sale_qtty
from
	(select sku_id
		   ,store_id
	       ,future_store_id
	       ,is_selected
	from dev.dev_ipc_ioa_hot_sale_store_all_sku
	where dt = '2018-09-18'
	) a
left join
	(select sku_id
	       ,store_id
	       ,out_wh_tm
	       ,parent_sale_ord_id as ord_id
	       ,sale_qtty
	from app.app_wil_hot_sale_store_sku_ord_sale
	where dt = '2018-09-18'
		and sale_qtty >= 0
	) b
on a.store_id = b.store_id
	and a.sku_id = b.sku_id;





drop table if exists dev.dev_ipc_ioa_hot_sale_store_verify_choice_ord;
create table dev.dev_ipc_ioa_hot_sale_store_verify_choice_ord stored as orc
as
select a.ord_id
       ,case when (a.f1=1 and a.f2=2) or (a.f1=2 and a.f2=2 and (a.g1=4 or (a.g2=1 and a.g3=2))) then 0 else 1 end as choice_flag1
       ,case when (a.f1=1 and a.f2=1) or (a.f1=2 and a.f2=1) or (a.f1=2 and a.f2=2 and (a.g1=2 or (a.g2=2 and a.g3=1))) then 1 else 0 end as choice_flag2
from
	(select ord_id
	       ,count(distinct future_store_id) as f1
	       ,count(distinct is_selected) as f2
	       ,count(distinct future_store_id,is_selected) as g1
	       ,count(distinct (case when is_selected = 1 then future_store_id else null end)) as g2
   	       ,count(distinct (case when is_selected = 0 then future_store_id else null end)) as g3
	from dev.dev_ipc_ioa_hot_sale_store_all_data
	group by ord_id
	) a




drop table if exists dev.dev_ipc_ioa_hot_sale_store_verify_result;
create table dev.dev_ipc_ioa_hot_sale_store_verify_result stored as orc
as
select a.sku_id
       ,sum(case when b.choice_flag2 = 1 then a.sale_qtty else 0 end) as sale_in_hot
       ,sum(a.sale_qtty) as sale_total
from dev.dev_ipc_ioa_hot_sale_store_verify a
left join
	dev.dev_ipc_ioa_hot_sale_store_verify_choice_ord b
on a.ord_id = b.ord_id
where a.is_selected = 1
group by a.sku_id;