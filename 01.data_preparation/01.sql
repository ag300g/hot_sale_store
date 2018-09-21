------------------------------------
-- get data from bigdataplatform -- 
------------------------------------

-- veryfy_data upload --
drop table if exists dev.dev_ipc_ioa_hot_sale_store_table12;
CREATE TABLE dev.dev_ipc_ioa_hot_sale_store_table12 (
    sku_id string comment 'SKU代码',
    store_id int comment '现行仓代码',
    future_store_id int comment '未来仓代码',
    is_selected int comment '是否被选入爆品仓')
COMMENT '爆品仓临时表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/cmo_ipc/michaelliu/hot_sale_store_20180905/verify1.csv' OVERWRITE INTO TABLE dev.dev_ipc_ioa_hot_sale_store_table12 PARTITION (dt='2018-09-18');
LOAD DATA LOCAL INPATH '/home/cmo_ipc/michaelliu/hot_sale_store_20180905/verify2.csv' OVERWRITE INTO TABLE dev.dev_ipc_ioa_hot_sale_store_table12 PARTITION (dt='2018-09-19');



-- 获取订单和销量的关系 --
set hive.cli.print.header=true;
select distinct future_store_id
from dev.dev_ipc_ioa_hot_sale_store_verify;



drop table if exists dev.dev_ipc_ioa_hot_sale_store_verify;
create table dev.dev_ipc_ioa_hot_sale_store_verify stored as orc
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
	from dev.dev_ipc_ioa_hot_sale_store_table12
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
	from dev.dev_ipc_ioa_hot_sale_store_verify
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



hive -e"
set hive.cli.print.header=true;
select * from dev.dev_ipc_ioa_hot_sale_store_verify_result;
" > hot_sale_store_verify_result1.csv





 -----------------





drop table if exists dev.dev_ipc_ioa_hot_sale_store_verify1;
create table dev.dev_ipc_ioa_hot_sale_store_verify1 stored as orc
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
	from dev.dev_ipc_ioa_hot_sale_store_table12
	where dt = '2018-09-19'
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




drop table if exists dev.dev_ipc_ioa_hot_sale_store_verify_choice_ord1;
create table dev.dev_ipc_ioa_hot_sale_store_verify_choice_ord1 stored as orc
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
	from dev.dev_ipc_ioa_hot_sale_store_verify1
	group by ord_id
	) a




drop table if exists dev.dev_ipc_ioa_hot_sale_store_verify_result1;
create table dev.dev_ipc_ioa_hot_sale_store_verify_result1 stored as orc
as
select a.sku_id
       ,coalesce(sum(case when b.choice_flag2 = 1 then a.sale_qtty else 0 end),0) as sale_in_hot
       ,coalesce(sum(a.sale_qtty),0) as sale_total
from dev.dev_ipc_ioa_hot_sale_store_verify1 a
left join 
	dev.dev_ipc_ioa_hot_sale_store_verify_choice_ord1 b
on a.ord_id = b.ord_id
where a.is_selected = 1
group by a.sku_id;



hive -e"
set hive.cli.print.header=true;
select * from dev.dev_ipc_ioa_hot_sale_store_verify_result1;
" > hot_sale_store_verify_result2.csv

