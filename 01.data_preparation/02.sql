------------------------------------------
-- verify_data upload --
------------------------------------------
drop table if exists dev.dev_ipc_ioa_hot_sale_store_sku_all;
CREATE TABLE dev.dev_ipc_ioa_hot_sale_store_sku_all (
    sku_id string comment 'SKU代码',
    current_source_store_id int comment '现行仓代码',
    future_source_store_id int comment '未来仓代码',
    target_store_id int comment '爆品仓代码',
    default_ratio float comment '默认比例')
COMMENT '爆品仓候选所有sku集合'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/cmo_ipc/michaelliu/hot_sale_store_20180905/sku_all_20180930.csv' OVERWRITE INTO TABLE dev.dev_ipc_ioa_hot_sale_store_sku_all PARTITION (dt='2018-09-30');



drop table if exists dev.dev_ipc_ioa_hot_sale_store_sku_selected;
CREATE TABLE dev.dev_ipc_ioa_hot_sale_store_sku_selected (
    sku_id string comment 'SKU代码',
    dc_id int comment '配送中心代码',
    future_store_id int comment '未来仓代码',
    target_store_id int comment '爆品仓代码')
COMMENT '爆品仓sku'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");
LOAD DATA LOCAL INPATH '/home/cmo_ipc/michaelliu/hot_sale_store_20180905/selected_20180930.csv' OVERWRITE INTO TABLE dev.dev_ipc_ioa_hot_sale_store_sku_selected PARTITION (dt='2018-09-30');


------------------------------------------
--- 开始验证 ---
------------------------------------------

1. 选出的sku是否是sku_all的子集

drop table if exists dev.dev_ipc_ioa_hot_sku_all;
create table dev.dev_ipc_ioa_hot_sku_all stored as orc
as
select distinct t.sku_id as sku_id
   ,t.current_source_store_id as store_id
   ,t.future_source_store_id as future_store_id
   ,case when t.b_sku_id is not null then 1 else 0 end as is_selected
from
(select a.sku_id as sku_id
       ,a.current_source_store_id as current_source_store_id
       ,a.future_source_store_id as future_source_store_id
       ,b.sku_id as b_sku_id
from dev.dev_ipc_ioa_hot_sale_store_sku_all a
left join
dev.dev_ipc_ioa_hot_sale_store_sku_selected b
on a.sku_id = b.sku_id
) t


sku_all的总个数: 21861
select count(distinct sku_id) from dev.dev_ipc_ioa_hot_sale_store_sku_all;

select_sku的总个数: 219
select count(distinct sku_id) from dev.dev_ipc_ioa_hot_sale_store_sku_selected;

21861中与多少个选中的: 219
select count(distinct sku_id) from dev.dev_ipc_ioa_hot_sku_all where is_selected = 1;






2. 得到最终结果


--- sku_all表
drop table if exists dev.dev_ipc_ioa_hot_data_all;
create table dev.dev_ipc_ioa_hot_data_all stored as orc
as
select a.sku_id as sku_id
       ,a.future_store_id as future_store_id
       ,a.is_selected as is_selected
       ,b.out_wh_tm as out_wh_tm
       ,b.ord_id as ord_id
       ,b.sale_qtty as sale_qtty
       ,b.sub_dt as sub_dt
from
	(select sku_id
		   ,store_id
	       ,future_store_id
	       ,is_selected
	from dev.dev_ipc_ioa_hot_sku_all
	) a
left join
	(select sku_id
	       ,store_id
	       ,out_wh_tm
	       ,parent_sale_ord_id as ord_id
	       ,sale_qtty
	       ,sub_dt
	from app.app_wil_hot_sale_store_sku_ord_sale
	where dt = sysdate(-1)
		and sale_qtty >= 0
	) b
on a.store_id = b.store_id
	and a.sku_id = b.sku_id;




--- 标记订单
drop table if exists dev.dev_ipc_ioa_hot_choice_ord;
create table dev.dev_ipc_ioa_hot_choice_ord stored as orc
as
select a.ord_id
       --,case when (a.f1=1 and a.f2=2) or (a.f1=2 and a.f2=2 and (a.g1=4 or (a.g2=1 and a.g3=2))) then 0 else 1 end as choice_flag1
       ,case when (a.f1=1 and a.f2=1) or (a.f1=2 and a.f2=1) or (a.f1=2 and a.f2=2 and (a.g1=2 or (a.g2=2 and a.g3=1))) then 1 else 0 end as choice_flag2
from
	(select ord_id
	       ,count(distinct future_store_id) as f1
	       ,count(distinct is_selected) as f2
	       ,count(distinct future_store_id,is_selected) as g1
	       ,count(distinct (case when is_selected = 1 then future_store_id else null end)) as g2
   	       ,count(distinct (case when is_selected = 0 then future_store_id else null end)) as g3
	from dev.dev_ipc_ioa_hot_data_all
	group by ord_id
	) a;

--- 汇总销量得出初版结果
drop table if exists dev.dev_ipc_ioa_hot_result;
create table dev.dev_ipc_ioa_hot_result stored as orc
as
select a.sku_id as sku_id
       ,sum(case when b.choice_flag2 = 1 then a.sale_qtty else 0 end) as sale_in_hot
       ,sum(a.sale_qtty) as sale_total
from dev.dev_ipc_ioa_hot_data_all a
left join
	dev.dev_ipc_ioa_hot_choice_ord b
on a.ord_id = b.ord_id
where a.is_selected = 1
group by a.sku_id;





drop table if exists dev.dev_ipc_ioa_hot_select_sku_info;
create table dev.dev_ipc_ioa_hot_select_sku_info stored as orc
as
select a.sku_id as sku_id
       ,a.dc_id as dc_id
       ,b.target_store_id as hot_sku_target_store_id
       ,b.future_source_store_id as future_source_store_id
       ,b.default_ratio as default_ratio
from dev.dev_ipc_ioa_hot_sale_store_sku_selected a
left join
	(select sku_id
		    --,dc_id
		    ,target_store_id
		    ,future_source_store_id
		    ,avg(default_ratio) as default_ratio

	from dev.dev_ipc_ioa_hot_sale_store_sku_all
	group by sku_id,future_source_store_id,target_store_id
	) b
on a.sku_id = b.sku_id;




drop table if exists dev.dev_ipc_ioa_hot_select_sku_stock;
create table dev.dev_ipc_ioa_hot_select_sku_stock stored as orc
as
select t.sku_id as sku_id
	   ,t.future_store_id as future_store_id
	   ,sum(t.stock) as stock_in_future_store_id   -- 如果sku当前来源仓为多个, 要把库存加起来
from
	(select a.*
		   ,coalesce(b.stock_qtty+b.in_transit_qtty,0) as stock
	from dev.dev_ipc_ioa_hot_sku_all a
	left join
		(select sku_id
			    ,store_id
	            ,max(case when stock_qtty > 0 then stock_qtty else 0 end) as stock_qtty   -- 现货库存
	            ,max(case when in_transit_qtty > 0 then in_transit_qtty else 0 end) as in_transit_qtty  -- 内配在途
	    from gdm.gdm_m08_item_stock_day_sum
	    where dt = sysdate(-1)
	    	and stock_qtty < 9999999
		group by sku_id,store_id
		) b
	on a.sku_id = b.sku_id
		and a.store_id = b.store_id
	) t
where t.is_selected = 1
group by t.sku_id,t.future_store_id;




-- 爆品仓商品最近三天销量数据
drop table if exists dev.dev_ipc_ioa_hot_select_sku_sale;
create table dev.dev_ipc_ioa_hot_select_sku_sale stored as orc
as
select sku_id
	   ,future_store_id
	   ,coalesce(sum(sale_qtty),0) as sale_total_3
from dev.dev_ipc_ioa_hot_data_all
where sub_dt between sysdate(-3) and sysdate(-1)
	and is_selected = 1
group by sku_id,future_store_id;



-- 内配数据
drop table if exists dev.dev_ipc_ioa_hot_select_sku_neipei;
create table dev.dev_ipc_ioa_hot_select_sku_neipei stored as orc
as
select t.sku_id as sku_id
	   ,t.future_store_id as future_store_id
	   ,coalesce(sum(t.neipei_qtty),0) as neipei_qtty
from
	(select a.*
	       ,b.neipei_qtty
	       ,b.inner_delv_ob_id
	from dev.dev_ipc_ioa_hot_sku_all a
	left join
		(select item_sku_id as sku_id
		       ,src_store_id as store_id
		       ,max(send_qty) as neipei_qtty
		       ,inner_delv_ob_id   --一个订单号只取一条数据
		from gdm.gdm_m08_ob_inner_delv_sum
		where dp='ACTIVE'
			and to_date(send_tm) between sysdate(-30) and sysdate(-1)
			and inner_delv_ob_status in ('41','42')
		group by item_sku_id,src_store_id,inner_delv_ob_id
		) b
	on a.sku_id = b.sku_id
		and a.store_id = b.store_id
	) t
where t.is_selected = 1
group by t.sku_id,t.future_store_id;


drop table if exists dev.dev_ipc_ioa_hot_final_result;
create table dev.dev_ipc_ioa_hot_final_result stored as orc
as
select a.sku_id as sku_id
       ,a.dc_id as dc_id
       ,a.hot_sku_target_store_id as hot_sku_target_store_id
       ,a.future_source_store_id as future_source_store_id
       ,coalesce(round((b.sale_in_hot+d.neipei_qtty)/(b.sale_total+d.neipei_qtty),4),a.default_ratio) as hot_sku_ratio
       ,coalesce(c.stock_in_future_store_id,0) as future_store_stock_qtty
       ,coalesce(b.sale_total,0) as sale_total
       ,coalesce(e.sale_total_3,0) as sale_total_3
from dev.dev_ipc_ioa_hot_select_sku_info a
left join dev.dev_ipc_ioa_hot_result b
on a.sku_id = b.sku_id
left join dev.dev_ipc_ioa_hot_select_sku_stock c
on a.sku_id = c.sku_id and a.future_source_store_id = c.future_store_id
left join dev.dev_ipc_ioa_hot_select_sku_neipei d
on a.sku_id = d.sku_id and a.future_source_store_id = d.future_store_id
left join dev.dev_ipc_ioa_hot_select_sku_sale e
on a.sku_id = e.sku_id and a.future_source_store_id = e.future_store_id;


hive -e"
set hive.cli.print.header=true;
select * from dev.dev_ipc_ioa_hot_final_result" > result_verafy_20180930.csv &