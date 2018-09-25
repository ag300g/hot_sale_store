drop table if exists app.app_wil_hot_sku_ratio_result;
create table app.app_wil_hot_sku_ratio_result stored as orc
as
with 
sku_all as
	(
	select distinct t.sku_id as sku_id
	   ,t.current_source_store_id as store_id
	   ,t.future_source_store_id as future_store_id
	   ,case when t.b_sku_id is not null then 1 else 0 end as is_selected
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
	),
data_all as
	(
	select a.sku_id as sku_id
	       ,a.future_store_id as future_store_id
	       ,a.is_selected as is_selected
	       ,b.out_wh_tm as out_wh_tm
	       ,b.ord_id as ord_id
	       ,b.sale_qtty as sale_qtty
	from
		(select sku_id
			   ,store_id
		       ,future_store_id
		       ,is_selected
		from sku_all
		) a
	left join
		(select sku_id
		       ,store_id
		       ,out_wh_tm
		       ,parent_sale_ord_id as ord_id
		       ,sale_qtty
		from app.app_wil_hot_sale_store_sku_ord_sale
		where dt = sysdate(-1)
			and sale_qtty >= 0
		) b
	on a.store_id = b.store_id
		and a.sku_id = b.sku_id
	),
choice_ord as
	(
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
		from data_all
		group by ord_id
		) a
	),
result as
	(
	select a.sku_id as sku_id
	       ,sum(case when b.choice_flag2 = 1 then a.sale_qtty else 0 end) as sale_in_hot
	       ,sum(a.sale_qtty) as sale_total
	from data_all a
	left join
		choice_ord b
	on a.ord_id = b.ord_id
	where a.is_selected = 1
	group by a.sku_id
	),
selected_sku_info as
	(
	select a.sku_id as sku_id
	       ,b.dc_id as dc_id
	       ,b.hot_sku_target_store_id as hot_sku_target_store_id
	       ,b.future_source_store_id as future_source_store_id
	       ,b.default_ratio as default_ratio
	from app.app_wil_hot_sku_selected a
	left join 
		(select sku_id
			    ,dc_id
			    ,hot_sku_target_store_id
			    ,future_source_store_id
			    ,avg(hot_sku_out_store_rate) as default_ratio

		from app.app_wil_hot_sku_all
		group by sku_id,dc_id,hot_sku_target_store_id,future_source_store_id
		) b
	on a.sku_id = b.sku_id
	)
select a.sku_id as sku_id
       ,a.dc_id as dc_id
       ,a.hot_sku_target_store_id as hot_sku_target_store_id
       ,a.future_source_store_id as future_source_store_id
       ,case when b.sale_total=0 then a.default_ratio else round(b.sale_in_hot/b.sale_total,4) end as hot_sku_ratio
from selected_sku_info a
left join result b
on a.sku_id = b.sku_id


