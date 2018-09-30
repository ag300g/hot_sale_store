------------------------------------------
-- veryfy_data upload --
------------------------------------------
drop table if exists dev.dev_ipc_ioa_hot_sale_store_sku_all;
CREATE TABLE dev.dev_ipc_ioa_hot_sale_store_sku_all (
    sku_id string comment 'SKU代码',
    store_id int comment '现行仓代码',
    future_store_id int comment '未来仓代码',
    target_store_id int comment '爆品仓代码',
    default_ratio int comment '默认比例')
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
