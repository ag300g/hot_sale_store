# -*- coding: utf-8 -*-
__author__ = 'dengzheng'
""""
CREATE TABLE if not exists `dev.dev_hot_sale_store_sku_ord_sale`(
    `sku_id` string COMMENT 'sku编码',
    `store_id` string COMMENT '仓库编码',
    `out_wh_tm` string COMMENT '出库时间',
    `item_third_cate_cd` string COMMENT '商品三级分类编码',
    `parent_sale_ord_id` string COMMENT '父订单id',
    `sale_qtty` string COMMENT '商品销量',
    `split_status_cd` string COMMENT '订单是否拆分'
)
COMMENT '订单销量表'
PARTITIONED BY (`dt` string,`sub_dt` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
"""


from pyspark import SparkContext, SparkConf
sc = SparkContext(conf=SparkConf().setAppName("app_wil_hotsku_order"))
import datetime as dt
from pyspark.sql import HiveContext
hc = HiveContext(sc)
hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
# --------------------参数----------------------------
today = dt.datetime.strptime("2018-09-01", '%Y-%m-%d').date()
date_len = 31
# ---------------------------------------------------
# 订单统计的开始日期是当前日期往前推30天
start_date = today - dt.timedelta(date_len)
# 日期+1递推取出30天的历史数据
s_date = start_date
delete_flag_date = today - dt.timedelta(4)
# 订单统计的结束时间是昨天，区间一共是30天
end_date = today - dt.timedelta(1)
# sale_ord_type_cd=0这个是一般订单
# 滚动计算6个月的数据取平均值
while True:
    sql = """ 
            select
                item_sku_id as sku_id,
                store_id,
                out_wh_tm,
                item_third_cate_cd,
                parent_sale_ord_id,
                sale_qtty,
                split_status_cd
            from gdm.gdm_m04_ord_det_sum
            where
                dt >= '""" + str(s_date) + """'
                and substring(out_wh_tm, 1, 10) = '""" + str(s_date) + """'
                and sale_ord_type_cd  = '0'
                and sale_ord_valid_flag = 1
                and sku_valid_flag = '1'    
    """
    hc.sql(sql).repartition(1).createOrReplaceTempView("tb_table")
    partition = "dt='" + str(today) + "',sub_dt='" + str(s_date) + "'"
    table_output = 'dev.dev_hot_sale_store_sku_ord_sale'
    sql_save_sd_result = """
                insert overwrite table """ + table_output + """ partition (""" + partition + """)
                select
                    sku_id,
                    store_id,
                    out_wh_tm,
                    item_third_cate_cd,
                    parent_sale_ord_id,
                    sale_qtty,
                    split_status_cd
                 from tb_table
        """
    hc.sql(sql_save_sd_result)
    s_date = s_date + dt.timedelta(1)
    if str(s_date) > str(end_date):
        break
# 删除很久之前的分区
dts = hc.sql("select distinct dt from dev.dev_hot_sale_store_sku_ord_sale where dt < '" + str(delete_flag_date) + "'")\
    .rdd.map(lambda x: map(lambda a: a, x)).collect()
for dt in dts:
    hc.sql("ALTER TABLE dev.dev_hot_sale_store_sku_ord_sale DROP IF EXISTS PARTITION(dt = '" + str(dt[0]) + "')")




