# -*- coding: utf-8 -*-
__author__ = 'dengzheng'
"""     
        CREATE
        TABLE `app.app_wil_hot_sku_all`
        (
             `dc_id` string COMMENT '配送中心编码',
             `wh_group_id` string COMMENT '仓群编码',
             `current_source_store_id` string COMMENT '爆品克隆仓来源仓',
             `hot_sku_target_store_id` string COMMENT '爆品克隆仓目的仓',
             `hot_sku_out_store_rate` float COMMENT '爆品出库比例',
             `sku_id` string COMMENT '商品编码',
             `future_source_store_id` string COMMENT '调整后的仓编码',
             `white_flag` int  COMMENT '供应商简码标志'   
        )
        COMMENT '爆品仓白名单' PARTITIONED BY
        (
            `dt` string,
            `pid` string,
            `ts` string
        )
        STORED AS ORC tblproperties
        (
            'orc.compress' = 'SNAPPY'
        )
"""
import sys
import datetime as dt
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

p = [["date", "输入数据的产生日期", "--date  输入数据的产生日期，选填，默认是今天，格式为yyyy-mm-dd", False, str(dt.date.today())],
     ["pid", "方案编号", "--pid  输入方案编号", False, None],
     ["ts", "方案时间戳", "--ts  输入方案时间戳", False, None],
     ["dc_id", "配送中心编号", "--dc_id  输入配送中心id", False, None]
     ]


def main():
    sc = SparkContext(conf=SparkConf().setAppName("wil_hot_sku_calc_online"))
    hc = HiveContext(sc)
    sc.addPyFile(sys.argv[1])
    from core.common import common
    param = common.init_params(sys.argv, p)
    # date, pid, ts, dc_id = param["date"], param["pid"], param["ts"], param["dc_id"]
    date = '2018-10-19'
    pid = '201810190031002'
    ts = '1539915102918'
    today = dt.datetime.strptime(date, "%Y-%m-%d").date()
    someday = dt.datetime.strptime('2018-10-24', "%Y-%m-%d").date()
    yesterday = today - dt.timedelta(1)
    three_days_ago = today - dt.timedelta(3)
    # thirty_days = today - dt.timedelta(30)
    # 2.1.关联订单数据
    # 未来在表a, b中要加上dc_id字段, 并在join时使用dc_id作关联
    sql_hot_sku_data_all = """
        select a.sku_id as sku_id
               ,a.future_source_store_id as future_source_store_id
               ,to_date(b.out_wh_tm) as sub_dt
               ,b.ord_id as ord_id
               ,b.sale_qtty as sale_qtty
        from
            (select sku_id
                   ,current_source_store_id
                   ,future_source_store_id
            from app.app_wil_hot_sku_all
            where dt='"""+str(today)+"""'
            and pid='""" + pid + """'
            and ts='""" + ts + """'
            ) a
        left join
            (select sku_id
                   ,store_id
                   ,parent_sale_ord_id as ord_id
                   ,sale_qtty
                   ,out_wh_tm
            from app.app_wil_hot_sale_store_sku_ord_sale
            where dt = '""" + str(someday) + """'
                and sale_qtty >= 0
               
            ) b
        on a.current_source_store_id = b.store_id
            and a.sku_id = b.sku_id
        where  to_date(out_wh_tm) is not null
    """
    hc.sql(sql_hot_sku_data_all).createOrReplaceTempView("tb_sku_data_all")

    # 2.2.得到sku的打标信息
    # 当前来源仓有多个的时候, 只有有可能被选入, 则认为这个sku可以被选品选出: white_flag = 1
    # 当前来源仓有多个的时候, 只要在set1出现过, 则认为这个sku在爆品仓中不动: unset1_flag = 0

    sql_hot_sku_all_flag ="""
        select sku_id
                ,future_source_store_id
                ,max(white_flag) as white_flag
                ,min(unset1_flag) as unset1_flag
        from
            (select sku_id
                   ,white_flag
                   ,future_source_store_id
                   ,case when future_source_store_id <> hot_sku_target_store_id then 1 else 0 end as unset1_flag
            from app.app_wil_hot_sku_all
            ) a
        group by sku_id,future_source_store_id

    """
    hc.sql(sql_hot_sku_all_flag).createOrReplaceTempView("tb_sku_all_flag")

    # 订单维度设置一个ord-sku 的权重值ord_weight, 一订单中如果含有3个sku, 这三个sku的这个订单对其总订单量的贡献为1/3)
    sql_hot_ord_cnt = """
        select aa.sub_dt
           ,aa.sku_id
           ,round(sum(case when aa.ord_weight is not null then aa.ord_weight else 1.0 end),2) as ord_cnt
        from
        (select a.*
               ,b.ord_weight
        from tb_sku_data_all a
        left join
            (
            select ord_id
                    ,cast(1/count(distinct sku_id) as float) as ord_weight
            from tb_sku_data_all
            group by ord_id
            ) b
        on a.ord_id = b.ord_id
        ) aa
    group by sub_dt,sku_id
    """
    hc.sql(sql_hot_ord_cnt).createOrReplaceTempView("tb_hot_ord_cnt")

    # -- 设目标是a, 则每天选出min(0.15 + a, 1)比例订单
    # --不同的sku根据其
    # future_source_store_id
    # 不同而有不同的
    # hot_sku_out_store_rate
    # 然后分别计算, 需要分量的比例
    # ---------- 如果分开计算, 相当于对订单进行了截断, 计算值相同, 分开计算的结果也不会和和在一起的结果一致
    # ---------- 所以需要实际设置是否是两个不同值判断记否要分开计算, 如果只有一个值, 就和在一起走另一个选品算法
    sql_sku_default_ratio = """
        select future_source_store_id
           ,round(avg(hot_sku_out_store_rate),2) as default_ratio
        from app.app_wil_hot_sku_all
        where future_source_store_id != hot_sku_target_store_id and future_source_store_id is not null
        group by future_source_store_id
    """
    value = hc.sql(sql_sku_default_ratio).rdd.map(lambda x: map(lambda a: a, x)).filter(lambda x: x[1] != None).collect()
    if len(value) == 2 and value[0][1] != value[1][1]:
        v_future_source_store_id_1 = value[0][0]
        v_future_source_store_id_2 = value[1][0]
        v_param_1 = min(0.15 + value[0][1], 1)
        v_param_2 =min(0.15 + value[1][1], 1)
        #     判断 不同的 future_source_store_id 是否对应不同的 default_ratio , 如果是 则 分别得到 两种 default_ratio: 并通过<<算法1>>分别选品
        #     示例sql对应参数为:
        #                         38 对应 0.50  (背景一共18960个sku)
        #                         39 对应 0.50  (背景一共2710个sku)
        # 2.4.2.1 算法1(两仓分别选品)
        sql_hot_sku_list_1 = """
               select c.sub_dt,
                       c.sku_id
                (
                select e.sub_dt
                ,e.sku_id
                from
                (select a.sub_dt
                       ,a.sku_id
                       ,cast(SUM(a.ord_cnt * b.canchoose_flag) OVER (PARTITION BY a.sub_dt ORDER BY a.ord_cnt desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as float) AS CumulativeTotal
                       ,cast(SUM(a.ord_cnt * b.background_flag) OVER (PARTITION BY a.sub_dt) * CAST ('""" + str(v_param_1) + """' as float) as float) AS TotalOrd
                from tb_hot_ord_cnt a
                left join
                    (select sku_id
                            ,case when white_flag = 1 and unset1_flag = 1 then 1 else 0 end as canchoose_flag
                            ,case when future_source_store_id = '"""+str(v_future_source_store_id_1)+"""' then 1 else 0 end as background_flag
                    from tb_sku_all_flag
                    ) b
                on a.sku_id = b.sku_id
                ) e
                where e.CumulativeTotal <= e.TotalOrd
                ) c 
                join
                (
                    select distinct sku_id
                    from 
                    tb_sku_all_flag 
                    where canchoose_flag = 1 
                )d
                on c.sku_id = d.sku_id          
                
        """
        hc.sql(sql_hot_sku_list_1).createOrReplaceTempView("tb_hot_sku_list_1")
        sql_select_result_1 = """
            select a.sku_id
               ,sum(a.cnt) as re_times
            from
            (select sku_id
                    ,1 as cnt
            from tb_hot_sku_list_1
            union all
            select sku_id
                   ,4 as cnt
            from tb_hot_sku_list_1
            where sub_dt between '""" + str(three_days_ago)+"""' and '""" + str(yesterday)+"""'
            ) a
          group by a.sku_id
          having re_times > 18
        """
        hc.sql(sql_select_result_1).createOrReplaceTempView("tb_select_result_1")
        sql_hot_sku_list_2 = """
        select c.sub_dt,
                       c.sku_id
                       FROM 
                (
                select a.sub_dt
                ,a.sku_id
                from
                (select a.sub_dt
                       ,a.sku_id
                       ,cast(SUM(a.ord_cnt * b.canchoose_flag) OVER (PARTITION BY a.sub_dt ORDER BY a.ord_cnt desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as float) AS CumulativeTotal
                       ,cast(SUM(a.ord_cnt * b.background_flag) OVER (PARTITION BY a.sub_dt) * CAST ('""" + str(v_param_2) + """' as float) as float) AS TotalOrd
                from tb_hot_ord_cnt a
                left join
                    (select sku_id
                            ,case when white_flag = 1 and unset1_flag = 1 then 1 else 0 end as canchoose_flag
                            ,case when future_source_store_id = '"""+str(v_future_source_store_id_2)+"""' then 1 else 0 end as background_flag
                    from tb_sku_all_flag
                    ) b
                on a.sku_id = b.sku_id
                ) a
                where a.CumulativeTotal <= a.TotalOrd
                ) c 
                join
                (
                    select distinct sku_id
                    from 
                    tb_sku_all_flag 
                    where canchoose_flag = 1 
                )d
                on c.sku_id = d.sku_id          
                  
                    
            """
        hc.sql(sql_hot_sku_list_2).createOrReplaceTempView("tb_hot_sku_list_2")

        sql_select_result_2 = """
                select a.sku_id
                   ,sum(a.cnt) as re_times
                from
                (select sku_id
                        ,1 as cnt
                from tb_hot_sku_list_2
                union all
                select sku_id
                       ,4 as cnt
                from tb_hot_sku_list_2
                where sub_dt between '""" + str(three_days_ago) + """' and '""" + str(yesterday) + """'
                ) a
              group by a.sku_id
              having re_times > 16
            """
        hc.sql(sql_select_result_2).createOrReplaceTempView("tb_select_result_2")
        #
        # sql_result = """
        #     insert overwrite table dev.dev_ipc_ioa_hot_select_result
        #     select * from tb_select_result_1
        #     union
        #     select * from tb_select_result_2
        # """
        # hc.sql(sql_result)
        # 最终选品结果
        partition = """dt='""" + today + """',pid='""" + pid + """',ts='""" + ts + """'"""
        sql_select_result = """
                        insert overwrite table app.app_wil_hot_sku_selected partition(""" + partition + """)
                        select a.sku_id
                        from 
                       ( select * from tb_select_result_1
                        union
                        select * from tb_select_result_2) a
                    """
        hc.sql(sql_select_result)
    else:
        v_param = min(0.15 + value[0][1], 1)
        sql_hot_sku_list = """
                    select c.sub_dt,
                       c.sku_id
                       FROM 
                (
                select e.sub_dt
                ,e.sku_id
                from
                (select a.sub_dt
                       ,a.sku_id
                       ,cast(SUM(a.ord_cnt * b.canchoose_flag) OVER (PARTITION BY a.sub_dt ORDER BY a.ord_cnt desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as float) AS CumulativeTotal
                       ,cast(SUM(a.ord_cnt) OVER (PARTITION BY a.sub_dt) * CAST ('""" + str(v_param) + """' as float) as float) AS TotalOrd
                from tb_hot_ord_cnt a
                left join
                    (select sku_id
                            ,case when white_flag = 1 and unset1_flag = 1 then 1 else 0 end as canchoose_flag
                    from tb_sku_all_flag
                    ) b
                on a.sku_id = b.sku_id
                ) e
                where e.CumulativeTotal <= e.TotalOrd
                ) c 
                join
                (
                    select distinct sku_id
                    from 
                    tb_sku_all_flag 
                    where canchoose_flag = 1 
                )d
                on c.sku_id = d.sku_id          
            """
        hc.sql(sql_hot_sku_list).createOrReplaceTempView("tb_hot_sku_list")
        partition = """dt='""" + str(today) + """',pid='""" + pid + """',ts='""" + ts + """'"""
        sql_select_result = """
                    insert overwrite table app.app_wil_hot_sku_selected partition(""" + partition +""")
                    select b.sku_id
                    from 
                   ( select a.sku_id
                       ,sum(a.cnt) as re_times
                    from
                    (
                    select sku_id
                            ,1 as cnt
                    from tb_hot_sku_list
                    union all
                    
                    select sku_id
                           ,4 as cnt
                    from tb_hot_sku_list
                    where sub_dt between '""" + str(three_days_ago) + """' and '""" + str(yesterday) + """') a
                  group by a.sku_id
                  having re_times > 0) b
                """
        hc.sql(sql_select_result)

if __name__ == "__main__":
    main()
