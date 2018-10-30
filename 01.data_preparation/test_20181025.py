sc = SparkContext(conf=SparkConf().setAppName("wil_hot_sku_calc_online"))
hc = HiveContext(sc)
sc.addPyFile(sys.argv[1])
from core.common import common

param = common.init_params(sys.argv, p)
date, pid, ts, dc_id = param["date"], param["pid"], param["ts"], param["dc_id"]
date = '2018-10-19'
pid = '201810190031002'
ts = '1539915102918'
today = dt.datetime.strptime(date, "%Y-%m-%d").date()
yesterday = today - dt.timedelta(1)
three_days_ago = today - dt.timedelta(3)
thirty_days = today - dt.timedelta(30)
# 分量算法
# 3.1. 把选品结果整合进division_sku_all和division_data_all中
sql_hot_sku_all_flag = """
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
                 where dt='""" + str(today) + """'
                 and pid='""" + pid + """'
                 and ts='""" + ts + """'
                ) a
            group by sku_id,future_source_store_id

        """
hc.sql(sql_hot_sku_all_flag).createOrReplaceTempView("tb_sku_all_flag")

# 3.1. 把选品结果整合进division_sku_all和division_data_all中,
sql_division_sku_all = """
        select a.*
          ,case when b.sku_id is not null then 1 else 0 end as is_selected
          ,case when c.sku_id is not null then 1 else 0 end as unset1_flag
        from 
        (SELECT  *
         FROM 
           app.app_wil_hot_sku_all
         WHERE 
             dt='""" + str(today) + """'
            and pid='""" + str(pid) + """'
            and ts = '""" + str(ts) + """'
            )
         a
        left join 
        (
        select sku_id
        from   
        app.app_wil_hot_sku_selected
         where  dt='""" + str(today) + """'
            and pid='""" + str(pid) + """'
            and ts = '""" + str(ts) + """') b
        on a.sku_id = b.sku_id
        left join
        (select *
        from tb_sku_all_flag
        where unset1_flag = 1
        ) c
        on a.sku_id = c.sku_id and a.future_source_store_id = c.future_source_store_id"""
hc.sql(sql_division_sku_all).createOrReplaceTempView("tb_division_sku_all")

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
                where dt='""" + str(today) + """'
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
                where dt = '""" + str(yesterday) + """'
                    and sale_qtty >= 0
                ) b
            on a.current_source_store_id = b.store_id
                and a.sku_id = b.sku_id
            where to_date(b.out_wh_tm) is not null
        """
hc.sql(sql_hot_sku_data_all).createOrReplaceTempView("tb_sku_data_all")

sql_division_data_all = """
        select a.*
       ,case when b.sku_id is not null then 1 else 0 end as is_selected
        from tb_sku_data_all a
        left join app.app_wil_hot_sku_selected b
        on a.sku_id = b.sku_id

    """
hc.sql(sql_division_data_all).createOrReplaceTempView("tb_division_data_all")

# -- 3.2. 标记订单
sql_hot_choice_ord = """
        select a.ord_id
           ,case when (a.f1=1 and a.f2=1) or (a.f1=2 and a.f2=1) or (a.f1=2 and a.f2=2 and (a.g1=2 or (a.g2=2 and a.g3=1))) then 1 else 0 end as choice_flag2
        from
        (select ord_id
               ,count(distinct future_source_store_id) as f1
               ,count(distinct is_selected) as f2
               ,count(distinct future_source_store_id, is_selected) as g1
               ,count(distinct (case when is_selected = 1 then future_source_store_id else null end)) as g2
               ,count(distinct (case when is_selected = 0 then future_source_store_id else null end)) as g3
        from tb_division_data_all
        group by ord_id
        ) a

    """
hc.sql(sql_hot_choice_ord).createOrReplaceTempView("tb_hot_choice_ord")

#  3.3. 汇总销量得出初版结果
sql_hot_result = """
            select a.sku_id as sku_id
               ,sum(case when b.choice_flag2 = 1 then a.sale_qtty else 0 end) as sale_in_hot
               ,sum(a.sale_qtty) as sale_total
            from tb_division_data_all a
            left join
            tb_hot_choice_ord b
            on a.ord_id = b.ord_id
            where a.is_selected = 1
            group by a.sku_id
    """
hc.sql(sql_hot_result).createOrReplaceTempView("tb_hot_result")

#  3.4. sku_all的sku+future_source_store_id的库存数据
sql_hot_selected_sku_stock = """
            select t.sku_id as sku_id
               ,t.future_source_store_id as future_source_store_id
               ,t.is_selected as is_selected
               ,t.white_flag as white_flag
               ,t.unset1_flag as unset1_flag
               ,sum(t.stock) as stock_in_future_store_id   -- 如果sku当前来源仓为多个, 要把库存加起来
        from
            (select a.*
                   ,cast(coalesce(b.stock_qtty+b.in_transit_qtty+b.pur_non_into_wh_qtty,0) as int) as stock
            from tb_division_sku_all a
            left join
                (select sku_id
                        ,store_id
                        ,delv_center_num as dc_id
                        ,max(case when stock_qtty > 0 then stock_qtty else 0 end) as stock_qtty   -- 现货库存
                        ,max(case when (in_transit_qtty > 0 and in_transit_qtty < 999999) then in_transit_qtty else 0 end) as in_transit_qtty  -- 内配在途
                        ,max(case when (pur_non_into_wh_qtty > 0 and pur_non_into_wh_qtty <999999) then pur_non_into_wh_qtty else 0 end) as pur_non_into_wh_qtty  --采购未入库
                from gdm.gdm_m08_item_stock_day_sum
                where dt = '""" + str(yesterday) + """'
                    and stock_qtty < 9999999
                group by sku_id,store_id,delv_center_num
                ) b
            on a.sku_id = b.sku_id
                and a.current_source_store_id = b.store_id
                and a.dc_id = b.dc_id
            ) t
        group by t.sku_id,t.future_source_store_id,t.is_selected,t.white_flag,t.unset1_flag
    """
hc.sql(sql_hot_selected_sku_stock).createOrReplaceTempView("tb_hot_selected_sku_stock")

#     3.5. 爆品仓商品最近三天销量数据
sql_hot_select_sku_sale = """
        select sku_id
           ,future_source_store_id
           ,coalesce(sum(sale_qtty), 0) as sale_total_3
    from tb_division_data_all
    where sub_dt between '""" + str(three_days_ago) + """' and '""" + str(yesterday) + """'
        and is_selected = 1
    group by sku_id,future_source_store_id

    """
hc.sql(sql_hot_select_sku_sale).createOrReplaceTempView("tb_hot_select_sku_sale")

# 3.6. 爆品仓商品的内配数据
sql_hot_select_sku_neipei = """

            select t.sku_id as sku_id
               ,t.future_source_store_id as future_source_store_id
               ,coalesce(sum(t.neipei_qtty),0) as neipei_qtty
        from
            (select a.*
                   ,b.neipei_qtty
                   ,b.inner_delv_ob_id
            from tb_division_sku_all a
            left join
                (select item_sku_id as sku_id
                       ,src_store_id as store_id
                       ,max(send_qty) as neipei_qtty
                       ,inner_delv_ob_id   --一个订单号只取一条数据
                from gdm.gdm_m08_ob_inner_delv_sum
                where dp='ACTIVE'
                    and to_date(send_tm) between '""" + str(thirty_days) + """"' and '""" + str(yesterday) + """'
                    and inner_delv_ob_status in ('41','42')
                group by item_sku_id,src_store_id,inner_delv_ob_id
                ) b
            on a.sku_id = b.sku_id
                and a.current_source_store_id = b.store_id
            ) t
        where t.is_selected = 1
        group by t.sku_id,t.future_source_store_id

    """
hc.sql(sql_hot_select_sku_neipei).createOrReplaceTempView("tb_hot_select_sku_neipei")

#     4.1. 获取已选sku默认比例数据
sql_hot_select_sku_info = """
           select sku_id
               ,dc_id
               ,hot_sku_target_store_id
               ,future_source_store_id
               ,round(avg(hot_sku_out_store_rate),2) as default_ratio
            from tb_division_sku_all
            where is_selected = 1
            group by sku_id,dc_id,future_source_store_id,hot_sku_target_store_id

    """
hc.sql(sql_hot_select_sku_info).createOrReplaceTempView("tb_hot_select_sku_info")

#  4.2. 整合得到sku粒度的结果数据
sql_hot_preliminary_result = """
                select a.sku_id as sku_id
               ,a.dc_id as dc_id
               ,a.hot_sku_target_store_id as hot_sku_target_store_id
               ,a.future_source_store_id as future_source_store_id
               ,coalesce(round((b.sale_in_hot+d.neipei_qtty)/(b.sale_total+d.neipei_qtty),4),a.default_ratio) as hot_sku_ratio
               ,coalesce(c.stock_in_future_store_id,0) as future_store_stock_qtty
               ,coalesce(b.sale_total,0) as sale_total
               ,coalesce(e.sale_total_3,0) as sale_total_3
        from tb_hot_select_sku_info a
        left join tb_hot_result b
        on a.sku_id = b.sku_id
        left join
            (select *
            from tb_hot_selected_sku_stock
            where is_selected = 1
            ) c
        on a.sku_id = c.sku_id and a.future_source_store_id = c.future_source_store_id
        left join tb_hot_select_sku_neipei d
        on a.sku_id = d.sku_id and a.future_source_store_id = d.future_source_store_id
        left join tb_hot_select_sku_sale e
        on a.sku_id = e.sku_id and a.future_source_store_id = e.future_source_store_id


    """
hc.sql(sql_hot_preliminary_result).createOrReplaceTempView("tb_hot_preliminary_result")

#     5.1. 按照目前出库比例计算得到的库存占比
sql_hot_select_stock_proportion = """
          select aa.future_source_store_id
         ,coalesce(round(sum(aa.stock_in_future_store_id)/sum(aa.is_selected*aa.hot_sku_ratio*aa.stock_in_future_store_id),4),0) as stock_proportion
          from
        (select a.future_source_store_id as future_source_store_id
             ,a.sku_id as sku_id
             ,a.stock_in_future_store_id as stock_in_future_store_id
             ,a.is_selected as is_selected
             ,a.unset1_flag as unset1_flag
             ,b.hot_sku_ratio as hot_sku_ratio
      from tb_hot_selected_sku_stock a
      left join
          tb_hot_preliminary_result b
      on a.sku_id = b.sku_id and a.future_source_store_id = b.future_source_store_id
      ) aa
      where aa.unset1_flag = 1
      group by aa.future_source_store_id
      """
hc.sql(sql_hot_select_stock_proportion).createOrReplaceTempView("tb_hot_select_stock_proportion")

# 5.2. 按照未来仓id分别标记订单,单量, 销量占比
sql_sku_default_ratio = """
        select future_source_store_id
           ,round(avg(hot_sku_out_store_rate),2) as default_ratio
        from app.app_wil_hot_sku_all
         where dt='""" + str(today) + """'
                and pid='""" + pid + """'
                and ts='""" + ts + """'
                and  future_source_store_id != hot_sku_target_store_id and future_source_store_id is not null
        group by future_source_store_id
    """
value = hc.sql(sql_sku_default_ratio).rdd.map(lambda x: map(lambda a: a, x)).filter(lambda x: x[1] != None).collect()
if len(value) == 2:
    v_future_source_store_id_1 = value[0][0]
    v_future_source_store_id_2 = value[1][0]
    sql_hot_select_ord_flag1 = """
                    select a.ord_id
                      ,a.sale_qtty
                      ,b.pick_flag
                from
                    tb_division_data_all a
                left join
                    (select ord_id
                           ,min(is_selected) as pick_flag
                    from tb_division_data_all
                    where future_source_store_id = '""" + str(v_future_source_store_id_1) + """'
                    group by ord_id
                    ) b
                on a.ord_id = b.ord_id

            """
    hc.sql(sql_hot_select_ord_flag1).createOrReplaceTempView("tb_hot_select_ord_flag1")

    sql_hot_select_ord_flag2 = """
                           select a.ord_id
                             ,a.sale_qtty
                             ,b.pick_flag
                       from
                           tb_division_data_all a
                       left join
                           (select ord_id
                                  ,min(is_selected) as pick_flag
                           from tb_division_data_all
                           where future_source_store_id = '""" + str(v_future_source_store_id_2) + """'
                           group by ord_id
                           ) b
                       on a.ord_id = b.ord_id

                   """
    hc.sql(sql_hot_select_ord_flag2).createOrReplaceTempView("tb_hot_select_ord_flag2")

    sql_hot_select_ord_proportion = """
            select  """ + str(v_future_source_store_id_1) + """ as future_source_store_id
           ,round(cast(count(case when pick_flag = 1 then ord_id else null end)/count(ord_id) as float),4) as ord_proportion
           ,round(cast(sum(case when pick_flag = 1 then sale_qtty else 0 end)/sum(sale_qtty) as float),4) as sale_proportion
            from tb_hot_select_ord_flag1
            union all
            select  """ + str(v_future_source_store_id_2) + """ as future_source_store_id
                   ,round(cast(count(case when pick_flag = 1 then ord_id else null end)/count(ord_id) as float),4) as ord_proportion
                   ,round(cast(sum(case when pick_flag = 1 then sale_qtty else 0 end)/sum(sale_qtty) as float),4) as sale_proportion
            from tb_hot_select_ord_flag2
        """
    hc.sql(sql_hot_select_ord_proportion).createOrReplaceTempView("tb_hot_select_ord_proportion")
else:
    sql_hot_select_ord_flag3 = """
            select a.ord_id
              ,a.sale_qtty
              ,b.pick_flag
            from
            tb_division_data_all a
            left join
            (select ord_id
                   ,min(is_selected) as pick_flag
            from tb_division_data_all 
            group by ord_id
            ) b
        on a.ord_id = b.ord_id
        """
    hc.sql(sql_hot_select_ord_flag3).createOrReplaceTempView("tb_hot_select_ord_flag3")
    sql_hot_select_ord_proportion = """
                    select  future_source_store_id
                   ,round(cast(count(case when pick_flag = 1 then ord_id else null end)/count(ord_id) as float),4) as ord_proportion
                   ,round(cast(sum(case when pick_flag = 1 then sale_qtty else 0 end)/sum(sale_qtty) as float),4) as sale_proportion
                    from tb_hot_select_ord_flag3
                    GROUP by future_source_store_id
                    """
    hc.sql(sql_hot_select_ord_proportion).createOrReplaceTempView("tb_hot_select_ord_proportion")
#  5.3. 汇总结果
sql_hot_store_result = """
        select a.*
       ,b.stock_proportion
        from tb_hot_select_ord_proportion a
        join
        tb_hot_select_stock_proportion b
        on a.future_source_store_id = b.future_source_store_id

    """
hc.sql(sql_hot_store_result).createOrReplaceTempView("tb_hot_store_result")
#    6 汇总得到最终结果
sql_final_result = """
         select a.*
           ,b.ord_proportion
           ,b.sale_proportion
           ,b.stock_proportion
        from  tb_hot_preliminary_result a
        left join tb_hot_store_result b
        on a.future_source_store_id = b.future_source_store_id
    """
hc.sql(sql_final_result).createOrReplaceTempView("tb_final_result")

# 把相关的结果整合到app_wil_hot_sku_ratio_result表，以dt,pid,ts为分区
partition = "dt='" + str(param["date"]) + "', pid='" + str(param["pid"]) + "', ts='" + str(param["ts"]) + "'"
table_output = 'app.app_wil_hot_sku_ratio_result'
sql_save_sd_result = """
                    insert overwrite table """ + table_output + """ partition (""" + partition + """)
                    select
                        sku_id,
                        dc_id,
                        hot_sku_target_store_id,
                        future_source_store_id,
                        hot_sku_ratio,
                        future_store_stock_qtty,
                        sale_total,
                        sale_total_3 as sale_total_three_days,
                        ord_proportion,
                        sale_proportion,
                        stock_proportion
                    from tb_final_result
                """
hc.sql(sql_save_sd_result)
