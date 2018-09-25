#!/usr/bin/env python3
# coding:utf-8
__author__ = 'ag300g'

'''
An example
Table name: app.app_iap_purchase
'''
import sys
import os
import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row


def get_format_yesterday(num=1,format = '%Y-%m-%d'):
    """
    获取昨天日期（字符串）默认'%Y-%m-%d'，
    format =‘%d' 获取昨天是本月中第几天
    """
    end_dt = (datetime.date.today() - datetime.timedelta(num)).strftime(format)
    start_dt = (datetime.date.today() - datetime.timedelta(num+90)).strftime(format)
    return start_dt,end_dt



def main():

    if(len(sys.argv)>1): # 传了系统参数
        end_dt = sys.argv[1]
        start_dt = (datetime.datetime.strptime(sys.argv[1],'%Y-%m-%d') - datetime.timedelta(90)).strftime( '%Y-%m-%d')
    else:
        end_dt = get_format_yesterday()[1]
        start_dt = get_format_yesterday()[0]


        # yesterday_monthDay =  datetime.datetime.strptime(yesterday, '%Y-%m-%d').strftime('%d')  # 当月几号
    # DROP TABLE IF EXISTS app.app_iap_purchase;
    # CREATE TABLE app.app_iap_purchase(
    #   pur_bill_id string comment '采购单ID',
    #   pur_bill_src_cd string comment '采购单来源代码',-- 1:ERP 2:BIP 3:VC 4:自动补货 5:下传EDI 6:POP 7:PBS 8:厂商直送 9:EDI系统传过来 10.补货工作台WBR 14.VMI 15.补货工作台-自动
    #   pur_bill_attribute_cd string comment '采购单属性',-- 1：新品订单(N) 2：手工补货单(M) 3：自动补货单(A) 4：问题区补单(Q) 5：有单备货单(R) 6：自动补货补给单(QA) 7：手工下单补给单(QM) 8：新品订单补给单(QN) 9：有单备货补给单(QR)
    #   pur_type_cd string comment '采购类型代码',-- """"" 正常采购单 0 正常采购单 1 虚拟采购单（去掉） 2 厂商直送 3 图书普通采购单 4 家电普通采购单 5 大件协同仓采购单 6 图书厂商直送采购单 7 图书EDI采购单 8 家电EDI采购单 9 其它EDI采购单 1000 POP采购单 2000 EPT定制采购单 2001 EPT非定制采购单 2005 中小件J仓共享库存单 3000 自动采购单（改为图书EDI共享库存单） 3001 补货工作台WBR（调整为正常采购单） 3002 自动采购JIT单（改为图书协同J仓共享库存单） 3003 自动共享库存单（改为非图共享库存单） 3004 VMI共享库存单 4000 海外采购单 5000 总代分销单 5001 通讯分销 6000 自营闪购单 6001 自营闪购JIT单 6002 自营闪购厂直单 6003 闪购以销定结 7000 自营全球购单 8000 产地仓采购单 9000 黄金自营以销定结 100 OTC自营 101 生鲜J仓共享库存单 102 ESD采购单
    #   cgdetail_yn int comment '采购明细单有效标志',-- 整个采购单删除时，为1，单条删除时候为0
    #   valid_flag int comment '有效标志',-- 整个采购单删除时为0，反之为1；在途和到货采购单，两个有效字段都为1
    #   sku_id string comment 'SKU编码',
    #   item_third_cate_cd string comment 'SKU三级分类',
    #   supp_brevity_cd string comment '供应商编号',
    #   supp_name string comment '供应商名称',
    #   create_tm string comment '采购单创建时间',
    #   complete_dt string comment '采购单完成时间',
    #   int_org_num string comment '配送中心编号',
    #   originalnum string comment '原始下单量',
    #   plan_pur_qtty string comment '回告数量',
    #   actual_pur_qtty string comment '实际到货量',
    #   purchase_ib_id string comment '采购单编号',
    #   arrive_tm string comment '到货时间',
    #   insp_qtty string comment '验收数量',
    #   insp_tm string comment '验收时间',
    #   shelves_qtty string comment '上架数量',
    #   shelves_tm string comment '上架时间',
    #   booktime string comment '预约时间',
    #   backtime string comment '回告时间',
    #   cancel_tm string comment '取消时间',
    #   check_tm string comment '审核时间',
    #   back_time string comment '回告时间',
    #   cancel_reason string comment '取消原因：对应po_process表的po_state状态',
    #   cancel_type string comment '取消操作描述',
    #   creator string comment '责任人',
    #   cancelcreatetime string comment '取消订单创建时间',
    #   cancel_state string comment '前置状态',
    #   book_delivery_time string COMMENT '预约到货时间',
    #   submit_tm string comment '提交时间',
    #   manager_check_tm string comment '经理审核时间',
    #   director_check_tm string comment '总监审核时间',
    #   qc_check_tm string comment '质控审核时间',
    #   last_order_tm string comment '最晚下单时间'
    #   submitter string comment '提交人',
    #   manager_checker string comment '经理审核人',
    #   director_checker string comment '总监审核人',
    #   qc_checker string comment '质控审核人',
    #   po_creator string comment '下单人',
    #   org_dc_id string comment '所属rdc_id',
    #   pur_agmt_status_cd string comment '采购单最新状态码0100dd'
    # )
    # PARTITIONED BY (dt string)
    # stored as TEXTFILE;

    cte = """
            with stock_out as
            (
                    select
                            sku_id,
                            delv_center_num,
                            vlt_ref
                    from
                            app.app_iap_sku_stockout_pv_result
                    where
                            dt = '""" + end_dt + """'
            ),
               dc_map as
            (
                select distinct dc_id,org_dc_id from dim.dim_dc_info where dc_type in (0,1)--中小件支援关系
            )
            """



    sql = cte + """
insert overwrite table app.app_iap_purchase partition(dt = '"""+ end_dt +"""')
select
    r1.pur_bill_id,
    r1.pur_bill_src_cd,
    r1.pur_bill_attribute_cd,
    r1.pur_type_cd,
    r1.cgdetail_yn,
    r1.valid_flag,
    r1.sku_id,
    r1.item_third_cate_cd,
    r1.supp_brevity_cd,
    r1.supp_name,
    substr(r1.create_tm,0,16) create_tm,
    substr(r1.complete_dt,0,16) complete_dt,
    r1.int_org_num,
    r1.originalnum,
    if(r4.backtime is not null,r1.plan_pur_qtty,null),
    null,
    r2.purchase_ib_id,
    substr(r2.arrive_tm,0,16) arrive_tm,
    cast(r2.insp_qtty as int),
    substr(r2.insp_tm,0,16) insp_tm,
    cast(r2.shelves_qtty as int),
    substr(r2.shelves_tm,0,16) shelves_tm,
    substr(r3.booktime,0,16) booktime,
    substr(r4.backtime,0,16) backtime,
    substr(r6.cancel_tm,0,16) cancel_tm,
    substr(r5.check_tm,0,16) check_tm,
    substr(r5.backtime,0,16) back_time,
    r6.cancel_reason cancel_reason,
    r6.cancel_type cancel_type,
    r6.creator,---需要增加咧
    r5.cancelcreatetime,
    r6.cancel_state cancel_state,
    r5.auto_check_flag,
    substr(r3.book_delivery_time,0,16) book_delivery_time,
    substr(r5.submit_tm,0,16) submit_tm,--提交时间
    case when r5.manager_check_tm >= r5.submit_tm then substr(r5.manager_check_tm,0,16) else null end manager_check_tm,--经理审核时间,取在提交行为之后的审核时间
    case when r5.director_check_tm >= r5.submit_tm then substr(r5.director_check_tm,0,16) else null end director_check_tm,--总监审核时间,取在提交行为之后的审核时间
    case when r5.qc_check_tm >= r5.submit_tm then substr(r5.qc_check_tm,0,16) else null end qc_check_tm,--质控审核时间,取在提交行为之后的审核时间
    --最晚下单时间,取最晚下单日的23时59分
    from_unixtime(floor(unix_timestamp('"""+end_dt+""" 23:59','yyyy-MM-dd HH:mm') - 86400 * (case when r7.vlt_ref is not null then r7.vlt_ref else 10 end) ) , 'yyyy-MM-dd HH:mm') last_order_tm--最晚下单时间
    ,r5.submitter --提交人
    ,r5.manager--经理
    ,r5.director--总监
    ,r5.qc--质控
    ,r1.purchaser_erp_acct --下单人
    ,dc_map.org_dc_id --所属rdc
    ,r1.pur_agmt_status_cd --采购单最新状态码
from
    (select
        t2.pur_bill_id,
        t2.pur_bill_src_cd,
        t2.pur_bill_attribute_cd,
        t2.pur_type_cd,
        --SUBSTRING(t2.create_tm,0,10) dt,
        t2.cgdetail_yn,
        t2.valid_flag,
        t2.sku_id,
        t1.third_cate_cd item_third_cate_cd,
        t2.supp_brevity_cd, -- 供应商编号
        t2.supp_name, -- 供应商名称
        t2.create_tm,
        t2.complete_dt,
        t2.int_org_num, -- 配送中心编号
        t2.originalnum, -- 原始下单量
        t2.plan_pur_qtty, -- 回告数量,未回告为实际下单量,回告未回告量
        t2.actual_pur_qtty, -- 实际量采购数,未上架为实际下单量,上架后为上架量
        t2.purchaser_erp_acct, --采购单下单人
        t2.pur_agmt_status_cd --采购单状态
    from
        app.app_iap_sku_info t1
    JOIN
        gdm.gdm_m04_pur_det_basic_sum t2
    on
        t1.item_sku_id = t2.sku_id
    where
        t2.pur_type_cd in ('0','3','4','7','8','9','3001','6000','100')--0 正常采购单 1 虚拟采购单（去掉） 2 厂商直送 3 图书普通采购单 4 家电普通采购单 5 大件协同仓采购单 6 图书厂商直送采购单 7 图书EDI采购单 8 家电EDI采购单 9 其它EDI采购单 1000 POP采购单 2000 EPT定制采购单 2001 EPT非定制采购单 2005 中小件J仓共享库存单 3000 自动采购单（改为图书EDI共享库存单） 3001 补货工作台WBR（调整为正常采购单） 3002 自动采购JIT单（改为图书协同J仓共享库存单） 3003 自动共享库存单（改为非图共享库存单） 3004 VMI共享库存单 4000 海外采购单 5000 总代分销单 5001 通讯分销 6000 自营闪购单 6001 自营闪购JIT单 6002 自营闪购厂直单 6003 闪购以销定结 7000 自营全球购单 8000 产地仓采购单 9000 黄金自营以销定结 100 OTC自营 101 生鲜J仓共享库存单 102 ESD采购单
    and     t2.pur_bill_src_cd not in ('6','8')--1:ERP 2:BIP 3:VC 4:自动补货 5:下传EDI 6:POP 7:PBS 8:厂商直送 9:EDI系统传过来 10.补货工作台WBR 14.VMI 15.补货工作台-自动
    and     t2.pur_bill_attribute_cd not in ('4','5','9')--1：新品订单(N) 2：手工补货单(M) 3：自动补货单(A) 4：问题区补单(Q) 5：有单备货单(R) 6：自动补货补给单(QA) 7：手工下单补给单(QM) 8：新品订单补给单(QN) 9：有单备货补给单(QR)
    and     t2.dt='"""+end_dt+"""'
    and     t1.dt='"""+end_dt+"""'
    and     create_tm<='"""+end_dt+"""'
    and     create_tm>='"""+start_dt+"""') r1
left join
    (-- 预约时间表
    select
        a2.poid,
        a2.businesstime as booktime,
        a2.booktime as book_delivery_time,
        a2.bookno
    from
        (select
            max(id) as id
        from
            fdm.fdm_procurement_purchase_booking_chain
        where
            start_date <='"""+end_dt+"""' and
            end_date >'"""+end_dt+"""' and
            yn = 1 and
            businesstime<='"""+end_dt+"""' and
            businesstime>='"""+start_dt+"""'
        group by
            poid) a1
    left join
        (select
            id,
            poid,
            businesstime,
            booktime,
            bookno
        from
            fdm.fdm_procurement_purchase_booking_chain
        where
            start_date <='"""+end_dt+"""' and
            end_date >'"""+end_dt+"""' and
            yn = 1 and
            businesstime<='"""+end_dt+"""' and
            businesstime>='"""+start_dt+"""') a2
    on a1.id = a2.id) r3
    on r1.pur_bill_id = r3.poid
left join
    (--仓储数据
    select
        regexp_replace(purchase_ib_id, 'C', '') purchase_ib_id, -- 入库单编号
        item_sku_id,
        distribute_no,
        arrive_tm,
        insp_qtty, -- 验收数量
        insp_tm, -- 验收时间
        shelves_qtty, -- 上架数量
        shelves_tm,-- 上架时间
        book_id
    from
        gdm.gdm_m08_ib_purchase_sum
    where
        dp='ACTIVE'
    and
        pur_create_date<='"""+end_dt+"""' and
        pur_create_date>='"""+start_dt+"""'
        ) r2
on
    r1.pur_bill_id = regexp_replace(r2.purchase_ib_id, 'C', '') and
    r1.sku_id = r2.item_sku_id
   -- and r2.book_id = r3.bookno
left join
   (--回告表
    select
       orderid,
       createtime as backtime
    from
        fdm.fdm_procurement_purchase_order_back_info_chain
    where
        backtype = '1' and
        start_date <='"""+end_dt+"""' and
        end_date >'"""+end_dt+"""' and
        createtime<='"""+end_dt+"""' and
        createtime>='"""+start_dt+"""'
        ) r4
on  r1.pur_bill_id = r4.orderid
left join
    (SELECT t2.poid,
       max(CASE WHEN t2.id = t1.submit_id THEN t2.creator END) AS submitter,
       max(CASE WHEN t2.id = t1.manager_id THEN t2.creator END) AS manager,
       max(CASE WHEN t2.id = t1.director_id THEN t2.creator END) AS director,
       max(CASE WHEN t2.id = t1.qc_id THEN t2.creator END) AS qc,
       max(CASE WHEN t2.id = t1.cancel_id THEN createtime END)AS cancelcreatetime,-- 取消时间
       max(CASE WHEN t2.id = t1.check_id THEN createtime END)AS check_tm,-- 审核时间
       max(t1.auto_check_flag) AS auto_check_flag,-- 1自动审核0非系统自动审核
       min(CASE WHEN t2.id = t1.back_id THEN createtime END) AS backtime,-- 回告时间
       max(CASE WHEN t2.id = t1.submit_id THEN createtime END)AS submit_tm,-- 最新提交时间
       max(CASE WHEN t2.id = t1.manager_id THEN createtime END)AS manager_check_tm,-- 最新经理审核时间
       max(CASE WHEN t2.id = t1.director_id THEN createtime END)AS director_check_tm,-- 最新总监审核时间
       max(CASE WHEN t2.id = t1.qc_id THEN createtime END)AS qc_check_tm -- 最新质控审核时间
FROM
  (SELECT poid,
          max(CASE WHEN (actiontype='100'
                         OR actiontype='102')
              AND yn=0 THEN id END) cancel_id,
          max(CASE WHEN (actiontype='123')
              AND yn=1 THEN id END) check_id,
          max(if(oldstate=6
                 AND oldyn=1
                 AND yn = 1
                 AND newState=6,1,0)) auto_check_flag,
          max(CASE WHEN actiontype='108'
              AND yn = 1 THEN id END) back_id,
          max(CASE WHEN (oldstate='0')
              AND (actiontype='104')
              AND yn=1 THEN id END) submit_id,
          max(CASE WHEN (oldstate='10')
              AND (actiontype='123')
              AND yn=1 THEN id END) manager_id,
          max(CASE WHEN (oldstate='11')
              AND (actiontype='123')
              AND yn=1 THEN id END) director_id,
          max(CASE WHEN (oldstate='12')
              AND (actiontype='123')
              AND yn=1 THEN id END) qc_id
   FROM fdm.fdm_procurement_lifecycle_chain
   WHERE dp='ACTIVE'
        and createtime<='"""+end_dt+"""'
        and createtime>='"""+start_dt+"""'
   GROUP BY poid) t1
LEFT JOIN
  (SELECT id,
          poid,
          creator,
          createtime
   FROM fdm.fdm_procurement_lifecycle_chain
   WHERE dp='ACTIVE'
        and createtime<='"""+end_dt+"""'
        and createtime>='"""+start_dt+"""'
        ) t2 ON t1.poid=t2.poid
GROUP BY t2.poid
        ) r5
    on r1.pur_bill_id = r5.poid
left join
    (select
        poid,
        oldstate cancel_state,
        actiontype cancel_type,
        descr cancel_reason,
        createTime as cancel_tm,
        creator
    from
        fdm.fdm_procurement_lifecycle_chain
    where
        actiontype in (120,119,124,127)
        and dp='ACTIVE'
        and createtime<='"""+end_dt+"""'
        and createtime>='"""+start_dt+"""') r6
on  r1.pur_bill_id = r6.poid
    left join
            stock_out r7
    on
            r1.sku_id          = r7.sku_id
            and r1.int_org_num = r7.delv_center_num
left join dc_map on r1.int_org_num = dc_map.dc_id
"""

    sql0 = """alter table app.app_iap_purchase drop if exists PARTITION (dt='""" + end_dt + """')"""
    print(sql)
    spark = SparkSession \
        .builder \
        .appName("app_iap_purchase") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql("SET spark.sql.shuffle.partitions=100")
    spark.sql(sql0)
    spark.sql(sql)
    spark.stop()

if __name__ == "__main__":
    main()