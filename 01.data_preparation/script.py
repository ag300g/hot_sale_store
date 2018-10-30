#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'dengzheng'

from subprocess import call

commond = "spark-submit --master yarn --num-executors 50 --executor-memory 20g --executor-cores 4 --driver-memory 4g" \
          " --conf spark.rdd.compress=true" \
          " --conf spark.broadcast.compress=true" \
          " --conf spark.shuffle.io.maxRetries=9" \
          " --conf spark.shuffle.file.buffer=128k"  \
          " --conf spark.reducer.maxSizeInFlight=96M"  \
          " --conf spark.shuffle.io.retryWait=60s"  \
          " --queue bdp_jmart_cmo_ipc_union.bdp_jmart_ipc_formal driver.py"

ok = call(commond.split(" "))

print("返回码：" + str(ok))
if ok == 0:
      print("------------------------------------------")
      print("-----------------运行成功！-----------------")
      print("------------------------------------------")
else:
      print("------------------------------------------")
      print("-------------运行失败：出现异常-------------")
      print("------------------------------------------")
      raise Exception('ERROR：' + str(ok))
