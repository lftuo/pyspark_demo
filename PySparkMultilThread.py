#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/7/7 下午9:36
# @File : PySparkMultilThread.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

from pyspark.sql import SparkSession
import datetime
import time
import numpy as np

import os
# 初始化spark对象
spark = SparkSession.builder.master("local").appName("PySparkDemo").getOrCreate()

def __load_mysql_data__(rownum):

    '''
    加载MySQL中的爬虫数据
    :return:
    '''

    print("rownum is : [%s]"%rownum)
    # 函数调用之前先休眠1s
    time.sleep(5)
    # spark加载MySQL数据
    jdbcDF = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://127.0.0.1:3306/spider_shouji"). \
        option("driver", "com.mysql.jdbc.Driver"). \
        option("dbtable", "shouji_all_spider_data"). \
        option("user", "root"). \
        option("password", "123456"). \
        load()
    jdbcDF.show()

# 设置程序开始时间
start = datetime.datetime.now()

executor = ThreadPoolExecutor(max_workers=100)
all_task = [executor.submit(__load_mysql_data__, (i)) for i in range(100)]
wait(all_task, return_when=ALL_COMPLETED)

## 设置程序结束时间
end = datetime.datetime.now()
## 计算时间间隔
interval_time = (end - start).seconds
## 打印时间间隔
print('time interval is : ', interval_time)