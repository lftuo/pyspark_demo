#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/7/7 上午9:48
# @File : PySparkSingleThread.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
from pyspark.sql import SparkSession
import datetime
import time

# 初始化spark对象
spark = SparkSession.builder.master("local").appName("PySparkDemo").getOrCreate()

def __load_mysql_data__():

    '''
    加载MySQL中的爬虫数据
    :return:
    '''

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

for i in range(100):
    __load_mysql_data__()

# 设置程序结束时间
end = datetime.datetime.now()
# 计算时间间隔
interval_time = (end - start).seconds
# 打印时间间隔
print('time interval is : ',interval_time)