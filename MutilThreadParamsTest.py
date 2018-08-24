#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/8/24 下午8:14
# @File : MutilThreadParamsTest.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
import sys
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

from pyspark.sql import Row
from pyspark.sql import SparkSession
import time
import json

def __load_mysql_data__(host_name, port, db_name, username, password):

    '''
    加载MySQL中的爬虫数据
    :return: MySQL配置表单的DataFrame数据
    '''

    # 函数调用之前先休眠1s
    # spark加载MySQL数据
    jdbcDF = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://%s:%s/%s" % (host_name, port, db_name)). \
        option("driver", "com.mysql.jdbc.Driver"). \
        option("dbtable", "model_source_monitor_cfg"). \
        option("user", "%s" % (username)). \
        option("password", "%s" % (password)). \
        load()
    return jdbcDF

def __check_data__(tab):

    try:
        conn_info = tab.conn_info
        dt = conn_info["dt"]
        host_name = conn_info["host_name"]
        port = conn_info["port"]
        db_name = conn_info["db_name"]
        username = conn_info["username"]
        password = conn_info["password"]
        print(dt, host_name, port, db_name, username, password)
        model_table_name = tab.model_table_name
        model_tab_type = tab.model_tab_type
        model_pri_key = tab.model_pri_key
        model_tab_par = tab.model_tab_par
        source_pri_tab = tab.source_pri_tab
        source_tab_type = tab.source_tab_type
        source_pri_key = tab.source_pri_key
        source_tab_par = tab.source_tab_par
        print(model_table_name, model_tab_type, model_pri_key, model_tab_par, source_pri_tab, source_tab_type, source_pri_key, source_tab_par)
    except Exception as e:
        print(e)

if __name__ == '__main__':

    # spark对象初始化
    spark = SparkSession.builder.master("local").appName("PySparkDemo").getOrCreate()
    spark.conf.set("spark.sql.hive.convertMetatoreParquet","false")

    # dt = sys.argv[1]
    # host_name = sys.argv[2]
    # port = sys.argv[3]
    # db_name = sys.argv[4]
    # user_name = sys.argv[5]
    # pass_word = sys.argv[6]
    dt = "20180824"
    host_name = "127.0.0.1"
    port = "3306"
    db_name = "test"
    username = "root"
    password = "123456"
    conn_info = {'dt' : dt, 'host_name' : host_name, 'port' : port, 'db_name' : db_name, 'username' : username, 'password' : password}
    print(conn_info["dt"])
    # 查询MySQL配置表单数据
    df = __load_mysql_data__(host_name, port, db_name, username, password)
    tabs = df.rdd.map(lambda x: Row(conn_info = conn_info,
                                    model_table_name = str(x[0]),
                                    model_tab_type = str(x[1]),
                                    model_pri_key = str(x[2]),
                                    model_tab_par = str(x[3]),
                                    source_pri_tab = str(x[4]),
                                    source_tab_type = str(x[5]),
                                    source_pri_key = str(x[6]),
                                    source_tab_par = str(x[7]))).collect()

    # 启动线程池提交任务
    executor = ThreadPoolExecutor(max_workers=len(tabs))
    all_task = [executor.submit(__check_data__, (tab)) for tab in tabs]
    # 阻塞等待所有任务执行完成后返回
    wait(all_task, return_when=ALL_COMPLETED)