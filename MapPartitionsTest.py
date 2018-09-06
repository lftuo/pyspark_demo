#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/9/4 下午2:27
# @File : MapPartitionsTest.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
import sys
import multiprocessing

from pyspark.sql import Row
from pyspark.sql import SparkSession
import logging.handlers


def get_logger(name):

    '''
    log日志输出格式方法
    :param name:
    :param level:
    :return:
    '''
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    th = logging.handlers.RotatingFileHandler(filename="test.log", maxBytes=1000, backupCount=5, encoding='utf-8')
    th.setLevel(logging.DEBUG)
    th.setFormatter(formatter)

    logger.addHandler(th)
    return logger


def load_mysql_data(host_name, port, db_name, username, password):

    '''
    加载MySQL中的爬虫数据
    :return: MySQL配置表单的DataFrame数据
    '''

    # spark加载MySQL数据
    jdbcDF = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf8" % (host_name, port, db_name)). \
        option("driver", "com.mysql.jdbc.Driver"). \
        option("dbtable", "model_source_monitor_cfg"). \
        option("user", "%s" % (username)). \
        option("password", "%s" % (password)). \
        load()
    return jdbcDF


def deal_date(x):
    #logger = get_logger("check_data")
    # print(x)
    dt = x.dt
    host_name = x.host_name
    port = x.port
    db_name = x.db_name
    username = x.username
    password = x.password
    print(dt, host_name, port, db_name, username, password)
    model_table_name = x.model_tab_name
    model_tab_type = x.model_tab_type
    model_pri_key = x.model_pri_key
    model_tab_par = x.model_tab_par
    source_pri_tab = x.source_pri_tab
    source_tab_type = x.source_tab_type
    source_pri_key = x.source_pri_key
    source_tab_par = x.source_tab_par
    status = x.status
    print(model_table_name, model_tab_type, model_pri_key, model_tab_par, source_pri_tab, source_tab_type, source_pri_key, source_tab_par, status)


if __name__ == '__main__':

    # spark对象初始化
    app_name = "PySparkDemo"
    spark = SparkSession.builder.master("local").appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.hive.convertMetatoreParquet", "false")
    sc = spark.sparkContext

    # 初始化spark日志：
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(app_name)

    dt = "20180824"
    host_name = "127.0.0.1"
    port = "3306"
    db_name = "test"
    username = "root"
    password = "123456"
    thread_num = 10

    conn_info = {'dt' : dt, 'host_name' : host_name, 'port' : port, 'db_name' : db_name, 'username' : username, 'password' : password}
    logger.info("params is {}, {}, {}, {}, {}, {}, {}".format(dt, host_name, port, db_name, username, password, thread_num))

    df = load_mysql_data(host_name, port, db_name, username, password)
    pd_df = df.toPandas()
    pd_df['dt'] = "20180824"
    pd_df['host_name'] = "127.0.0.1"
    pd_df['port'] = "3306"
    pd_df['db_name'] = "test"
    pd_df['username'] = "root"
    pd_df['password'] = "123456"
    pd_df['thread_num'] = 10
    spark.createDataFrame(pd_df).rdd.map(deal_date).collect()