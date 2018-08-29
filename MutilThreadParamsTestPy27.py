#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/8/24 下午8:14
# @File : MutilThreadParamsTest.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
import sys
import multiprocessing

from pyspark.sql import Row
from pyspark.sql import SparkSession
import logging
from logging import handlers


def get_logger(name, level=logging.INFO):

    '''
    log日志输出格式方法
    :param name:
    :param level:
    :return:
    '''

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        pass
    else:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        ch.setFormatter(formatter)
        th = handlers.TimedRotatingFileHandler(filename="/Users/tuotuo/Documents/log.txt",backupCount=5,encoding='utf-8')
        th.setFormatter(formatter)

        logger.addHandler(ch)
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

def check_data(tab):

    logger = get_logger("check_data",logging.DEBUG)
    try:
        conn_info = tab.conn_info
        dt = conn_info["dt"]
        host_name = conn_info["host_name"]
        port = conn_info["port"]
        db_name = conn_info["db_name"]
        username = conn_info["username"]
        password = conn_info["password"]
        logger.info("conn params is {0},{1},{2},{3},{4},{5}".format(dt, host_name, port, db_name, username, password))

        model_table_name = tab.model_table_name
        model_tab_type = tab.model_tab_type
        model_pri_key = tab.model_pri_key
        model_tab_par = tab.model_tab_par
        source_pri_tab = tab.source_pri_tab
        source_tab_type = tab.source_tab_type
        source_pri_key = tab.source_pri_key
        source_tab_par = tab.source_tab_par
        status = tab.status
        logger.debug("model params is {0},{1},{2},{3},{4},{5},{6},{7},{8}".format(model_table_name, model_tab_type, model_pri_key, model_tab_par, source_pri_tab, source_tab_type, source_pri_key, source_tab_par, status))
        logger.error("error test")
        logger.warning("warning test")
    except Exception as e:
        print(e)

if __name__ == '__main__':

    # spark对象初始化
    app_name = "PySparkDemo"
    spark = SparkSession.builder.master("local").appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.hive.convertMetatoreParquet", "false")
    sc = spark.sparkContext

    # 初始化spark日志：
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(app_name)

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
    thread_num = 10

    conn_info = {'dt' : dt, 'host_name' : host_name, 'port' : port, 'db_name' : db_name, 'username' : username, 'password' : password}
    logger.info("params is {}, {}, {}, {}, {}, {}, {}".format(dt, host_name, port, db_name, username, password, thread_num))
    # 查询MySQL配置表单数据
    df = load_mysql_data(host_name, port, db_name, username, password)
    tabs = df.rdd.map(lambda x: Row(conn_info = conn_info,
                                    model_table_name = str(x[0]),
                                    model_tab_type = x[1],
                                    model_pri_key = str(x[2]),
                                    model_tab_par = str(x[3]),
                                    source_pri_tab = str(x[4]),
                                    source_tab_type = x[5],
                                    source_pri_key = str(x[6]),
                                    source_tab_par = str(x[7]),
                                    status = x[8])).collect()

    # 定义同时至多起几个线程
    pool = multiprocessing.Pool(10)
    for tab in tabs:
        pool.apply_async(check_data, args=(tab,))

    # 用来阻止多余的进程涌入进程池 Pool 造成进程阻塞。
    pool.close()
    # 方法实现进程间的同步，等待所有进程退出。
    pool.join()