#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/8/24 下午3:56
# @File : ModelSourceTableMonitor.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
from pyspark.sql import SparkSession
from pyspark.sql import Row
import dateutil.parser as dp
import datetime

import multiprocessing
import sys
import logging
#from impala import dbapi
#from impala.dbapi import connect
#from impala.util import as_pandas


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
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


def last_month(dt):

    '''
    工具函数：获取当前日期的上个月
    :param dt: 时间参数，如：20180824
    :return:
    '''
    today=dp.parse(dt)
    first = datetime.date(day=1, month=today.month, year=today.year)
    lastMonth = str(first - datetime.timedelta(days=1)).replace("-", "")[0:6]
    return int(lastMonth)


def load_mysql_data(host_name, port, db_name, username, password):

    '''
    加载MySQL中的爬虫数据
    :return: MySQL配置表单的DataFrame数据
    '''

    # 函数调用之前先休眠1s
    # spark加载MySQL数据
    jdbcDF = spark.read.format("jdbc"). \
        option("url", "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf8" % (host_name, port, db_name)). \
        option("driver", "com.mysql.jdbc.Driver"). \
        option("dbtable", "model_source_monitor_cfg"). \
        option("user", "%s" % (username)). \
        option("password", "%s" % (password)). \
        load()
    return jdbcDF


#def query_result_on_hadoop(query):
#    try:
#        conn = dbapi.connect(host='168.9.65.71', port='21050')
#        cursor = conn.cursor(user='crmpicturewr')
#        cursor.executor(query)
#        return as_pandas(cursor)
#    except Exception as e:
#        logger.error("conn imapa failed ! error msg is [{0}]".format(e))
#    finally:
#        cursor.close()
#        conn.close()


def check_data(tab):

    '''
    单线程实现函数，计算表单总量，空值比例
    :param tab:
    :return:
    '''

    logger = get_logger("check_data")
    conn_info = tab.conn_info
    dt = conn_info["dt"]
    host_name = conn_info["host_name"]
    port = conn_info["port"]
    db_name = conn_info["db_name"]
    username = conn_info["username"]
    password = conn_info["password"]
    logger.info("conn params is [{0},{1},{2},{3},{4},{5}]".format(dt, host_name, port, db_name, username, password))

    model_table_name = tab.model_tab_name
    model_tab_type = tab.model_tab_type
    model_pri_key = tab.model_pri_key
    model_tab_par = tab.model_tab_par
    source_pri_tab = tab.source_pri_tab
    source_tab_type = tab.source_tab_type
    source_pri_key = tab.source_pri_key
    source_tab_par = tab.source_tab_par
    status = tab.status
    logger.info("model params is [{0},{1},{2},{3},{4},{5},{6},{7},{8}]".format(model_table_name, model_tab_type, model_pri_key, model_tab_par, source_pri_tab, source_tab_type, source_pri_key, source_tab_par, status))

    # 如果status==1，则进行数据监控
    if status == 1:
        logger.info("---------------------------------1")

        logger.info("---------------------------------2")
        # 如果当前表单为月表，则根据dt获取上月时间数据：
        # 如：dt=20180824 则时间日期为201807
        if model_tab_type == 2:
            model_real_dt = last_month(dt)
        else:
            model_real_dt = dt
        logger.info("---------------------------------3")
        # 监控表单数据总量查询SQL
        cnt_sql = """
            select 
                count(1) as cnt 
            from 
                {} 
            where 
                {} = '{}'
        """.format(model_table_name, model_tab_par, model_real_dt)
        logger.info("cnt_sql is [{0}]".format(cnt_sql))
        cnt = spark.sql(cnt_sql).rdd.collect()[0].cnt
        logger.info("cnt is [{0}]".format(cnt))


        logger.info("---------------------------------4")
        if cnt == 0:
            logger.info("---------------------------------5")
            # 如果当前监控表单数据总量为0，则空值比例为-1，表示当前表数据未到，spark SQL插入查询结果
            rst_df = [{'tab_name':model_table_name, 'dt':dt, 'last_count':0, 'null_rate':-1}]
            logger.info("rst_df is [{0}]".format(rst_df))
            spark.createDataFrame(rst_df).write.\
                mode("append").\
                format("jdbc").\
                option("url", "jdbc:mysql://%s:%s/%s" % (host_name, port, db_name)). \
                option("dbtable", "model_source_monitor_rslt"). \
                option("user", "%s" % (username)). \
                option("password", "%s" % (password)).\
                save()
        elif cnt > 0:
            logger.info("---------------------------------6")
            # 如果当前监控表单的数据总量>0，则计算主表数据总量
            if source_tab_type == 2:
                source_real_dt = last_month(dt)
            else:
                source_real_dt = dt
            logger.info("---------------------------------7")
            pri_cnt_sql = """
                select 
                    count(1) as cnt 
                from 
                    {}
                where
                    {} = '{}'
            """.format(source_pri_tab, source_tab_par, source_real_dt)
            logger.info("pri_cnt_sql is [{0}]".format(pri_cnt_sql))
            pri_cnt = spark.sql(pri_cnt_sql).rdd.collect()[0].cnt
            logger.info("---------------------------------8")
            if pri_cnt == 0:
                logger.info("---------------------------------9")
                # 如果主表的数据总量为0，则空值比例为-100，表示主表数据未到
                rst_df = [{'tab_name':model_table_name, 'dt':dt, 'last_count':cnt, 'null_rate':-100}]
                logger.info("rst_df is [{0}]".format(rst_df))
                logger.info("---------------------------------10")
                spark.createDataFrame(rst_df).write. \
                    mode("append"). \
                    format("jdbc"). \
                    option("url", "jdbc:mysql://%s:%s/%s" % (host_name, port, db_name)). \
                    option("dbtable", "model_source_monitor_rslt"). \
                    option("user", "%s" % (username)). \
                    option("password", "%s" % (password)). \
                    save()
                logger.info("---------------------------------11")
            elif pri_cnt > 0:
                logger.info("---------------------------------12")
                # 关联查询计算空值比例
                join_sql = """
                    select
                        count(1)
                    from 
                    (
                        select
                            {},{}
                        from
                            {}
                        where
                            {} = '{}'
                    ) t1  
                    left join
                    (
                        select
                            {},{}
                        from
                            {}
                        where
                            {} = '{}'
                    ) t2 on t1.{} = t2.{} where t1.{} is null
                """.format(model_pri_key, model_tab_par, model_table_name, model_tab_par, model_real_dt, source_pri_key, source_tab_par,
                           source_pri_tab, source_tab_par, source_real_dt, model_tab_par, source_tab_par, model_pri_key)
                logger.info("join_sql is [{0}]".format(join_sql))
                null_cnt = spark.sql(join_sql).rdd.collect()[0].cnt
                logger.info("null cnt is [{}]".format(null_cnt))
                logger.info("---------------------------------13")
                nullRate = format(float(null_cnt)/float(pri_cnt), '.9f')
                rst_df = [{'tab_name':model_table_name, 'dt':dt, 'last_count':cnt, 'null_rate':nullRate}]
                logger.info("rst_df is [{0}]".format(rst_df))
                logger.info("---------------------------------14")
                spark.createDataFrame(rst_df).write. \
                    mode("append"). \
                    format("jdbc"). \
                    option("url", "jdbc:mysql://%s:%s/%s" % (host_name, port, db_name)). \
                    option("dbtable", "model_source_monitor_rslt"). \
                    option("user", "%s" % (username)). \
                    option("password", "%s" % (password)). \
                    save()
                logger.info("---------------------------------15")


if __name__ == '__main__':

    # spark对象初始化
    app_name = "ModelSourceTableMonitor"
    spark = SparkSession.builder.master("yarn").appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.hive.convertMetatoreParquet", "false")
    sc = spark.sparkContext

    # 初始化spark日志：
    log4jLogger = sc._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(app_name)

    # 配置参数
    dt = sys.argv[1]
    host_name = sys.argv[2]
    port = sys.argv[3]
    db_name = sys.argv[4]
    username = sys.argv[5]
    password = sys.argv[6]
    thread_num = sys.argv[7]
    logger.error("params is dt:{}, host_name:{}, port:{}, db_name:{}, username:{}, password:{}, thread_num:{}".format(dt, host_name, port, db_name, username, password, thread_num))

    conn_info = {'dt' : dt, 'host_name' : host_name, 'port' : port, 'db_name' : db_name, 'username' : username, 'password' : password}
    # 查询MySQL配置表单数据
    df = load_mysql_data(host_name, port, db_name, username, password)

    tabs = df.rdd.map(lambda x: Row(conn_info = conn_info,
                                    model_tab_name = str(x[0]),
                                    model_tab_type = x[1],
                                    model_pri_key = str(x[2]),
                                    model_tab_par = str(x[3]),
                                    source_pri_tab = str(x[4]),
                                    source_tab_type = x[5],
                                    source_pri_key = str(x[6]),
                                    source_tab_par = str(x[7]),
                                    status = x[8])).collect()

    # 定义同时至多起几个线程
    pool = multiprocessing.Pool(int(thread_num))
    for tab in tabs:
        pool.apply_async(check_data, args=(tab,))

    # 用来阻止多余的进程涌入进程池 Pool 造成进程阻塞。
    pool.close()
    # 方法实现进程间的同步，等待所有进程退出。
    pool.join()