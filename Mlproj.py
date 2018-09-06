#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/9/6 下午2:32
# @File : Mlproj.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com

#!/usr/bin/python
# -*- coding:utf8 -*-
import pandas as pd
from impala import dbapi
from impala.util import as_pandas
import sys
from sklearn.externals import joblib
import hdfs
import codecs
import os
import logging
from logging import handlers
import numpy as np
import json
import pymysql.cursors

import warnings
warnings.filterwarnings('ignore')

"""
文件名称：rzrq_latent_20180824.py
模型：融资融券潜在客户预测
模型名称：rzrq_latent
模型版本：20180824
描述：融资融券模型优化，数据中心宽表加CRM数据
JIRA 单号：
修改人：脱利锋
工号：K0590023
修改时间：2018/8/23 10:27
修改内容：新增
"""
# 模型名：
MODEL_NAME="rzrq_latent"
# 版本号
MODEL_VER="20180824"
# 模型名称
PROJ_NAME = "{0}_{1}".format(MODEL_NAME, MODEL_VER)

def get_logger(name):

    '''
    log日志输出格式方法
    :param name:
    :return:
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        pass
    else:
        formatter = logging.Formatter('[%(asctime)s][%(name)s] - %(levelname)s - %(message)s')
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        # backupCount: 保留最近5个
        filename = os.path.join(os.environ.get('MLFLOW_LOGS'), '{0}.log'.format(PROJ_NAME))
        th = handlers.RotatingFileHandler(filename=filename, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8')
        th.setLevel(logging.DEBUG)
        th.setFormatter(formatter)

        logger.addHandler(ch)
        logger.addHandler(th)
    return logger

def query_result_on_hadoop(query):
    '''
    查询impala数据
    :param query:
    :return:
    '''
    # 获取环境变量配置
    db_host1 = os.environ.get('IMPALA_IP1')
    db_host2 = os.environ.get('IMPALA_IP2')
    db_port = os.environ.get('IMPALA_PORT')
    db_user = os.environ.get('HADOOP_USER')

    try:
        try:
            logger.debug("start conn impala client, ip [{0}], port [{1}]".format(db_host1, db_port))
            conn = dbapi.connect(host=db_host1, port=int(db_port))
        except Exception as e:
            logger.warning("retry conn impala client ...")
            try:
                conn = dbapi.connect(host=db_host2, port=int(db_port))
            except Exception as e:
                logger.error("reconn impala failed, error msg is [{0}], ip [{1}], port [{2}]".format(e, db_host2, db_port))
                sys.exit(1)

        if 'conn' in vars() and conn is not None:
            logger.debug("conn impala succeed! start query data ...")
            cursor = conn.cursor(user=db_user)
            cursor.execute(query)
            return as_pandas(cursor)
        else:
            logger.error("conn impala failed!")
            sys.exit(1)
    except Exception as e:
        logger.error("query hadoop failed! error msg is {0}".format(e))
        sys.exit(1)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def write_hdfs(dt, df):
    '''
    将预测结果上传Hive结果表
    :df: 数据日期
    :return:
    '''
    # 获取mlflow存放临时文件路径
    file_path = os.environ.get('MLFLOW_TMP')
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    file_name = os.path.join(file_path, "{0}.parquet".format(PROJ_NAME))

    # DataFrame转parquet
    try:
        df.to_parquet(file_name)
    except Exception as e:
        logger.error("dataframe to parquet failed! error msg is {0}".format(e))
        sys.exit(1)

    # 初始化HDFS客户端
    hdfs_host1 = os.environ.get('WEBHDFS_IP1')
    hdfs_host2 = os.environ.get('WEBHDFS_IP2')
    hdfs_port = os.environ.get('WEBHDFS_PORT')
    hdfs_user = os.environ.get('HADOOP_USER')

    try:
        logger.info("start conn hdfs client , hdfs namenode is [{0}], port is [{1}]".format(hdfs_host1, hdfs_port))
        client = hdfs.client.InsecureClient("http://{0}:{1}".format(hdfs_host1, hdfs_port), user=hdfs_user)
        logger.info("conn hdfs client succeed ! ip [{0}], port [{0}]".format(hdfs_host1, hdfs_port))
    except Exception as e:
        logger.warning("retry conn hdfs client ...")
        try:
            client = hdfs.client.InsecureClient("http://{0}:{1}".format(hdfs_host2, hdfs_port), user=hdfs_user)
            logger.info("conn hdfs client succeed ! ip [{0}], port [{1}]".format(hdfs_host2, hdfs_port))
        except Exception as e:
            logger.error("reconn hdfs failed, error msg is [{0}], ip [{1}], port [{2}]".format(e, hdfs_host2, hdfs_port))
            sys.exit(1)

    # parquet文件写入HDFS
    if 'client' in vars() and client is not None:
        fi = codecs.open(file_name, 'r', encoding='utf-8')
        model_results = "/user/hive/warehouse/ana_crmpicture.db/model_results"
        parquet_path = os.path.join(model_results, "dt={0}/model={1}/ver={2}/{3}.parquet".format(dt, MODEL_NAME, MODEL_VER, PROJ_NAME))
        client.write(parquet_path, fi.stream, overwrite=True, permission=777)
        logger.info("write parquet file to hdfs [{0}] succeed!".format(parquet_path))
    else:
        logger.error("hdfs client init failed!")
        sys.exit(1)

    # 删除本地parquet文件
    if os.path.exists(file_name):
        logger.info("delete local parquet file from dir [{0}]".format(file_name))
        os.remove(file_name)
    fi.close()

def mysql_util(dt, data):
    '''
    将监控结果插入MySQL
    :param dt:数据日期
    :param data:监控结果数据，json格式kv对
    :return:
    '''
    # 获取MySQL配置信息
    logger.info("start insert monitor result to mysql ....")
    host_name = os.environ.get("MYSQL_IP")
    port = int(os.environ.get("MYSQL_PORT"))
    database = os.environ.get("MYSQL_DATABASE")
    username = os.environ.get("MYSQL_USER")
    password = os.environ.get("MYSQL_PASS")
    logger.info("mysql conn info : host_name:[{0}], port:[{1}], username:[{2}], password:[{3}]".format(host_name, port, username, password))

    # 获取连接
    try:
        connect = pymysql.Connect(
            host=host_name,
            port=port,
            user=username,
            passwd=password,
            db=database,
            charset='utf8'
        )
        logger.info("mysql conn succeed ! start insert data ...")
    except Exception as e:
        logger.error("mysql conn failed ! error msg is [{0}]".format(e))
        sys.exit(1)

    # 获取游标
    cursor = connect.cursor()

    # 清除数据
    try:
        sql = "DELETE FROM ana_labelmodel.model_rslt_metrics WHERE dt = '{0}' and model = '{1}' and ver = '{2}'".format(dt, MODEL_NAME, MODEL_VER)
        logger.info("delete sql is [{0}]".format(sql))
        cursor.execute(sql)
        connect.commit()
        logger.info("delete data from mysql succeed !")
    except Exception as e:
        logger.error("delete data from mysql failed ! error msg is [{0}]".format(e))
        sys.exit(1)

    # 获取游标，插入数据
    try:

        sql = "INSERT INTO ana_labelmodel.model_rslt_metrics (dt, model, ver, metrics) " \
              "VALUES ('{0}', '{1}', '{2}', '{3}')".format(dt, MODEL_NAME, MODEL_VER, data)
        logger.info("mysql insert sql is [{0}]".format(sql))
        cursor.execute(sql)
        connect.commit()
        logger.info("insert into mysql succeed !")
    except Exception as e:
        logger.error("insert data to mysql failed ! error msg is [{0}]".format(e))
        sys.exit(1)

def calc_target(rst_df):
    rst_df.results = rst_df.results.astype(np.float)
    #总记录数
    count = len(rst_df)
    logger.info("cnt val is [{0}]".format(count))

    # 空值记录数
    count_na = rst_df.results.isnull().sum()
    logger.info("null cnt val is [{0}]".format(count_na))

    # 0值记录数
    count_zore = (rst_df.results == 0).sum()
    logger.info("zore cnt val is [{0}]".format(count_zore))

    # 计算最大值
    max = rst_df.results.max()
    logger.info("max val is [{0}]".format(max))

    # 计算最小值
    min = rst_df.results.max()
    logger.info("max val is [{0}]".format(min))

    # 计算均值
    mean = rst_df.results.mean()
    logger.info("mean val is [{0}]".format(mean))

    # 计算方差
    var = rst_df.results.var()
    logger.info("var val is [{0}]".format(var))

    # 计算标准差
    std = rst_df.results.std()
    logger.info("std val is [{0}]".format(std))

    # 计算四分位
    quantile = rst_df.results.quantile(q=[0.25, 0.50, 0.75])
    logger.info("quantile is \n{0}".format(quantile))
    pct_25 = quantile.iloc[0:1].values[0]
    logger.info("pct_25 is [{0}]".format(pct_25))
    pct_50 = quantile.iloc[1:2].values[0]
    logger.info("pct_50 is [{0}]".format(pct_50))
    pct_75 = quantile.iloc[2:3].values[0]
    logger.info("pct_75 is [{0}]".format(pct_75))

    # 计算相似度
    # 今天前1000名获取
    today_top_1k_df = pd.DataFrame(rst_df.sort_values(by=['results']).head(1000).id, columns=['id'])
    # logger.info("today_top_1k is [{0}]".format(today_top_1k))

    # 昨天前1000名获取
    yesterday = (pd.to_datetime(dt)+pd.tseries.offsets.DateOffset(days=-1)).strftime("%Y%m%d")
    ytd_top_1k_sql = "select id, results from ana_crmpicture.model_results where dt = '{0}' and model = '{1}' and ver = '{2}' order by results desc limit 1000".format(yesterday, MODEL_NAME, MODEL_VER)
    logger.info("ytd_top_1k_sql is [{0}]".format(ytd_top_1k_sql))
    ytd_top_1k = query_result_on_hadoop(ytd_top_1k_sql)
    ytd_top_1k_df = pd.DataFrame(ytd_top_1k.id, columns=['id'])

    # 计算今天前1000在昨天出现的个数
    top_1k_count = len(today_top_1k_df.merge(ytd_top_1k_df, on='id', how='inner'))
    logger.info("top_1k_count is [{0}]".format(top_1k_count))

    # 今天前1000名与昨天前1000名的相似度
    top_1k_sim = top_1k_count/1000
    logger.info("top_1k_sim is [{0}]".format(top_1k_sim))

    # 将检查结果拼接为字符串
    metrics = {"max": max, "min": min, "var": str(var), "count": str(count), "pct_25": pct_25, "pct_50": pct_50, "pct_75": pct_75, "stddev": str(std), "count_na": str(count_na), "count_zero": str(count_zore), "top_1k_sim": top_1k_sim, "top_1k_count": str(top_1k_count)}
    metrics_jsoin = json.dumps(metrics)
    logger.info("json result is [{0}]".format(metrics_jsoin))

    # 结果插入MySQL
    mysql_util(dt, metrics_jsoin)


if __name__ == '__main__':
    ###############################################
    # 0. 准备
    ###############################################
    # 0.1 设置log日志
    log_path = os.environ.get('MLFLOW_LOGS')
    logger = get_logger("%s_%s" % (MODEL_NAME, MODEL_VER))

    logger.info("################################################################################")
    logger.info("#")
    logger.info("# model [{0}], ver [{1}] start predict ...".format(MODEL_NAME, MODEL_VER))
    logger.info("#")
    logger.info("################################################################################")

    logger.info("PID: {0}".format(os.getpid()))
    logger.info("input params: {0}".format(sys.argv))
    # 0.2 参数个数判断
    if len(sys.argv) != 2:
        logger.error("argvs is [{0}], argvs incorrect! params [dt] needed!".format(sys.argv))
        sys.exit(1)

    # 0.3 获取脚本参数
    dt = str(sys.argv[1]).strip()
    logger.info("date is {0}".format(dt))

    # 0.4 加载模型文件
    try:
        logger.info("start load model file ...")
        model = joblib.load("{0}.model".format(PROJ_NAME))
        logger.info("load model file succeed !")
    except Exception as e:
        logger.error("load model file failed !, error msg is [{0}]".format(e))
        sys.exit(1)

    ###############################################
    # 1. 获取特征数据
    ###############################################
    query_str = """select concat(id, concat(",", features)) as col from ana_crmpicture.model_feature_sets where fset = '{0}' and ver = '{1}' and dt = '{2}'""".format(MODEL_NAME, MODEL_VER, dt)
    logger.info("query sql is [{0}]".format(query_str))

    df = query_result_on_hadoop(query_str)
    if len(df) == 0:
        logger.error("features data is empty!")
        sys.exit(1)

    logger.info("data cnt is {0}".format(len(df)))
    features = pd.DataFrame(df.col.str.split(",").tolist())
    features.columns = ['pty_id', 'aa_d_73_3', 'minor_cust_flag', 'nianling', 'tran_age_derived', 'aa_d_5_7', 'ba_b_6', 'pa_d_15_3', 'pa_d_15_4', 'aa_d_10_3', 'aa_b_9', 'pa_d_13_3', 'aa_d_14_7','pa_d_13_4', 'pa_d_14_3', 'ba_b_5','pa_d_15_4_rat_pa_d_15_3', 'stk_aset_scale_cd', 'pa_d_13_5','inv_month', 'pa_d_15_5', 'cad_d_2_1', 'ta_d_6_1', 'aa_d_70_4', 'pa_d_14_4', 'aa_b_1', 'aa_b_1_day_avg', 'pa_d_14_4_rat_pa_d_14_3', 'pa_d_15_5_rat_pa_d_13_3', 'aa_d_5_6','pa_d_15_6', 'aa_d_70_2', 'pa_d_15_7', 'ta_d_2_3','pa_d_18_7', 'cad_d_1_7', 'ta_d_2_5', 'pa_d_13_6','sz_mkt', 'ta_d_2_4', 'cad_d_3_1', '90d_ylv', 'ba_b_33', 'aa_d_70_6', 'aa_d_5_5', 'ta_d_1_7', 'aa_b_70','aa_b_10', 'aa_d_70_7', 'aa_d_5_4', 'aa_b_74','ta_d_6_1_day_avg', 'aa_d_73_7', 'cad_d_2_1_day_avg','aa_d_70_3', 'new_stk_tran_cnt_mt', 'min_aset_scale_y', 'aa_d_18_5', 'a_30d_stock_rate', 'avg_hold_price','aa_d_28_7', 'max_aset_scale_y', 'pa_b_10', 'aa_d_68_7', 'aa_d_70_5', 'aa_d_28_6', 'aa_d_5_3', 'aa_d_6_3','aset_cycle_m', 'aa_d_68_6', 'aa_d_18_4', 'aa_d_72_7','aa_d_28_5', 'aa_d_28_4', 'ta_d_2_5_rat_ta_d_2_3', 'pa_d_18_5', 'cad_d_1_4', 'pa_d_8_4', 'aa_d_14_3', 'ba_d_28_1', 'ta_d_1_4', 'aa_d_72_6', 'cad_d_1_3','ta_d_1_3', 'ta_d_3_3', 'pa_b_7', 'a_30d_cur_rmb_rate', 'aa_d_18_3', 'aa_b_75', 'ta_d_3_4', 'pa_d_18_4', 'aa_d_68_5', 'a_30d_zxb_rate', 'aa_d_28_3', 'aa_d_68_4', 'pa_d_10_6', 'pa_d_7_7', 'pa_d_7_4', 'pa_d_8_5','aa_d_72_4', 'aa_b_16', 'ta_d_2_2', 'pa_d_10_4','aa_b_14', 'aa_d_68_3', 'aa_d_12_3', 'aa_d_73_7_rat_aa_d_73_3', 'pa_d_7_6']
    logger.info("features columns is {0}".format(features.columns))

    ###############################################
    # 2. 特征工程
    ###############################################
    # 2.1 minor_cust_flag特征映射
    logger.info("features engineering steps begin ...")
    features.minor_cust_flag = features.minor_cust_flag.map({'Y': 1, 'N': 0})
    logger.info("features engineering steps end!")

    ###############################################
    # 3. 模型预测
    ###############################################
    logger.info("model predict steps begin ...")
    rst_df = features[['pty_id']]
    # 开始预测
    if 'model' in vars() and model is not None:
        logger.info("start model predict ...")
        data = features[features.columns.drop('pty_id')]
        rst_df['prob'] = model.predict_proba(data.values)[:, 1]
        if False:
            rst_df['features'] = df.col
        else:
            rst_df['features'] = ''

        # DataFrame列名与Hive表列保持一致
        rst_df.columns = ['id', 'results', 'features']

        # DataFrame列数据类型与Hive表列保持一致
        rst_df.id = rst_df.id.astype(np.str)
        rst_df.results = rst_df.results.astype(np.str)
        logger.info("model predict steps end!")


    ###############################################
    # 4. 模型预测结果上传
    ###############################################
    logger.info("results upload steps begin ...")
    if (len(rst_df.columns) > 1):
        write_hdfs(dt, rst_df)
    else:
        logger.error("no results to upload")
        sys.exit(1)
    logger.info("results upload steps end!")

    ###############################################
    # 5. 模型预测结果统计监控
    ###############################################
    logger.info("monitor steps begin ...")
    calc_target(rst_df)
    logger.info("monitor steps end!")

    ###############################################
    # 成功结束
    ###############################################
    logger.info("SUCCESS！")

    logger.info("################################################################################")
    logger.info("#")
    logger.info("# model [{0}], ver [{1}] end predict .".format(MODEL_NAME, MODEL_VER))
    logger.info("#")
    logger.info("################################################################################")