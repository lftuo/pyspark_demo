#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/8/31 下午2:26
# @File : LogTest.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
import time
# import logging
import logging.handlers
import sys
# logging初始化工作


def get_logger(name):
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    # 写入文件，如果文件超过100个Bytes，仅保留5个文件。
    th = logging.handlers.RotatingFileHandler(filename="test.log", maxBytes=50, backupCount=5, encoding='utf-8')
    th.setLevel(logging.DEBUG)
    th.setFormatter(formatter)
    # 设置后缀名称，跟strftime的格式一样
    logger.addHandler(th)
    return logger

def log_test():
    logger = get_logger("test")
    logger.info("testnadjgshfdgasdkgnjnsfdsfdnjvnjnsjtestnadjgshfdgasdkgnjnsfdsfdnjvnjnsjtestnadjgshfdgasdkgnjnsfdsfdnjvnjnsjtestnadjgshfdgasdkgnjnsfdsfdnjvnjnsj")
    logger.error("testtestnadjgshfdgasdkgnjnsfdsfdnjvnjnsjtestnadjgshfdgasdkgnjnsfdsfdnjvnjnsjtestnadjgshfdgasdkgnjnsfdsfdnjvnjnsjtestnadjgshfdgasdkgnjnsfdsfdnjvnjnsj")

if __name__ == '__main__':
    while True:
        time.sleep(0.1)
        log_test()
