#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/8/29 下午2:26
# @File : LoggingTest.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
import sys
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
        formatter = logging.Formatter('[%(asctime)s][%(name)s] - %(levelname)s - %(message)s')
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        ch.setFormatter(formatter)
        th = handlers.TimedRotatingFileHandler(filename="/Users/tuotuo/Documents/log.txt",backupCount=5,encoding='utf-8')
        th.setFormatter(formatter)

        logger.addHandler(ch)
        logger.addHandler(th)
    return logger


def log_test():
    logger.info("info test")
    logger.warning("warning test")
    logger.error("error test")
    logger.debug("debug test")


if __name__ == '__main__':
    logger = get_logger("rzrq_20180824",logging.DEBUG)
    log_test()