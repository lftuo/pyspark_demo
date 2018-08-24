#!/usr/bin/python
# -*- coding:utf8 -*-
# @Author : tuolifeng
# @Time : 2018/8/24 下午7:29
# @File : DateUtil.py
# @Software : IntelliJ IDEA
# @Email ： 909709223@qq.com
import datetime
import dateutil.parser as dp

def __last_month__(dt):
    today=dp.parse(dt)
    first = datetime.date(day=1, month=today.month, year=today.year)
    lastMonth = str(first - datetime.timedelta(days=1)).replace("-","")[0:6]
    return int(lastMonth)

print(__last_month__("20170101"))