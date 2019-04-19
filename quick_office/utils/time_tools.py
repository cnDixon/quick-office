#!/usr/bin/python3
# -*- coding:utf-8 -*-

# File Name: time_tools.py
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2019/1/3


import time

from datetime import datetime, timedelta


def timestamp_second():
    """
    秒级时间戳(十位)
    """
    return int(time.time())


def get_date_str(time_delta: int = 0, str_format: str = '%Y%m%d', appoint_date: datetime = None):
    """
    获取日期字符串
    :param time_delta: 时间间隔(天)
    :param str_format: 格式化字符串
    :param appoint_date: 格式化字符串
    :return:
    """
    if appoint_date:
        return (appoint_date + timedelta(time_delta)).strftime(str_format)
    else:
        return (datetime.today() + timedelta(time_delta)).strftime(str_format)


def date_str_parser(date_str):
    """
    解析时间字符串, 若格式不正确返回None
    :param date_str:
    :return:
    """
    try:
        return datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        return None


def list_slicing(data_list: list, size: int):
    """
    列表切片
        当 size = 2 时
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]  ->  [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]
    """
    while True:
        if data_list:
            yield data_list[:size]
            data_list = data_list[size:]
        else:
            break
