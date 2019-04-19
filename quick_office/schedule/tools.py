#!/usr/bin/python3
# -*- coding:utf-8 -*-

# File Name: utils.py
# Author: Dixon
# Mail: cndixon@163.com
# Created Time: 2019/4/18


import json


def result(code=0, data=None, msg=str()):
    data = data if data else {}
    return json.dumps({'code': code, 'msg': msg}) if code else json.dumps({'code': code, 'data': data})
