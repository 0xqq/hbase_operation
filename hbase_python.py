#! /usr/bin/env python
# -*- coding: utf-8 -*-

# 参考教程
# http://dbaspace.blog.51cto.com/6873717/1950002
# http://blog.csdn.net/dutsoft/article/details/60328341

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol


from hbase import Hbase
from hbase.ttypes import *

import threading
import time


# 创建 socket
transport = TSocket.TSocket('192.xxx.xxx.xxx', 9090)
transport.setTimeout(6000000)
transport = TTransport.TBufferedTransport(transport)
# protocol = TCompactProtocol.TCompactProtocol(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
#打开传输
transport.open()

# 定时插入数据
for i in range(5):
    print(i)
    #插入数据
    mutations = [Mutation(column="info:Name", value='Li'), Mutation(column="info:Age", value='18')]
    client.mutateRow('test2', str(i+30), mutations)
    time.sleep(120)

#删除表时必须先把表禁用了
# client.disableTable('studentInfo2')
#
# client.deleteTable('studentInfo2')

# hbase的列簇信息
# contents = [ColumnDescriptor(name='info', maxVersions=1), ColumnDescriptor(name='other', maxVersions=1)]

# client.createTable('test2', contents)

# 定时器
# def fun_timer():
#     print('Hello Timer!')
#     # 插入数据
#     mutations = [Mutation(column="info:Name", value='Li'), Mutation(column="info:Age", value='18')]
#     client.mutateRow('test2', '1', mutations)
#     global timer
#     timer = threading.Timer(2, fun_timer)
#     timer.start()

# timer = threading.Timer(2, fun_timer)
# timer.start()
#
# time.sleep(15) # 15秒后停止定时器
# timer.cancel()



# 获取数据
# datas = client.getRow('studentInfo2', '1')
# print('datas of hbase:', datas)
# for d in datas:
#     print('the row is ', d.row)
#     print('the values is ', d.columns.get('info:Name').value)
#
# # 获取hbase中的表
# print(client.getTableNames())

