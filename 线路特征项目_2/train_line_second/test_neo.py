# coding:utf-8
# from py2neo import Graph,Node,Relationship
#
# ## 连接neo4j数据库，输入地址、用户名、密码
# graph = Graph("http://192.168.252.132:7474", username="neo4j", password='123456')
#
# ##创建节点
# test_node_1 = Node(label='ru_yi_zhuan',name='皇帝')
# test_node_2 = Node(label='ru_yi_zhuan',name='皇后')
# test_node_3 = Node(label='ru_yi_zhuan',name='公主')
# graph.create(test_node_1)
# graph.create(test_node_2)
# graph.create(test_node_3)
#
# ##关键关系
# # 分别建立了test_node_1指向test_node_2和test_node_2指向test_node_1两条关系
#
# node_1_zhangfu_node_1 = Relationship(test_node_1, '丈夫', test_node_2)
# node_1_zhangfu_node_1['count'] = 1
# node_2_qizi_node_1 = Relationship(test_node_2, '妻子', test_node_1)
# node_2_munv_node_1 = Relationship(test_node_2, '母女', test_node_3)
#
# node_2_qizi_node_1['count'] = 1
#
# graph.create(node_1_zhangfu_node_1)
# graph.create(node_2_qizi_node_1)
# graph.create(node_2_munv_node_1)
#
# print(graph)
# print(test_node_1)
# print(test_node_2)
# print(node_1_zhangfu_node_1)
# print(node_2_qizi_node_1)
# print(node_2_munv_node_1)

import numpy as np
print(np.pi)
