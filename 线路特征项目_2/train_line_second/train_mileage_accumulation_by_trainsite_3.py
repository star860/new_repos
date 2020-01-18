#!/home/phmdm/anaconda3-2019.10-Linux-x86_64/bin/python
# -*- coding: utf-8 -*-
######################################################################
#
# 创建时间：2019年12月10日
# 创 建 者：wxj
# 功能：列车行驶里程计算(每天)---根据起始站-终点站获取开行信息
#
######################################################################
import re
import datetime
import sys
import traceback
import pymysql
import param_conf
from pyhive.hive import connect
import itertools
####################################
# 生产环境
mysql_host = param_conf.mysql_host
mysql_port = param_conf.mysql_port
mysql_userName = param_conf.mysql_userName
mysql_password = param_conf.mysql_password
mysql_dbName = param_conf.mysql_dbName

hive_host = param_conf.hive_host
hive_port = param_conf.hive_port
hive_user = param_conf.hive_user
hive_passwd = param_conf.hive_passwd
hive_db = param_conf.hive_db

#开发环境
dev_mysql_host = param_conf.dev_mysql_host
dev_mysql_port = param_conf.dev_mysql_port
dev_mysql_userName = param_conf.dev_mysql_userName
dev_mysql_password = param_conf.dev_mysql_password
dev_mysql_dbName = param_conf.dev_mysql_dbName

#本地环境
local_mysql_host = param_conf.local_mysql_host
local_mysql_port = param_conf.local_mysql_port
local_mysql_userName = param_conf.local_mysql_userName
local_mysql_password = param_conf.local_mysql_password
local_mysql_dbName = param_conf.local_mysql_dbName
###################################

class TrainLineMileageAccumulation():
    def __init__(self):
        try:
            # self.mysql_conn = pymysql.connect(
            #     host=mysql_host,port=mysql_port,user=mysql_userName,
            #     passwd=mysql_password,database=mysql_dbName,charset='utf8')
            # self.dev_mysql_conn = pymysql.connect(
            #     host=dev_mysql_host, port=dev_mysql_port, user=dev_mysql_userName,
            #     passwd=dev_mysql_password, database=dev_mysql_dbName, charset='utf8')
            # self.hive_conn = connect(host=hive_host, port=hive_port, auth='CUSTOM', username=hive_user,
            #                               password=hive_passwd, database=hive_db)
            self.local_mysql_conn = pymysql.connect(
                host=local_mysql_host, port=local_mysql_port, user=local_mysql_userName,
                passwd=local_mysql_password, database=local_mysql_dbName, charset='utf8')
        except Exception as e:
            print('%s 数据库连接异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit()

    def get_station_info_by_trainsiteid(self,trainsite_id):
        # 获取线路上的起始站点
        try:
            sqlStr = """
           select   
                    station_name
                    ,mile
                    ,order_id
            from dm_train_line_trainsite_detail_info_from_huochepiao 
            where trainsite_id = '{}'
            """.format(trainsite_id)
            cur = self.dev_mysql_conn.cursor()
            cur.execute(sqlStr)
            data_tuple = cur.fetchall()
        except Exception as e:
                print('%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, traceback.format_exc()))
        finally:
            cur.close()  # 关闭游标
        return data_tuple

    def get_station_start_end(self,S_TRAINNO):
        # 获取线路上的起始站点
        try:
            sqlStr = """
           select   trainsite_id
                    ,start_station
                    ,end_station
            from dm_train_line_trainsite_brief_info_from_huochepiao 
            where trainsite_id ='{}'
            """.format(S_TRAINNO)
            cur = self.dev_mysql_conn.cursor()
            cur.execute(sqlStr)
            data_tuple = cur.fetchall()
        except Exception as e:
                print('%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, traceback.format_exc()))
        finally:
            cur.close()  # 关闭游标
        return data_tuple

    def get_station(self,start_stop_station):
        # 获取线路上的站点及对应里程
        try:
            sqlStr = """
            select  station_name
                    ,station_mile
            from dm_train_line_route_detail_info_from_chinaemu 
            where start_stop_station ='{}'
            """.format(start_stop_station)
            cur = self.dev_mysql_conn.cursor()
            cur.execute(sqlStr)
            data_tuple = cur.fetchall()
        except Exception as e:
                print('%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, traceback.format_exc()))
        finally:
            cur.close()  # 关闭游标
        return data_tuple

    def get_line_info_from_route(self,line_start_end,line_end_start):
        # 获取开行信息
        try:
            sqlStr = """
            select   train_line_code  
                      ,train_line_name
                      ,train_line_start_end                                               
            from dm_train_line_route_brief_info_from_chinaemu
            where  train_line_start_end = '{}' 
            or  train_line_start_end = '{}' 
                    """ .format(line_start_end,line_end_start)
            cursor = self.dev_mysql_conn.cursor()
            print('sql', sqlStr)
            cursor.execute(sqlStr)
            data_list = cursor.fetchall()
            if data_list:
                return data_list[0]
            else:
                sqlStr = """
                select   train_line_code  
                          ,train_line_name
                          ,road_start_end                                               
                from dm_train_line_route_brief_info_from_chinaemu
                where  road_start_end = '{}'
                or  road_start_end = '{}'
                        """.format(line_start_end,line_end_start)
                cursor = self.dev_mysql_conn.cursor()
                print('sql', sqlStr)
                cursor.execute(sqlStr)
                data_list = cursor.fetchall()
                if data_list:
                    return data_list[0]
        except Exception:
            print('%s MySQL获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
    #
    def get_openInfo_by_station_bytime(self,op_day,time_new):
        # 获取开行信息
        try:
            sqlStr = """
            select   S_DATE   
                      ,S_TRAINNO                   
                      ,S_TRAINSETNAME              
                      ,S_STARTTIME                 
                      ,S_ENDTIME                  
                      ,S_STARTSTATION              
                      ,S_ENDSTATION                
                      ,S_RUNTIME                   
                      ,I_RUNMILE                  
                      ,TIME                       
            from ODS_CUX_PHM_OPENSTRINGINFO_FORSF_V
            where op_day='{}' and TIME ='{}'
                    """.format(op_day,time_new)
            cursor = self.hive_conn.cursor()
            print('sql', sqlStr)
            cursor.execute(sqlStr)
            data_list = cursor.fetchall()
        except Exception:
            print('%s MySQL获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
        return data_list
    # 获取站点详细信息
    def get_info_from_detail(self,road_name):
        # 获取线路上的起始站点
        try:
            sqlStr2 = """
            SELECT 
                station_index,
                station_name,
                station_mile
            FROM
                dm_train_line_route_detail_info_from_chinaemu  
            WHERE
                road_name = '{}'
            and point_size in ('14','20')
            and station_mile  not in ('/','*','**','***')
            ORDER BY station_index
            """.format(road_name)
            cur = self.local_mysql_conn.cursor()
            print(sqlStr2)
            cur.execute(sqlStr2)
            data_tuple = cur.fetchall()
        except Exception as e:
                print('%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr2, traceback.format_exc()))
        finally:
            cur.close()  # 关闭游标
        return data_tuple

    # 生成线路上最小的站点区间表
    def station_min(self,line_name,line_direction_code,station_name,station_mileage):
        line_info=[]
        station_info_dict={}
        # 合并所有的多段线路
        try:
            sqlStr1 = """
            SELECT 
                train_line_name,
                road_num,
                road_index,
                road_name,
                road_mile
            FROM
                dm_train_line_route_brief_info_from_chinaemu 
            """
            cur = self.local_mysql_conn.cursor()
            cur.execute(sqlStr1)
            data_tuple = cur.fetchall()
            for item in data_tuple:
                train_line_name,road_num, road_index, road_name, road_mile=item
                if road_num == 1:
                    data_tuple_info= self.get_info_from_detail(road_name)
                    for item in data_tuple_info:
                        station_index, station_name, station_mile = item
                        line_info.append((train_line_name, station_index, station_name, station_mile))
                elif road_num == 2:
                    # 去查询根据road_num,和roadIndex
                    if road_index == 1:
                        # 查询详细表
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 2:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index='2_'+str(station_index)
                            road_mile = self.get_add_mile(road_num, train_line_name,road_index-1)
                            road_mile = re.sub('\D', '', road_mile)
                            station_mile = int(float(road_mile))+ int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))

                elif road_num == 3:
                    if road_index == 1:
                        # 查询详细表
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 2:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '2_' + str(station_index)
                            road_mile = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile = re.sub('\D', '', road_mile)
                            station_mile = int(float(road_mile)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 3:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '3_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            print(road_mile_1)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            print(road_mile_1)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))

                elif road_num == 4:
                    # 去查询根据road_num,和roadIndex
                    if road_index == 1:
                        # 查询详细表
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 2:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '2_' + str(station_index)
                            road_mile = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile = re.sub('\D', '', road_mile)
                            station_mile = int(float(road_mile)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 3:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '3_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            print(road_mile_1)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            print(road_mile_1)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 4:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '4_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(
                                float(road_mile_3)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))

                elif road_num == 5:
                    if road_index == 1:
                        # 查询详细表
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 2:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '2_' + str(station_index)
                            road_mile = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile = re.sub('\D', '', road_mile)
                            station_mile = int(float(road_mile)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 3:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '3_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            print(road_mile_1)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            print(road_mile_1)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 4:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '4_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(
                                float(road_mile_3)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 5:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '5_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))

                elif road_num == 6:
                    if road_index == 1:
                        # 查询详细表
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 2:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '2_' + str(station_index)
                            road_mile = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile = re.sub('\D', '', road_mile)
                            station_mile = int(float(road_mile)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 3:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '3_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            print(road_mile_1)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            print(road_mile_1)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 4:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '4_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(
                                float(road_mile_3)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 5:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '5_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 6:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '6_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_5 = self.get_add_mile(road_num, train_line_name, road_index - 5)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            road_mile_5 = re.sub('\D', '', road_mile_5)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(road_mile_5)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))

                elif road_num == 7:
                    # 去查询根据road_num,和roadIndex
                    if road_index == 1:
                        # 查询详细表
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 2:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '2_' + str(station_index)
                            road_mile = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile = re.sub('\D', '', road_mile)
                            station_mile = int(float(road_mile)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 3:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '3_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            print(road_mile_1)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            print(road_mile_1)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 4:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '4_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2))+ int(float(road_mile_3)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 5:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '5_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2))\
                                           + int(float(road_mile_3))+ int(float(road_mile_4))\
                                           + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 6:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:

                            station_index, station_name, station_mile = item
                            station_index = '6_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_5 = self.get_add_mile(road_num, train_line_name, road_index - 5)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            road_mile_5 = re.sub('\D', '', road_mile_5)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(road_mile_5))+ int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 7:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '7_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_5 = self.get_add_mile(road_num, train_line_name, road_index - 5)
                            road_mile_6 = self.get_add_mile(road_num, train_line_name, road_index - 6)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            road_mile_5 = re.sub('\D', '', road_mile_5)
                            road_mile_6 = re.sub('\D', '', road_mile_6)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(road_mile_5))+ int(float(road_mile_6))\
                                           + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))

                elif road_num == 8:
                    # 去查询根据road_num,和roadIndex
                    if road_index == 1:
                        # 查询详细表
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 2:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '2_' + str(station_index)
                            road_mile = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile = re.sub('\D', '', road_mile)
                            station_mile = int(float(road_mile)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 3:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '3_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            print(road_mile_1)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            print(road_mile_1)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 4:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '4_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2))+ int(float(road_mile_3)) + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 5:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '5_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2))\
                                           + int(float(road_mile_3))+ int(float(road_mile_4))\
                                           + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 6:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:

                            station_index, station_name, station_mile = item
                            station_index = '6_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_5 = self.get_add_mile(road_num, train_line_name, road_index - 5)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            road_mile_5 = re.sub('\D', '', road_mile_5)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(road_mile_5))+ int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 7:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '7_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_5 = self.get_add_mile(road_num, train_line_name, road_index - 5)
                            road_mile_6 = self.get_add_mile(road_num, train_line_name, road_index - 6)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            road_mile_5 = re.sub('\D', '', road_mile_5)
                            road_mile_6 = re.sub('\D', '', road_mile_6)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(road_mile_5))+ int(float(road_mile_6))\
                                           + int(float(station_mile))
                            line_info.append((train_line_name, station_index, station_name, station_mile))
                    elif road_index == 8:
                        data_tuple_info = self.get_info_from_detail(road_name)
                        for item in data_tuple_info:
                            station_index, station_name, station_mile = item
                            station_index = '8_' + str(station_index)
                            road_mile_1 = self.get_add_mile(road_num, train_line_name, road_index - 1)
                            road_mile_2 = self.get_add_mile(road_num, train_line_name, road_index - 2)
                            road_mile_3 = self.get_add_mile(road_num, train_line_name, road_index - 3)
                            road_mile_4 = self.get_add_mile(road_num, train_line_name, road_index - 4)
                            road_mile_5 = self.get_add_mile(road_num, train_line_name, road_index - 5)
                            road_mile_6 = self.get_add_mile(road_num, train_line_name, road_index - 6)
                            road_mile_7 = self.get_add_mile(road_num, train_line_name, road_index - 7)
                            road_mile_1 = re.sub('\D', '', road_mile_1)
                            road_mile_2 = re.sub('\D', '', road_mile_2)
                            road_mile_3 = re.sub('\D', '', road_mile_3)
                            road_mile_4 = re.sub('\D', '', road_mile_4)
                            road_mile_5 = re.sub('\D', '', road_mile_5)
                            road_mile_6 = re.sub('\D', '', road_mile_6)
                            road_mile_7 = re.sub('\D', '', road_mile_7)
                            station_mile = int(float(road_mile_1)) + int(float(road_mile_2)) \
                                           + int(float(road_mile_3)) + int(float(road_mile_4)) \
                                           + int(float(road_mile_5)) + int(float(road_mile_6)) \
                                           + int(float(road_mile_7)) + int(float(station_mile))

                            line_info.append((train_line_name, station_index, station_name, station_mile))
            print(line_info)
            # 获取所有线路上的站点和历程值
            sqlStr = """
            select    
                      train_line_name
                      ,station_index
                      ,station_name
                      ,station_mile                                              
            from dm_train_line_route_detail_info_from_chinaemu
            where  point_size in ('14','20')
            ORDER BY train_line_name,station_index 
                    """.format()
            cursor = self.dev_mysql_conn.cursor()
            print('sql', sqlStr)
            cursor.execute(sqlStr)
            data_list = cursor.fetchall()
            # 将数据存入mysql中--排列组合
            # num_list =[1,1,1]
            # for num in itertools.combinations(num_list,2):
            #     print(num)
        except Exception:
            print('%s MySQL获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标


    def insert_into_mysql(self,train_id, data_time, trainsite_id,line_name,line_code,line_direction_code,line_start_end,station_name,station_mileage,order_id):
        # 将数据存入mysql中
        try:
            sqlStr = """
            insert into webdata.dm_train_line_running_mileage(train_id, data_time, trainsite_id,line_name,line_code,line_direction_code,line_start_end,station_name,station_mileage,order_id)
            values
                ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                  """.format(train_id, data_time, trainsite_id,line_name,line_code,line_direction_code,line_start_end,station_name,station_mileage,order_id)
            cursor = self.dev_mysql_conn.cursor()
            cursor.execute(sqlStr)
            self.dev_mysql_conn.commit()
            print('sql',sqlStr)
        except Exception:
            print('%s MySQL获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标

    def main(self):
        op_day = datetime.datetime.now() - datetime.timedelta(days=1)
        op_day = op_day.strftime('%Y%m%d')
        time_new = datetime.datetime.now()- datetime.timedelta(days=1)
        time_new = time_new.strftime('%Y-%m-%d 00:00:00.0')
        openInfo_list = self.get_openInfo_by_station_bytime(op_day,time_new)
        for item in openInfo_list:
            S_DATE, trainsite_id, S_TRAINSETNAME, S_STARTTIME, S_ENDTIME, start_station, end_station, S_RUNTIME, I_RUNMILE, TIME=item
            if '/' in trainsite_id:
                trainsite_id = trainsite_id.split('/')[0]
            station_info = self.get_station_info_by_trainsiteid(trainsite_id)  # 从车次表中获取站点信息
            # 缺少线路的信息--只能在站点表中搜索根据起始站和终点站匹配交路表信息，在交路主表中凭接字符串得到线路
            print(start_station, end_station)
            line_start_end_0 = start_station + '-' + end_station
            line_end_start_1 = end_station + '-' + start_station
            line_info = self.get_line_info_from_route(line_start_end_0, line_end_start_1)    # 根据起始站和终点站,得到线路名
            if line_info is None:
                print('根据起始站和终点站 未匹配到线路',line_start_end_0,line_end_start_1)
                continue
            else:
                line_code, line_name, line_start_end = line_info
                if line_start_end == line_start_end_0:
                    line_direction_code = '0'  # 上行
                elif line_start_end == line_end_start_1:
                    line_direction_code = '1'  # 下行
            if station_info:
                for item in station_info:
                    station_name,mile,order_id= item
                    self.insert_into_mysql(S_TRAINSETNAME, TIME[:10], trainsite_id,line_name,line_code,
                                               line_direction_code,line_start_end,station_name,mile,order_id)
                    print('存入mysql')
            else:
                print('根据开行表中的车次 未匹配到站点信息',trainsite_id)

if __name__ == '__main__':
    print('开始时间', datetime.datetime.now())
    m = TrainLineMileageAccumulation()
    m.main()
    print('over')


