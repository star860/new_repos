#!/home/phmdm/anaconda3-2019.10-Linux-x86_64/bin/python
# -*- coding: utf-8 -*-
######################################################################
#
# 创建时间：2019年12月10日
# 创 建 者：wxj
# 功能：列车行驶里程计算(每天)---根据起始站-终点站获取开行信息
#
######################################################################
import datetime
import sys
import traceback
import pymysql
import param_conf
from pyhive.hive import connect
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
        # 获取线路上的起始站点-结束站点
        try:
            sqlStr = """
           select   
                    station_name
                    ,mile
                    ,order_id
            from dm_train_line_trainsite_detail_info_from_huochepiao 
            where trainsite_id = '{}'
            """.format(trainsite_id)
            cur = self.local_mysql_conn.cursor()
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
            cur = self.local_mysql_conn.cursor()
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
            cur = self.local_mysql_conn.cursor()
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
            cursor = self.local_mysql_conn.cursor()
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
                cursor = self.local_mysql_conn.cursor()
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

    def get_openInfo_by_station_bytime(self,time_new):
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
            where TIME ='{}'
                    """.format(time_new)
            cursor = self.local_mysql_conn.cursor()
            print('sql', sqlStr)
            cursor.execute(sqlStr)
            data_list = cursor.fetchall()
        except Exception:
            print('%s MySQL获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
        return data_list

    def insert_into_mysql(self,train_id, data_time, trainsite_id,line_name,line_code,line_direction_code,line_start_end,station_name,station_mileage,order_id):
        # 将数据存入mysql中
        try:
            sqlStr = """
            insert into webdata.dm_train_line_running_mileage(train_id, data_time, trainsite_id,line_name,line_code,line_direction_code,line_start_end,station_name,station_mileage,order_id)
            values
                ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                  """.format(train_id, data_time, trainsite_id,line_name,line_code,line_direction_code,line_start_end,station_name,station_mileage,order_id)
            cursor = self.local_mysql_conn.cursor()
            cursor.execute(sqlStr)
            self.local_mysql_conn.commit()
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
        openInfo_list = self.get_openInfo_by_station_bytime('2019-12-25 00:00:00.0')  # 获取开行信息
        for item in openInfo_list:
            S_DATE, trainsite_id, S_TRAINSETNAME, S_STARTTIME, S_ENDTIME, start_station, end_station, S_RUNTIME, I_RUNMILE, TIME=item
            if '/' in trainsite_id:
                trainsite_id = trainsite_id.split('/')[0]  # 获取车次
            print(trainsite_id)
            station_info = self.get_station_info_by_trainsiteid(trainsite_id)  # 通过车次从车次表中获取站点信息
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


