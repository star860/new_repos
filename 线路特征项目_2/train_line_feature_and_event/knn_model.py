# -*- coding: utf-8 -*-
import sys
import numpy as np
import datetime
import traceback
import pymysql
import param_conf
from sklearn.neighbors import KNeighborsRegressor
from sklearn.externals import joblib

####################################
# 生产环境
mysql_host = param_conf.mysql_host
mysql_port = param_conf.mysql_port
mysql_userName = param_conf.mysql_userName
mysql_password = param_conf.mysql_password
mysql_dbName = param_conf.mysql_dbName

hbase_host = param_conf.hbase_host

hive_host = param_conf.hive_host
hive_port = param_conf.hive_port
hive_user = param_conf.hive_user
hive_passwd = param_conf.hive_passwd
hive_db = param_conf.hive_db

# 开发环境
dev_hbase_host=param_conf.dev_hbase_host

# dev_hive_host = param_conf.dev_hive_host
# dev_hive_port = param_conf.dev_hive_port
# dev_hive_user = param_conf.dev_hive_user
# dev_hive_passwd = param_conf.dev_hive_passwd
# dev_hive_db = param_conf.dev_hive_db

#local环境
dev_mysql_host = param_conf.dev_mysql_host
dev_mysql_port = param_conf.dev_mysql_port
dev_mysql_userName = param_conf.dev_mysql_userName
dev_mysql_password = param_conf.dev_mysql_password
dev_mysql_dbName = param_conf.dev_mysql_dbName

#local环境
dev_mysql_host = param_conf.local_mysql_host
dev_mysql_port = param_conf.local_mysql_port
dev_mysql_userName = param_conf.local_mysql_userName
dev_mysql_password = param_conf.local_mysql_password
dev_mysql_dbName = param_conf.local_mysql_dbName
###################################

dev_mysql_conn = pymysql.connect(
                host=dev_mysql_host, port=dev_mysql_port, user=dev_mysql_userName,
                passwd=dev_mysql_password, database=dev_mysql_dbName, charset='utf8')

def get_gps_from_mysql(line_code,line_direation):
    # 获取全线的 gps
    x=[]
    y=[]
    try:
        sqlStr = """
        SELECT
             mileages,
             longitude,
             latitude 
        FROM
            gps_line_data 
        where line_code = '{}'
        and   line_direation ='{}'
                """.format(line_code,line_direation)
        cursor = dev_mysql_conn.cursor()
        cursor.execute(sqlStr)
        gps_list = cursor.fetchall()
        for item in gps_list:
            mileage,longitude,latitude=item

            mileage=int(mileage)
            longitude=float(longitude)
            latitude=float(latitude)
            x.append([longitude,latitude])
            y.append(mileage)
    except Exception:
        print('%s MySQL获取数据异常:  %s' % (datetime.datetime.now(), sqlStr, traceback.format_exc()))
        sys.exit(1)
    finally:
        cursor.close()  # 关闭游标
    return x,y
line_code,line_direation ='3002','0'
x,y=get_gps_from_mysql(line_code,line_direation)
print(x)
print(y)
x= np.array(x)
neigh = KNeighborsRegressor(n_neighbors=2)
neigh.fit(x,y)
joblib.dump(neigh, './knn_model/knn_model_{}-{}.m'.format(line_code,line_direation))







