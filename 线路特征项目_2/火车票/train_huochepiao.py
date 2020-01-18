# -*- coding:utf-8 -*-
import re           # 导入re模块
import requests     # 导入request模块
from bs4 import BeautifulSoup
from lxml import etree
import csv
import time
import database_handler

class cowal_train():
    # 车次列表
    trainsite_list = []
    # 车次详细列表
    trainsite_breif_list_www = []
    # 检查url地址
    dist_info = {}
    def check_link(self,url):
        n = 0
        while True:
            n += 1
            try:
                response = requests.get(url)
                response_str = response.content.decode('GBK')
                # 解析字符串,返回html对象
                html = etree.HTML(response_str, parser=etree.HTMLParser())
            except Exception as e:
                if n <= 10:
                    print(e)
                    print('\033[1;31;0m request报错,等待5秒再访问 \033[0m')
                    time.sleep(5)
                else:
                    raise Exception('访问被终止')
                # 以字符串格式返回网页内容
            return html

    # 爬取资源
    def get_contents(self,html):
        info = html.xpath("//table[4]")[0]
        temp_trainsite_list = info.xpath('.//tr/td[1]/a/text()')
        temp_trainsite_detail_list = info.xpath('.//tr/td[1]/a/@href')
        # self.trainsite_list.extend(temp_trainsite_list)
        # print(self.trainsite_list)
        self.trainsite_breif_list_www.extend(temp_trainsite_detail_list)
        # print(self.trainsite_breif_list_www)

    # 保存资源 --mysql
    def save_contents(self):
        for url in self.trainsite_breif_list_www:
            html = self.check_link(url)
            if html.xpath('//tr/td/center/a[contains(text(),"点此可查询上一版本数据")]'):  # xpath 模糊匹配
                continue
            else:
                checi=html.xpath('//tr[1]/td[3]/text()')[2]
                if '/' in checi:
                    for checi_i in checi.split('/'):
                        self.dist_info['trainsite_id'] = checi_i
                        self.dist_info['time_long']     = html.xpath('//tr[1]/td[5]/text()')[1]
                        self.dist_info['start_station'] = html.xpath('//tr[2]/td[2]/text()')[0]
                        self.dist_info['end_station']   = html.xpath('//tr[2]/td[4]/text()')[0]
                        self.dist_info['start_time'] = html.xpath('//tr[3]/td[2]/text()')[0]
                        self.dist_info['end_time'] = html.xpath('//tr[3]/td[4]/text()')[0]
                        self.dist_info['trainsite_type'] = html.xpath('//tr[4]/td[2]/text()')[0]
                        self.dist_info['mile'] = html.xpath('//tr[4]/td[4]/text()')[0]
                        self.dist_info['update_day'] = html.xpath('//tr[5]/td[1]/text()')[0].split(' ')[1]
                        database_handler.DatabaseHandler().insert_into_mysql(self.dist_info)
                else:
                    self.dist_info['trainsite_id'] = html.xpath('//tr[1]/td[3]/text()')[2]
                    self.dist_info['time_long']     = html.xpath('//tr[1]/td[5]/text()')[1]
                    self.dist_info['start_station'] = html.xpath('//tr[2]/td[2]/text()')[0]
                    self.dist_info['end_station']   = html.xpath('//tr[2]/td[4]/text()')[0]
                    self.dist_info['start_time'] = html.xpath('//tr[3]/td[2]/text()')[0]
                    self.dist_info['end_time'] = html.xpath('//tr[3]/td[4]/text()')[0]
                    self.dist_info['trainsite_type'] = html.xpath('//tr[4]/td[2]/text()')[0]
                    self.dist_info['mile'] = html.xpath('//tr[4]/td[4]/text()')[0]
                    self.dist_info['update_day'] = html.xpath('//tr[5]/td[1]/text()')[0].split(' ')[1]
                    database_handler.DatabaseHandler().insert_into_mysql(self.dist_info)
        print('\033[1;33;0m 完成爬取 \033[0m')
        self.trainsite_breif_list_www.clear()
    def main(self):
        for num in range(1,44):# 446
            url = "http://search.huochepiao.com/update/bianhao/?p={}&key=".format(num)  # 446页
            html_data = self.check_link(url)
            self.get_contents(html_data)
            self.save_contents()

if __name__ == '__main__':
    ct = cowal_train()
    ct.main()
    # print('\033[1;34;0m 没有查到link信息 \033[0m')
    # str = '111122223333'
    # print(str[:3]) # 字符串的提取