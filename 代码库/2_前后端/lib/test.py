import sys
import datetime
import traceback
import random
import urllib3
import requests
import param_conf
from lxml import etree

if __name__ == '__main__':
    session = requests.Session()
    # headers = {"User-Agent": random.choice(param_conf.my_headers),
    #            "Accept": "text/html,application/xhtml+xml,application/xml; q=0.9,image/webp,*/*;q=0.8"}
    #
    # trainsite = 'G1268'
    # # url = "http://checi.114piaowu.com/G1"
    # url = "https://www.china-emu.cn/lines/?Page-SA-1-1.html"  ## 运营中线路
    # req = session.get(url, headers=headers)
    # html = etree.HTML(req.text)
    # train_line = html[1].xpath("//h1[@class='tm-site-name-h3']")
    # train_line_code = html[1].xpath("//span[@class='tm-site-name-h4']")
    # otherLineInfo = html[1].xpath("//h4[@class='tm-site-data-h4']")

