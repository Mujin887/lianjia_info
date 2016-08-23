#!/usr/bin/env python
'''
    app usage:
        1.chmod +x lianjia.py
        2../lianjia.py bj >> logfile
    crontab usage: crontab lianjia_crawler_job.cron
'''

import requests
from bs4 import BeautifulSoup
import redis
from datetime import datetime, timedelta
import sys

def trade_spider(area_arg):
    print("-"*20, date_score, "-"*20)
    url = 'http://' + area_arg + '.lianjia.com/fangjia/'
    soup = get_url_soup(url)
    set_area_sum_data(area_arg, area_arg, soup)
    link = soup.find_all('div',{'class':'hide'})[1]
    for location in link.find_all('a'):
        location_url = "http://bj.lianjia.com"+location.get('href')
        # print(location.string,location_url)
        location_soup = get_url_soup(location_url)
        area = location_url.split("/")[-2]
        set_area_sum_data(area, location.string, location_soup)

        area_link = location_soup.find_all('div',{'class':'hide'})[1]
        for inner_location in area_link.find_all('a')[area_offset:]:
            inner_location_url = "http://bj.lianjia.com"+inner_location.get('href')
            # print(inner_location.string,inner_location_url)
            inner_location_soup = get_url_soup(inner_location_url)
            inner_area= inner_location_url.split("/")[-2]
            set_area_sum_data(area + ":" +inner_area, inner_location.string, inner_location_soup)

    del_result = del_pipe.execute()
    print("del_result:\n", del_result)
    add_result = pipe.execute()
    print("add_result:\n", add_result)
    if 0 in add_result:
        print('Something wrong!') # TODO:部分数据执行错误，需要发邮件出来

# 通过URL获取soup
def get_url_soup(url):
    source_code = requests.get(url)
    source_code.encoding = 'utf-8' #显式地指定网页编码，一般情况可以不用
    plain_text = source_code.text
    # print(plain_text)
    return BeautifulSoup(plain_text,"html.parser")

'''
    存储每个区域的房价数据
    存储的时候：
        key：demo:lianjia:house_price:[城市]:[区域]
        score：日期（YYYYMMDD）
        value：[日期],[区域],[当天成交量],[当天房源带看量],[在售房源总套数],[最近90天成交套数]
    获取最近60天的记录的命令：
        zrange [key] -60 -1
'''
def set_area_sum_data(area, area_string, area_soup):
    # !!!在Python中，只有模块，类以及函数才会引入新的作用域，其它的代码块是不会引入新的作用域的。!!!
    # 以下都注释掉
    #     today_sold = 0 # 当天成交量
    #     today_check = 0 # 当天房源带看量
    #     on_sale = 0 # 在售房源总套数
    #     ninety_days_sold = 0 # 最近90天成交套数

    data_sold_check = area_soup.find_all('div',{"class":"num"}) # 对城市来说，第一个是上月均价，第二个是昨日成交量，第三个是昨日房源带看量；对区域来说只有第二和第三
    if area is area_arg: # 判断是否是市级别的数据
        today_sold = data_sold_check[1].contents[0].string
        today_check = data_sold_check[2].contents[0].string
    else:
        today_sold = data_sold_check[0].contents[0].string
        today_check = data_sold_check[1].contents[0].string
    data_sale_sold = area_soup.find_all('a',{"class":"txt"}) # 第一个是在售房源总套数，第二个是最近90天成交套数
    on_sale = data_sale_sold[0].contents[0].string[4:-1] # string例子：在售房源1980套，将数字截取出来
    ninety_days_sold = data_sale_sold[1].contents[0].string[10:-1] # string例子：最近90天内成交房源36051套，将数字截取出来

    if "暂无数据" in str(today_sold):
        today_sold = str(0)
    if "暂无数据" in str(today_check):
        today_check = str(0)
    if "暂无数" in str(on_sale):
        on_sale = str(0)
    if "暂无数" in str(ninety_days_sold):
        ninety_days_sold = str(0)

    area_key = module_prefix + area
    area_value = date_score + "," + area_string + "," + today_sold + "," + today_check + "," + on_sale + "," + ninety_days_sold
    print("key: ", area_key, "\nscore: ", date_score + "\nvalue: " + area_value)
    del_pipe.zremrangebyscore(area_key, date_score, date_score)
    pipe.zadd(area_key, date_score, area_value)

# 业务逻辑开始
area_arg = sys.argv[1] # 第一个参数
global_prefix = "demo:lianjia:"
module_prefix = global_prefix + "house_price:" + area_arg + ":"
date_score = (datetime.now() + timedelta(days = -1)).strftime("%Y%m%d")
# date_score = datetime.now().strftime("%Y%m%d") # 由于部署服务器在美国，时差的关系不需要减去1天，上线时调整过来
area_offset = 18 # TODO:这个区域偏移量只对北京有效，针对每个行政区下面小区域的处理逻辑，去除大的行政区域
r = redis.StrictRedis(host='localhost', port=6379, db=1)
pipe = r.pipeline(transaction=False)
del_pipe = r.pipeline() # 删除作为同一个事务

# 开始执行！
trade_spider(area_arg)