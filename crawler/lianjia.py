import requests
from bs4 import BeautifulSoup
import redis
from datetime import datetime, timedelta
import sys

def trade_spider(area_arg):
    url = 'http://' + area_arg + '.lianjia.com/fangjia/'
    soup = get_url_soup(url)
    set_area_sum_data(area_arg, soup)
    for link in soup.find_all('div',{'class':'hide'}):
        for location in link.find_all('a'):
            location_url = "http://bj.lianjia.com"+location.get('href')
            # print(location.string,location_url)
            location_soup = get_url_soup(location_url)
            area = location_url.split("/")[-2]
            set_area_sum_data(area, location_soup)
    result_list = pipe.execute()

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
        value：[日期],[当天成交量],[当天房源带看量],[在售房源总套数],[最近90天成交套数]
    获取最近60天的记录的命令：
        zrange [key] -60 -1
'''
def set_area_sum_data(area, area_soup):
    area_prefix = module_prefix + area
    print(area_prefix)
    # pipe.zadd()
    today_sold = 0 # 当天成交量
    today_check = 0 # 当天房源带看量
    on_sale = 0 # 在售房源总套数
    ninety_days_sold = 0 # 最近90天成交套数

    data_sold_check = area_soup.find_all('div',{"class":"num"}) # 对城市来说，第一个是上月均价，第二个是昨日成交量，第三个是昨日房源带看量；对区域来说只有第二和第三
    if area is area_arg:
        today_sold = data_sold_check[1].contents[0].string
        today_check = data_sold_check[2].contents[0].string
    else:
        today_sold = data_sold_check[0].contents[0].string
        today_check = data_sold_check[1].contents[0].string
    if today_sold is "暂无数据":
        today_sold = 0
    if today_check is "暂无数据":
        today_check = 0
    print(today_sold, today_check)
    data_on_sale = area_soup.find_all(text="在售房源")
    print(data_on_sale)
    data_old_sold = area_soup.find_all(text="最近90天内成交房源")
    print(data_old_sold)


# 业务逻辑开始
area_arg = sys.argv[1] # 第一个参数
global_prefix = "demo:lianjia:"
module_prefix = global_prefix + "house_price:" + area_arg + ":"
yesterday_score = (datetime.now() + timedelta(days = -1)).strftime("%Y%m%d")
r = redis.StrictRedis(host='localhost', port=6379, db=0)
pipe = r.pipeline(transaction=False)
trade_spider(area_arg)