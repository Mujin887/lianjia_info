import requests
from bs4 import BeautifulSoup
import redis
from datetime import datetime, timedelta

global_pre_fix = "demo:lianjia:"
yesterday = (datetime.now() + timedelta(days = -1)).strftime("%Y-%m-%d")
self_pre_fix = global_pre_fix + yesterday + ":"
print(self_pre_fix)
r = redis.StrictRedis(host='localhost', port=6379, db=0)
pipe = r.pipeline(transaction=False)


def trade_spider():
    url = 'http://bj.lianjia.com/fangjia/'
    soup = get_url_soup(url)
    set_area_sum_data('beijing', soup)
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

def set_area_sum_data(area, area_soup):
    pre_fix = self_pre_fix + area
    today_sold = 0 # 当天成交量
    today_check = 0 # 当天房源带看量
    on_sale = 0 # 在售房源总套数
    ninety_days_sold = 0 # 最近90天成交套数




'''
    source_code = requests.get(area_url)
    plain_text = source_code.text
    soup = BeautifulSoup(plain_text,"html.parser")
    for director in soup.find_all('a',{"rel":"v:directedBy"}):
        print("导演:",director.string)
'''

trade_spider()