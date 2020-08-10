# -*- coding: GB2312 -*-

"""
@Author : Lily
@Date   : 2020/4/8 9:16
@Desc   : python爬虫-国家统计局统计用区划代码和城乡划分代码爬虫 https://tding.top/archives/796bd537.html
"""
import requests
from lxml import etree
import csv
import time
import pandas as pd
from queue import Queue
from threading import Thread
from fake_useragent import UserAgent

"""
from fake_useragent import UserAgent  #生成随机请求头

ua = UserAgent()
print(ua.ie)   #随机打印ie浏览器任意版本
print(ua.firefox) #随机打印firefox浏览器任意版本
print(ua.chrome)  #随机打印chrome浏览器任意版本
print(ua.random)  #随机打印任意厂家的浏览器
"""

"""
1、网页爬取函数
"""
def getUrl(url,num_retries=20):
    ua = UserAgent()
    headers = {'User-Agent':ua.random}
    try:
        response = requests.get(url,headers = headers)
        response.encoding = response.apparent_encoding  #获取网站的字符集编码
        data = response.content.decode('gbk')
        if num_retries <= 0:
            print("retry time too much!")
            return
        if response.status_code==502:
            print('502 Bad Gateway, retry!') # 爬虫请求过多，会报502错误，会发现有空值。找出这些空值，重新爬取。
            time.sleep(10)
            return getUrl(url, num_retries - 1)
        return data
    except Exception as e: # 如果出现错误，则休眠10s后重试
        if num_retries > 0:
            time.sleep(10)
            print(url)
            print("requests fail, retry!")
            return getUrl(url,num_retries-1) # 递归调用
        else:
            print("retry fail!")
            print("error: {}".format(e + " " + url))
            return

"""
xpath提取网页内容 selector模块https://blog.csdn.net/it_arookie/article/details/82825448
//title[@lang=‘eng’]  选取所有 title 元素，且这些元素拥有值为 eng 的 lang 属性。
"""

"""
2、获取省级代码函数
"""
def getProvince(url):
    province = []
    data = getUrl(url)
    selector = etree.HTML(data)
    provinceList = selector.xpath('//tr[@class="provincetr"]') # 一个<tr class='provincetr'>为页面中的一行，这里有多行。
    for i in provinceList: # 多行
        provinceName = i.xpath('td/a/text()')
        provinceLink = i.xpath('td/a/@href')
        for j in range(len(provinceLink)): # 提取每一行的省份名和URL
            provinceURL = url[:-10] + provinceLink[j] #根据获取到的每个省的链接进行补全，得到真实的URL。
            province.append({'province_code': provinceLink[j][:2], 'link': provinceURL,'province_name': provinceName[j]})
    print("省级代码获取完毕！")
    return  province

pro = getProvince("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/index.html")
df_province = pd.DataFrame(pro)
df_province['key']=df_province['link'].str[-7:-5]
df_province.to_csv('province.csv', sep=',', header=True, index=False)
# 打开province.csv，查看有无漏数据。
"""
获取市级代码函数
/bookstore/book[1]  选取属于 bookstore 子元素的第一个 book 元素。
"""
def getCity(url_list):
    city_all = []
    for url in url_list:
        print("获取该省代码对应的城市代码:{}".format(url))
        data = getUrl(url)
        selector = etree.HTML(data)
        cityList = selector.xpath('//tr[@class="citytr"]')
        city = []
        for i in cityList:
            cityCode = i.xpath('td[1]/a/text()')
            cityLink = i.xpath('td[1]/a/@href')
            cityName = i.xpath('td[2]/a/text()')
            for j in range(len(cityLink)):
                cityURL = url[:-7]+cityLink[j]
                city.append({ 'city_code': cityCode[j], 'link': cityURL,'city_name': cityName[j]})
        city_all.extend(city)  # 所有省的城市信息合并在一起
    print('城市代码获取完毕！')
    return city_all

city = getCity(df_province['link'])
df_city = pd.DataFrame(city)
df_city['key_l']=df_city['link'].str[-9:-7]
df_city['key_r']=df_city['link'].str[-9:-5]
df_city.to_csv('city.csv', sep=',', header=True, index=False)

df=df_province.set_index('key').join(df_city.set_index('key_l'),lsuffix="_l")
df.to_csv('df.csv', sep=',', header=True, index=False)
# 查看df.csv有无漏数据

"""
获取区级代码函数-多线程实现
"""


def getCounty(url_list):
    queue_county = Queue()  # 队列
    thread_num = 10  # 进程数
    county = []  # 记录区级信息的字典（全局）

    def produce_url(url_list):
        for url in url_list:
            queue_county.put(url)  # 生成URL存入队列，等待其他线程提取

    def getData():
        while not queue_county.empty():  # 保证url遍历结束后能退出线程
            url = queue_county.get()  # 从队列中获取URL
            data = getUrl(url)
            selector = etree.HTML(data)
            countyList = selector.xpath('//tr[@class="countytr"]')
            # 下面是爬取每个区的代码、URL
            for i in countyList:
                countyCode = i.xpath('td[1]/a/text()')
                countyLink = i.xpath('td[1]/a/@href')
                countyName = i.xpath('td[2]/a/text()')
                # 上面得到的是列表形式的，下面将其每一个用字典存储
                for j in range(len(countyLink)):
                    countyURL = url[:-9] + countyLink[j]
                    county.append({'county_code': countyCode[j], 'link': countyURL, 'county_name': countyName[j]})

    def run(url_list):
        produce_url(url_list)

        ths = []
        for _ in range(thread_num):
            th = Thread(target=getData)
            th.start()
            ths.append(th)
        for th in ths:
            th.join()

    run(url_list)
    print('区级代码获取完毕！')
    return county

county = getCounty(df_city['link'])
df_county = pd.DataFrame(county)
df_county['key_l']=df_county['link'].str[-11:-7]
df_county['key_r']=df_county['link'].str[-11:-5]
# 由于多线程的关系，数据的顺序已经被打乱，所以这里按照区代码进行 “升序” 排序。
df_county_sorted = df_county.sort_values(by = ['county_code']) #按1列进行升序排序
df_county_sorted.to_csv('county.csv', sep=',', header=True, index=False)

df1=df.set_index('key_r').join(df_county.set_index('key_l'),lsuffix="_l_l")
df1.to_csv('df1.csv', sep=',', header=True, index=False)
#补数据，这三个城市的编码规则是特殊的，需补。
for index,item in df1[df1['city_name'].isin(['中山市','东莞市','儋州市'])].iterrows():
    df1.loc[index,'county_code']=item['city_code']
    df1.loc[index,'link']=item['link_l_l']
    df1.loc[index,'county_name']=item['city_name']
    df1.loc[index,'key_r']=item['link_l_l'][-9:-5]+'00'
# 查看df1.csv有无漏数据
"""
获取街道代码函数-多线程实现
"""
def getTown(url_list):
    queue_town = Queue() #队列
    thread_num = 50 #进程数
    town = [] #记录街道信息的字典（全局）

    def produce_url(url_list):
        for url in url_list:
            queue_town.put(url) #生成URL存入队列，等待其他线程提取

    def getData(url_list):
        for url in url_list: # 保证url遍历结束后能退出线程
            #url = queue_town.get() # 从队列中获取URL
            print("获取该区级代码对应的街道代码:{}".format(url))
            data = getUrl(url)
            print(data)
            selector = etree.HTML(data)
            townList = selector.xpath('//tr[@class="towntr"]')
            #下面是爬取每个区的代码、URL
            for i in townList:
                townCode = i.xpath('td[1]/a/text()')
                townLink = i.xpath('td[1]/a/@href')
                townName = i.xpath('td[2]/a/text()')
                #上面得到的是列表形式的，下面将其每一个用字典存储
                for j in range(len(townLink)):
                    townURL = url[:-11] + townLink[j]
                    town.append({'town_code':townCode[j],'link':townURL,'town_name':townName[j]})

    def run(url_list):
        produce_url(url_list)

        ths = []
        for _ in range(thread_num):
            th = Thread(target = getData)
            th.start()
            ths.append(th)
        for th in ths:
            th.join()

    getData(url_list)
    print('街道代码获取完毕！')
    return town

# 这里需要对中山市、东莞市做特殊处理，它们的编码规则不一样,它们的街道链接在df_city中。
url_list = list()
for url in df_county['link']:
    url_list.append(url)
town_link_list = df_city[df_city['city_name'].isin(['中山市','东莞市','儋州市'])]['link'].values
for town_link in town_link_list:
    url_list.append(town_link)
town = getTown(url_list)

df_town = pd.DataFrame(town)
df_town['key_l']=df_town['link'].str[-14:-8]
df_town['key_r']=df_town['link'].str[-14:-5]
df_town_sorted = df_town.sort_values(by = ['town_code']) #按1列进行升序排序
df_town_sorted.to_csv('town.csv', sep=',', header=True, index=False)

df2=df1.set_index('key_r').join(df_town.set_index('key_l'),lsuffix="_l_l_l")
df2.to_csv('df3.csv', sep=',', header=True, index=False)

# 查找漏掉的数据。
for i in range(50):
    mkp = df2[df2['town_name'].isna()]
    if len(mkp)>0:
        print('有漏掉{}条数据'.format(len(mkp)))
        town = getTown(mkp['link_l_l_l'])
        df_town = pd.DataFrame(town)
        df_town['key_l']=df_town['link'].str[-14:-8]
        df_town['key_r']=df_town['link'].str[-14:-5]
        mkp=mkp[['province_code','link_l','province_name','city_code','link_l_l','city_name','county_code','link_l_l_l','county_name']].join(df_town.set_index('key_l'))
        df2=df2.drop(index=mkp.index)
        df2=pd.concat([df2,mkp])
    else:
        print('已无数据遗漏！')
        break
"""
获取居委会代码函数-多线程实现
"""
def getVillage(url_list):
    queue_village = Queue()  # 队列
    thread_num = 20  # 进程数
    village = []  # 记录居委会信息的字典（全局）

    def produce_url(url_list):
        for url in url_list:
            queue_village.put(url)  # 生成URL存入队列，等待其他线程提取

    def getData(url_list):
        for url in url_list:  # 保证url遍历结束后能退出线程
            # url = queue_village.get()  # 从队列中获取URL
            # print("获取该街道代码对应的居委会代码:{}".format(url))
            data = getUrl(url)
            print(data)
            selector = etree.HTML(data)
            villageList = selector.xpath('//tr[@class="villagetr"]')
            # 下面是爬取每个区的代码、URL
            for i in villageList:
                villageCode = i.xpath('td[1]/text()')
                UrbanRuralCode = i.xpath('td[2]/text()')
                villageName = i.xpath('td[3]/text()')
                # 上面得到的是列表形式的，下面将其每一个用字典存储
                for j in range(len(villageCode)):
                    village.append({'village_code': villageCode[j], 'UrbanRuralCode': UrbanRuralCode[j], 'village_name': villageName[j]})

    def run(url_list):
        produce_url(url_list)

        ths = []
        for _ in range(thread_num):
            th = Thread(target=getData)
            th.start()
            ths.append(th)
        for th in ths:
            th.join()

    getData(url_list)
    print('居委会代码获取完毕！')
    return village


village = getVillage(df2['link'])
df_village = pd.DataFrame(village)
df_village = df_village.astype('str')
df_village['key']=df_village['village_code'].str[:9]
df_village_sorted = df_village.sort_values(by = ['village_code']) #按1列进行升序排序
df_village_sorted.to_csv('village.csv', sep=',', header=True, index=False)

df3=df2.set_index('key_r').join(df_village.set_index('key'),lsuffix="_l_l_l_l")
df3.to_csv('df3.csv', sep=',', header=True, index=False)
# 查找漏掉的数据。
for i in range(10):
    mkp = df3[df3['village_name'].isna()]
    if len(mkp)>0:
        print('有漏掉{}条数据'.format(len(mkp)))
        Village = getVillage(mkp['link'])
        df_Village = pd.DataFrame(Village)
        df_Village['key'] = df_Village['village_code'].str[:9]
        mkp=mkp[['province_code','link_l','province_name','city_code','link_l_l','city_name','county_code','link_l_l_l','county_name',
                 'town_code','link','town_name']].join(df_Village.set_index('key'))
        df3=df3.drop(index=mkp.index)
        df3=pd.concat([df3,mkp],axis=0)
    else:
        print('已无数据遗漏！')
        break

# map
UrbanRuralDict1 = {"100":"城镇","110":"城区","111":"主城区","112":"城乡结合区","120":"镇区"
                  ,"121":"镇中心区","122":"镇乡结合区","123":"特殊区域","200":"乡村","210":"乡中心区","220":"村庄"}
UrbanRuralDict2 = {'1':'城市','2':'农村'}

df3['UrbanRural'] = df3['UrbanRuralCode'].map(UrbanRuralDict1)
df3['UrbanRural2'] = df3.UrbanRuralCode.str[-3:-2].map(UrbanRuralDict2)

df3[['province_code','province_name','city_code','city_name','county_code','county_name','town_code','town_name',
'village_code','village_name','UrbanRuralCode','UrbanRural','UrbanRural2']].to_csv('df3.csv', sep=',', header=True, index=False, encoding='gbk')

import re
regex='.*([a-zA-Z\?]+[\u4E00-\u9FA5]+)+.*'
file=r"data.csv"
with open(file,encoding='gbk') as f:
    df3=pd.read_csv(f)
df3=df3.set_index('village_code')
count=0;url_list=[]
for tup in df3.itertuples():
    if not (re.match(regex,tup.village_name) is None):
        url_list.append(tup.link)
        count = count + 1
url_list=set(url_list)
print(count)
village = getVillage(url_list)
df_village = pd.DataFrame(village)
df_village['key']=df_village['village_code'].str[:9]
df_village=df_village.set_index('key')
a=set(df_village.index)
for i in a:
    try:
        df3.loc[df3['key']==i,'village_name']=df_village.loc[i,'village_name'].values
        print(i)
    except:
        pass
# df3.to_csv('df3.csv', sep=',', header=True, index=True)


