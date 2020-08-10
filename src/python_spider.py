# -*- coding: GB2312 -*-

"""
@Author : Lily
@Date   : 2020/4/8 9:16
@Desc   : python����-����ͳ�ƾ�ͳ������������ͳ��绮�ִ������� https://tding.top/archives/796bd537.html
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
from fake_useragent import UserAgent  #�����������ͷ

ua = UserAgent()
print(ua.ie)   #�����ӡie���������汾
print(ua.firefox) #�����ӡfirefox���������汾
print(ua.chrome)  #�����ӡchrome���������汾
print(ua.random)  #�����ӡ���⳧�ҵ������
"""

"""
1����ҳ��ȡ����
"""
def getUrl(url,num_retries=20):
    ua = UserAgent()
    headers = {'User-Agent':ua.random}
    try:
        response = requests.get(url,headers = headers)
        response.encoding = response.apparent_encoding  #��ȡ��վ���ַ�������
        data = response.content.decode('gbk')
        if num_retries <= 0:
            print("retry time too much!")
            return
        if response.status_code==502:
            print('502 Bad Gateway, retry!') # ����������࣬�ᱨ502���󣬻ᷢ���п�ֵ���ҳ���Щ��ֵ��������ȡ��
            time.sleep(10)
            return getUrl(url, num_retries - 1)
        return data
    except Exception as e: # ������ִ���������10s������
        if num_retries > 0:
            time.sleep(10)
            print(url)
            print("requests fail, retry!")
            return getUrl(url,num_retries-1) # �ݹ����
        else:
            print("retry fail!")
            print("error: {}".format(e + " " + url))
            return

"""
xpath��ȡ��ҳ���� selectorģ��https://blog.csdn.net/it_arookie/article/details/82825448
//title[@lang=��eng��]  ѡȡ���� title Ԫ�أ�����ЩԪ��ӵ��ֵΪ eng �� lang ���ԡ�
"""

"""
2����ȡʡ�����뺯��
"""
def getProvince(url):
    province = []
    data = getUrl(url)
    selector = etree.HTML(data)
    provinceList = selector.xpath('//tr[@class="provincetr"]') # һ��<tr class='provincetr'>Ϊҳ���е�һ�У������ж��С�
    for i in provinceList: # ����
        provinceName = i.xpath('td/a/text()')
        provinceLink = i.xpath('td/a/@href')
        for j in range(len(provinceLink)): # ��ȡÿһ�е�ʡ������URL
            provinceURL = url[:-10] + provinceLink[j] #���ݻ�ȡ����ÿ��ʡ�����ӽ��в�ȫ���õ���ʵ��URL��
            province.append({'province_code': provinceLink[j][:2], 'link': provinceURL,'province_name': provinceName[j]})
    print("ʡ�������ȡ��ϣ�")
    return  province

pro = getProvince("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/index.html")
df_province = pd.DataFrame(pro)
df_province['key']=df_province['link'].str[-7:-5]
df_province.to_csv('province.csv', sep=',', header=True, index=False)
# ��province.csv���鿴����©���ݡ�
"""
��ȡ�м����뺯��
/bookstore/book[1]  ѡȡ���� bookstore ��Ԫ�صĵ�һ�� book Ԫ�ء�
"""
def getCity(url_list):
    city_all = []
    for url in url_list:
        print("��ȡ��ʡ�����Ӧ�ĳ��д���:{}".format(url))
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
        city_all.extend(city)  # ����ʡ�ĳ�����Ϣ�ϲ���һ��
    print('���д����ȡ��ϣ�')
    return city_all

city = getCity(df_province['link'])
df_city = pd.DataFrame(city)
df_city['key_l']=df_city['link'].str[-9:-7]
df_city['key_r']=df_city['link'].str[-9:-5]
df_city.to_csv('city.csv', sep=',', header=True, index=False)

df=df_province.set_index('key').join(df_city.set_index('key_l'),lsuffix="_l")
df.to_csv('df.csv', sep=',', header=True, index=False)
# �鿴df.csv����©����

"""
��ȡ�������뺯��-���߳�ʵ��
"""


def getCounty(url_list):
    queue_county = Queue()  # ����
    thread_num = 10  # ������
    county = []  # ��¼������Ϣ���ֵ䣨ȫ�֣�

    def produce_url(url_list):
        for url in url_list:
            queue_county.put(url)  # ����URL������У��ȴ������߳���ȡ

    def getData():
        while not queue_county.empty():  # ��֤url�������������˳��߳�
            url = queue_county.get()  # �Ӷ����л�ȡURL
            data = getUrl(url)
            selector = etree.HTML(data)
            countyList = selector.xpath('//tr[@class="countytr"]')
            # ��������ȡÿ�����Ĵ��롢URL
            for i in countyList:
                countyCode = i.xpath('td[1]/a/text()')
                countyLink = i.xpath('td[1]/a/@href')
                countyName = i.xpath('td[2]/a/text()')
                # ����õ������б���ʽ�ģ����潫��ÿһ�����ֵ�洢
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
    print('���������ȡ��ϣ�')
    return county

county = getCounty(df_city['link'])
df_county = pd.DataFrame(county)
df_county['key_l']=df_county['link'].str[-11:-7]
df_county['key_r']=df_county['link'].str[-11:-5]
# ���ڶ��̵߳Ĺ�ϵ�����ݵ�˳���Ѿ������ң��������ﰴ����������� ������ ����
df_county_sorted = df_county.sort_values(by = ['county_code']) #��1�н�����������
df_county_sorted.to_csv('county.csv', sep=',', header=True, index=False)

df1=df.set_index('key_r').join(df_county.set_index('key_l'),lsuffix="_l_l")
df1.to_csv('df1.csv', sep=',', header=True, index=False)
#�����ݣ����������еı������������ģ��貹��
for index,item in df1[df1['city_name'].isin(['��ɽ��','��ݸ��','������'])].iterrows():
    df1.loc[index,'county_code']=item['city_code']
    df1.loc[index,'link']=item['link_l_l']
    df1.loc[index,'county_name']=item['city_name']
    df1.loc[index,'key_r']=item['link_l_l'][-9:-5]+'00'
# �鿴df1.csv����©����
"""
��ȡ�ֵ����뺯��-���߳�ʵ��
"""
def getTown(url_list):
    queue_town = Queue() #����
    thread_num = 50 #������
    town = [] #��¼�ֵ���Ϣ���ֵ䣨ȫ�֣�

    def produce_url(url_list):
        for url in url_list:
            queue_town.put(url) #����URL������У��ȴ������߳���ȡ

    def getData(url_list):
        for url in url_list: # ��֤url�������������˳��߳�
            #url = queue_town.get() # �Ӷ����л�ȡURL
            print("��ȡ�����������Ӧ�Ľֵ�����:{}".format(url))
            data = getUrl(url)
            print(data)
            selector = etree.HTML(data)
            townList = selector.xpath('//tr[@class="towntr"]')
            #��������ȡÿ�����Ĵ��롢URL
            for i in townList:
                townCode = i.xpath('td[1]/a/text()')
                townLink = i.xpath('td[1]/a/@href')
                townName = i.xpath('td[2]/a/text()')
                #����õ������б���ʽ�ģ����潫��ÿһ�����ֵ�洢
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
    print('�ֵ������ȡ��ϣ�')
    return town

# ������Ҫ����ɽ�С���ݸ�������⴦�����ǵı������һ��,���ǵĽֵ�������df_city�С�
url_list = list()
for url in df_county['link']:
    url_list.append(url)
town_link_list = df_city[df_city['city_name'].isin(['��ɽ��','��ݸ��','������'])]['link'].values
for town_link in town_link_list:
    url_list.append(town_link)
town = getTown(url_list)

df_town = pd.DataFrame(town)
df_town['key_l']=df_town['link'].str[-14:-8]
df_town['key_r']=df_town['link'].str[-14:-5]
df_town_sorted = df_town.sort_values(by = ['town_code']) #��1�н�����������
df_town_sorted.to_csv('town.csv', sep=',', header=True, index=False)

df2=df1.set_index('key_r').join(df_town.set_index('key_l'),lsuffix="_l_l_l")
df2.to_csv('df3.csv', sep=',', header=True, index=False)

# ����©�������ݡ�
for i in range(50):
    mkp = df2[df2['town_name'].isna()]
    if len(mkp)>0:
        print('��©��{}������'.format(len(mkp)))
        town = getTown(mkp['link_l_l_l'])
        df_town = pd.DataFrame(town)
        df_town['key_l']=df_town['link'].str[-14:-8]
        df_town['key_r']=df_town['link'].str[-14:-5]
        mkp=mkp[['province_code','link_l','province_name','city_code','link_l_l','city_name','county_code','link_l_l_l','county_name']].join(df_town.set_index('key_l'))
        df2=df2.drop(index=mkp.index)
        df2=pd.concat([df2,mkp])
    else:
        print('����������©��')
        break
"""
��ȡ��ί����뺯��-���߳�ʵ��
"""
def getVillage(url_list):
    queue_village = Queue()  # ����
    thread_num = 20  # ������
    village = []  # ��¼��ί����Ϣ���ֵ䣨ȫ�֣�

    def produce_url(url_list):
        for url in url_list:
            queue_village.put(url)  # ����URL������У��ȴ������߳���ȡ

    def getData(url_list):
        for url in url_list:  # ��֤url�������������˳��߳�
            # url = queue_village.get()  # �Ӷ����л�ȡURL
            # print("��ȡ�ýֵ������Ӧ�ľ�ί�����:{}".format(url))
            data = getUrl(url)
            print(data)
            selector = etree.HTML(data)
            villageList = selector.xpath('//tr[@class="villagetr"]')
            # ��������ȡÿ�����Ĵ��롢URL
            for i in villageList:
                villageCode = i.xpath('td[1]/text()')
                UrbanRuralCode = i.xpath('td[2]/text()')
                villageName = i.xpath('td[3]/text()')
                # ����õ������б���ʽ�ģ����潫��ÿһ�����ֵ�洢
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
    print('��ί������ȡ��ϣ�')
    return village


village = getVillage(df2['link'])
df_village = pd.DataFrame(village)
df_village = df_village.astype('str')
df_village['key']=df_village['village_code'].str[:9]
df_village_sorted = df_village.sort_values(by = ['village_code']) #��1�н�����������
df_village_sorted.to_csv('village.csv', sep=',', header=True, index=False)

df3=df2.set_index('key_r').join(df_village.set_index('key'),lsuffix="_l_l_l_l")
df3.to_csv('df3.csv', sep=',', header=True, index=False)
# ����©�������ݡ�
for i in range(10):
    mkp = df3[df3['village_name'].isna()]
    if len(mkp)>0:
        print('��©��{}������'.format(len(mkp)))
        Village = getVillage(mkp['link'])
        df_Village = pd.DataFrame(Village)
        df_Village['key'] = df_Village['village_code'].str[:9]
        mkp=mkp[['province_code','link_l','province_name','city_code','link_l_l','city_name','county_code','link_l_l_l','county_name',
                 'town_code','link','town_name']].join(df_Village.set_index('key'))
        df3=df3.drop(index=mkp.index)
        df3=pd.concat([df3,mkp],axis=0)
    else:
        print('����������©��')
        break

# map
UrbanRuralDict1 = {"100":"����","110":"����","111":"������","112":"��������","120":"����"
                  ,"121":"��������","122":"��������","123":"��������","200":"���","210":"��������","220":"��ׯ"}
UrbanRuralDict2 = {'1':'����','2':'ũ��'}

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


