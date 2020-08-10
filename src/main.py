# -*- coding: utf-8 -*-

"""
@Author : Lily
@Date   : 2020/7/8 17:54
@Desc   : 通过地址爬取经纬度
"""
from __future__ import unicode_literals
import requests
import pandas as pd
from src.db import postgresql
from src.conf import Config
from src.utils.logfactory import LogFactory
import traceback
import time
import os

class address_api():
    def __init__(self):
        self.logging = LogFactory()
        self.path = os.getcwd()
        self.conn = postgresql()
        self.config_data = Config().config_data

    def cut_csv(self):
        """
        由于爬取次数限制，30万/天，将文件切割分别爬取
        :return:
        """
        df = self.conn.pandas_readsql(sql=self.config_data.get('GET_DATA'))
        num = len(df)//6 # 将文件切割为6份爬取
        for i in range(6):
            data = df.iloc[i*(num+1) : (i+1)*(num+1)]
            data.to_csv(path_or_buf=self.path+r'/data/address{}.csv'.format(i),index=False,header=True)
        return

    def parse(self):
        """
        读取文件
        :return:
        """
        with open(file=self.path+r'/data/test.csv',encoding='gbk') as f:
            df = pd.read_csv(f, dtype='object')
        return df['str_code'].values,df['address'].values,df['city'].values

    def search_location(self,address,city):
        """
        将地址编码为经纬度
        :param address:
        :param city:
        :return:
        """
        params = {'address': address,
                  'city': city,
                  'key': self.config_data.get('KEY')}
        base_url = self.config_data.get('GEO_URL')
        try:
            response = requests.get(url=base_url, params=params)
            json_data = response.json()
            if json_data['status'] == '1' and len(json_data['geocodes']) > 0:
                return json_data['geocodes'][0]['location'] if json_data['geocodes'][0]['location'] else 'errorlocation'
            time.sleep(1)
            return 'errorlocation'
        except:
            self.logging.error(traceback.format_exc())
            return 'errorlocation'

    def search_address_detail(self,location):
        """
        将经纬度反编码为地址
        :param location:
        :return:
        """
        params = {'location': location,
                  'key': self.config_data.get('KEY')}
        base_url = self.config_data.get('REGEO_URL')
        try:
            response = requests.get(url=base_url, params=params)
            json_data = response.json()
            if json_data['status'] == '1' and json_data['regeocode']['addressComponent']['township'] and json_data['regeocode']['addressComponent']['district']:
                return json_data['regeocode']['addressComponent']['district'],json_data['regeocode']['addressComponent']['township']
            return 'errorSearchAddress', 'errorSearchAddress'
        except:
            self.logging.error(traceback.format_exc())
            return 'errorSearchAddress','errorSearchAddress'

    def main(self):
        id, address_list, city_list = self.parse()
        index = 0
        for id, real_address, city in zip(id, address_list, city_list):
            location_str = self.search_location(address=real_address, city=city)
            str_list = location_str.split(',')
            if len(str_list)==2:
                try:
                    longitude,latitude = location_str.split(',')
                    self.conn.update_pg(query=self.config_data.get('INSERT_DATA').format(longitude,latitude,id))
                    self.logging.info("insert id={}, longitude={}, latitude={} success!".format(id,longitude,latitude))
                except:
                    self.logging.info("insert id={} fail!".format(id))
                    self.logging.error(traceback.format_exc())
            else:
                self.logging.info("insert id={} fail!".format(id))
            index = index + 1
            self.logging.info("query record {}...".format(index))
        return

if __name__ == '__main__':
    address_api = address_api()
    address_api.main()
