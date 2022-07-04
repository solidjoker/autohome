# %%
import asyncio
from asyncio import coroutines
import re
import json

import pandas as pd
import bs4
from functools import reduce

from async_spider import Async_Spider
from autohome_model import Autohome_Model
from autohome_dealer import Autohome_Dealer


# %%
class Autohome_Dealer_Price(Async_Spider):
    name = 'autohome_dealer_price'

    def __init__(
        self,
        max_retry=3,
        async_num=100,
        threading_init_driver=False,
    ):
        super().__init__(max_retry=max_retry,
                         async_num=async_num,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion()

    def init_preparetion(self):

        self.url_dealer_price = 'https://carif.api.autohome.com.cn/dealer/LoadDealerPrice.ashx?_callback=LoadDealerPrice&type=1&seriesid={model_id}&city={city_id}'
        self.url_spec = 'https://www.autohome.com.cn/spec/{spec_id}/'

        self.url_test = 'https://carif.api.autohome.com.cn/dealer/LoadDealerPrice.ashx?_callback=LoadDealerPrice&type=1&seriesid=3554&city=310100'

        self.dirname_dp_output = self.dirname_output.joinpath('dealer_price')
        if not self.dirname_dp_output.exists():
            self.dirname_dp_output.mkdir()

        self.result_columns = {
            'SpecId': 'spec_id',
            'spec_name': 'spec_name',
            'SeriesId': 'model_id',
            'Price': 'price',
            'MinPrice': 'min_price',
            'OriginalPrice': 'original_price',
            'Url': 'url_dealer_spec',
            'DealerId': 'dealer_id',
            'CityName': 'name_city',
            'CityId': 'id_city'
        }

        self.df_models = Autohome_Model(verbose=False).read_exist_data(
            clipboard=False)
        self.autohome_dealer = Autohome_Dealer()

    def read_df_dealer_price_by_model_id(self, model_id, opencsv=True):
        filename = self.dirname_dp_output.joinpath('%s.pkl' % model_id)
        if filename.exists():
            df_exist = pd.read_pickle(filename)
        else:
            df_exist = self.get_df_dealer_price_by_model_id(model_id,
                                                            opencsv=False)
        if opencsv:
            self.df_to_csv(df_exist)
        return df_exist

    def get_df_dealer_price_by_model_id(self, model_id, opencsv=True):

        # 获取brand_id
        model_id = '3554'
        brand_id = self.df_vlookup(self.df_models,
                                   'model_id',
                                   'brand_id',
                                   str(model_id),
                                   top=True)
        # 获取市场清单
        df_brand_dealer = self.autohome_dealer.get_df_brand_dealer_by_id(
            brand_id=brand_id)
        city_ids = df_brand_dealer['CID'].drop_duplicates().to_list()

        coroutines = [
            self.async_get_response(
                self.url_dealer_price.format(model_id=model_id,
                                             city_id=city_id))
            for city_id in city_ids
        ]
        tasks = self.run_async_loop(coroutines, tqdm_desc='async_get_dealer_price')
        # 处理结果
        dfs = []
        for task in tasks:
            df = self.fetch_dealer_price_from_response(task.result())
            if df is not None:
                dfs.append(df)
        df_dealer_price = pd.concat(dfs)

        df_dealer_price = df_dealer_price.rename(columns=self.result_columns)
        df_dealer_price = df_dealer_price.drop_duplicates(
            ['dealer_id', 'model_id', 'spec_id'])
        df_dealer_price = df_dealer_price.sort_values(['model_id', 'spec_id'])
        df_dealer_price['model_id'] = df_dealer_price['model_id'].astype(str)
        # 获取spec_name
        spec_ids = df_dealer_price['spec_id'].drop_duplicates().to_list()
        spec_names = self.get_spec_names(spec_ids)
        df_dealer_price['spec_name'] = df_dealer_price['spec_id'].apply(lambda x: spec_names.get(x))


        return df_dealer_price

        if opencsv:
            self.df_to_csv(df_dealer_price.merge(self.df_models, how='left'))

        # 保存
        filename = self.dirname_dp_output.joinpath('%s.pkl' % model_id)
        df_dealer_price.to_pickle(filename)
        return df_dealer_price

    def fetch_dealer_price_from_response(self, response):
        try:
            data = json.loads(
                response.text.replace('LoadDealerPrice', '')[1:-1])
            page = data['body']['pages']
            items = data['body']['item']
            df_price = pd.DataFrame(items)
            return df_price
        except Exception as e:
            print(e)

    def get_spec_names(self, spec_ids):
        names = {}
        coroutines = [
            self.async_get_response(self.url_spec.format(spec_id=spec_id),
            meta={'spec_id':spec_id})
            for spec_id in spec_ids
        ]
        tasks = self.run_async_loop(coroutines,tqdm_desc='async_get_spec_names')
        # return tasks
        for task in tasks:
            try:
                response = task.result()
                end = response.content.find(b'</title>')
                text = response.content[:end].decode('gb2312')
                start = text.find('【图】')
                end = text.find('报价_图片')
                names[response.meta['spec_id']] = text[start + len('【图】'):end]
            except Exception as e:
                print(e)
                print(response.url)
        return names


# %%
# 按照model_ids更新价格
if __name__ == '__main__':
    self = Autohome_Dealer_Price()
    model_id = '3554'
    df_dealer_price = self.get_df_dealer_price_by_model_id(model_id)

# %%