# %%
import asyncio
import aiohttp
import json
import time
import pandas as pd
import datetime
from tqdm.auto import tqdm

from async_spider import Async_Spider
from autohome_model import Autohome_Model
from autohome_dealer import Autohome_Dealer

# %%


async def aiohttp_fetch(session, url, data):
    async with session.post(url, data=data) as response:
        return await response.text(), response.status


async def aiohttp_main(url, data):
    async with aiohttp.ClientSession() as session:
        response, status_code = await aiohttp_fetch(session, url, data)
        return response, status_code


class Autohome_Sale(Async_Spider):
    name = 'autohome_sale'

    def __init__(self, max_retry=3, async_num=100, threading_init_driver=False, group_num=200, max_update_interval=90, sleep=1):
        super().__init__(max_retry=max_retry,
                         async_num=async_num,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion(group_num, max_update_interval, sleep)

    def init_preparetion(self, group_num, max_update_interval, sleep):
        # 预读信息
        self.df_models = Autohome_Model(
            verbose=False).get_df_models(False, False, False)
        self.model_names = self.df_models['model_name'].to_list()
        self.df_province_city = Autohome_Dealer().get_df_province_city(False, False)
        self.province_dict = {k: v for k, v in self.df_province_city[[
            'id_province', 'name_province']].drop_duplicates().values}

        self.group_num = group_num  # 分组最大数值
        self.sd = '2016-01-01'  # 数据最早从2016年开始
        self.ed = datetime.date(
            self.today.year, self.today.month, 1).isoformat()
        self.max_update_interval = max_update_interval  # 最大更新间隔
        self.sleep = sleep

        self.url_home = 'https://www.autohome.com.cn/hangye/carkanban/?type=2'  # 人工访问网站
        self.url_api = 'https://www.autohome.com.cn/Channel2/Conjunction/ashx/GetDataReportCarKanban.ashx'
        self.dirname_output_sales = self.dirname_output.joinpath('sales')
        if not self.dirname_output_sales.exists():
            self.dirname_output_sales.mkdir()

    def decorater_save_data(func):
        def wrap(self, *args, **kwargs):
            # 读取现有数据
            province_name = kwargs['province_name']
            force = kwargs['force']
            df_exist = self.read_exist_data(province_name, False)
            # 判断是否更新, 如果不需要更新, 直接返回
            if force is False and self.check_need_update(province_name, False) is False:
                return df_exist
            res = func(self, *args, **kwargs)
            if res is not None:
                # 与先前结果合并保存
                if df_exist is None:
                    df = res
                else:
                    df = pd.concat([df_exist, res])
                df = df.drop_duplicates()
                df = df.sort_values(['dt', 'model_name', 'province_name'])
                df = df.reset_index(drop=True)
                filename = self.dirname_output_sales.joinpath(
                    'result_%s.pkl' % province_name)
                df.to_pickle(filename)
            return res
        return wrap

    def check_need_update(self, province_name, verbose=True):
        # 如果60天没有更新，强制更新
        df_exist = self.read_exist_data(province_name, False)
        try:
            max_dt = datetime.date.fromisoformat(df_exist['dt'].max())
            ed = datetime.date.fromisoformat(self.ed)
            if (ed - max_dt).days <= self.max_update_interval:
                if verbose:
                    print('%s does not need to update data' % province_name)
                return False
        except Exception as e:
            pass
        if verbose:
            print('%s need to update data' % province_name)
        return True

    def read_exist_data(self, province_name=None, opencsv=True):
        filename = self.dirname_output_sales.joinpath(
            'result_%s.pkl' % province_name)
        if filename.exists():
            df_exist = pd.read_pickle(filename)
        else:
            df_exist = pd.DataFrame()
        if opencsv:
            self.df_to_csv(df_exist)
        return df_exist

    def make_data_payload(self, model_name, province_id=None):
        # model_name可以是车型也可以是车型列表
        if isinstance(model_name, list):
            models = model_name
        elif isinstance(model_name, str):
            models = [model_name]

        # 填充数据
        data = {
            "start": self.sd,
            "end": self.ed,
            "seriesnames": models,
            "brandids": [],
            "factoryids": [],
            "energytypes": [],
            "seriesplaces": [],
            "carlevelnames": [],
            "displacementtypes": [],
            "pricetypes": [],
            "areanames": [],
            "provinceids": [],
            "citylevel": [],
            "type": "2"
        }
        if province_id is not None:
            data.update({'provinceids': [province_id]})
        data = json.dumps(data)
        return data

    def get_all_df_sales(self, force=False, opencsv=True):
        # 更新所有市场的销量
        province_names = [v for v in self.province_dict.values() if v not in [
            '澳门', '香港', '台湾']]
        df_sales = []
        with tqdm(desc='get_all_df_sales', total=len(province_names)) as pbar:
            for province_name in province_names:
                pbar.update(1)
                time.sleep(self.sleep)
                _df_sale = self.get_df_sale_by_province_name(
                    province_name=province_name, force=force, opencsv=False)
                df_sales.append(_df_sale)
        df_sale = pd.concat(df_sales)
        if opencsv:
            self.df_to_csv(df_sale)
        return df_sale

    @decorater_save_data
    def get_df_sale_by_province_name(self, province_name=None, force=False, opencsv=True):
        if province_name == '全国':
            province_id = None
        else:
            province_id = {v: k for k, v in self.province_dict.items()
                           }.get(province_name)
        df_sale = self.get_df_sale_by_province_id(province_id=province_id)
        if opencsv:
            self.df_to_csv(df_sale)
        return df_sale

    def get_df_sale_by_province_id(self, province_id=None):
        # 车型分组
        lenth = len(self.model_names)
        model_names = self.split_groups(
            self.model_names, (lenth-1)//self.group_num+1)
        # 获取数据
        df_sales = []
        with tqdm(desc='fecth sales in province_id:%s province_name:%s' % (
                province_id, self.province_dict.get(province_id)), total=len(model_names)) as pbar:
            for model_name in model_names:
                pbar.update(1)
                time.sleep(self.sleep)
                data = self.make_data_payload(model_name, province_id)
                loop = self.init_async_loop()
                response, status_code = loop.run_until_complete(
                    aiohttp_main(self.url_api, data=data))
                if status_code == 200:
                    _df_sale = self.fetch_df_sale_from_response(response)
                else:
                    _df_sale = self.process_404(model_name, province_id)

                # response = self.session.post(self.url_api, data=data)
                # if response.status_code == 200:
                #     _df_sale = self.fetch_df_sale_from_response(response)
                # else:
                #     _df_sale = self.process_404(model_name, province_id)
                if _df_sale is not None:
                    print(len(df_sale))
                    df_sales.append(_df_sale)
        if len(df_sales):
            df_sale = pd.concat(df_sales)
            df_sale['province_id'] = province_id
            if province_id is not None:
                df_sale['province_name'] = df_sale['province_id'].apply(
                    self.province_dict.get)
            else:
                df_sale['province_name'] = '全国'
            return df_sale

    def process_404(self, model_name, province_id):
        # model_name 是组
        if isinstance(model_name, list):
            model_names = model_name
        else:
            model_names = [model_name]
        df_sales = []
        with tqdm(desc='process 404', total=len(model_names)) as pbar:
            for model_name in model_names:
                pbar.update(1)
                time.sleep(self.sleep)
                data = self.make_data_payload(model_name, province_id)
                response = self.session.post(self.url_api, data=data)
                _df_sale = self.fetch_df_sale_from_response(response)
                if _df_sale is not None:
                    df_sales.append(_df_sale)
        if len(df_sales):
            df_sale = pd.concat(df_sales)
            return df_sale

    def fetch_df_sale_from_response(self, response):
        try:
            data = response.json()
            data = data['cheqikanbanOpt']
            df_sale = pd.concat(
                map(self.process_df_sale_data, data.keys(), data.values()))
            return df_sale
        except Exception as e:
            pass

    def process_df_sale_data(self, key, value):
        df = pd.DataFrame(value)
        df['model_name'] = key
        return df

    def test_get_df_sale_by_model_name_province_id(self, model_names=None, province_id=None):
        # 车型分组
        lenth = len(model_names)
        model_names = self.split_groups(
            model_names, (lenth-1)//self.group_num+1)
        # 获取数据
        df_sales = []
        responses = []
        with tqdm(desc='fecth sales in province_id:%s province_name:%s' % (
                province_id, self.province_dict.get(province_id)), total=len(model_names)) as pbar:
            for model_name in model_names:
                pbar.update(1)
                data = self.make_data_payload(model_name, province_id)
                response = self.session.post(self.url_api, data=data)
                responses.append(response)
                _df_sale = self.fetch_df_sale_from_response(response)
                if _df_sale is not None:
                    df_sales.append(_df_sale)
        return df_sales, responses


# %%
# 根据省份名称更新销量
if __name__ == '__main__':
    self = Autohome_Sale()
    for k, v in self.province_dict.items():
        print(k, v)
    print('---')
    province_name = '上海'
    force = False
    self.check_need_update(province_name)
    df_sale = self.get_df_sale_by_province_name(
        province_name=province_name, force=force)
    print('df_sale has lenth:%s' % len(df_sale))
# %%
# 测试
if __name__ == '__main__':
    self = Autohome_Sale()
    df_sales, responses = self.test_get_df_sale_by_model_name_province_id(
        self.model_names, None)

# %%
# %%