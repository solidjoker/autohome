# %%
import requests
import re
import time
import json
import pandas as pd
import bs4
from tqdm.auto import tqdm

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from async_spider import Async_Spider

from autohome_model import Autohome_Model
from autohome_dealer import Autohome_Dealer

# %%


class Autohome_Sales(Async_Spider):
    name = 'autohome_sales'

    def __init__(self, max_retry=3, async_num=100, threading_init_driver=False):
        super().__init__(max_retry=max_retry,
                         async_num=async_num,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion()

    def init_preparetion(self):
        # 数据最早从2016年开始
        self.df_models = Autohome_Model(
            verbose=False).get_df_models(False, False, False)
        self.df_province_city = Autohome_Dealer().get_df_province_city(False, False)

        self.url_home = 'https://www.autohome.com.cn/hangye/carkanban/?type=2'
        self.url_api = 'https://www.autohome.com.cn/Channel2/Conjunction/ashx/GetDataReportCarKanban.ashx'
        self.dirname_output_sales = self.dirname_output.joinpath('sales')
        if not self.dirname_output_sales.exists():
            self.dirname_output_sales.mkdir()

    def read_exist_data(self, clipboard=True):
        if self.result_pkl.exists():
            df_exist = pd.read_pickle(self.result_pkl)
        else:
            df_exist = pd.DataFrame()
        if clipboard:
            self.df_to_clipboard(df_exist)
        return df_exist

    def make_data_payload(self, sd, ed, model_name, province_id=None):
        data = {
            "start": sd,  # "2022-12-01"
            "end": ed,
            "seriesnames": [model_name],
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
            "type": "2"}
        if province_id is not None:
            data.update({'provinceids': [province_id]})
        data = json.dumps(data)
        return data

    def get_df_sales_by_province(self, sd=None, ed=None, province_id=None):

        coroutines = [self.async_post_response(
            url=self.url_api,
            data=self.make_data_payload(sd, ed, model_name, province_id)
        ) for model_name in self.df_models['model_name']]
        tasks = self.run_async_loop(coroutines)
        df_results = []
        for task in tasks:
            response = task.result()
            df_result = self.fetch_df_sales_from_response(response)
            if df_result is not None:
                df_results.append(df_result)
        if len(df_results):
            df_sales = pd.concat(df_result)
            return df_sales


# %%
self = Autohome_Sales()
# %%
provices_ids = self.df_province_city['id_province'].drop_duplicates().to_list()
provices_ids
# %%
model_names = self.df_models['model_name'].head().to_list()
model_names
# %%
model_name = '大众ID.X'
data = {
    "start": "2020-01-01",
    "end": "2022-12-01",
    "seriesnames": [model_name],
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
    "type": "2"}

data = json.dumps(data)
response = requests.post(self.url_api, data=data)
response.text
# %%
sd = '2020-01-01'
ed = '2022-05-01'
province_id = '310000'
model_names = ['朗逸','大众ID.X']
model_names = ['朗逸','帕萨特']
# model_names = ['大众ID.X']

responses = []
for model_name in model_names:
    data = self.make_data_payload(sd, ed, model_name, province_id)   
    response = self.session.post(self.url_api, data=data)
    try:
        response.json()
        responses.append(response)
    except:
        pass
responses
# %%
coroutines = []
for model_name in model_names:
    coroutine = self.async_post_response(
        self.url_api,
        data = data,
        dumps=False,
    )
    coroutines.append(coroutine)
tasks = self.run_async_loop(coroutines)
# %%
response = tasks[0].result()
data = response.json()
data.keys()
# %%
data['Xdata']
# %%
pd.DataFrame(data['cheqikanbanOpt']['朗逸'])
# %%
# %%
# %%
# %%
# %%
# %%


# %%
   def sort_save_result(self, df_result, clipboard=True):
        # 排序, 先yyyymm, 后date
        df_result = df_result.drop_duplicates(subset=['model_id'], keep='last')
        df_result = df_result.sort_values(
            ['segment_id', 'brand_id', 'manu_id'])
        # 去掉全空信息
        df_result = df_result[~pd.isna(df_result).all(axis=1)]
        df_result = df_result.reset_index(drop=True)
        df_result.to_pickle(self.result_pkl)
        if clipboard:
            self.df_to_clipboard(df_result)
        return df_result

    def get_df_models(self, force=False, clipboard=True, opencsv=True):
        # force=True 强制重新运行
        # return df_models:
        df_exist = self.read_exist_data(clipboard=False)
        if force or len(df_exist) == 0:
            self.init_df_segments(force=force)
            df_result = self.fetch_df_models()
        else:
            df_result = df_exist.copy()

        self.quit_driver()
        # 保存
        self.sort_save_result(df_result, clipboard=clipboard)
        self.df_to_csv(df_result, opencsv=opencsv)
        return df_result

    def get_df_all_models_by_selenium(self, save=True, opencsv=True):
        self.init_driver()
        task = self.test_async_url(self.url_car, method='get')
        response = task.result()
        data = self.fetch_data_from_selenium(self.driver, response)
        if len(data):
            df_all_models = pd.DataFrame(data, columns=self.result_columns)
            model_ids = df_all_models['model_id'].to_list()
            # 判断是否有遗留任务
            df_exist = self.read_exist_data(False)
            model_ids_exist = df_exist['model_id'].to_list()
            model_ids = [m for m in model_ids if m not in model_ids_exist]

            # 遗留数据处理
            if len(model_ids):
                df_remain_result = self.get_df_models_by_url_model(
                    model_ids, save=False, opencsv=False)
                df_result = df_exist.append(df_remain_result).drop_duplicates(
                    subset=['model_id'], keep='last')
            else:
                df_result = df_exist
            # 保存
            if save:
                self.sort_save_result(df_result, clipboard=False)
            if opencsv:
                self.df_to_csv(df_result)
            return df_result

    def fetch_df_models(self):
        # 首先协程运行, 后续通过driver运行
        # 协程运行, 速度快，但会有2个问题:
        # 1. 页面动态加载, 如: https://www.autohome.com.cn/mpvb/
        # 2. 无法加载车型, 如: https://www.autohome.com.cn/13/, https://www.autohome.com.cn/6786/
        coroutines = [
            self.async_get_response(
                self.url_segment % segment_id,
                meta={
                    'segment_id': segment_id,
                    'segment_name': segment_name
                },
                max_retry=self.max_retry,
            ) for segment_id, segment_name in self.df_segments[
                ['segment_id', 'segment_name']].values
        ]
        coroutines.append(
            self.async_get_response(
                self.url_wk,
                meta={
                    'segment_id': 'wk',
                    'segment_name': '微卡',
                },
                max_retry=self.max_retry,
            ))
        tasks = self.run_async_loop(coroutines, tqdm_desc='async_get_model')
        datas = []
        responses_miss = []
        for task in tasks:
            data = self.fetch_data_from_response(task.result())
            if data:
                datas.append(data)
            else:
                response = task.result()
                responses_miss.append(response)

        for response in responses_miss:
            if self._counts.get('init_driver') is None and len(
                    self.drivers) == 0:
                self.init_driver()
            data = self.fetch_data_from_selenium(self.driver, response)
            if data:
                datas.append(data)

        df_datas = [
            pd.DataFrame(data, columns=self.result_columns) for data in datas
        ]
        df_models = pd.concat(df_datas)

        df_models = df_models.drop_duplicates(subset=['model_id'],
                                              keep='first')

        return df_models

    def fetch_data_from_response(self, response):
        bsobj = bs4.BeautifulSoup(response.content, features='lxml')
        data = self.fetch_data_from_bsobj(bsobj)
        if len(data):
            if hasattr(response, 'meta'):
                segment_id = response.meta['segment_id']
                segment_name = response.meta['segment_name']
            else:
                segment_id = None
                segment_name = None
            data = [d + [segment_id, segment_name] for d in data]
            return data

    def fetch_data_from_bsobj(self, bsobj):
        # ul > li > h4
        data = []
        bm_compile = re.compile(r'.*?/brand-(\d*)-(\d*).*?.html.*')
        # ul_img 比ul_list多图片信息
        uls_img = bsobj.select('.rank-img-ul')
        lis_img = []
        for ul in uls_img:
            lis_img += ul.select('li')
        lis_img = [li for li in lis_img if li.attrs.get('id')]
        # uls_list = bsobj.select('.rank-list-ul')
        # lis_list = self.bsobj_get_lis_from_uls(uls_list)
        with tqdm(desc='fetch_data_from_bsobj', total=len(lis_img)) as pbar:
            for li in lis_img:
                pbar.update(1)
                try:
                    li_h4 = li.find('h4')
                    li_h4_a = li.find('a')
                    li_h4_sibling_sibling = li_h4.next_sibling.next_sibling
                    li_img = li.find('img')
                    li_parent_previous_sibling = li.parent.find_previous_sibling(
                    )
                    li_parent_parent_parent_dt = li.parent.parent.parent.dt

                    model_id = li.attrs['id'].replace('s', '')
                    model_name = li_h4_a.text
                    model_status = True if li_h4_a.attrs.get(
                        'class') else False
                    if hasattr(li_h4_sibling_sibling, 'text'):
                        model_price = li_h4_sibling_sibling.text
                    else:
                        model_price = li_h4_sibling_sibling.__str__()
                    model_url = self.url_model % model_id
                    model_picture_url = 'https:%s' % li_img.attrs.get(
                        'data-original')

                    brand_id, manu_id = bm_compile.match(
                        li_parent_previous_sibling.a['href']).groups()
                    manu_name = li_parent_previous_sibling.text
                    brand_name = li_parent_parent_parent_dt.text.strip()

                    data.append([
                        model_id,
                        model_name,
                        model_price,
                        model_status,
                        model_url,
                        model_picture_url,
                        manu_id,
                        manu_name,
                        brand_id,
                        brand_name,
                    ])
                except Exception as e:
                    self.logger.warning(e)
        return data

    def fetch_data_from_selenium(self, driver, response):
        # response 包含segment_id, segment_name
        driver.maximize_window()
        driver.refresh()
        time.sleep(1)
        url = response.url.__str__()
        print('get_model_data_from_selenium: %s' % url)
        driver.get(url)
        locator = (By.XPATH, '//div[@class="footer_auto"]')
        WebDriverWait(driver, 5,
                      0.5).until(EC.presence_of_element_located(locator))
        time.sleep(2)
        # 图片模式
        icon_locator = (By.XPATH, '//i[@class="icon16 icon16-img"]/..')
        icon_img = WebDriverWait(driver, 5, 0.5).until(
            EC.presence_of_element_located(icon_locator))
        icon_img.click()
        # 列表模式
        # icon_locator = (By.XPATH, '//i[@class="icon16 icon16-list"]/..')
        # i_list = WebDriverWait(driver, 5,
        #                    0.5).until(EC.presence_of_element_located(icon_locator))
        # i_list.click()
        time.sleep(2)

        # 确保加载
        counts = 5
        while True:
            self.driver_scroll_to_end(driver, locator)
            bsobj = bs4.BeautifulSoup(driver.page_source, features='lxml')
            uls_img = bsobj.select('.rank-img-ul')
            lis_img = []
            for ul in uls_img:
                lis_img += ul.select('li')
            lis_img = [li for li in lis_img if li.attrs.get('id')]
            counts -= 1
            if len(lis_img) >= self.fetch_series_count_from_bsobj(
                    bsobj) or counts <= 0:
                break

        if len(lis_img) < self.fetch_series_count_from_bsobj(bsobj):
            print('loading page error because of lack of lis_img!')
            return

        data = self.fetch_data_from_bsobj(bsobj)
        if len(data):
            if hasattr(response, 'meta'):
                segment_id = response.meta['segment_id']
                segment_name = response.meta['segment_name']
            else:
                segment_id = None
                segment_name = None
            data = [d + [segment_id, segment_name] for d in data]
            return data

    def get_df_models_by_url_model(self, model_ids, save=False, opencsv=False):

        # 预备查询字典
        df_models = self.read_exist_data(False)
        self.brand_dict = {
            k: v
            for k, v in df_models[['brand_id', 'brand_name'
                                   ]].drop_duplicates().values
        }
        self.manu_dict = {
            k: v
            for k, v in df_models[['manu_id', 'manu_name'
                                   ]].drop_duplicates().values
        }
        self.segment_dict = {
            k: v
            for k, v in df_models[['segment_id', 'segment_name'
                                   ]].drop_duplicates().values
        }

        coroutines = [
            self.async_get_response(self.url_model % model_id,
                                    meta={'model_id': model_id})
            for model_id in model_ids
        ]
        tasks = self.run_async_loop(coroutines)
        datas = []
        for task in tasks:
            response = task.result()
            bsobj = bs4.BeautifulSoup(response.content, features='lxml')
            data = self.fetch_data_by_url_model_from_bsobj(bsobj)
            if data:
                datas.append(data)

        if len(datas):
            df_result = pd.DataFrame(datas, columns=self.result_columns)
        else:
            df_result = pd.DataFrame()

        # 是否保存
        if save:
            df_models = df_models.append(df_result)
            df_models = df_models.drop_duplicates(subset=['model_id'],
                                                  keep='last')
            self.sort_save_result(df_models, clipboard=False)

        if opencsv:
            self.df_to_csv(df_result)

        return df_result

    def fetch_data_by_url_model_from_bsobj(self, bsobj):
        # 判断停售款
        if self.is_stop_version_from_bsobj(bsobj):
            # url_model = 'https://www.autohome.com.cn/4830/'
            try:
                div = bsobj.find('div', {'class': 'path'})
                segment, model = div.find_all('a')[-2:]
                model_id = model['href'].replace('/', '')
                model_name = model.text
                model_price = '指导价：暂无'
                model_status = False
                model_url = self.url_model % model_id
                model_picture_url = bsobj.find('dl', {
                    'class': 'models_pics'
                }).dt.a.img['src']
                segment_id = segment['href'].replace('/', '')
                segment_name = segment.text.split('\n')[0]
                manu = bsobj.find('div', {
                    'class': 'subnav-title-name'
                }).text.strip()
                manu_name = manu[:len(manu) - len(model_name) - 1]
                manu_id = {v: k
                           for k, v in self.manu_dict.items()}.get(manu_name)
                brand_name = bsobj.title.text
                l = brand_name.find('%s_' % model_name)
                r = brand_name.find('_%s' % model_name)
                brand_name = brand_name[l + len(model_name) + 1:r]
                brand_id = {v: k
                            for k, v in self.brand_dict.items()
                            }.get(brand_name)
                data = [
                    model_id,
                    model_name,
                    model_price,
                    model_status,
                    model_url,
                    model_picture_url,
                    manu_id,
                    manu_name,
                    brand_id,
                    brand_name,
                    segment_id,
                    segment_name,
                ]
                return data
            except Exception as e:
                print(e)
        else:
            # url_model = 'https://www.autohome.com.cn/6777/'
            try:
                div = bsobj.find('div', {'class': 'container athm-crumb'})
                brand = div.find_all('a')[-1]
                brand_name = brand.text
                pattern = re.compile(
                    r'//car.autohome.com.cn/price/brand-(\d.*)\..*')
                brand_href = brand['href']
                m = pattern.match(brand_href)
                if m:
                    brand_id = m.groups(1)[0]
                else:
                    brand_id = {v: k
                                for k, v in self.brand_dict.items()
                                }.get(brand_name)
                model_name = div.find_all('span')[-1].text.strip()

                price = bsobj.find('dl', {'class': 'information-price'}).dd
                model_price = price.text.split('\n')[1].strip()
                model_price = model_price.replace('厂商指导价暂无', '指导价：暂无')
                model_price = model_price.replace('预售价', '预售价：')
                if '暂无' in model_price:
                    model_status = False
                else:
                    model_status = True

                manu_a = bsobj.find('div', {
                    'class': 'athm-sub-nav__car__name'
                }).a
                model_id = manu_a['href'].split('/')[1]
                model_url = self.url_model % model_id
                div_pic = bsobj.find('div', {'class': 'pic-main'})
                model_picture_url = 'https:%s' % div_pic.a.img['src']

                manu_name = manu_a.text
                manu_name = manu_a.text[:len(manu_a.text) -
                                        len(manu_a.h1.text) - 2].strip()
                manu_id = {v: k
                           for k, v in self.manu_dict.items()}.get(manu_name)
                segment_name = bsobj.find('dd', {
                    'class': 'type'
                }).span.text.strip()
                segment_id = {v: k
                              for k, v in self.segment_dict.items()
                              }.get(segment_name)

                data = [
                    model_id,
                    model_name,
                    model_price,
                    model_status,
                    model_url,
                    model_picture_url,
                    manu_id,
                    manu_name,
                    brand_id,
                    brand_name,
                    segment_id,
                    segment_name,
                ]
                return data
            except Exception as e:
                print(e)

    def is_stop_version_from_bsobj(self, bsobj):
        # return True 停售款
        h2s = bsobj.find_all('h2')
        if h2s:
            h2s_text = [h2.text for h2 in h2s if '停售款' in h2.text]
            if len(h2s_text):
                return True

    def get_series_count(self, urls=None, force=False, opencsv=True):
        # 获取级别信息
        self.init_df_segments(force=force)
        if urls is None:
            urls = [
                self.url_segment % segment_id
                for segment_id in self.df_segments['segment_id']
            ]

        coroutines = [self.async_get_response(url) for url in urls]
        tasks = self.run_async_loop(coroutines)
        series_count_dict = {}
        for task in tasks:
            response = task.result()
            bsobj = bs4.BeautifulSoup(response.content, features='lxml')
            count = self.fetch_series_count_from_bsobj(bsobj)
            series_count_dict[response.url.__str__()] = count

        series_count = pd.DataFrame([[k, v]
                                     for k, v in series_count_dict.items()],
                                    columns=['url', 'series_count'])
        if opencsv:
            self.df_to_csv(series_count)
        return series_count

    def fetch_series_count_from_bsobj(self, bsobj):
        count = bsobj.select_one('#series-count')
        if count:
            result = int(count.text)
            return result


# %%
# 获取df_segment
if __name__ == '__main__':
    self = Autohome_Segment()
    df_segments = self.get_df_segments(force=True)


# %%
# 通过model_ids页面更新
if __name__ == '__main__':
    self = Autohome_Model()
    model_ids = ['4830', '4480']
    df_result = self.get_df_models_by_url_model(model_ids, save=True)
# %%
# 获取series_count
if __name__ == '__main__':
    self = Autohome_Model()
    series_count = self.get_series_count()

# %%
# %%
# %%
