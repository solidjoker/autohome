# %% [markdown]
# --- 20220721: 更换bsobj解析库为html5lib
# - cmd > pip install html5lib
# - bsobj = bs4.BeautifulSoup(response.content, features='html5lib')


# %%
import pickle
import re
import time
import bs4
import logging
import string
import pandas as pd
from pyquery import PyQuery as pq
from tqdm.auto import tqdm

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from async_spider import Async_Spider

# %%


class Autohome_Segment(Async_Spider):
    name = 'autohome_segment'

    # 轻型卡车页面不在默认级别中
    def __init__(self,
                 max_retry=3,
                 concurrency=100,
                 threading_init_driver=False,
                 logging_level=logging.ERROR,
                 verbose=True):
        super().__init__(max_retry=max_retry,
                         concurrency=concurrency,
                         threading_init_driver=threading_init_driver,
                         logging_level=logging_level)
        self.init_preparetion(verbose=verbose)

    def init_preparetion(self, verbose=True):
        self.result_columns = ['segment_id', 'segment_name']
        self.url_car = 'https://www.autohome.com.cn/car/'
        self.result_pkl = self.dirname_output.joinpath('%s_result.pkl' %
                                                       self.name)
        if verbose:
            msgs = [
                'self.result_pkl is %s' % self.result_pkl.as_posix(),
                'you can use self.session or self.driver',
                'init_preparetion done',
                '-' * 20,
            ]
            for msg in msgs:
                print(msg)

    def read_exist_data(self, clipboard=True):
        if self.result_pkl.exists():
            df_exist = pd.read_pickle(self.result_pkl)
        else:
            df_exist = pd.DataFrame(columns=self.result_columns)
        if clipboard:
            self.df_to_clipboard(df_exist)
        return df_exist

    def sort_save_result(self, df_result, clipboard=True):
        # 排序, 先yyyymm, 后date
        df_result = df_result.sort_values(['segment_id'])
        # 去掉全空信息
        df_result = df_result[~pd.isna(df_result).all(axis=1)]
        df_result = df_result.reset_index(drop=True)
        df_result.to_pickle(self.result_pkl)
        if clipboard:
            self.df_to_clipboard(df_result)
        return df_result

    def get_df_segments(self, force=True, clipboard=True):
        # force=True 强制重新运行
        # return df_segment: segment_id, segment_name
        df_exist = self.read_exist_data(clipboard=False)
        if force or len(df_exist) == 0:
            df_result = self.update_df_segments()
        else:
            df_result = pd.DataFrame(columns=self.result_columns)
        df_result = pd.concat([df_exist, df_result])
        df_result = df_result.drop_duplicates('segment_id')
        # 保存
        self.sort_save_result(df_result, clipboard=clipboard)
        return df_result

    def update_df_segments(self):
        coroutines = [
            self.async_get_response(self.url_car, max_retry=self.max_retry)
        ]
        tasks = self.run_async_loop(coroutines, tqdm_desc='async_get_segment')
        task = tasks[0]
        df_segments = self.get_df_segment_from_response(task.result())
        return df_segments

    def get_df_segment_from_response(self, response):
        bsobj = bs4.BeautifulSoup(response.content, features='html5lib')
        dl = bsobj.find('dl', {'class': 'caricon-list'})
        hrefs = dl.find_all('a')
        datas = [[ele.attrs['href'][1:-1], ele.text] for ele in hrefs
                 if len(ele.attrs) == 2 and '全部' not in ele.text.upper()]
        df_segment = pd.DataFrame(datas, columns=self.result_columns)
        return df_segment


class Autohome_Model(Async_Spider):
    name = 'autohome_model'

    def __init__(self, max_retry=3, concurrency=100, threading_init_driver=False,
                 logging_level=logging.ERROR, verbose=True):
        super().__init__(max_retry=max_retry,
                         concurrency=concurrency,
                         threading_init_driver=threading_init_driver,
                         logging_level=logging_level)
        self.init_preparetion(verbose=verbose)

    def init_preparetion(self, verbose=True):
        self.result_columns = [
            'model_id',
            'model_name',
            'model_price',
            'model_status',
            'model_url',
            'model_picture_url',
            'manu_id',
            'manu_name',
            'brand_id',
            'brand_name',
            'segment_id',
            'segment_name',
        ]
        self.url_car = 'https://www.autohome.com.cn/car/'
        self.url_segment = 'https://www.autohome.com.cn/%s/'  # segment_name
        self.url_wk = 'https://www.autohome.com.cn/car/0_0-0.0_0.0-0-0-9-0-0-0-0-0/'  # 微卡
        self.url_model = 'https://www.autohome.com.cn/%s/'
        self.url_test = 'https://www.autohome.com.cn/suva/'
        self.url_list_by_letter = 'https://www.autohome.com.cn/grade/carhtml/%s.html'
        self.url_img_by_letter = 'https://www.autohome.com.cn/grade/carhtml/%s_photo.html'

        # string.ascii_uppercase

        self.result_pkl = self.dirname_output.joinpath('%s_result.pkl' %
                                                       self.name)

        self.brand_dict_pkl = self.dirname_output.joinpath(
            '%s_brand_dict_pkl' % self.name)
        self.manu_dict_pkl = self.dirname_output.joinpath(
            '%s_manu_dict_pkl' % self.name)

        if verbose:
            msgs = [
                'self.result_pkl is %s' % self.result_pkl.as_posix(),
                'you can use self.session or self.driver',
                'init_preparetion done',
                '-' * 20,
            ]
            for msg in msgs:
                print(msg)

    def init_df_segments(self, force=True):
        autohome_segment = Autohome_Segment(verbose=False)
        self.df_segments = autohome_segment.get_df_segments(force=force,
                                                            clipboard=False)

    def read_exist_data(self, clipboard=True):
        if self.result_pkl.exists():
            df_exist = pd.read_pickle(self.result_pkl)
        else:
            df_exist = pd.DataFrame(columns=self.result_columns)
        if clipboard:
            self.df_to_clipboard(df_exist)
        return df_exist

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
                df_result = pd.concat([df_exist, df_remain_result])
                df_result = df_result.drop_duplicates(
                    subset=['model_id'], keep='last')
            else:
                df_result = df_exist
            # 保存
            if save:
                self.sort_save_result(df_result, clipboard=False)
            if opencsv:
                self.df_to_csv(df_result)
            return df_result

    def get_df_all_models_by_letter(self, save=True, opencsv=True):
        # 20220721 更新
        # 通过字母页面更新所有车型
        coroutines = [self.async_get_response(
            self.url_img_by_letter % l) for l in string.ascii_uppercase]
        tasks = self.run_async_loop(coroutines, tqdm_desc='get_df_all_models')
        results = [task.result() for task in tasks]
        responses = [result for result in results if result]

        lis_type = '.rank-img-ul'

        datas = [self.fetch_data_from_response(
            response, lis_type=lis_type) for response in responses]
        df_datas = [pd.DataFrame(data, columns=self.result_columns)
                    for data in datas]

        self.brand_dict = {}
        self.manu_dict = {}
        print('update brand_dict and manu_dict')
        for response in responses:
            self.update_brand_manu_dict(
                response, self.brand_dict, self.manu_dict)
        with open(self.brand_dict_pkl, 'wb') as f:
            pickle.dump(self.brand_dict, f)
        with open(self.manu_dict_pkl, 'wb') as f:
            pickle.dump(self.manu_dict, f)

        if len(df_datas):
            df_all_models = pd.concat(df_datas)
            df_all_models = df_all_models.drop_duplicates(
                subset=['model_id'], keep='first')
            # 更新segment_id, segment_name
            df_exist = self.read_exist_data(False)
            self.segment_dict = {
                k: v
                for k, v in df_models[['segment_id', 'segment_name'
                                       ]].drop_duplicates().values
            }
            df_all_models['segment_id'] = df_all_models['model_id'].apply(
                lambda key: self.df_vlookup(
                    df_exist, 'model_id', 'segment_id', key, top=True))
            df_all_models['segment_name'] = df_all_models['model_id'].apply(
                lambda key: self.df_vlookup(
                    df_exist, 'model_id', 'segment_name', key, top=True))

            # 判断是否有遗留任务
            df_misses = df_all_models[pd.isna(
                df_all_models['segment_id'])].copy()
            if len(df_misses):
                df_misses['segment_id'] = df_misses['model_url'].apply(
                    lambda x: self.get_segment_id_from_url(x))
                df_misses['segment_name'] = df_misses['segment_id'].apply(
                    lambda x: self.segment_dict.get(x))

                df_result = pd.concat([df_all_models, df_misses])
                df_result = df_result.drop_duplicates(
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

        lis_type = '.rank-img-ul'  # 多出model_pic_url信息
        # lis_type = '.rank-list-ul'
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
            data = self.fetch_data_from_response(
                task.result(), lis_type=lis_type)
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

    def fetch_data_from_response(self, response, lis_type='.rank-list-ul'):
        print('process data of response from url:%s' % response.url)
        bsobj = bs4.BeautifulSoup(response.content, features='html5lib')
        data = self.fetch_data_from_bsobj(bsobj, lis_type=lis_type)
        if len(data):
            if hasattr(response, 'meta'):
                segment_id = response.meta['segment_id']
                segment_name = response.meta['segment_name']
            else:
                segment_id = None
                segment_name = None
            data = [d + [segment_id, segment_name] for d in data]
            return data

    def fetch_data_from_bsobj(self, bsobj, lis_type='.rank-list-ul'):
        # ul > li > h4
        data = []
        bm_compile = re.compile(r'.*?/brand-(\d*)-(\d*).*?.html.*')
        # ul_img 比ul_list多图片信息
        # ul_img 动态加载了...
        # uls_img = bsobj.select('.rank-img-ul')
        # lis_img = []
        # for ul in uls_img:
        #     lis_img += ul.select('li')
        # lis_img = [li for li in lis_img if li.attrs.get('id')]

        lis_list = self.fetch_lis_from_bsobj(bsobj, lis_type=lis_type)

        with tqdm(desc='process_data_from_bsobj', total=len(lis_list)) as pbar:
            for li in lis_list:
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
                    if li_img:
                        model_picture_url = 'https:%s' % li_img.attrs.get(
                            'data-original')
                    else:
                        model_picture_url = ''
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
        '''
        20220720 改版以后bs4有动态加载问题
        '''
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
        # icon_locator = (By.XPATH, '//i[@class="icon16 icon16-img"]/..')
        # icon_img = WebDriverWait(driver, 5, 0.5).until(
        # EC.presence_of_element_located(icon_locator))
        # icon_img.click()
        # lis_type = '.rank-img-ul'

        # 列表模式
        icon_locator = (By.XPATH, '//i[@class="icon16 icon16-list"]/..')
        i_list = WebDriverWait(driver, 5,
                               0.5).until(EC.presence_of_element_located(icon_locator))
        i_list.click()
        lis_type = '.rank-list-ul'
        time.sleep(2)
        # 确保加载
        # counts = 5
        # while True:
        #     self.driver_scroll_to_end(driver, locator)
        #     bsobj = bs4.BeautifulSoup(driver.page_source, features='html5lib')
        #     lis_list = self.fetch_lis_from_bsobj(bsobj, lis_type=lis_type)
        #     counts -= 1
        #     if len(lis_list) >= self.fetch_series_count_from_bsobj(
        #             bsobj) or counts <= 0:
        #         break

        self.scroll_page(driver, False, 2)
        bsobj = bs4.BeautifulSoup(driver.page_source, features='html5lib')
        lis_list = self.fetch_lis_from_bsobj(bsobj, lis_type=lis_type)
        if len(lis_list) < self.fetch_series_count_from_bsobj(bsobj):
            print('loading page error because of lack of lis_img!')
            # return

        data = self.fetch_data_from_bsobj(bsobj, lis_type=lis_type)
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
        if self.brand_dict_pkl.exists():
            with open(self.brand_dict_pkl, 'rb') as f:
                self.brand_dict = pickle.load(f)
        else:
            self.brand_dict = {
                k: v
                for k, v in df_models[['brand_id', 'brand_name'
                                       ]].drop_duplicates().values
            }
        if self.manu_dict_pkl.exists():
            with open(self.manu_dict_pkl, 'rb') as f:
                self.manu_dict = pickle.load(f)
        else:
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
        tasks = self.run_async_loop(
            coroutines, tqdm_desc='get_df_models_by_url', record=False)
        results = [task.result() for task in tasks]
        responses = [result for result in results if result]
        datas = [self.fetch_data_by_url_model_from_bsobj(bs4.BeautifulSoup(
            response.content, features='html5lib')) for response in responses]
        df_datas = [pd.DataFrame(data, columns=self.result_columns)
                    for data in datas]

        if len(df_datas):
            df_result = pd.concat(df_datas)
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
                data = [[
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
                ]]
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

                data = [[
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
                ]]
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
            bsobj = bs4.BeautifulSoup(response.content, features='html5lib')
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

    def fetch_lis_from_bsobj(self, bsobj, lis_type='.rank-list-ul'):
        # lis_type = '.rank-list-ul' or lis_type = '.rank-img-ul'
        uls_list = bsobj.select(lis_type)
        lis_list = []
        for ul in uls_list:
            lis_list += ul.select('li')
        lis_list = [li for li in lis_list if li.attrs.get('id')]
        return lis_list

    def update_brand_manu_dict(self, response, brand_dict, manu_dict):
        # get_df_all_models_by_letter 中更新self.brand_dict, self.manu_dict
        bsobj = bs4.BeautifulSoup(response.content, features='html5lib')
        dls = [dl for dl in bsobj.find_all(
            'dl') if dl.attrs.get('id') and dl.attrs.get('olr')]
        if len(dls) == 0:
            return

        brand_compile = re.compile(r'.*?/brand-(\d*).*?.html.*')
        manu_compile = re.compile(r'.*?/brand-(\d*)-(\d*).*?.html.*')

        for dl in dls:
            # 更新brand_dict
            div_brand = dl.div
            try:
                brand_name = div_brand.text
                brand_id = brand_compile.match(
                    div_brand.a.attrs['href']).groups()[0]
                brand_dict |= {brand_id: brand_name}
            except:
                pass
            # 更新manu_dict
            divs_manu = dl.find_all('div', {'class': 'h3-tit'})
            for div_manu in divs_manu:
                try:
                    manu_name = div_manu.text
                    manu_id = manu_compile.match(
                        div_manu.a.attrs['href']).groups()[1]
                    manu_dict |= {manu_id: manu_name}
                except:
                    pass

    def get_segment_id_from_url(self, url):
        response = self.session.get(url)
        bsobj = bs4.BeautifulSoup(
            response.content, features='html5lib')
        if self.is_stop_version_from_bsobj(bsobj):
            # url_model = 'https://www.autohome.com.cn/4830/'
            try:
                div = bsobj.find('div', {'class': 'path'})
                segment, _ = div.find_all('a')[-2:]
                segment_id = segment['href'].replace('/', '')
                return segment_id
            except Exception as e:
                pass
        else:
            # url_model = 'https://www.autohome.com.cn/6777/'
            try:
                segment_name = bsobj.find('dd', {
                    'class': 'type'
                }).span.text.strip()
                segment_id = {v: k
                              for k, v in self.segment_dict.items()
                              }.get(segment_name)
                return segment_id
            except Exception as e:
                print(e)


# %%
# 获取df_segment
if __name__ == '__main__':
    self = Autohome_Segment()
    df_segments = self.get_df_segments(force=True)

# %%
# 获取df_models
if __name__ == '__main__':
    self = Autohome_Model()
    force = input('更新df_models? 输入y强制更新, 其他读取现有数据')
    if force.upper() == 'Y':
        df_models = self.get_df_models(force=True)
    else:
        df_models = self.get_df_models(force=False)

# %%
# 获取所有的df_models
if __name__ == '__main__':
    self = Autohome_Model()
    force = input('更新df_all_models? 输入y强制更新, 其他读取现有数据')
    if force.upper() == 'Y':
        # df_all_models = self.get_df_all_models_by_selenium(save=True)
        df_all_models = self.get_df_all_models_by_letter(save=True)

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