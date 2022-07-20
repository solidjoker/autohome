# %%
import re
import time
import pandas as pd
import bs4
import logging
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
                 verbose=True):
        super().__init__(max_retry=max_retry,
                         concurrency=concurrency,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion(verbose=verbose)
        self.console.setLevel(logging.CRITICAL)

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
        df_result = df_exist.append(df_result)
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
        bsobj = bs4.BeautifulSoup(response.content, features='lxml')
        dl = bsobj.find('dl', {'class': 'caricon-list'})
        hrefs = dl.find_all('a')
        datas = [[ele.attrs['href'][1:-1], ele.text] for ele in hrefs
                 if len(ele.attrs) == 2 and '全部' not in ele.text.upper()]
        df_segment = pd.DataFrame(datas, columns=self.result_columns)
        return df_segment

class Autohome_Model(Async_Spider):
    name = 'autohome_model'

    def __init__(self, max_retry=3, concurrency=100, threading_init_driver=False,
                 verbose=True):
        super().__init__(max_retry=max_retry,
                         concurrency=concurrency,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion(verbose=verbose)
        self.console.setLevel(logging.CRITICAL)

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

        bsobj = bs4.BeautifulSoup(driver.page_source, features='lxml')
        uls_img = bsobj.select('.rank-img-ul')
        lis_img = []
        for ul in uls_img:
            lis_img += ul.select('li')
        lis_img = [li for li in lis_img if li.attrs.get('id')]

        if len(lis_img) < self.fetch_series_count_from_bsobj(bsobj):
            print('loading page error because of lack of lis_img!')
            # return

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
        if opencsv: self.df_to_csv(series_count)
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
        df_all_models = self.get_df_all_models_by_selenium(save=True)
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