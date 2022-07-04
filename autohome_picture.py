# %%
import os
import re
import pickle

import pandas as pd

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from async_spider import Async_Spider
from autohome_model import Autohome_Model


# %%
class Autohome_Picture(Async_Spider):
    name = 'autohome_picture'

    def __init__(self,
                 max_retry=3,
                 async_num=100,
                 threading_init_driver=False):
        super().__init__(max_retry=max_retry,
                         async_num=async_num,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion()

    def init_preparetion(self):
        self.url_brand = 'https://www.autohome.com.cn/car/'  # 动态加载
        self.dirname_output_brand = self.dirname_output.joinpath('brand')
        if not self.dirname_output_brand.exists():
            self.dirname_output_brand.mkdir()
        self.dirname_output_model = self.dirname_output.joinpath('model')
        if not self.dirname_output_model.exists():
            self.dirname_output_model.mkdir()
        self.check_dict_pkl = self.dirname_output.joinpath('check_dict.pkl')

    def download_brand_picture(self, force=False):
        # 获取品牌和图片链接
        self.init_driver()
        self.driver.get(self.url_brand)
        # 拼音区域
        locator = (By.XPATH, '//div[@class="find-letter fn-left"]')
        div = WebDriverWait(self.driver, 5,
                            0.5).until(EC.presence_of_element_located(locator))
        dds = div.find_element_by_xpath(
            './following::*').find_element_by_xpath(
                './following::*').find_elements_by_tag_name('dd')

        brand_dict = {
            'https:%s' %
            dd.find_element_by_tag_name('img').get_attribute('data-original'):
            self.dirname_output_brand.joinpath('%s.jpg' % self.set_filename(
                dd.find_element_by_tag_name('a').get_attribute('cname')))
            for dd in dds
        }

        if force is False:
            brand_dict = self.check_exist_filenames(self.dirname_output_brand,
                                                    brand_dict)

        print('brand picture in %s' % self.dirname_output_brand)
        self.open_dirname(self.dirname_output_brand)

        if len(brand_dict) > 0:
            pickle.dump(brand_dict, open(self.check_dict_pkl, 'wb'))
            coroutines = [
                self.async_get_response_to_file(k,
                                                headers=self.headers,
                                                meta={
                                                    'url': k,
                                                    'filename': v
                                                })
                for k, v in brand_dict.items()
            ]
            tasks = self.run_async_loop(coroutines)
            miss_pd_index = self.check_async_tasks(tasks)
            if len(miss_pd_index):
                df_miss = pd.DataFrame([[k, v] for k, v in brand_dict.items()],
                                       columns=['url', 'filename'])
                df_miss = df_miss[miss_pd_index]
                return df_miss

    def download_model_picture(self, force=False, total=None):
        df_models = Autohome_Model().read_exist_data(False)
        if total is not None and isinstance(total, int):
            df_models = df_models.head(total)

        if len(df_models) == 0:
            return

        model_dict = {
            model_picture_url:
            self.create_model_picture_filename(brand_name, model_name)
            for model_name, model_picture_url, brand_name in df_models[
                ['model_name', 'model_picture_url', 'brand_name']].values
        }

        if force is False:
            model_dict = self.check_exist_filenames(self.dirname_output_model,
                                                    model_dict)

        print('model picture in %s' % self.dirname_output_model)
        self.open_dirname(self.dirname_output_model)

        if len(model_dict) > 0:
            pickle.dump(model_dict, open(self.check_dict_pkl, 'wb'))
            # 下载数据
            coroutines = [
                self.async_get_response_to_file(k,
                                                headers=self.headers,
                                                meta={
                                                    'url': k,
                                                    'filename': v
                                                })
                for k, v in model_dict.items()
            ]
            tasks = self.run_async_loop(coroutines)
            miss_pd_index = self.check_async_tasks(tasks)
            if len(miss_pd_index):
                df_miss = pd.DataFrame([[k, v] for k, v in model_dict.items()],
                                       columns=['url', 'filename'])
                df_miss = df_miss[miss_pd_index]
                return df_miss

    def check_exist_filenames(self, dirname, task_dict=None):
        exist_filenames = list(dirname.rglob('*.*'))
        print('%s exist files in %s' % (len(exist_filenames), dirname))
        if task_dict:
            task_dict = {
                k: v
                for k, v in task_dict.items() if not v in exist_filenames
            }
            return task_dict

    def create_model_picture_filename(self, brand_name, model_name):
        dirname = self.dirname_output_model.joinpath(
            self.set_filename(brand_name))
        if not dirname.exists():
            dirname.mkdir()
        filename = dirname.joinpath(self.set_filename(('%s.jpg' % model_name)))
        return filename

    def read_check_dict_pkl(self):
        dic = pickle.load(open(self.check_dict_pkl, 'rb'))
        df_check = pd.DataFrame([[k, v] for k, v in dic.items()],
                                columns=['url', 'filename'])
        return df_check


# %%
# 下载品牌图片
if __name__ == '__main__':
    self = Autohome_Picture()
    self.check_exist_filenames(self.dirname_output_brand)
    df_missis = self.download_brand_picture(force=False)
    if df_missis is not None:
        self.df_to_csv(df_missis)
# %%
# 下载车型图片
if __name__ == '__main__':
    self = Autohome_Picture()
    self.check_exist_filenames(self.dirname_output_model)
    df_missis = self.download_model_picture(force=True, total=1000)
    if df_missis is not None:
        self.df_to_csv(df_missis)
# %%
# %%
# %%