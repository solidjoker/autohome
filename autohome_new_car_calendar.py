# coding: utf-8
# %%
import datetime
import time
import re
import pandas as pd
import bs4

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from async_spider import Async_Spider

# mission
# re 提取 script中的var 

# %%
class Autohome_New_Car_Calendar(Async_Spider):
    name = 'autohome_new_car_calendar'

    def __init__(self, max_retry=3, concurrency=100, threading_init_driver=False):
        super().__init__(max_retry=max_retry, concurrency=concurrency, threading_init_driver=threading_init_driver)
        self.init_preparetion()

    def init_preparetion(self):

        # 'model_id': str, 汽车之家modelid
        # 'title': str, 标题
        # 'url': str, 新闻链接
        # 'yyyymm': str, 年月
        # 'date': datetime.date 日期
        self.result_columns = ['model_id', 'title', 'url', 'yyyymm', 'date']
        self.url_calendar = 'https://www.autohome.com.cn/newbrand/0-0-0-%s-0.html'
        self.url_test = 'https://www.autohome.com.cn/newbrand/0-0-0-%s-0.html' % self.today.year
        self.result_pkl = self.dirname_output.joinpath('%s_result.pkl' %
                                                       self.name)
        msgs = [
            'self.result_pkl is %s' % self.result_pkl.as_posix(),
            'you can use self.session or self.driver',
            'init_preparetion done',
            '-' * 20,
        ]
        for msg in msgs:
            print(msg)

    def decorator_add_info_df_result(func):
        # decorator for add_info_df_result, 带参数
        def wrapper(self, *args, **kwargs):
            res = func(self, *args, **kwargs)
            filename = self.dirname_output.joinpath('autohome_model_result.pkl')
            opencsv = kwargs.get('opencsv',True)
            # 通过autohome_model获取
            if filename.exists():
                df_models = pd.read_pickle(filename)
                df_result = res.merge(df_models, on='model_id', how='left')        
                self.df_to_csv(df_result, opencsv=opencsv)
            return res
        return wrapper

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
        df_result = df_result.sort_values(['yyyymm', 'date'])
        # 去掉全空信息
        df_result = df_result[~pd.isna(df_result).all(axis=1)]
        df_result = df_result.reset_index(drop=True)
        df_result.to_pickle(self.result_pkl)
        if clipboard:
            self.df_to_clipboard(df_result)
        return df_result

    # 更新数据
    @decorator_add_info_df_result
    def update_all_years(self):
        years = list(range(2008, datetime.date.today().year + 1))
        ans = input('update all years from %s to %s?\ninput y for update' %
                    (years[0], years[1]))
        if ans.lower() == 'y':
            for year in years:
                self.update_calendar_by_year(year, clipboard=False,opencsv=False)
        self.quit_driver()
        df_result = self.read_exist_data()
        return df_result

    @decorator_add_info_df_result
    def update_calendar_by_year(self, year=None, clipboard=True, **kwargs):
        available_months = self.get_available_months()
        df_exist = self.read_exist_data(False)
        # 检查年
        if year is None:
            year = available_months[-1][:4]
        else:
            year = self.check_year(year)

        if year is None:
            print('year %s is not available' % year)
            return df_exist
        print('update_calendar_by_year:%s' % year)

        # 获取df_news
        df_news = self.get_df_news_by_year(year, available_months)

        # 生成任务
        df_task = df_exist.append(df_news)

        # 从url获取df_date
        df_exist, df_remain_url = self.get_exist_remain(
            df_task, ['url'], 'date')
        if len(df_remain_url) == 0:
            return df_exist
        print('len of remain_url is %s' % len(df_remain_url))
        df_date = self.get_dates_from_df_remain(df_remain_url)
        df_result_url = df_remain_url.drop(columns=['date']).merge(
            df_date, left_index=True, right_index=True)

        # return df_result_url
        # 从selenium获取df_date
        if len(df_result_url) == 0:
            return df_exist

        # return df_result_url
        df_exist_url, df_remain_selenium = self.get_exist_remain(
            df_result_url, ['url'], 'date')
        df_exist = df_exist.append(df_exist_url)

        if len(df_remain_selenium) == 0:
            df_result = df_exist
        else:
            print('len of remain_selenium is %s' % len(df_remain_selenium))
            df_result_selenium = self.get_dates_from_df_remain_selenium(
                df_remain_selenium)
            df_result = df_exist.append(df_result_selenium)

        # 保存
        df_result = self.sort_save_result(df_result, clipboard)
        return df_result

    def get_exist_remain(self, df_task, subset=['url'], check_column='date'):
        # 任务: 去重
        df_task = df_task.drop_duplicates(subset=subset, keep='first')
        df_exist = df_task[~pd.isna(df_task[check_column])]
        df_remain = df_task[pd.isna(df_task[check_column])]
        return df_exist, df_remain

    def get_df_news_by_year(self, year=None, available_months=None):
        if available_months is None:
            available_months = self.get_available_months()
        if year is None:
            year = available_months[-1][:4]
        coroutines = [
            self.async_get_response(self.url_calendar % year,
                                    max_retry=self.max_retry)
        ]
        tasks = self.run_async_loop(coroutines,
                                    tqdm_desc='async_get_news_of_year:%s' %
                                    year)

        df_news = pd.concat(
            self.get_df_news_from_response(task.result(), available_months)
            for task in tasks)
        df_news = df_news.reset_index(drop=True)
        return df_news

    # 获取基础信息, 不含date
    def get_df_news_from_response(self, response, available_months=None):
        # 从url_calendar的response获取df_news:
        # df_news 包含: url_news, model_id
        if available_months is None:
            available_months = self.get_available_months()
        bsobj = bs4.BeautifulSoup(response.content, 'lxml')
        dls = bsobj.find_all('dl', 'all-list')
        # 检查dl, 包含dt: yyyy年mm月, dd 新车信息
        dls = [dl for dl in dls if self.dldt_to_yyyymm(dl, available_months)]
        # 从dl中获取信息
        news = []
        for dl in dls:
            yyyymm = self.dldt_to_yyyymm(dl, available_months)
            dds = dl.find_all('dd')
            for dd in dds:
                links = dd.find_all('a')
                try:
                    url_news = links[0].attrs['href']
                    url_model = links[1].attrs['href']
                    span = dd.find('span')
                    title = span.text
                    news.append([url_model, title, url_news, yyyymm, None])
                except Exception as e:
                    self.logger.warning(dd)
                    self.logger.warning(e)

        df_news = pd.DataFrame(news, columns=self.result_columns)
        # url_news 调整, 去除#
        df_news['url'] = df_news['url'].apply(self.check_url_news)
        # 获取model_id, 格式文本
        df_news['model_id'] = df_news['model_id'].apply(self.check_model_id)
        return df_news

    def get_available_months(self,
                             start_yyyymm='200801',
                             end_yyyymm=None,
                             output_format='%Y%m'):
        # 获取 "yyyymm", 匹配url_calendar中的dl yyyy年mm月
        sy = int(start_yyyymm[:4])
        sm = int(start_yyyymm[-2:])
        if end_yyyymm is None:
            today = datetime.date.today()
            ey = today.year
            em = today.month
        else:
            ey = int(end_yyyymm[:4])
            em = int(end_yyyymm[-2:])
        available_months = []
        if sy < ey:
            # sy 当年
            for m in range(sm, 13):
                available_months.append(datetime.date(sy, m, 1))
            # sy+1到ey
            for y in range(sy + 1, ey):
                for m in range(1, 13):
                    available_months.append(datetime.date(y, m, 1))
            # ey 当年
            for m in range(1, em + 1):
                available_months.append(datetime.date(ey, m, 1))
        elif sy == ey:
            for m in range(sm, em + 1):
                available_months.append(datetime.date(sy, m, 1))
        if output_format is not None:
            available_months = [
                datetime.date.strftime(d, output_format)
                for d in available_months
            ]
        return available_months

    def check_year(self, year):
        year = int(year)
        if year > 2007 and year <= datetime.date.today().year:
            year = str(year)
            return year

    def dldt_to_yyyymm(self, dl, available_months):
        pattern = re.compile(r'(\d*)\D*(\d*).*')
        txt = dl.dt.text.strip()
        m = pattern.match(txt)
        if m:
            res = ''.join(m.groups())
            if res in available_months:
                return res

    def check_url_news(self, url):
        if url:
            url = url.strip().split('#')[0]
            pattern = re.compile(r'.*\.com\.cn\/(.*)')
            m = pattern.match(url)
            if m:
                url = 'https://www.autohome.com.cn/' + m.groups()[0]
        return url

    def check_model_id(self, url):
        res = url
        if url:
            pattern = re.compile(r'.*\/(\d*)\/.*')
            m = pattern.match(url)
            if m:
                res = m.groups()[0]
        return res

    # 获取基础信息, date部分by asyncio
    def get_dates_from_df_remain(self, df_remain):
        # df_remain 不含date
        df_remain = self.df_drop_columns(df_remain, columns=['date'])
        coroutines = [
            self.async_get_response(url, max_retry=self.max_retry)
            for url in df_remain['url']
        ]
        tasks = self.run_async_loop(coroutines,
                                    tqdm_desc='async_get_dates_from_df_remain')
        responses = [_.result() for _ in tasks]
        df_date = pd.DataFrame(responses,
                               columns=['date'],
                               index=df_remain.index)
        df_date['date'] = df_date['date'].apply(self.get_date_from_response)
        return df_date

    def get_date_from_response(self, response):
        if response:
            bsobj = bs4.BeautifulSoup(response.content, 'lxml')
            span = bsobj.find('span', {'class': 'time'})
            if span is not None:
                date = self.dttext_to_dt(span.text)
                return date

    def dttext_to_dt(self, dttext):
        dttext = dttext.strip().split(' ')[0]
        pattern = re.compile(r'(\d*)年(\d*)月(\d*)日')
        m = pattern.match(dttext)
        if m:
            dt = datetime.date(*[int(x) for x in m.groups()])
            return dt

    # 获取基础信息, date部分by selenium
    def get_dates_from_df_remain_selenium(self, df_remain_selenium):
        self.init_driver()

        df_remain_selenium = df_remain_selenium[pd.isna(
            df_remain_selenium['date'])]

        for row in df_remain_selenium.iterrows():
            dt = None  # 初始化dt
            idx = row[0]  # id
            yyyymm = row[1]['yyyymm']  # yyyymm 作为默认值
            url = row[1]['url']  # 查询url
            dt = self.get_date_by_selenium(url)
            try:
                if dt is None:
                    dt = datetime.date(int(yyyymm[:4]), int(yyyymm[-2:]), 1)
            except:
                dt = 'blank'
            # 添加信息到df_exist
            print(url, dt)
            df_remain_selenium.loc[idx, 'date'] = dt
        return df_remain_selenium

    def get_date_by_selenium(self, url):
        # https://blog.csdn.net/wycaoxin3/article/details/74017971
        # https://blog.csdn.net/sinat_41774836/article/details/88965281
        try:
            self.driver.get(url)
            max_try = self.max_retry
            while self.driver.current_url != url and max_try > 0:
                time.sleep(3)
                max_try -= 1
            locator = (By.CLASS_NAME, 'time')
            ele = WebDriverWait(self.driver, 5, 0.5).until(
                EC.presence_of_element_located(locator))
            if ele:
                dt = self.dttext_to_dt(ele.text)
                return dt
        except Exception as e:
            print(e)

    @decorator_add_info_df_result
    def get_current_calendar(self):
        df_calendar = self.read_exist_data(clipboard=False)
        return df_calendar



# %%
# 更新本年数据
if __name__ == '__main__':
    self = Autohome_New_Car_Calendar()
    df_result = self.update_calendar_by_year()

# %%
# 更新所有年份数据
if __name__ == '__main__':
    self = Autohome_New_Car_Calendar()
    df_result = self.update_all_years()
# %%
# 获取现有数据
if __name__ == '__main__':
    self = Autohome_New_Car_Calendar(threading_init_driver=False)
    df_result = self.get_current_calendar()
# %%
# test for self.get_df_news_from_response
if __name__ == '__main__':
    response = self.session.get(self.url_test)
    df_news = self.get_df_news_from_response(response)
# %%
# test for self.get_df_news_by_year
if __name__ == '__main__':
    df_news = self.get_df_news_by_year(2021)
# %%
# %%
