# %%
import re
import time
import pandas as pd
import bs4
from tqdm.auto import tqdm

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from async_spider import Async_Spider
from autohome_model import Autohome_Model


# %%
class Autohome_Dealer(Async_Spider):
    '''
    0. need 'AutohomeManuUrls.xlsx'

    1. get_df_province_city
    2.get_df_city_distribution

    3.1 getDealerinfoBrandManu or 3.2 getDealerinfo of All 
    4.1 readDealerInfoBrandManu or 4.2 readDealerinfo of All
    '''

    name = 'autohome_dealer'

    def __init__(self,
                 max_retry=3,
                 async_num=100,
                 threading_init_driver=False):
        super().__init__(max_retry=max_retry,
                         async_num=async_num,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion()

    def init_preparetion(self):

        self.url_province_city = 'https://dealer.autohome.com.cn/DealerList/GetAreasAjax?provinceId=0&cityId=0&brandid=0&manufactoryid=0&seriesid=0&isSales=0'
        self.url_city_distritution = 'https://dealer.autohome.com.cn/%s'
        # self.url_dealer_brand = 'https://dealer.autohome.com.cn/{pinyin_city}/{id_distribution}/{brand_id}/0/{manu_id}/{page_idx}/0/0/0.html'
        self.url_dealer_brand = 'https://dealer.autohome.com.cn/{pinyin_city}/0/{brand_id}/0/{manu_id}/{page_idx}/0/0/0.html'  # 简化id_distribution=0
        self.url_dealer_brand_manu_count = 'https://dealer.autohome.com.cn/DealerList/GetAreasAjax?provinceId=0&cityId=0&brandid={brand_id}&manufactoryid={manu_id}&seriesid=0&isSales=0'
        self.url_dealer_info = 'https://dealer.autohome.com.cn/Ajax/GetDealerInfo?DealerId=%s'
        self.url_check_brand_ids = 'https://dealer.autohome.com.cn/beijing/0/0/0/0/1/0/0/0.html'

        self.num_in_page = 15
        self.dirname_output_dealer = self.dirname_output.joinpath('dealer')
        if not self.dirname_output_dealer.exists():
            self.dirname_output_dealer.mkdir()
        self.result_province_city_pkl = self.dirname_output_dealer.joinpath(
            '%s_result_province_city.pkl' % self.name)
        self.result_city_distribution_pkl = self.dirname_output_dealer.joinpath(
            '%s_result_city_distribution.pkl' % self.name)
        self.result_check_brand_ids_pkl = self.dirname_output_dealer.joinpath(
            '%s_result_check_brand_ids.pkl' % self.name)

        self.dirname_output_dealer_brand = self.dirname_output_dealer.joinpath(
            'brand')
        if not self.dirname_output_dealer_brand.exists():
            self.dirname_output_dealer_brand.mkdir()

        self.columns_province_city = [
            'id_province', 'name_province', 'pinyin_province',
            'count_province', 'firstchar', 'count_city', 'id_city',
            'name_city', 'pinyin_city'
        ]
        self.columns_city_distribution = [
            'pinyin_city', 'name_distribution', 'id_distribution'
        ]
        self.columns_dealer = [[
            'dealer_url', 'dealer_name', 'dealer_type', 'dealer_zhuying',
            'dealer_tel', 'dealer_address'
        ]]

        self.get_df_province_city(force=False, opencsv=False)
        self.df_models = Autohome_Model(verbose=False).get_df_models(
            force=False, clipboard=False, opencsv=False)
        self.df_brand_manu = self.df_models[[
            'brand_id', 'brand_name', 'manu_id', 'manu_name'
        ]].drop_duplicates()
        self.df_check_brand_ids = self.get_df_check_brand_ids(force=False)

        # 注册特殊方法
        self.ohterway_dict = {
            '133': self.otherway_get_df_dealer_tesla,  # 特斯拉
        }

        msgs = [
            'you can use self.session or self.driver',
            'init_preparetion done',
            '-' * 20,
        ]
        for msg in msgs:
            print(msg)

    def read_exist_pickle(self, filename):
        if filename.exists():
            df = pd.read_pickle(filename)
            return df

    def get_df_province_city(self, force=False, opencsv=False):
        '''
        get df province and city
        '''
        filename = self.result_province_city_pkl
        if force or not filename.exists():
            coroutines = [self.async_get_response(self.url_province_city)]
            task = self.run_async_loop(coroutines)[0]
            df_province_city = self.fetch_province_city_data_from_response(
                task.result())
            df_province_city.to_pickle(filename)
        else:
            df_province_city = self.read_exist_pickle(filename)
            if df_province_city is None:
                df_province_city = self.get_df_province_city(True)
        if opencsv:
            self.df_to_csv(df_province_city)

        self.df_province_ciy = df_province_city
        return df_province_city

    def fetch_province_city_data_from_response(self, response):
        datas = response.json()['AreaInfoGroups']
        df_provinces = []
        for data in datas:
            for k, v in data.items():
                if k != 'Key':
                    for province in v:
                        df_province = pd.DataFrame(province)
                        df_city = pd.DataFrame(dict(
                            df_province['Cities'])).transpose()
                        df_province = df_province.merge(df_city,
                                                        left_index=True,
                                                        right_index=True,
                                                        suffixes=('_Province',
                                                                  '_City'))
                        df_provinces.append(df_province)
        df_province_city = pd.concat(df_provinces, ignore_index=True)
        df_province_city = df_province_city.drop(columns=['Cities'])
        df_province_city = df_province_city.rename(
            columns={k: k.lower()
                     for k in df_province_city.columns})
        return df_province_city

    def get_df_city_distribution(self, force=False, opencsv=False):
        '''
        get df province and city
        '''
        df_province_city = self.get_df_province_city(force=force,
                                                     opencsv=False)
        filename = self.result_city_distribution_pkl

        if force or not filename.exists():
            coroutines = [
                self.async_get_response(self.url_city_distritution %
                                        pinyin_city) for pinyin_city in
                df_province_city['pinyin_city'].drop_duplicates()
            ]
            tasks = self.run_async_loop(coroutines)

            datas = []
            with tqdm(desc='fetch_city_distribution',
                      total=len(tasks)) as pbar:
                for task in tasks:
                    pbar.update(1)
                    data = self.fetch_city_distribution_data_from_response(
                        task.result())
                    if data:
                        datas += data

            df_city_distribution = pd.DataFrame(
                datas, columns=self.columns_city_distribution)
            df_city_distribution.to_pickle(filename)

        else:
            df_city_distribution = self.read_exist_pickle(filename)
            if df_city_distribution is None:
                df_city_distribution = self.get_df_city_distribution(
                    force=True, opencsv=False)

        if opencsv:
            self.df_to_csv(df_city_distribution)
        return df_city_distribution

    def fetch_city_distribution_data_from_response(self, response):
        regex = re.compile(r'/(.*?)/(.*?)/.*')
        data = []
        try:
            bsobj = bs4.BeautifulSoup(response.content, features='lxml')
            div = bsobj.find('div', {'class': 'item-box'})
            divas = div.select('a')
            for index in range(len(divas)):
                diva = divas[index]
                areainfo = diva.get('href')
                f = regex.search(areainfo).groups()
                data.append([f[0], diva.text, f[1]])
            return data
        except Exception as e:
            print(e, response.url)

    def get_df_brand_dealer_by_ids(self,
                                   brand_ids,
                                   force=False,
                                   opencsv=False):
        df_results = []
        for brand_id in brand_ids:
            df_brand = self.get_df_brand_dealer_by_id(brand_id,
                                                      force=force,
                                                      opencsv=False)
            if df_brand is not None:
                df_results.append(df_brand)
        df_result = pd.concat(df_results)
        if opencsv:
            self.df_to_csv(df_result)
        return df_result

    def get_df_brand_dealer_by_id(self, brand_id, force=False, opencsv=False):
        # brand_dealer主程序

        # 数据准备
        df_brand_manu_select = self.df_brand_manu[
            self.df_brand_manu['brand_id'] == brand_id]
        brand_name = df_brand_manu_select['brand_name'].values[0]
        filename = self.dirname_output_dealer_brand.joinpath('%s.pkl' %
                                                             brand_name)

        # 检查是否在auto中
        if brand_id in self.df_check_brand_ids['brand_id'].to_list():
            print('brand_id: %s in autohome dealers' % brand_id)
        elif brand_id in self.ohterway_dict:
            print('brand_id: %s in special func' % brand_id)
            return self.ohterway_dict[brand_id](force=force, opencsv=opencsv)
        else:
            print('brand_id: %s not in autohome dealers' % brand_id)
            return

        if force or not filename.exists():
            df_brand_dealer = pd.DataFrame()  # 预留空结果
            manu_dealer_ids = {
                k: []
                for k in df_brand_manu_select['manu_id'].to_list()
            }

            # 获取manu_dealer_ids
            for manu_id in manu_dealer_ids:
                manu_name = df_brand_manu_select[
                    df_brand_manu_select['manu_id'] ==
                    manu_id]['manu_name'].values[0]
                df_count = self.count_brand_manu_dealer(brand_id, manu_id)
                if df_count is None or len(df_count) == 0:
                    continue
                pinyin_cities = df_count[df_count['count'] > 0][
                    'pinyin'].drop_duplicates().to_list()
                print(
                    'brand_name: %s brand_id: %s manu_name:%s manu_id: %s has %s cities dealers'
                    % (brand_name, brand_id, manu_name, manu_id,
                       len(pinyin_cities)))
                manu_dealer_ids[manu_id] = self.get_manu_dealer_ids(
                    pinyin_cities, brand_id, manu_id)

            # 根据dealer_ids获取经销商信息
            df_results = []
            if len(manu_dealer_ids):
                # 处理manu_ids
                for manu_id, dealer_ids in manu_dealer_ids.items():
                    manu_name = df_brand_manu_select[
                        df_brand_manu_select['manu_id'] ==
                        manu_id]['manu_name'].values[0]
                    if len(dealer_ids) == 0:
                        continue
                    df_dealer_info = self.get_df_dealer_info(dealer_ids)
                    df_dealer_info['brand_name'] = brand_name
                    df_dealer_info['manu_name'] = manu_name
                    df_results.append(df_dealer_info)

            # 处理并合并经销商信息
            if len(df_results):
                df_left = df_results[0]
                if len(df_results) > 1:
                    columns = [c for c in df_left.columns if c != 'manu_name']
                    for df_right in df_results[1:]:
                        df_left = pd.merge(df_left,
                                           df_right,
                                           how='outer',
                                           on=columns)
                        df_left['manu_name'] = df_left[[
                            'manu_name_x', 'manu_name_y'
                        ]].apply(lambda row: self.concat_str_in_dataframe(
                            row['manu_name_x'], row['manu_name_y']),
                                 axis=1)
                        df_left = df_left.drop(
                            columns=['manu_name_x', 'manu_name_y'])
                df_brand_dealer = df_left
        else:
            df_brand_dealer = self.read_exist_pickle(filename)

        if len(df_brand_dealer):
            if opencsv:
                self.df_to_csv(df_brand_dealer)
            df_brand_dealer.to_pickle(filename)
        return df_brand_dealer

    def count_brand_manu_dealer(self, brand_id, manu_id):
        try:
            url = self.url_dealer_brand_manu_count.format(brand_id=brand_id,
                                                          manu_id=manu_id)
            response = self.session.get(url)
            data = response.json()
            cities = []
            groups = data['AreaInfoGroups']
            for key_dict in groups:
                values = key_dict['Values']
                for value in values:
                    v_cities = value.get('Cities')
                    if v_cities:
                        cities += v_cities
                    else:
                        for sub_key in value:
                            v_cities = sub_key.get('Cities')
                            if v_cities:
                                cities += v_cities
            df_count = pd.DataFrame(cities).drop_duplicates()
            df_count = df_count.rename(
                columns={k: k.lower()
                         for k in df_count.columns})
            return df_count
        except Exception as e:
            return

    def get_manu_dealer_ids(self, pinyin_cities, brand_id, manu_id):
        # 循环生成manu_dealer_ids
        manu_dealer_ids = []
        # 初始化
        # id_distribution = 0
        page_idx = 1
        coroutines = [
            self.async_get_response(self.url_dealer_brand.format(
                pinyin_city=pinyin_city,
                brand_id=brand_id,
                manu_id=manu_id,
                page_idx=page_idx),
                                    meta={
                                        'pinyin_city': pinyin_city,
                                        'brand_id': brand_id,
                                        'manu_id': manu_id,
                                        'page_idx': page_idx,
                                    }) for pinyin_city in pinyin_cities
        ]
        tasks = self.run_async_loop(coroutines)
        # 初始化完毕, 进入循环
        while True:
            _dealer_ids, _coroutines = self.loop_run_dealer_manu_by_city(tasks)
            if len(_dealer_ids):
                manu_dealer_ids += _dealer_ids
            if len(_coroutines) == 0:
                break
            else:
                tasks = self.run_async_loop(_coroutines)

        return manu_dealer_ids

    def loop_run_dealer_manu_by_city(self, tasks):
        _dealer_ids = []
        _coroutines = []
        for task in tasks:
            response = task.result()
            # dealer_ids = self.fetch_brand_dealer_ids_from_response(response)
            dealer_ids = self.fetch_brand_dealer_ids_from_response(response)
            if dealer_ids:
                # 保存结果
                _dealer_ids += dealer_ids
                if len(dealer_ids) == 15:
                    # 更新page_idx
                    meta = response.meta
                    page_idx = meta['page_idx'] + 1
                    meta['page_idx'] = page_idx
                    _coroutines.append(
                        self.async_get_response(
                            self.url_dealer_brand.format(**meta), meta=meta))

        return _dealer_ids, _coroutines

    def fetch_brand_dealer_ids_from_response(self, response):
        try:
            bsobj = bs4.BeautifulSoup(response.content, features='lxml')
            div = bsobj.find('div', {'class': 'dealer-list-wrap'})
            ul = div.find('ul', {'class': 'list-box'})
            lis = ul.findAll('li', {'class': 'list-item'})
            if len(lis):
                lis_id = [li.attrs['id'] for li in lis]
                return lis_id
        except Exception as e:
            pass

    def get_df_dealer_info(self, dealer_ids):
        coroutines = [
            self.async_get_response(url=self.url_dealer_info % dealer_id)
            for dealer_id in dealer_ids
        ]
        tasks = self.run_async_loop(coroutines)

        dfs = []
        for task in tasks:
            try:
                j = task.result().json()
                df = pd.DataFrame([j.values()], columns=j.keys())
                if len(df):
                    dfs.append(df)
            except Exception as e:
                pass
        df_dealer_info = pd.concat(dfs)
        return df_dealer_info

    def concat_str_in_dataframe(self, *args):
        args = [str(arg) for arg in args if not pd.isna(arg)]
        res = ','.join(args)
        return res

    def get_df_check_brand_ids(self, force=False):

        filename = self.result_check_brand_ids_pkl
        if force or not filename.exists():
            response = self.session.get(self.url_check_brand_ids)
            regex = re.compile('/beijing/0/(\d.*?)/.*')
            bsobj = bs4.BeautifulSoup(response.content, features='lxml')
            lis = bsobj.find_all('li',
                                 {'class': 'row row-hide data-brand-item'})
            results = []
            for li in lis:
                results += li.find_all('a')
            results = [r for r in results if r.text != '全部']
            results = [r.attrs['href'] for r in results]
            results = [
                regex.match(r).groups()[0] for r in results if regex.match(r)
            ]
            df_check_brand_ids = pd.DataFrame(results, columns=['brand_id'])
            df_check_brand_ids.to_pickle(filename)
        else:
            df_check_brand_ids = pd.read_pickle(filename)
        return df_check_brand_ids

    def otherway_get_df_dealer_tesla(self, force=False, opencsv=False):

        brand_name = '特斯拉'
        filename = self.dirname_output_dealer_brand.joinpath('%s.pkl' %
                                                             brand_name)

        if force or not filename.exists():
            url_list = 'https://www.tesla.cn/findus/list'
            # 获取市场信息
            response = self.session.get(url_list)
            bsobj = bs4.BeautifulSoup(response.content, features='lxml')
            lis = bsobj.find('body').find_all('li')

            provinces = []
            for li in lis:
                if '特斯拉体验店' in li.text:
                    provinces.append(li.span.text)

            datas = []
            url_experience = 'https://www.tesla.cn/findus/list/stores/{province}'
            coroutines = [
                self.async_get_response(
                    url_experience.format(province=parse.quote(province)),
                    headers=self.headers,
                    meta={'province': province},
                ) for province in provinces
            ]
            tasks = self.run_async_loop(coroutines)

            for task in tasks:
                response = task.result()
                province = response.meta['province']
                bsobj = bs4.BeautifulSoup(response.content, features='lxml')
                addresses = bsobj.find('body').find_all('address')
                for address in addresses:
                    datas.append([province] + address.text.split('\n')[:-2])
            columns = [
                'province',
                'blank_0',
                'name',
                'address',
                'address_detail',
                'city',
                'blank_1',
                'blank_2',
                'phone_1',
                'phone_2',
                'phone_3',
                'phone_4',
                'blank_3',
            ]
            df_experience = pd.DataFrame(datas, columns=columns)
            df_experience.to_pickle(filename)
        else:
            df_experience = pd.read_pickle(filename)

        if opencsv:
            self.df_to_csv(df_experience)
        return df_experience


# %%
# 获取省份城市
if __name__ == '__main__':
    self = Autohome_Dealer()
    df_province_city = self.get_df_province_city(force=False, opencsv=True)
    df_province_city
# %%
# 获取城市区域
if __name__ == '__main__':
    self = Autohome_Dealer()
    df_city_distribution = self.get_df_city_distribution(force=False,
                                                         opencsv=True)
# %%
# 获取经销商信息
if __name__ == '__main__':
    self = Autohome_Dealer()
    brand_id = '279'
    # brand_id = '351'
    df_brand_dealer = self.get_df_brand_dealer_by_id(brand_id=brand_id,
                                                     force=False,
                                                     opencsv=True)
# %%
# 获取经销商信息 Tesla
if __name__ == '__main__':
    self = Autohome_Dealer()
    brand_id = '133'
    df_brand_dealer = self.get_df_brand_dealer_by_id(brand_id=brand_id,
                                                     force=False,
                                                     opencsv=True)

# %%