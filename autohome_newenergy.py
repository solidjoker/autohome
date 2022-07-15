# %%
import pandas as pd
import bs4

from async_spider import Async_Spider

# %%
class Autohome_Newenergy(Async_Spider):
    name = 'autohome_newenergy'

    def __init__(self, max_retry=3, concurrency=100, threading_init_driver=False):
        super().__init__(max_retry=max_retry,
                         concurrency=concurrency,
                         threading_init_driver=threading_init_driver)
        self.init_preparetion()

    def init_preparetion(self):
        self.result_columns = ['model_id']
        self.url_ne = 'https://www.autohome.com.cn/car/0_0-0.0_0.0-0-0-0-0-701-0-0-0/'  # all
        self.url_test = self.url_ne
        self.url_ne_types = {
            '纯电动':
            'https://www.autohome.com.cn/car/0_0-0.0_0.0-0-0-0-0-4-0-0-0/',
            '插电式混动':
            'https://www.autohome.com.cn/car/0_0-0.0_0.0-0-0-0-0-5-0-0-0/',
            '增程式':
            'https://www.autohome.com.cn/car/0_0-0.0_0.0-0-0-0-0-6-0-0-0/',
            '氢燃料':
            'https://www.autohome.com.cn/car/0_0-0.0_0.0-0-0-0-0-7-0-0-0/',
        }
        # 1: 汽油, 2: 柴油, 3: 油电混合, 701: 新能源, 801: 轻混系统
        # 1是异步加载...
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
            filename = self.dirname_output.joinpath(
                'autohome_model_result.pkl')
            opencsv = kwargs.get('opencsv', True)
            # 通过autohome_model获取
            if filename.exists():
                df_models = pd.read_pickle(filename)
                df_result = res.merge(
                    df_models, on='model_id',
                    how='right')  # 不显示微卡, 需要定期更新autohome_model
                # df_result = res.merge(df_models, on='model_id', how='left') # 显示微卡
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
        df_result = df_result.sort_values(['model_id'])
        # 去掉全空信息
        df_result = df_result[~pd.isna(df_result).all(axis=1)]
        df_result = df_result.reset_index(drop=True)
        df_result.to_pickle(self.result_pkl)
        if clipboard:
            self.df_to_clipboard(df_result)
        return df_result

    @decorator_add_info_df_result
    def get_df_nemodels(self, force=True, clipboard=True, opencsv=True):
        # force=True 强制重新运行
        # return df_segment: segment_id, segment_name
        df_exist = self.read_exist_data(clipboard=False)
        if force or len(df_exist) == 0:
            df_result = self._get_df_nemodels()
        else:
            df_result = pd.DataFrame(columns=self.result_columns)
        df_result = df_exist.append(df_result)
        df_result = df_result.drop_duplicates('model_id')
        self.sort_save_result(df_result, clipboard=clipboard)
        return df_result

    def _get_df_nemodels(self):
        coroutines = [
            self.async_get_response(self.url_ne, max_retry=self.max_retry)
        ]
        tasks = self.run_async_loop(coroutines, tqdm_desc='async_get_nemodels')
        task = tasks[0]
        df_nemodels = self.get_df_nemodels_from_response(task.result())
        return df_nemodels

    def get_df_nemodels_from_response(self, response):
        bsobj = bs4.BeautifulSoup(response.content, features='lxml')
        datas = []
        h4s = bsobj.select('h4')  # 所有车型 按品牌 按关注度
        h4s = h4s[:(len(h4s) // 2)]  # 所有车型 按品牌
        for h4 in h4s:
            try:
                model_id = h4.parent['id'][1:]
                datas.append(model_id)
            except Exception as e:
                print(e)
        df_nemodels = pd.DataFrame(datas, columns=self.result_columns)
        return df_nemodels

    @decorator_add_info_df_result
    def get_df_ne_types(self, force=True, opencsv=True):
        df_nemodels = self.get_df_nemodels(force=force,
                                           clipboard=False,
                                           opencsv=False)

        coroutines = [
            self.async_get_response(v,
                                    meta={'ne_type': k},
                                    max_retry=self.max_retry)
            for k, v in self.url_ne_types.items()
        ]
        tasks = self.run_async_loop(coroutines, tqdm_desc='async_get_ne_type')
        df_ne_types = [
            self.get_df_ne_type_from_response(task.result()) for task in tasks
        ]

        df_result = df_nemodels.copy()
        for df_ne_type in df_ne_types:
            df_result = df_result.merge(df_ne_type, on='model_id', how='left')
        df_result['new_energy'] = True
        df_result.to_pickle(
            self.dirname_output.joinpath('%s_netypes_result.pkl' % self.name))
        return df_result

    def get_df_ne_type_from_response(self, response):
        bsobj = bs4.BeautifulSoup(response.content, features='lxml')
        datas = []
        h4s = bsobj.select('h4')
        h4s = h4s[:(len(h4s) // 2)]
        ne_type = response.meta['ne_type']
        for h4 in h4s:
            try:
                model_id = h4.parent['id'][1:]
                datas.append(model_id)
            except Exception as e:
                print(e)
        df_ne_type = pd.DataFrame(datas, columns=self.result_columns)
        df_ne_type[ne_type] = True
        return df_ne_type


# %%
if __name__ == '__main__':
    self = Autohome_Newenergy()
    # df_nemodels = self.get_df_nemodels(force=False, clipboard=False, opencsv=False)
    df_netypes = self.get_df_ne_types()

# %%