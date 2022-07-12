# %%[markdown]

# - [汽车之家配置参数抓取](https://www.cnblogs.com/qiyueliuguang/p/8144248.html)
# %%

import asyncio
import re
import json

import pandas as pd
import bs4
from functools import reduce

from async_spider import Async_Spider
from autohome_model import Autohome_Model


# %%
class Autohome_Configuration(Async_Spider):
    name = 'autohome_configuration'

    # 轻型卡车页面不在默认级别中
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

        self.url_configuration = 'https://car.autohome.com.cn/config/series/%s.html'

        self.url_test_current = 'https://car.autohome.com.cn/config/series/2896.html'
        self.url_test_nolaunch = 'https://car.autohome.com.cn/config/series/6100.html'
        self.url_test_stop = 'https://www.autohome.com.cn/874'

        self.df_models = Autohome_Model(verbose=False).read_exist_data(
            clipboard=False)

        # url停售款, {series_id}_{year_id}
        self.url_configuration_for_stop = 'https://car.autohome.com.cn/config/series/{series_id}-{year_id}.html'

        self.result_pkl = self.dirname_output.joinpath('%s_result_series.pkl' %
                                                       self.name)
        self.result_keylink_pkl = self.dirname_output.joinpath(
            '%s_result_keylink.pkl' % self.name)

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
            df_exist = pd.DataFrame()
        if clipboard:
            self.df_to_clipboard(df_exist)
        return df_exist

    def read_exist_keylink(self, clipboard=True):
        if self.result_keylink_pkl.exists():
            df_keylink = pd.read_pickle(self.result_keylink_pkl)
            if clipboard:
                self.df_to_clipboard(df_keylink)
            return df_keylink

    def decorator_add_info_df_result(func):
        # decorator for add_info_df_result, 带参数
        def wrapper(self, *args, **kwargs):
            res = func(self, *args, **kwargs)
            opencsv = kwargs.get('opencsv', True)
            # 通过autohome_model获取
            df_result = res.merge(self.df_models,
                                  on='model_id',
                                  how='left',
                                  sort=False)
            self.df_to_csv(df_result, opencsv=opencsv)
            return res

        return wrapper

    def js_generate_js(self, response):
        try:
            js_result = (
                "var rules = '';"
                "var document = {};"
                "document.createElement = function() {"
                "      return {"
                "              sheet: {"
                "                      insertRule: function(rule, i) {"
                "                              if (rules.length == 0) {"
                "                                      rules = rule;"
                "                              } else {"
                "                                      rules = rules + '#' + rule;"
                "                              }"
                "                      }"
                "              }"
                "      }"
                "};"
                "document.querySelectorAll = function() {"
                "      return {};"
                "};"
                "document.head = {};"
                "document.head.appendChild = function() {};"
                "var window = {};"
                "window.decodeURIComponent = decodeURIComponent;")

            js_lst = re.findall(
                '(\(function\([a-zA-Z]{2}.*?_\).*?\(document\);)',
                response.text)
            for js in js_lst:
                js_result += js
            return js_result
        except Exception as e:
            return

    def js_generate_css_dict(self, rules):
        rules = rules.split('#')
        regex_key = re.compile(r'\.(.*?)::')
        regex_value = re.compile(r'content:"(.*)"')
        css_dict = {
            "<span class='%s'></span>" % regex_key.search(rule).group(1):
            regex_value.search(rule).group(1)
            for rule in rules
        }
        return css_dict

    def js_fetch_var(self, response, var, css_dict={}):
        # 查找css返回正常的值
        css_dict.update({'&nbsp': ' '})
        regex_var_pattern = '.*var {var} = (.*);'.format(var=var)
        regex_replace = re.compile('|'.join(map(re.escape, css_dict)))
        # 寻找var的定义
        value = re.compile(regex_var_pattern.format(var=var)).search(
            response.text).group(1)
        # 替换特殊字符和css混淆
        value = regex_replace.sub(lambda m: css_dict[m.group(0)], value)
        return value

    def find_year_id(self, response, return_type='bool'):
        # return_type: 'bool'
        # 如果含有data-yearid, 返回True, 是停售车型
        # 如果没有, 返回False, 2种情况, 一种是在售，一种是待售, 根据self.find_spec_id 判断
        # return_type: 'dict'
        # 返回 {year_id: 年款}

        try:
            if return_type == 'bool':
                # print(response.content)
                if response.content.find(b'data-yearid') > 0:
                    return True
            elif return_type == 'dict':
                bsobj = bs4.BeautifulSoup(response.content, 'lxml')
                div = bsobj.find('div', {'class': 'title-subcnt'})
                if div:
                    lis = div.find_all('li')
                    year_ids = {
                        li.a.attrs['data-yearid']: li.a.text
                        for li in lis
                    }
                    return year_ids
        except Exception as e:
            print(e)
            if hasattr(response, 'meta'):
                print(response.meta.get('model_id', False))
            return True  # 终端运行

    def find_spec_id(self, response):
        # 查找spec_id, 如果有返回列表
        # 待售款没有
        regex_specid = re.compile('.*(\[\{\"showstate\".*?\}\])', re.S)
        m = regex_specid.match(response.text)
        if m:
            data = json.loads(m.groups(0)[0])
            spec_ids = [d['specid'] for d in data]
            return spec_ids

    # var_dict 含有['keyLink', 'config', 'option'] 3类数据

    def process_keylink(self, json_keylink, opencsv=False):
        # json_keylink = var_dict['keyLink']
        df_keylink = pd.DataFrame(json.loads(json_keylink))
        df_keylink = df_keylink.sort_values('id')
        df_keylink.to_pickle(self.result_keylink_pkl)
        if opencsv:
            self.df_to_csv(df_keylink)
        return df_keylink

    def process_config(self, json_config):
        # json_config = var_dict['config']
        configs = json.loads(json_config)
        columns_config = [
            'cattype_1', 'cattype_2', 'cattype_3', 'cattype_4', 'spec_id',
            'value'
        ]
        cattype_1 = 'config'
        cattype_4 = None  # 预留, 与option对齐
        datas = []
        paramtypeitems = configs['result']['paramtypeitems']  # 3层嵌套 列表+字典
        for paramtypeitem in paramtypeitems:
            # 第1层 含有 ['name','paramitems']
            cattype_2 = paramtypeitem['name']
            for valueitem in paramtypeitem['paramitems']:
                # 第2层 含有 ['name','valueitems']
                cattype_3 = valueitem['name']
                for items in valueitem['valueitems']:
                    # 第3层 含有 ['specid','value']
                    spec_id = items['specid']
                    v = items['value']
                    datas.append([
                        cattype_1, cattype_2, cattype_3, cattype_4, spec_id, v
                    ])
        df_config = pd.DataFrame(datas, columns=columns_config)
        # 转置处理
        df_config = df_config.pivot(
            'spec_id',
            columns=['cattype_1', 'cattype_2', 'cattype_3', 'cattype_4'],
            values='value').reset_index()
        return df_config

    def process_option(self, json_option):
        options = json.loads(json_option)
        columns_option = [
            'cattype_1', 'cattype_2', 'cattype_3', 'cattype_4', 'spec_id',
            'value'
        ]
        cattype_1 = 'option'
        datas = []
        configtypeitems = options['result']['configtypeitems']  # 4层嵌套 列表+字典
        for configtypeitem in configtypeitems:
            # 第1层 含有 ['name','paramitems']
            cattype_2 = configtypeitem['name']
            for valueitem in configtypeitem['configitems']:
                # 第2层 含有 ['name','valueitems']
                cattype_3 = valueitem['name']
                for items in valueitem['valueitems']:
                    # 第3层 含有 ['specid','value', 'sublist]
                    spec_id = items['specid']
                    sublist = items['sublist']
                    if len(sublist):
                        for sub in sublist:
                            cattype_4 = sub['subname']
                            v = sub['subvalue']
                            datas.append([
                                cattype_1, cattype_2, cattype_3, cattype_4,
                                spec_id, v
                            ])
                    else:
                        cattype_4 = None
                        v = items['value']
                        datas.append([
                            cattype_1, cattype_2, cattype_3, cattype_4,
                            spec_id, v
                        ])
        df_option = pd.DataFrame(datas, columns=columns_option)
        # 转置处理
        df_option = df_option.pivot(
            'spec_id',
            columns=['cattype_1', 'cattype_2', 'cattype_3', 'cattype_4'],
            values='value').reset_index()
        return df_option

    async def async_update_keylink(self, response, css_dict):
        # async 更新keylink
        if not self.result_keylink_pkl.exists():
            json_keylink = self.js_fetch_var(response, 'keyLink', css_dict)
            self.df_keylink = self.process_keylink(json_keylink, opencsv=False)

    async def async_get_df_configuration_by_model_id(self,
                                                     model_id,
                                                     update_keylink=False,
                                                     opencsv=False):
        # async 更新gonfiguration
        max_retry = self.max_retry
        while True:
            max_retry -= 1
            response = await self.async_get_response(
                self.url_configuration % model_id, meta={'model_id': model_id})
            if response is not None and max_retry < 0:
                break
        # response = self.run_async_loop([
        #     self.async_get_response(self.url_configuration % model_id,
        #                             meta={'model_id': model_id})
        # ])[0].result()
        # response = self.session.get(self.url_configuration % model_id)
        # print(self.url_configuration % model_id)

        # 判断是否是停售
        year_id = self.find_year_id(response)
        if year_id:  # 停售
            return
        # 判断是否有spec_ids
        spec_ids = self.find_spec_id(response)
        if spec_ids is None or self.check_spec_id(spec_ids,
                                                  self.df_exist):  # 待售
            return

        js = self.js_generate_js(response)
        rules = self.js_return_var(js, 'rules')
        css_dict = self.js_generate_css_dict(rules)

        if update_keylink:
            await self.async_update_keylink(update_keylink, response, css_dict)
        # if update_keylink or not self.result_keylink_pkl.exists():
        #     json_keylink = self.js_fetch_var(response, 'keyLink', css_dict)
        #     self.df_keylink = self.process_keylink(json_keylink, opencsv=False)

        var_dict = {
            var: self.js_fetch_var(response, var, css_dict)
            for var in ['config', 'option']
        }

        df_config = self.process_config(var_dict['config'])
        df_option = self.process_option(var_dict['option'])
        # 合并
        df_configuration = df_config.merge(df_option, on='spec_id')
        # 添加model_id
        df_configuration['model_id'] = model_id
        if opencsv:
            self.df_to_csv(df_configuration)
        return df_configuration

    async def async_get_df_configuration_by_model_ids(self,
                                                      model_ids,
                                                      update_keylink=False):
        tasks = [
            self.async_get_df_configuration_by_model_id(
                model_id, update_keylink) for model_id in model_ids
        ]
        results = await asyncio.gather(*tasks)
        return results

    def df_configuration_columns_sort(self, df_configuration):
        # 调整顺序
        columns = df_configuration.columns.copy().to_list()
        column_left = []
        column_right = []
        for k in columns:
            if not k[1]:
                column_left.append(k)
            else:
                column_right.append(k)
        df_configuration = self.dfs_merge(
            [df_configuration[column_left], df_configuration[column_right]],
            index=True)
        return df_configuration

    def check_spec_id(self, spec_ids, df_exist):
        # series = df_exist['spec_id']
        # 当spec_id中有任何一个不在series中, 返回False

        if df_exist is None:
            return False  # 当series不存在, 直接返回False

        s = pd.Series(spec_ids, dtype=str)
        series = df_exist['spec_id']
        series = series.astype(str)
        check = s.map(lambda x: x in series.to_list())
        return check.all()

    @decorator_add_info_df_result
    def get_df_configuration_by_model_ids_main(self,
                                               model_ids,
                                               update_keylink=False,
                                               opencsv=False):
        filename = self.result_pkl
        if not filename.exists():
            self.df_exist = None
            lenth = 0
        else:
            self.df_exist = self.read_exist_data(clipboard=False)
            lenth = len(self.df_exist)

        df_results = asyncio.run(
            self.async_get_df_configuration_by_model_ids(
                model_ids, update_keylink))

        df_results = [self.df_exist] + df_results
        df_results = [d for d in df_results if d is not None]
        # 合并
        df_configuration = self.dfs_append(df_results)
        # return df_configuration
        if len(df_configuration) > lenth:
            # 删除重复
            df_configuration = df_configuration.drop_duplicates(subset=[
                ('spec_id', '', '', '')
            ],
                                                                keep='last')
            # 排序
            df_configuration = df_configuration.sort_values(
                ['model_id', 'spec_id'])
            df_configuration = df_configuration.reset_index(drop=True)
            # 调整columns顺序
            df_configuration = self.df_configuration_columns_sort(
                df_configuration)
            # 保存
            df_configuration.to_pickle(filename)
        else:
            print(
                'These is no new configuration data currently of model_ids %s'
                % model_ids)
            df_configuration = self.df_exist
        return df_configuration

    def get_df_configuration_by_df_models(self, df_models):
        model_ids = df_models['model_id'].drop_duplicates().to_list()
        df_configuration = self.get_df_configuration_by_model_ids_main(
            model_ids)
        return df_configuration


# %%
# 按照model_ids更新df_configuration
if __name__ == '__main__':
    self = Autohome_Configuration()
    model_ids = ['2896', '5200']
    # model_ids = ['906', '700']
    df_configuration = self.get_df_configuration_by_model_ids_main(
        model_ids, opencsv=False)

# %%
# 按照df_models更新df_configuration
if __name__ == '__main__':
    self = Autohome_Configuration()
    df_configuration = self.get_df_configuration_by_df_models(self.df_models)

# %%
# 读取df_configuration
if __name__ == '__main__':
    self = Autohome_Configuration()
    df_configuration = self.read_exist_data()
# %%
# 读取keyling
if __name__ == '__main__':
    self = Autohome_Configuration()
    df_keylink = self.read_exist_keylink()
# %%
# %%
