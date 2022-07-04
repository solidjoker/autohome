# %%[markdown]
# update: 20220628

#### 目标：
# 1. post数据: 分车型保存
# 2. 用户数据: 追踪发帖
# 3. 帖子数据: 机器学习破解
# 4. 帖子交互: 放弃

# %%
import re
import time
import pandas as pd
import bs4
import pickle

from tqdm.auto import tqdm

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from async_spider import Async_Spider
from autohome_model import Autohome_Model
from autohome_font import Autohome_Font

# %%
class Autohome_BBS(Async_Spider):
    name = 'autohome_bbs'

    # 轻型卡车页面不在默认级别中
    def __init__(self,
                 max_retry=3,
                 async_num=100,
                 size=100,
                 threading_init_driver=False,
                 backend=None):
        super().__init__(max_retry=max_retry,
                         async_num=async_num,
                         threading_init_driver=threading_init_driver)

        self.size = size
        if backend is None:
            backend = 'matplotlib'
        self.backend = backend
        self.init_preparetion()

    def init_preparetion(self):
        self.df_models = Autohome_Model(verbose=False).read_exist_data(
            clipboard=False)

        self.url_bbs_homepage = 'https://club.autohome.com.cn/bbs/forum-c-%s-1.html'

        self.dirname_output_bbs = self.dirname_output.joinpath('bbs')
        if not self.dirname_output_bbs.exists():
            self.dirname_output_bbs.mkdir()

        self.url_post_model = 'https://la.autohome.com.cn/club/get_club_small_video_list?_appid=club&bbs_id={model_id}&size={size}&order_type=2&page_num={page}'
        self.dirname_post_model = self.dirname_output_bbs.joinpath('model')
        if not self.dirname_post_model.exists():
            self.dirname_post_model.mkdir()

        self.url_topic_author = 'https://iservice.autohome.com.cn/clubapp/OtherTopic-{author_id}-all-{page}.html'
        self.url_topic_status = 'https://clubajax.autohome.com.cn/topic/rv?&ids=%s'
        self.dirname_topic_author = self.dirname_output_bbs.joinpath('author')
        if not self.dirname_topic_author.exists():
            self.dirname_topic_author.mkdir()

        self.dirname_biz = self.dirname_output_bbs.joinpath('biz')
        if not self.dirname_biz.exists():
            self.dirname_biz.mkdir()
        self.regex_biz = re.compile(
            'https?://club\\.autohome\\.com\\.cn/bbs/thread/.*?/(\d.*)\\-1\\.html'
        )

        msgs = [
            'you can use self.session or self.driver',
            'init_preparetion done',
            '-' * 20,
        ]
        for msg in msgs:
            print(msg)

    def get_post_model_by_model_id(self, model_id, opencsv=False):
        filename = self.dirname_post_model.joinpath('post_model_id_%s.pkl' %
                                                    model_id)
        df_exist = self.read_post_model_by_model_id(model_id, False)
        if df_exist is None:
            df_exist = pd.DataFrame()

        pages = range(1, (10000 - 1) // self.size + 1 + 1)  # 最多10000条
        coroutines = [
            self.async_get_response(
                self.url_post_model.format(model_id=model_id,
                                           size=self.size,
                                           page=page)) for page in pages
        ]
        tasks = self.run_async_loop(coroutines,
                                    tqdm_desc='async_fetch_bbs_post')

        df_posts = []
        for task in tasks:
            _df_post = self.fetch_post_model_from_response(task.result())
            if _df_post is not None:
                df_posts.append(_df_post)

        if len(df_posts) > 0:
            df_post = pd.concat(df_posts)
            df_post = df_exist.append(df_post)
            df_post = df_post.drop_duplicates('biz_id', keep='last')
            df_post = df_post.sort_values('publish_time')
            df_post = df_post.reset_index(drop=True)
            df_post.to_pickle(filename)
        else:
            df_post = df_exist

        if opencsv:
            self.df_to_csv(df_post)

        return df_post

    def fetch_post_model_from_response(self, response):
        if response is not None:
            data = response.json()
            if data['returncode'] == 0:
                df_post = pd.DataFrame(data['result']['items'])
                return df_post

    def read_post_model_by_model_id(self, model_id, opencsv=False):
        filename = self.dirname_post_model.joinpath('post_model_id_%s.pkl' % model_id)
        if filename.exists():
            df_exist = pd.read_pickle(filename)
            if opencsv:
                self.df_to_csv(df_exist)
            return df_exist            

    def get_topic_author_by_author_id(self, author_id, opencsv=False):
        filename = self.dirname_topic_author.joinpath(
            'topic_author_id_%s.pkl' % author_id)
        df_exist = self.read_topic_author_by_author_id(model_id, False)
        if df_exist is None:
            df_exist = pd.DataFrame()

        # 初始页
        coroutines = [
            self.async_get_response(self.url_topic_author.format(
                author_id=author_id, page=1),
                                    meta={'author_id': author_id})
        ]
        response = self.run_async_loop(
            coroutines, tqdm_desc='async_fetch_topic_author')[0].result()
        df_topic = self.fetch_topic_author_from_response(response)
        pages = self.get_author_topic_pages(response)

        if pages:
            coroutines = [
                self.async_get_response(
                    self.url_topic_author.format(author_id=author_id,
                                                 page=page),
                    meta={'author_id': author_id}) for page in pages
            ]
            tasks = self.run_async_loop(coroutines,
                                        tqdm_desc='async_fetch_topic_author')
            df_topics = []
            for task in tasks:
                _df_topic = self.fetch_topic_author_from_response(
                    task.result())
                if _df_topic is not None:
                    df_topics.append(_df_topic)
            if len(df_topics):
                df_topic = pd.concat([df_topic] + df_topics, ignore_index=True)

        if len(df_topic) > 0:
            df_topic = df_exist.append(df_topic)
            df_topic = df_topic.drop_duplicates('topic_id', keep='last')
            df_topic = df_topic.sort_values('topic_time')
            df_topic = df_topic.reset_index(drop=True)
            df_topic.to_pickle(filename)
        else:
            df_topic = df_exist

        if opencsv:
            self.df_to_csv(df_topic)
        return df_topic

    def fetch_topic_author_from_response(self, response):
        try:
            author_id = response.meta['author_id']

            bsobj = bs4.BeautifulSoup(response.content, 'lxml')
            table = bsobj.find('table', {'class': 'topicList'})
            trs = table.find_all('tr')[1:]  # 去除第一个

            columns = [
                'author_id', 'topic_id', 'topic_type', 'topic_title',
                'topic_url', 'topic_address', 'topic_address_url', 'topic_time'
            ]
            datas = []
            for tr in trs:
                div = tr.find('div', {'class': 'pr fullWidth'})
                div_icon = div.find('div', {'class': 'uh4'})
                # 是否精华帖 图片帖
                icon = div_icon.span.attrs['class']
                if icon == ['icon_wz2']:
                    topic_type = '图'
                elif icon == ['icon_wz4']:
                    topic_type = '精'
                else:
                    topic_type = '无'

                p_title, p_address = div.find_all('p')
                topic_title = p_title.text.strip()
                topic_url = 'https:%s' % p_title.a.attrs['href']
                topic_id = self.regex_biz.match(topic_url).group(1)
                topic_address = p_address.text.strip()
                topic_address_url = 'https:%s' % p_address.a.attrs['href']
                topic_time = tr.find_all('td')[-1].text.strip().split('\r')[0]
                datas.append([
                    author_id, topic_id, topic_type, topic_title, topic_url,
                    topic_address, topic_address_url, topic_time
                ])

            df_topic = pd.DataFrame(datas, columns=columns)
            topic_ids = ','.join(df_topic['topic_id'].to_list())

            url_topic_status = 'https://clubajax.autohome.com.cn/topic/rv?&ids=%s' % topic_ids
            df_topic_status = pd.DataFrame(
                self.session.get(url_topic_status).json())
            df_topic_status = df_topic_status.rename(
                columns={'topicid': 'topic_id'})
            df_topic_status['topic_id'] = df_topic_status['topic_id'].astype(
                str)
            df_topic = df_topic.merge(df_topic_status, on='topic_id')
            return df_topic

        except Exception as e:
            print(e)

    def get_author_topic_pages(self, response):
        bsobj = bs4.BeautifulSoup(response.content, 'lxml')
        div = bsobj.find('div', {'class': 'paging'})
        if div:
            pa = div.find_all('a')[-2]
            pages = range(2, int(pa.text.strip()))
            return pages
        else:
            return

    def read_topic_author_by_author_id(self, author_id, opencsv=False):
        filename = self.dirname_topic_author.joinpath(
            'topic_author_id_%s.pkl' % author_id)
        if filename.exists():
            df_exist = pd.read_pickle(filename)
            if opencsv:
                self.df_to_csv(df_exist)
            return df_exist       

    def read_biz_by_biz_id(self, biz_id):
        filename = self.dirname_biz.joinpath('%s.pkl' % biz_id)
        if filename.exists():
            biz_dict = pickle.load(open(filename,'rb'))
            return biz_dict

    def get_biz_by_url_biz(self, url_biz, recognize=True):
        url_biz = url_biz.replace('http://', 'https://')
        biz_id = self.regex_biz.match(url_biz).group(1)
        response = self.session.get(url_biz)

        # if css_process is False:
        # 没有解析css文字混淆

        # 作者, 标题, 正文
        author_id, biz_title, biz_contents = self.get_biz_content(response)
        # 图片
        df_biz_imgs = self.get_biz_imgs(response)
        # 回复
        df_biz_replies, biz_ttfs = self.get_biz_replies(response)

        result = {
            'biz_id': biz_id,
            'author_id': author_id,
            'biz_title': biz_title,
            'biz_contents': biz_contents,
            'df_biz_imgs': df_biz_imgs,
            'df_biz_replies': df_biz_replies,
            'biz_ttfs': biz_ttfs,
        }

        if recognize:
            af = Autohome_Font(backend=self.backend)
            result = af.replace_biz_content(result)

        filename = self.dirname_biz.joinpath('%s.pkl' % biz_id)
        pickle.dump(result, open(filename, 'wb'))

        return result

    def get_biz_content(self, response):
        bsobj = bs4.BeautifulSoup(response.content, 'lxml')
        div = bsobj.select_one('div.post-wrap')
        div_author = div.select_one('div.js-user-info-container')
        author_id = div_author.attrs['data-user-id']
        div_title = div.select_one('div.post-title')
        biz_title = div_title.text.strip()
        div_paragraphs = div.select('div.tz-paragraph')
        biz_contents = [
            div_papagraph.text.strip() for div_papagraph in div_paragraphs
        ]
        return author_id, biz_title, biz_contents

    def get_biz_imgs(self, response):
        # 原帖图片
        bsobj = bs4.BeautifulSoup(response.content, 'lxml')
        div = bsobj.select_one('div.post-wrap')
        div_imgs = div.select('div.tz-figure')
        data_imgs = []
        for div_img in div_imgs:
            img_text = div_img.text.strip()
            img_src = 'https:%s' % div_img.img.attrs['data-src']
            data_imgs.append([img_text, img_src])
        df_img = pd.DataFrame(data_imgs, columns=['img_text', 'img_url'])
        df_img['img_content'] = df_img['img_url'].apply(
            lambda x: self.session.get(x).content)
        return df_img

    def get_biz_replies(self, response):
        url_biz = response.url

        df_reply = self._get_biz_replies(response, 1)
        biz_ttfs = {1: self.ttf_get_ttf(response)}
        pages = self._get_biz_pages(response)
        
        # 分页符
        if pages:
            responses = [
                self.session.get(url_biz.replace('-1', '-%s' % page))
                for page in pages
            ]
            df_replies = []
            for page, res in enumerate(responses,2):
                _df_reply = self._get_biz_replies(res, page)
                if _df_reply is not None:
                    df_replies.append(_df_reply)
                    biz_ttfs[page] = self.ttf_get_ttf(res)
            if df_replies:
                df_reply = pd.concat([df_reply] + df_replies, ignore_index=True)


        return df_reply, biz_ttfs

    def _get_biz_replies(self, response, page):
        # 没有解析css文字混淆
        try:
            bsobj = bs4.BeautifulSoup(response.content, 'lxml')
            ul = bsobj.select_one('ul.reply-wrap')
            lis = ul.select('li.js-reply-floor-container')
            datas = []
            for li in lis:
                try:
                    reply_author_id = li.attrs['data-member-id']
                    reply_content = li.select_one(
                        'div.reply-detail').text.strip()
                    datas.append([reply_author_id, reply_content, page])
                except Exception as e:
                    print(e)
            df_reply = pd.DataFrame(
                datas, columns=['reply_author_id', 'reply_content', 'page'])
            return df_reply
        except Exception as e:
            print(e)

    def _get_biz_pages(self, response):
        bsobj = bs4.BeautifulSoup(response.content, 'lxml')
        pagination = bsobj.select_one('section.pagination-container')
        div = pagination.select_one('div.athm-page__editor')
        div.text.strip()
        page = int(re.match('\/(\d.*)页', div.text.strip()).group(1))
        if page > 1:
            return range(2, page + 1)

    # 处理字体混淆
    def ttf_find_url(self, response):
        # 找到字体链接
        regex = re.compile('.*(k3.autoimg.*?\.ttf).*', re.S)
        m = regex.match(response.text)
        url_ttf = 'https://%s' % m.group(1)
        return url_ttf

    def ttf_get_ttf(self, response):
        url_ttf = self.ttf_find_url(response)
        response_ttf = self.session.get(url_ttf)
        if response_ttf.status_code == 200:
            ttf = response_ttf.content
            return ttf

# %%
# 根据model_id 获取bbs post
if __name__ == '__main__':
    self = Autohome_BBS(size=500)
    model_id = '3554'  # 昂科威
    df_post_model = self.get_post_model_by_model_id(model_id)
# %%
# 根据author_id 获取bbs post
if __name__ == '__main__':
    self = Autohome_BBS(size=500)
    author_id = '81093146'
    df_topic_author = self.get_topic_author_by_author_id(author_id)
# %%
# 根据url_biz获取biz_content dict
if __name__ == '__main__':
    self = Autohome_BBS(size=500)
    url_biz = 'https://club.autohome.com.cn/bbs/thread/52db01a35971f950/102697565-1.html'  # 多页回复
    url_biz = df_post_model['pc_url'].iloc[0]
    biz_content = self.get_biz_by_url_biz(url_biz, recognize=True)
    print(biz_content.keys())
    print(biz_content['biz_contents'])
    print(biz_content['df_biz_replies'])

# %%
# 读取biz_content dict
if __name__ == '__main__':
    biz_id = '102697565'
    biz_content = self.read_biz_by_biz_id(biz_id)
    print(biz_content.keys())
    print(biz_content['biz_contents'])
    print(biz_content['df_biz_replies'])
# %%