# %%
import base64
import requests
from baidu_aksk import baidu_api_key, baidu_secret_key
# %%
class Baidu_ORC:
    def __init__(self):
        self.access_token = self.get_access_token()
        # '24.78ec14cfb4796c3416b9e7232c547e86.2592000.1659110746.282335-26575674'

    def get_access_token(self):
        api_key = baidu_api_key
        secret_key = baidu_secret_key
        url = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}'.format(
            client_id=api_key, client_secret=secret_key)
        try:
            response = requests.get(url)
            data = response.json()
            access_token = data['access_token']
            return access_token
        except Exception as e:
            print(e)

    def general_basic(self, filename):
        # 通用文字识别
        # url = "https://aip.baidubce.com/rest/2.0/ocr/v1/general_basic"
        url = "https://aip.baidubce.com/rest/2.0/ocr/v1/general_basic?access_token={access_token}".format(
            access_token=self.access_token)
        with open(filename, 'rb') as f:
            im = base64.b64encode(f.read())
        params = {"image": im}
        # access_token = '[调用鉴权接口获取的token]'
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        response = requests.post(url, data=params, headers=headers)
        if response:
            result = response.json()
            return result
# %%
if __name__ == '__main__':
    bo = Baidu_ORC()
    access_token = bo.get_access_token()
    # result = bo.general_basic('mat.png')
    # print(result)
# %%

# %%