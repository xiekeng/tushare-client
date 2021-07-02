import math

import requests, json


def get_data_list(page=1, per_page=100):
    iwencai_url = f'http://ai.iwencai.com/urp/v7/landing/getDataList?query=%E7%94%B3%E4%B8%87%E8%A1%8C%E4%B8%9A&page={page}&perpage={per_page}&comp_id=5722297&uuid=24087'
    headers={
        'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'http://www.iwencai.com'
             }
    r = requests.post(iwencai_url, headers=headers)
    return r.content.decode()
    # print(r.content)
    # with open('sws_industry_tmp', 'w') as f:
    #     f.write(r.content.decode())


jsonValue = json.loads(get_data_list())
data = jsonValue['answer']['components'][0]['data']['datas']
row_count = jsonValue['answer']['components'][0]['data']['meta']['extra']['row_count']

for i in range(1, row_count, 100):
    for record in data:
        if record['所属申万行业'] is not None:
            sws = record['所属申万行业'].split('--')
        else:
            sws = ['', '', '']
        print([record['股票代码'], record['股票简称'], sws[0], sws[1], sws[2]])

    jsonValue = json.loads(get_data_list(math.floor(i/100) + 2))
    data = jsonValue['answer']['components'][0]['data']['datas']
