import requests


def _get_data_list(page=1, per_page=100, date='2021-07-07'):
    #url_sse_etf_size = f'http://query.sse.com.cn/commonQuery.do?isPagination=true&pageHelp.pageSize={per_page}&pageHelp.pageNo={page}&pageHelp.cacheSize=1&sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE={date}'
    url_sse_etf_size = f'http://query.sse.com.cn/commonQuery.do?sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE={date}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Referer': 'http://www.sse.com.cn/'
    }
    r = requests.get(url_sse_etf_size, headers=headers)
    return r.content.decode()


print(_get_data_list())




