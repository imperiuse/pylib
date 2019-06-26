import requests
import random

from urllib.error import URLError
from urllib.request import Request, urlopen

from ..logger import CLogger

"""
########################################################################################################################
###################### H T T P     R E Q E S T S     H E L P E R S    ##################################################
########################################################################################################################

"""

"""
   >>> import requests
   >>> r = requests.get('https://www.python.org')
   >>> r.status_code
   200
   >>> 'Python is a programming language' in r.content
   True

... or POST:

   >>> payload = dict(key1='value1', key2='value2')
   >>> r = requests.post('https://httpbin.org/post', data=payload)
   >>> print(r.text)
   {
     ...
     "form": {
       "key2": "value2",
       "key1": "value1"
     },
     ...
   }
"""


def simple_http_get(url: str, data: dict) -> any:
    """
    Simple GET request
    :param url: url
    :param data: dict with http param
    :return: Reponse
    """
    # url = "https://url.com"
    # data = {
    #     'param1': 'value_param_1'
    # }
    return requests.get(url, params=data)


def simple_http_post(url: str, data: dict, json: dict, kwargs) -> any:
    """
    Simple POST request
    :param url: url
    :param data: dist with param
    :param json: dict
    :param kwargs: other
    :return:
    """
    return requests.post(url, data, json, **kwargs)


def http_request(url: str = None, method: str = None, data=None, headers: dict = dict, origin_req_host: any = None,
                 unverifiable: bool = False, save_html_path: str = None) -> (str, any):
    """
    Функция обертка над стандартной http Request либой Python3, для удобной работы с http запросам
    :param url: полный url для реквеста
    :param method: get/post/head and etc.
    :param data: data for post request
    :param headers: http headers
    :param origin_req_host:
    :param unverifiable:
    :param save_html_path: сохранить полученный response (html файл) на диск в указанное место (save_html_path)
    :return: str  -  html, json, xml  response
    """
    r = Request(
        url=url,
        method=method,
        headers=headers,
        data=data,
        origin_req_host=origin_req_host,
        unverifiable=unverifiable
    )
    try:
        CLogger.debug(f"Try HTTP request: {r.get_full_url()}")
        response = urlopen(r)
    except URLError as e:
        CLogger.debug(e)
        # if hasattr(e, 'reason'):
        #     CLogger.debug('HTTP error request')
        #     CLogger.debug(f"Reason: {e.reason}")
        # elif hasattr(e, 'code'):
        #     CLogger.debug("The server couldn't fulfill the request.")
        #     CLogger.debug(f"Error code: {e.code}")
        return None, e
    else:  # OK
        binary_html = None
        try:
            binary_html = response.read()
            charset = response.info().get_content_charset() if response.info().get_content_charset() else 'UTF-8'
            utf8_html = binary_html.decode(charset)  # UTF-8 f.e.
            if save_html_path:
                try:
                    with open(save_html_path, "w") as f:
                        f.write(utf8_html)
                except Exception as inst2:
                    CLogger.exception(inst2, "Cant save html response. [Func: http_request]")
            return utf8_html, None
        except Exception as inst:
            CLogger.exception(inst, "Read html request error. [Func: http_request]")
            CLogger.error(f"\nDetailed info\nURL:{r.get_full_url()}")
            CLogger.error(f"\nBinary_html:{binary_html}")
            return None, inst


def get_random_user_agent() -> str:
    """
    Функция рандомизатор USER AGENTS
    :return: random user agent
    """
    user_agents_list = [
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
        "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"
    ]

    return user_agents_list[random.randrange(len(user_agents_list))]
