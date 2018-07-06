from lxml.html import fromstring
import requests
from itertools import cycle
import os
import time
from functools import reduce
import re
from urllib.parse import urlparse
import user_agents
import shutil
import traceback

URLS_DIR_PATH = '../WRK/Links/'
DOWNLOADS_DIR_PATH = '../WRK/Downloads/'
REFUSED_DIR_PATH = '../WRK/Refused/'
urls_file_name = "urls.txt"
wrk_urls_file_name = "urls_wrk.txt"
skipped_urls_file_name = "urls_skiipped.txt"
refused_urls_file_name = "urls_refused.txt"
loaded_urls_file_name = "urls_loaded.txt"
DEFENCE_INTERVAL = 10
REFUSE_INTERVAL = 180
MAX_ALL_PROXIES_REFUSED_COUNT = 20
CONTENT_TYPE = "application/pdf"
PROXY_LIST_DOWNLOAD_REPEAT = 5
PROXY_LIST_DOWNLOAD_REPEAT_DELAY = 3
CLEAR_DOWNLOADS_DIR = True
CLEAR_REFUSED_DIR = True
class UnexpectedResponce(Exception):
    pass

def prepare_dir(dir_name, delete = False):
    if os.path.exists(dir_name) and  delete:
        shutil.rmtree(dir_name)
        os.mkdir(dir_name)
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

def remove_files(file_list):
    for f in file_list:
        if os.path.exists(f):
            os.remove(f)

def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = []
    for i in parser.xpath('//tbody/tr'):
        proxy = {}
        # if i.xpath('.//td[7][contains(text(),"yes")]') and i.xpath('.//td[5][contains(text(),"elite proxy")]'):
        if i.xpath('.//td[5][contains(text(),"elite proxy")]'):
            proxy["ip"] = i.xpath('.//td[1]/text()')[0]
            # proxy["ip"] = "127.0.0.1"
            proxy["port"] = i.xpath('.//td[2]/text()')[0]
            proxy["country"] = i.xpath('.//td[4]/text()')[0]
            proxy["anonymity"] = i.xpath('.//td[5]/text()')[0]
            proxy["https"] = i.xpath('.//td[7]/text()')[0]
            proxy["time"] = 0
            proxy["active"] = True
            proxy["refused"] = False
            proxies.append(proxy)
    return proxies
    # return proxies[:1]



#If you are copy pasting proxy ips, put in the list below
#proxies = ['121.129.127.209:80', '124.41.215.238:45169', '185.93.3.123:8080', '194.182.64.67:3128', '106.0.38.174:8080', '163.172.175.210:3128', '13.92.196.150:8080']
if __name__ == "__main__":
    prepare_dir(os.path.abspath(URLS_DIR_PATH), delete = False)
    prepare_dir(os.path.abspath(REFUSED_DIR_PATH), delete = CLEAR_REFUSED_DIR)
    prepare_dir(os.path.abspath(DOWNLOADS_DIR_PATH), delete = CLEAR_DOWNLOADS_DIR)

    urls_path =os.path.abspath( URLS_DIR_PATH+urls_file_name)
    wrk_urls_path =os.path.abspath( URLS_DIR_PATH+wrk_urls_file_name)
    refused_urls_path =os.path.abspath( URLS_DIR_PATH + refused_urls_file_name)
    skipped_urls_path =os.path.abspath( URLS_DIR_PATH + skipped_urls_file_name)
    loaded_urls_path =os.path.abspath( URLS_DIR_PATH + loaded_urls_file_name)

    remove_files([wrk_urls_path, refused_urls_path, skipped_urls_path, loaded_urls_path])

    shutil.copy2(urls_path,wrk_urls_path)
    urls = None
    with open(wrk_urls_path) as f:
        urls = f.readlines()
    urls = set([x.strip() for x in urls])
    urls = dict((x,{"loaded":False,"skipped":False}) for x in urls)
    repeat=0
    success = -1
    while repeat < PROXY_LIST_DOWNLOAD_REPEAT:
        print("Loading proxy list ...",end='')
        repeat = repeat +1
        try:
            proxies = get_proxies()
            if len(proxies)>=1:
                print ("OK")
                success = 0
                break
            else:
                print ("Try {} failed. Proxy list is empty.".format(repeat))
                success = 1
                time.sleep(PROXY_LIST_DOWNLOAD_REPEAT_DELAY)
                continue
        except Exception as e:
            print ("Try {} failed. {}".format(repeat, e))
            success = 2
            time.sleep(PROXY_LIST_DOWNLOAD_REPEAT_DELAY)
            continue
    if success != 0:
        exit (-1)

    proxy_pool = cycle(proxies)
    proxy = None

    test_url = 'https://httpbin.org/ip'
    no_active_proxies = False
    all_proxies_refused_count = 0
    all_proxies_refused_count_exceed = False
    url_number = len(urls)
    url_loaded = 0
    url_count = 0
    for url,url_attr in urls.items():
        url_count = url_count + 1
        print ("\n")
        while True:
            if  reduce(lambda x, y:  x or (y["active"] and not y["refused"]),proxies,False):
                while True:
                    proxy = next(proxy_pool)
                    if proxy["active"] and not proxy["refused"]:
                        break
            elif reduce(lambda x, y:  x or y["active"],proxies,False):
                all_proxies_refused_count = all_proxies_refused_count + 1
                if all_proxies_refused_count > MAX_ALL_PROXIES_REFUSED_COUNT:
                    all_proxies_refused_count_exceed = True
                    print ("\nSkip all downloads. All all proxies repeatedly refused more then {} time(s)".format(MAX_ALL_PROXIES_REFUSED_COUNT))
                    break
                print ("\nAll proxies refused {} time(s). Wait {}s".format(all_proxies_refused_count,REFUSE_INTERVAL) )
                time.sleep(REFUSE_INTERVAL)
                for proxy in proxies:
                    if proxy["active"]:
                        proxy["refused"] = False
                continue
            else:
                print ("\nSkip all downloads. No active proxies")
                no_active_proxies = True
                break
            proxy_ip_port = ":".join([proxy["ip"],proxy["port"]])
            headers = {'User-Agent': user_agents.get_user_agent()}
            try:
                print("Test proxy:  ip = {}; port = {}; country = {}; https = {} ...".format(proxy["ip"],proxy["port"],proxy["country"],proxy["https"]), end='')
                response = requests.get(test_url,proxies={"http": proxy_ip_port, "https": proxy_ip_port},headers=headers)
                print ("OK")
                proxy["time"] = time.clock()
            except  requests.exceptions.RequestException as e:
                proxy["active"] = False
                print("Fail")
                print("\nSkipping proxy. Proxy connnection error while testing: {}".format(e))
                continue
            try:
                print("Load {}/{}/{}: url = {}".format(url_count, url_loaded, url_number, url))
                response = requests.get(url,proxies={"http": proxy_ip_port, "https": proxy_ip_port},headers=headers)
                expected_length = response.headers['content-length']
                if expected_length is not None:
                    actual_length = response.raw.tell()
                    expected_length = int(expected_length)
                print("Status code: {} Content type: {} Content length(expected) : {} Actual length: {}".format(response.status_code,
                                                response.headers['content-type'], expected_length, actual_length))
                if response.status_code !=200:
                    raise UnexpectedResponce("Unexpected responce status: {}: {}".format(response.status_code, response.reason))
                if response.headers['content-type'] !=CONTENT_TYPE:
                    print("Skipping proxy. URL refused: unexpected content-type: expected {} actual {}".format(CONTENT_TYPE,response.headers['content-type']))
                    proxy["refused"] = True
                    refused_f_name = "_".join([proxy["ip"],proxy["port"],proxy["country"],re.sub(r'[^0-9_\-a-zA-Zа-яА-Я\.]', '', os.path.basename(urlparse(url).path))])+".html"
                    refused_file = os.path.abspath( REFUSED_DIR_PATH+refused_f_name)
                    with open(refused_file, 'wb') as f:
                        f.write(response.content)
                    continue
                if expected_length is not None:
                    if actual_length < expected_length:
                        raise UnexpectedResponce(
                            "Unexpected actual content length (incomplete read): expected length{} actual length{}".format(
                                expected_length, actual_length ))
                loaded_f_name = re.sub(r'[^0-9_\-a-zA-Zа-яА-Я\.]', '', os.path.basename(urlparse(url).path))
                loaded_file = os.path.abspath( DOWNLOADS_DIR_PATH+loaded_f_name)
                with open(loaded_file, 'wb') as f:
                    f.write(response.content)
                url_attr["loaded"] = True
                all_proxies_refused_count = 0
                url_loaded = url_loaded + 1
                break
            except requests.exceptions.ProxyError as e:
                print("Skipping proxy. Connnection error while url downloading: {}".format(e))
                proxy["active"] = False
            except (requests.exceptions.RequestException,UnexpectedResponce) as e:
                print("Skipping URL. Reason: {}".format(e))
                url_attr["skipped"] = True
                break
        if no_active_proxies or all_proxies_refused_count_exceed:
            break
    skipped_urls_path =os.path.abspath( URLS_DIR_PATH + skipped_urls_file_name)
    with open(skipped_urls_path, "w") as skipped_f, open(refused_urls_path, "w") as refused_f, open(loaded_urls_path, "w") as loaded_f:
        for url,url_attr in urls.items():
            if not url_attr["loaded"] and url_attr["skipped"]:
                skipped_f.writelines(url+"\n")
            elif not url_attr["loaded"] and not url_attr["skipped"]:
                refused_f.writelines(url+"\n")
            elif url_attr["loaded"] and not url_attr["skipped"]:
                loaded_f.writelines(url+"\n")
