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
import random
import traceback

# ******* create before first run *****
URLS_DIR_PATH = '../WRK/Links/'
#**************************************

DOWNLOADS_DIR_PATH = '../WRK/Downloads/'
REFUSED_DIR_PATH = '../WRK/Refused/'

# ******* place in URLS_DIR_PATH before first run *****
urls_file_name = "urls.txt"
#******************************************************

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

STEP_LOAD_DELAY_INTERVAL = (3*60,8*60)
INTER_LOAD_DELAY_INTERVAL = (5,20)
STEP_LOAD_NUMBER_INTERVAL = (100,200)

RESPONSE_TIMEOUT = 10

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
def safe_list_get (l, idx, default):
    try:
        return l[idx]
    except IndexError:
        return default

def load_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url, timeout=RESPONSE_TIMEOUT)
    parser = fromstring(response.text)
    proxies = []
    for i in parser.xpath('//tbody/tr'):
        proxy = {}
        # if i.xpath('.//td[7][contains(text(),"yes")]') and i.xpath('.//td[5][contains(text(),"elite proxy")]'):
        if i.xpath('.//td[5][contains(text(),"elite proxy")]'):
            proxy["ip"] = safe_list_get (i.xpath('.//td[1]/text()'), 0, "---").strip()
            # proxy["ip"] = "127.0.0.1"
            proxy["port"] = safe_list_get (i.xpath('.//td[2]/text()'),0,"---").strip()
            proxy["country"] = safe_list_get (i.xpath('.//td[4]/text()'),0,"---")
            proxy["anonymity"] = safe_list_get (i.xpath('.//td[5]/text()'),0,"---")
            proxy["https"] = safe_list_get (i.xpath('.//td[7]/text()'),0,"---")
            proxy["time"] = 0
            proxy["active"] = True
            proxy["refused"] = False
            if not re.match("[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}",proxy["ip"]) :
                continue
            if not (re.match("[0-9]{1,5}",proxy["port"]) and int(proxy["port"])<=65535 and int(proxy["port"])> 0):
                continue
            proxies.append(proxy)
    return proxies
    # return proxies[:1]
def get_proxies():
    repeat=0
    success = -1
    proxies = None
    while repeat < PROXY_LIST_DOWNLOAD_REPEAT:
        print("Loading proxy list ...",end='', flush=True)
        repeat = repeat +1
        try:
            proxies = load_proxies()
            if len(proxies)>=1:
                print ("OK")
                success = 0
                break
            else:
                print ("Try {} failed. Proxy list is empty.".format(repeat))
                success = 1
                time.sleep(PROXY_LIST_DOWNLOAD_REPEAT_DELAY)
                continue
        except requests.exceptions.RequestException as e:
            print ("Try {} failed. {}".format(repeat, e))
            success = 2
            time.sleep(PROXY_LIST_DOWNLOAD_REPEAT_DELAY)
            continue
    if success != 0:
        return None
    return proxies

def load_urls(urls,proxies,step_load_delay_interval,inter_load_delay_interval,step_load_number_interval):
    proxy_pool = cycle(proxies)
    proxy = None

    test_url = 'https://httpbin.org/ip'
    no_active_proxies = False
    all_proxies_refused_count = 0
    all_proxies_refused_count_exceed = False
    url_number = len(urls)
    url_loaded = len([(url, url_attr) for url, url_attr in  urls.items() if url_attr["loaded"]== True])
    url_count = url_loaded
    step_load_count = 0
    if url_loaded != 0:
        step_load_delay = random.randint(*step_load_delay_interval)
        print("Load delay: {}".format(step_load_delay ), flush=True)
        time.sleep(step_load_delay)
    step_load_number = random.randint(*step_load_number_interval)
    for url,url_attr in [(url, url_attr) for url, url_attr in  urls.items() if url_attr["skipped"]== False
                                                                             and url_attr["loaded"]== False]:

        if step_load_count+1 > step_load_number:
            break
        step_load_count = step_load_count  +1
        inter_load_delay = random.randint(*inter_load_delay_interval)
        print ("\n")
        print("interload delay {} start ...".format(inter_load_delay),end="", flush=True)
        time.sleep(inter_load_delay)
        print ("finish")
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
            # headers = {'User-Agent': user_agents.get_user_agent()}
            headers = {}
            try:
                print("Test proxy:  ip = {}; port = {}; country = {}; https = {} ...".format(proxy["ip"],proxy["port"],proxy["country"],proxy["https"]), end='', flush=True)
                response = requests.get(test_url, proxies={"http": proxy_ip_port, "https": proxy_ip_port}, headers=headers, timeout=RESPONSE_TIMEOUT)
                print ("OK")
                proxy["time"] = time.clock()
            except  requests.exceptions.RequestException as e:
                proxy["active"] = False
                print("Fail", flush=True)
                print("Skipping proxy. Proxy connnection error while testing: {}".format(e), flush=True)
                continue
            try:
                print("Load {}/{}/{}: url = {}".format(url_count, url_loaded, url_number, url), flush=True)
                response = requests.get(url, proxies={"http": proxy_ip_port, "https": proxy_ip_port}, headers=headers, timeout=RESPONSE_TIMEOUT)
                expected_length = response.headers.get('content-length')
                if expected_length is not None:
                    actual_length = response.raw.tell()
                    expected_length = int(expected_length)
                else:
                    raise UnexpectedResponce("content-length header not received")
                print("Status code: {} Content type: {} Content length(expected) : {} Actual length: {}".format(response.status_code,
                                                                                                                response.headers['content-type'], expected_length, actual_length), flush=True)
                if response.status_code !=200:
                    raise UnexpectedResponce("Unexpected responce status: {}: {}".format(response.status_code, response.reason))
                if response.headers['content-type'] !=CONTENT_TYPE:
                    print("Skipping proxy. URL refused: unexpected content-type: expected {} actual {}".format(CONTENT_TYPE,response.headers['content-type']), flush=True)
                    proxy["refused"] = True
                    refused_f_name = "_".join([proxy["ip"],proxy["port"],proxy["country"],re.sub(r'[^0-9_\-a-zA-Zа-яА-Я.]', '', os.path.basename(urlparse(url).path))])+".html"
                    refused_file = os.path.abspath( REFUSED_DIR_PATH+refused_f_name)
                    with open(refused_file, 'wb') as f:
                        f.write(response.content)
                    continue
                if expected_length is not None:
                    if actual_length < expected_length:
                        raise UnexpectedResponce(
                            "Unexpected actual content length (incomplete read): expected length{} actual length{}".format(
                                expected_length, actual_length ))
                loaded_f_name = re.sub(r'[^0-9_\-a-zA-Zа-яА-Я.]', '', os.path.basename(urlparse(url).path))
                loaded_file = os.path.abspath( DOWNLOADS_DIR_PATH+loaded_f_name)
                with open(loaded_file, 'wb') as f:
                    f.write(response.content)
                url_attr["loaded"] = True
                all_proxies_refused_count = 0
                url_loaded = url_loaded + 1
                break
            except requests.exceptions.ProxyError as e:
                print("Skipping proxy. Connnection error while url downloading: {}".format(e), flush=True)
                proxy["active"] = False
            except (requests.exceptions.RequestException,UnexpectedResponce) as e:
                print("Skipping URL. Reason: {}".format(e), flush=True)
                url_attr["skipped"] = True
                break
        if no_active_proxies or all_proxies_refused_count_exceed:
            break
    return (url_number,step_load_count,url_loaded)


if __name__ == "__main__":
    urls_path =os.path.abspath( URLS_DIR_PATH+urls_file_name)
    if not os.path.exists(urls_path):
        print ("File with urls not found:{}".format(urls_path))
        exit (-1)
    # prepare_dir(os.path.abspath(URLS_DIR_PATH), delete = False)
    prepare_dir(os.path.abspath(REFUSED_DIR_PATH), delete = CLEAR_REFUSED_DIR)
    prepare_dir(os.path.abspath(DOWNLOADS_DIR_PATH), delete = CLEAR_DOWNLOADS_DIR)


    # wrk_urls_path =os.path.abspath( URLS_DIR_PATH+wrk_urls_file_name)
    refused_urls_path =os.path.abspath( URLS_DIR_PATH + refused_urls_file_name)
    skipped_urls_path =os.path.abspath( URLS_DIR_PATH + skipped_urls_file_name)
    loaded_urls_path =os.path.abspath( URLS_DIR_PATH + loaded_urls_file_name)

    # remove_files([wrk_urls_path, refused_urls_path, skipped_urls_path, loaded_urls_path])

    # shutil.copy2(urls_path,wrk_urls_path)
    # urls = None
    with open(urls_path) as f:
        urls = f.readlines()
    urls = set([x.strip() for x in urls])
    urls = dict((x,{"loaded":False,"skipped":False}) for x in urls)
    # ********************************************************************************************
    url_loaded_last = -1
    url_loaded = 0
    load_iteration = 0
    url_number = len(urls)
    while url_loaded > url_loaded_last and url_number > url_loaded:
        url_loaded_last = url_loaded
        load_iteration = load_iteration + 1
        print("******************** Load step {} ********************".format(load_iteration), flush=True)
        tryed_urls_number = 0
        for url in urls:
            urls[url]["skipped"]=False
        proxies = get_proxies()
        if proxies is None:
            exit (-1)
        url_number,url_tryed,url_loaded = load_urls(urls,proxies,STEP_LOAD_DELAY_INTERVAL,INTER_LOAD_DELAY_INTERVAL,STEP_LOAD_NUMBER_INTERVAL)
        with open(skipped_urls_path, "w") as skipped_f, open(refused_urls_path, "w") as refused_f, open(loaded_urls_path, "w") as loaded_f:
            for url,url_attr in urls.items():
                if not url_attr["loaded"] and url_attr["skipped"]:
                    skipped_f.writelines(url+"\n")
                elif not url_attr["loaded"] and not url_attr["skipped"]:
                    refused_f.writelines(url+"\n")
                elif url_attr["loaded"] and not url_attr["skipped"]:
                    loaded_f.writelines(url+"\n")
        print(" ******************** End of load step {} *********************".format(load_iteration), flush=True)
        print("Tryed {} , loaded {} . Totally loaded {} of {}".format(url_tryed,
                                                                url_loaded - url_loaded_last, url_loaded, url_number), flush=True)
        print("********************************************************************", flush=True)

    print("\n\n****************************** Finish ******************************", flush=True)
    print("Over all steps({}) loaded {} of {}".format(load_iteration, url_loaded, url_number), flush=True)
    print("********************************************************************", flush=True)