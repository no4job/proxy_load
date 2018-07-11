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


GRAB_DIR_PATH = '../WRK/Grab/'

DEFENCE_INTERVAL = 5

CLEAR_DOWNLOADS_DIR = True


# STEP_LOAD_DELAY_INTERVAL = (3*60,8*60)
# INTER_LOAD_DELAY_INTERVAL = (5,20)
# STEP_LOAD_NUMBER_INTERVAL = (100,200)

RESPONSE_TIMEOUT = 10

# class UnexpectedResponce(Exception):
#     pass

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

def grab_1(result_file_path):
    base_grab_url = "http://www.vashamashina.ru/info/adresa-mreo/"
    parsed_file_name = "grab_1.html"
    # URLS_DIR_PATH = '../WRK/Links/'
    # parsed_file_path =os.path.abspath( URLS_DIR_PATH + parsed_file_name)
    f_str = ""
    # with open(parsed_file_path, 'r',encoding='utf-8') as f:
    #     f_str = f.read()
    text = []
    pages_number = 26
    for i in range(1,pages_number+1):
        print ("{} of {}".format(i,pages_number))
        grab_url = base_grab_url+"page{}".format(i)
        response = requests.get(grab_url, timeout=RESPONSE_TIMEOUT)
        f_str = response.text
        # for i in parser.xpath("a[@class='ShopHeaderLink']"):
        parser = fromstring(f_str)
        nodes = parser.xpath("//a[@class='ShopHeaderLink']")
        for node in nodes:
            result = safe_list_get (node.xpath('./text()'),0,"---").strip()
            print(result)
            text.append(result)
        time.sleep(DEFENCE_INTERVAL)

    with open(result_file_path, 'w',encoding='utf-8') as f:
        f.writelines(map(lambda str: str+"\n",text))

def grab_2(result_file_path):
    base_grab_url = "https://shtrafovnet.ru/info/divisions/"
    # parsed_file_name = "grab_1.html"
    # URLS_DIR_PATH = '../WRK/Links/'
    # parsed_file_path =os.path.abspath( URLS_DIR_PATH + parsed_file_name)
    f_str = ""
    # with open(parsed_file_path, 'r',encoding='utf-8') as f:
    #     f_str = f.read()
    division_code_list_url = "https://shtrafovnet.ru/info/divisions"
    divisions = []
    response = requests.get(division_code_list_url, timeout=RESPONSE_TIMEOUT)
    f_str = response.text
    # for i in parser.xpath("a[@class='ShopHeaderLink']"):
    parser = fromstring(f_str)
    nodes = parser.xpath("//div[@class='col-xs-2']")
    for node in nodes:
        result = safe_list_get (node.xpath('./text()'),0,"").strip()
        if not result.isdigit():
            continue
        print(result)
        divisions.append(result)
    text = []
    pages_number = len( divisions)
    for i in range(0,pages_number):
        print ("{} of {}".format(i,pages_number))
        grab_url = base_grab_url+divisions[i]
        response = requests.get(grab_url, timeout=RESPONSE_TIMEOUT)
        f_str = response.text
        # for i in parser.xpath("a[@class='ShopHeaderLink']"):
        parser = fromstring(f_str)
        nodes = parser.xpath("//div[@itemtype='http://schema.org/Organization']/descendant::span[@itemprop='name']")
        for node in nodes:
            result = safe_list_get (node.xpath('./text()'),0,"---").strip()
            print(result)
            text.append(result)
        time.sleep(DEFENCE_INTERVAL)

    with open(result_file_path, 'w',encoding='utf-8') as f:
        f.writelines(map(lambda str: str+"\n",text))

if __name__ == "__main__":
    prepare_dir(os.path.abspath(GRAB_DIR_PATH), delete = False)

    # result_file_name = "result_1.txt"
    # result_file_path =os.path.abspath( GRAB_DIR_PATH + result_file_name)
    # grab_1(result_file_path)

    result_file_name = "result_2.txt"
    result_file_path =os.path.abspath( GRAB_DIR_PATH + result_file_name)

    grab_2(result_file_path)
    exit(0)