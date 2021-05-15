import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from pathlib import Path
from selenium.webdriver.chrome.options import Options

# import chromedriver_binary

HOST = 'localhost'
DB_NAME = 'NBA3'
ALPHABET = 'abcdefghijklmnopqrstuvwxyz'

def get_mysql_pass():
    with open('/home/gesogeso/デスクトップ/mysql_user_and_pwd.txt', 'r') as f:
        data = f.read()
        user, passwd = data.replace('\n', '').split(',')
        # host = 'localhost'
        # db_name = 'NBA'
        port = 3306
    return user, passwd

def get_soup_by_url(url: str, selenium: bool):
    if not selenium:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        return soup
    else:
        # JavaScriptによる動的コンテンツがあるページはseleniumでブラウザを起動する
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-application-cache')
        options.add_argument('--disable-infobras')
        driver = webdriver.Chrome('./chromedriver', chrome_options=options)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source.encode('utf-8'), 'html.parser')
        return soup



