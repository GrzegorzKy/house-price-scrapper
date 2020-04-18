from bs4 import BeautifulSoup
import urllib.request as urllib2
from urllib.error import URLError
from socket import timeout
from time import sleep
from pymongo import MongoClient
import datetime
import re


valid_url = re.compile(".+?\/(\d+?)$")
max_iters = 10
client = None
cur = None
try:
    client = MongoClient('', port=27017)
    db = client.mieszkania
    cur = client.mieszkania.links.find().skip(15867)
except Exception as e:
    if client:
        client.close()
    print(e)
    exit()

for link in cur:
    page = None
    ver_url = "a"
    code = -1
    i = 0
    flg_404 = False
    flg_url = False
    flg_success = False
    flg_stuck = False
    page_url = link['link']
    # print(page_url)
    wojewodztwo = link['wojewodztwo']
    req = urllib2.Request(
        page_url,
        data=None,
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36',
            'Referer': 'http://www.google.pl'
        }
    )

    while flg_success is False and flg_stuck is False:
        try:
            page = urllib2.urlopen(req)
            code = page.getcode()
            ver_url = page.geturl()
            if code >= 400 and code < 500:
                flg_404 = True
            # szybki debugging
            if code != 200:
                print(code)
            if ver_url != page_url and flg_url is False:
                page_sfx = re.match(valid_url, page_url)
                valid_sfx = re.match(valid_url, ver_url)
                if page_sfx is not None and valid_sfx is not None:
                    page_sfx = page_sfx.group(1)
                    valid_sfx = valid_sfx.group(1)
                    if page_sfx == valid_sfx:
                        flg_url = True
            else:
                flg_url = True
            # warunek wyjscia
            if i >= max_iters:
                continue
            if flg_404 is True:
                continue
            if code == 200 and flg_url is True:
                flg_success = True
        except URLError:
            # pewnie timeout
            sleep(10)
        except timeout:
            # pewnie timeout
            sleep(10)
        finally:
            i += 1
            if i >= max_iters:
                flg_stuck = True
            sleep(0.5)

    if flg_success is False:
        continue

    # tutaj pomyślne otworzenie strony
    page_soup = BeautifulSoup(page, "html.parser")
    price = page_soup.select_one("#wrapper > div > div.vip-header-and-details > div.vip-content-header > div.vip-title.clearfix > div > span > span")
    if price is not None:
        price = price.text
    title = page_soup.select_one("#wrapper > div > div.vip-header-and-details > div.vip-content-header > div.vip-title.clearfix > h1 > span")
    if title is not None:
        title = title.text
    attributes = page_soup.select(".selMenu > li > div.attribute > span.name")
    values = page_soup.select(".selMenu > li > div.attribute > span.value")
    description = page_soup.select_one("#wrapper > div:nth-child(1) > div.vip-header-and-details > div.vip-details > div > span")
    if description is not None:
        description = description.text
    # przerabiamy listy attr i vals na slownik
    # powinny miec te samą sługość
    attrs_txt = [p.get_text() for p in attributes]
    vals_txt = [p.get_text() for p in values]
    attrs_dict = dict(zip(attrs_txt, vals_txt))

    rec = {
        "link": page_url,
        "title": title,
        "voivod": wojewodztwo,
        "price": price,
        "attributes": attrs_dict,
        "desc": description,
        "_load_id": 1,
        "_load_date": datetime.datetime.now()
    }
    db.stage.insert_one(rec)
    # odczekanie 0.1 sekundy
    print(rec["title"], ", ", rec["link"])
if cur:
    cur.close()
