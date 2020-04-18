# to jest kod scrappera gumtree
from bs4 import BeautifulSoup
import urllib.request as urllib2
from urllib.error import URLError
from socket import timeout
from time import sleep
from pymongo import MongoClient
import datetime


# storona bazowa dla mieszkan, poniewaz paginacja nie uwzglednia wszystkich mieszkan,
# stad bedziemy pobierac ~1k mieszkan per wojewodztwo
# na tym etapie wstepna selekcja: mieszkania, z ustalona cena
PATH = "https://www.gumtree.pl/s-mieszkania-i-domy-sprzedam-i-kupie/mieszkanie/v1c9073a1dwp1"
BASE = "https://www.gumtree.pl"
woj_allowed = ('Małopolskie','Dolnośląskie','Śląskie','Pomorskie','Łódzkie','Wielkopolskie','Podkarpackie','Świętokrzyskie','Kujawsko - pomorskie','Zachodniopomorskie','Lubelskie','Warmińsko-mazurskie','Podlaskie','Lubuskie','Opolskie','Mazowieckie')
url_params = ["priceType=FIXED"]
MAX_PAGE_NO = 50
wojewodztwa_dict = {}
code = 0
page = None
max_iters = 50
i = 0
#tu do zmiany, jak dodamy inkrementalny load
load_id = 1
# połączenie do bazy
try:
    client = MongoClient('', port=27017)
    db = client.mieszkania
except:
    if client:
        client.close()

while code != 200 and i <= max_iters:
    page = urllib2.urlopen(PATH)
    code = page.getcode()
    i += 1
    sleep(1)

if page == None:
    print("Otwarcie strony głównej się nie powiodło")
    exit()

woj_soup = BeautifulSoup(page, "html.parser")
# wojewodztwa = woj_soup.find_all("li", class_="selectOption finalsub alt000 inv002 level1")
wojewodztwa_links = woj_soup.select(".selectOption.finalsub > span.text > a[href]")
for woj in wojewodztwa_links:
    if woj.text in woj_allowed:
        wojewodztwa_dict[woj.text] = BASE + woj['href']

# po uzyskaniu linków do województw, przechodzimy po wszystkich
# ich podstronach i zapisujemy linki do bazy
# load datetime
dt = datetime.datetime.now()
# pętla wojewodztw
for woj, link in wojewodztwa_dict.items():
    # pętla podstron
    for page in range(1, MAX_PAGE_NO + 1):
        page_url = link[:-1] + str(page) + "?" + "&".join(url_params)
        print(page_url)
        i = 0
        code = -1
        ver_url = "a"
        # commitujemy co podstrona
        urls = []
        # ustawiamy parametry połączenia http
        req = urllib2.Request(
            page_url,
            data=None,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36',
                'Referer': 'http://www.google.pl'
            }
        )
        # próbujemy uzyskać kod 200 dla podstrony
        # inaczej przekierowanie, lub timeout
        while i <= max_iters and code != 200 and ver_url != page_url:
            try:
                page = urllib2.urlopen(req)
                code = page.getcode()
                ver_url = page.geturl()
                if code == 404:
                    continue
                elif i > 0:
                    sleep(1)
                else:
                    sleep(0.5)
            except URLError:
                #pewnie timeout
                sleep(20)
            except timeout:
                #inny sposob na timeout
                sleep(20)
            finally:
                i += 1
        page_soup = BeautifulSoup(page, "html.parser")
        # #wrapper > div.results.list-view > div.view > div.titleV1 > .title > a
        ads = page_soup.select("#wrapper > div.results.list-view > div.view > div > div.title > a")
        urls.extend(ads)
        for url in urls:
            doc = {
                "link": BASE + url['href'],
                "wojewodztwo": woj,
                "_load_id": load_id,
                "_load_date": dt

            }
            try:
                res = db.links.insert_one(doc)
                print(res)
            except Exception as e:
                print(e)
            finally:
                if client:
                    client.close()


