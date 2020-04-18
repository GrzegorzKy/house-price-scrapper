# program pobiera dane ze stage, przetwarza i zapisuje do core
# - docelowej bazy, z której prowarzona jest analiza
from pymongo import MongoClient
from nltk import sent_tokenize
from pprint import pprint
from datetime import datetime
from stop_words import get_stop_words
import re, morfeusz2, slownik


# połączenie z bazą
client = None
try:
    client = MongoClient('', port=27017)
    db = client.mieszkania
except Exception as e:
    print("Problem łączenia się z bazą\n", e)
    if client:
        client.close()
    exit()
if client is None:
    print("Problem łączenia się z bazą")
    exit()

# kompilujemy regexy do przetwarzania tekstu
r_cena = re.compile("\\xa0")
r_pokoje = re.compile("^(\d+?)")
r_miasto = re.compile(".*?,\s{1}(.+)")
r_lazienki = re.compile("^(\d+).*")

cursor = client.mieszkania.stage.find().skip(140)
for cur in cursor:
    # pprint(cur)
    # extract cena
    if cur["price"] is None:
        cena_list = None
    else:
        cena_list = re.split(r_cena, cur["price"])
    if cena_list is None:
        cena = None
    else:
        # usuwamy końcówkę 'zł'
        cena_list[-1] = cena_list[-1][:-3]
        try:
            cena = int("".join(cena_list))
        except Exception:
            print(cena)
            cena = int(input("Podaj cenę ręcznie"))
    # teraz atrybuty
    data_dodania = None
    liczba_pokoi = None
    miasto = None
    wlasciciel = None
    metraz = None
    lazienki = None
    parking = None
    for key, val in cur["attributes"].items():
        if key == "Data dodania":
            data_dodania = datetime.strptime(val, "%d/%m/%Y")
        if key == "Liczba pokoi":
            o = r_pokoje.match(val)
            if o is not None:
                liczba_pokoi = int(o.group(1))
            else:
                liczba_pokoi = None
        if key == "Lokalizacja":
            o = r_miasto.match(val)
            if o is not None:
                miasto = o.group(1)
            else:
                miasto = None
        if key == "Na sprzedaż przez":
            wlasciciel = val.lower()
        if key == "Wielkość (m2)":
            try:
                metraz = float(val)
            except Exception:
                print(val)
                metraz = float(input("Podaj metraż ręcznie: "))
        if key == "Liczba łazienek":
            o = r_lazienki.match(val)
            if o is not None:
                lazienki = int(o.group(1))
            else:
                lazienki = None
        if key == "Parking":
            parking = val.lower()
    # wojewodztwo
    wojewodztwo = cur["voivod"]

    # przetważanie opisu
    # morf = morfeusz2.Morfeusz(generate=False)
    # opis = cur["desc"].replace("\n", "")
    # analysis = morf.analyse(opis)
    # for a, b, c in analysis:
    #     print(a, b, c)

    stop_words = get_stop_words(language="polish")
    grammar_words = [",", ";", "\n", "\t"]
    blacklist = grammar_words

    if cur["desc"] is not None:
        opis = cur["desc"].lower()
        # usuwanie stop words i gramatyki z opisu
        for w in blacklist:
            opis = opis.replace(w, "")

        # komunikacja
        komunikacja = []
        for key, val in slownik.komunikacja.items():
            for item in val:
                if item in opis:
                    komunikacja.append(key)
                    break

        # stan mieszkania
        stan_mieszkania = []
        for key, val in slownik.stan_mieszkania.items():
            for item in val:
                if item in opis:
                    stan_mieszkania.append(key)
                    break
        # ulica
        ulica = re.findall(slownik.r_ulica, opis)
        if len(ulica) != 1:
            ulica = None
        else:
            ulica = ulica[0]

        # rok budynku
        rok_budynku = re.findall(slownik.r_rok, opis)
        if len(rok_budynku) != 1:
            rok_budynku = None
        else:
            rok_budynku = int("".join(rok_budynku[0]))
    else:
        komunikacja = None
        stan_mieszkania = None
        ulica = None
        rok_budynku = None

    rec = {
    "wojewodztwo": cur["voivod"],
    "cena": cena,
    "data_dodania": data_dodania,
    "liczba_pokoi": liczba_pokoi,
    "miasto": miasto,
    "wlasciciel": wlasciciel,
    "metraz": metraz,
    "lazienki": lazienki,
    "parking": parking,
    "ulica": ulica,
    "rok_budynku": rok_budynku,
    "_load_date": datetime.now(),
    "_load_id": 1,
    "_integration_id": cur["_id"]
    }
    print(rec)
    db.core.insert_one(rec)


