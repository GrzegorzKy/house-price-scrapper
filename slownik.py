# biblioteka zawierająca słowniki oraz regexy
# do wykrywania dodatkowych cech w opisie
import re

komunikacja = {
    "metro": ["metro", "metra"],
    "inna_komunikacja": ["autobus", "autobusowy", "autobusu", "tramwaj",
                         "tramwaju", "tramwajowy", "autobusy", "tramwaje"]
}

stan_mieszkania = {
    "nowe": ["wyremontowane", "poremontowe", "od dewelopera", "wykończone",
             "stan deweloperski"],
    "do_remontu": ["do remontu", "wymaga remontu"]
}

r_ulica = re.compile("ul\.\s(\w+)")
r_rok = re.compile("z (\d{4}) roku|z (\d{4})\s?r")
