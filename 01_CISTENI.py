# CISTENI ORIGINALNIHO DATASETU

# importy
import pandas as pd
from datetime import datetime
from datetime import time

# nacteni originalniho datasetu
df = pd.read_csv('Traffic_accidents_gps_changes.csv',
                 encoding='utf-8-sig', low_memory=False)


# dropnuti nepotrebnych sloupcu
df = df.drop(columns=['zuj',
                      'osobni_prepravnik',
                      'TARGET_FID',
                      'Join_Count',
                      'TARGET_FID.1',
                      'Join_Count.1',
                      'OBJECTID.1',
                      'vek',
                      'hodina',
                      'p48a',
                      'p59d',
                      'id_vozidla',
                      'x',
                      'y',
                      'e',
                      'd',
                      'GlobalID'])


# prejmenovani sloupcu
df = df.rename(columns={
    'povetrnostni_podm': 'povetrnostni_podminky',
    'druh_komun': 'druh_komunikace',
    'mesic_t': 'mesic_v_roku',
    'ozn_osoba': 'bezpecnost_osoby',
    'osoba': 'pozice_osoby',
    'den': 'den_v_tydnu',
    'rok_nar': 'rok_narozeni',
    'lz': 'lehce_zranena_osoba',
    'tz': 'tezko_zranena_osoba',
    'smrt': 'smrt_osoba',
    'lehce_zran_os': 'pocet_lehce_zranenych',
    'tezce_zran_os': 'pocet_tezce_zranenych',
    'usmrceno_os': 'pocet_usmrcenych',
    'datum': 'datum_nehody'
})


# cisteni sloupce rok_narozeni
df['rok_narozeni'] = df['rok_narozeni'].replace(['null', None], pd.NA)
df['rok_narozeni'] = df['rok_narozeni'].fillna(0)
df['rok_narozeni'] = df['rok_narozeni'].astype(int)


# konverze datumu nehody a vypocet veku ucastniku v case nehody
df['datum_nehody'] = pd.to_datetime(df['datum_nehody'], errors='coerce')
df['rok_nehody'] = df['datum_nehody'].dt.year
df['vek_v_roku_nehody'] = df['rok_nehody'] - df['rok_narozeni']
df['vek_v_roku_nehody'] = df['vek_v_roku_nehody'].where(
    df['rok_narozeni'] != 0, pd.NA)
df['vek_v_roku_nehody'] = df['vek_v_roku_nehody'].astype('Int64')


# uprava sloupce smrt_dny
df['smrt_dny'] = df['smrt_dny'].replace('null', pd.NA)
df['smrt_dny'] = pd.to_numeric(df['smrt_dny']).fillna(0).astype(int)


# pretypovani sloupcu
df['hmotna_skoda'] = pd.to_numeric(df['hmotna_skoda']).astype(float)
df['skoda_vozidlo'] = pd.to_numeric(df['skoda_vozidlo']).astype(float)


# uprava hodnot v sloupci nasledek,stav_ridic a hlavni_pricina
df['nasledek'] = df['nasledek'].replace('bez zraněn', 'bez zranění')
df['hlavni_pricina'] = df['hlavni_pricina'].replace(
    'nesprávé předjíždění', 'nesprávné předjíždění')
df['hlavni_pricina'] = df['hlavni_pricina'].replace(
    'nedání přenosti v jízdě', 'nedání přednosti v jízdě')
df['stav_ridic'] = df['stav_ridic'].replace(
    'pod vlivem léků, narkoti', 'pod vlivem léků, narkotik')
df['stav_ridic'] = df['stav_ridic'].replace(
    'dobrý -žádné nepříznivé okolnosti nebyly zjiště',
    'dobrý -žádné nepříznivé okolnosti nebyly zjištěny')
df['stav_ridic'] = df['stav_ridic'].replace(
    'jiný nepříznivý sta', 'jiný nepříznivý stav')
df['stav_ridic'] = df['stav_ridic'].replace(
    'pokus o sebevraždu, sebevražd', 'pokus o sebevraždu, sebevražda')
df['stav_ridic'] = df['stav_ridic'].replace(
    'řidič při jízdě zemřel (infarkt apod.',
    'řidič při jízdě zemřel (infarkt apod.)')
df['stav_ridic'] = df['stav_ridic'].replace(
    'unaven, usnul, náhlá fyzická indispozic',
    'unaven, usnul, náhlá fyzická indispozice')


# odstranenie prefixu Brno- z mestskej casti (vynechani Brno-jih, sever, stred)
df['mestska_cast'] = df['mestska_cast'].str.replace(
    r'^Brno-(?!jih|sever|střed)', '', regex=True)


# uprava casu - nahradenie hodnot konciacich na 60 a zacinajucich nulou
def convert_cas_to_time(cas_value):
    if pd.isna(cas_value):
        return None
    cas_value = int(cas_value)
    if cas_value % 100 == 60:
        cas_value = cas_value - 60
    if cas_value == 2500:
        return None
    hours = cas_value // 100
    minutes = cas_value % 100
    if 0 <= hours < 24 and 0 <= minutes < 60:
        return time(hour=hours, minute=minutes)
    else:
        return None


df['cas'] = pd.to_numeric(df['cas'], errors='coerce')
df['cas_time'] = df['cas'].apply(convert_cas_to_time)
df = df.drop(columns=['cas'])


# odstraneni zaznamu s datumom starsim ako 2016 a ulozeni df
df = df[df['rok_nehody'] >= 2016]
df.to_csv('Traffic_accidents_cleaned.csv', encoding='utf-8-sig', index=False)


# rozdeleni sloupcu do 3 csv pro jednodussi praci v powerBI
columns_ucastnici = [
    'OBJECTID',
    'id_nehody',
    'pohlavi',
    'rok_narozeni',
    'vek_v_roku_nehody',
    'nasledek',
    'pneumatiky',
    'druh_pohonu',
    'druh_vozidla',
    'bezpecnost_osoby',
    'pozice_osoby',
    'chovani_chodce',
    'stav_chodce',
    'drogy_chodec',
    'alkohol_chodec',
    'reflexni_prvky',
    'kategorie_chodce',
    'lehce_zranena_osoba',
    'tezko_zranena_osoba',
    'smrt_osoba',
    'alkohol_vinik',
    'alkohol',
    'stav_ridic',
    'smrt_dny']


columns_nehody = [
    'id_nehody', 'datum_nehody', 'cas_time', 'mestska_cast',
    'Longitude', 'Latitude', 'GoogleMapsLink', 'GoogleMapsHTML',
    'hlavni_pricina', 'srazka', 'nasledky', 'pricina', 'stav_vozovky',
    'povetrnostni_podminky', 'rozhled', 'misto_nehody', 'druh_komunikace',
    'zavineni', 'viditelnost', 'situovani', 'doba',
    'den_v_tydnu', 'mesic_v_roku', 'rok', 'mesic'
]


columns_metriky = [
    'id_nehody', 'pocet_lehce_zranenych', 'pocet_tezce_zranenych',
    'pocet_usmrcenych'
]


# vytvoreni dataframov
df_ucastnici = df[columns_ucastnici]
df_nehody = df[columns_nehody]
df_metriky = df[columns_metriky]


# odstraneni duplicit v id_nehody
df_nehody = df_nehody.drop_duplicates(subset='id_nehody')
df_metriky = df_metriky.drop_duplicates(subset='id_nehody')


# ulozeni 3 souboru
df_ucastnici.to_csv('ucastnici_nehod.csv', index=False, encoding='utf-8-sig')
df_nehody.to_csv('nehody.csv', index=False, encoding='utf-8-sig')
df_metriky.to_csv('metriky.csv', index=False, encoding='utf-8-sig')
