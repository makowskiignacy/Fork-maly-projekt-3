import pandas as pd
import requests
import zipfile
import io

from DownloadClean import *

def test_clean_data_2015(): # test dla roku 2015 - czy dobrze usuwane są niepotrzebne elementy
    df = pd.DataFrame({
        0: [
            'Wskaźnik',
            'Czas uśredniania',
            '2015-01-01 01:00:00',
            '2015-01-01 07:00:00'
        ],
        1: ['PM25', '1h', 10, 50]
    })

    result = clean_data(df, 2015)

    assert len(result) == 1
    assert result.index.name == "Data poboru danych"

def test_clean_data_2018(): # test dla roku 2018 - czy dobrze usuwane są niepotrzebne elementy
    df = pd.DataFrame({
        0: [
            'Nr',
            'Wskaźnik',
            'Czas uśredniania',
            'Jednostka',
            'Czas pomiaru',
            '2018-01-01 01:00:00',
            '2018-01-01 07:00:00'
        ],
        1: ['1', 'PM25', '1h', 'µg/m3', 'grr', 20, 70]
    })

    result = clean_data(df, 2018)

    assert len(result) == 1
    assert result.index.name == "Data poboru danych"

def test_clean_data_2021(): # test dla roku 2021 - czy dobrze usuwane są niepotrzebne elementy
    df = pd.DataFrame({
        0: [
            'Nr',
            'Wskaźnik',
            'Czas uśredniania',
            'Jednostka',
            'Kod stanowiska',
            '2021-01-01 01:00:00',
            '2021-01-01 07:00:00'
        ],
        1: ['1', 'PM25', '1h', 'µg/m3', 'ABC', 15, 60]
    })

    result = clean_data(df, 2021)

    assert len(result) == 1
    assert result.index.name == "Data poboru danych"


def test_clean_data_midnight_shift(): # test dla przesunięcia godziny 00:00 na 23:59:59 poprzedniego dnia
    
    df = pd.DataFrame({
        0: [
            'Nr',
            'Wskaźnik',
            'Czas uśredniania',
            'Jednostka',
            'Kod stanowiska',
            'sample',
            '2021-01-02 00:00:00',
            '2021-01-02 01:00:00'
        ],
        1: ['1', 'PM25', '1h', 'µg/m3', 'ABC', 'sample', 10, 20]
    })

    result = clean_data(df, 2021)

    assert result.index[0] == pd.Timestamp("2021-01-01 23:59:59")

def test_map_station_codes(): # test mapowania kodów stacji

    metadane = pd.DataFrame({
        'Kod stacji': ['001', '002', '003'],
        'Nazwa stacji': ['Stacja A', 'Stacja B', 'Stacja C']
    })

    mapping_dict = dict(zip(metadane['Kod stacji'], metadane['Nazwa stacji']))

    df = pd.DataFrame({
        '001': [10, 20],
        '002': [30, 40],
        '003': [50, 60]
    })

    map_station_codes(df, mapping_dict)

    assert df.columns.tolist() == ['Stacja A', 'Stacja B', 'Stacja C']

def test_make_multi_index(): # test dla poprawnego tworzenia MultiIndex 
    meta = pd.DataFrame({
        "Kod stacji": ["S1", "S2"],
        "Miejscowość": ["Warszawa", "Kraków"]
    })

    idx = make_multi_index(meta, ["S1", "S2"])

    assert ("S1", "Warszawa") in idx
    assert ("S2", "Kraków") in idx


def test_make_multi_index_unknown_city(): # test dla nieznanej miejscowości w MultiIndex
    meta = pd.DataFrame({
        "Kod stacji": ["S1"],
        "Miejscowość": ["Warszawa"]
    })

    idx = make_multi_index(meta, ["S1", "S2"])

    assert ("S2", "Nieznana") in idx



    

