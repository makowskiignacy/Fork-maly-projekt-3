import pandas as pd
import requests
import zipfile
import io


# Stałe
META_URL = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/584"
GIOS_ARCHIVE_URL = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/"

GIOS_ID = {
    2014: '302',
    2019: '322',
    2024: '582'
}

GIOS_PM25_FILE = {
    2014: '2014_PM2.5_1g.xlsx',
    2019: '2019_PM25_1g.xlsx',
    2024: '2024_PM25_1g.xlsx'
}


# Pobieranie archiwum GIOŚ
def download_gios_archive(year):
    url = f"{GIOS_ARCHIVE_URL}{GIOS_ID[year]}"
    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        with z.open(GIOS_PM25_FILE[year]) as f:
            df = pd.read_excel(f, header=None)

    return df



# Czyszczenie danych
def clean_data(df):
    df = df.set_index(0)

    if {'Kod stanowiska', 'Jednostka', 'Nr'}.issubset(df.index):
        df = df.drop([
            'Wskaźnik', 'Kod stanowiska',
            'Czas uśredniania', 'Jednostka', 'Nr'
        ])
    else:
        df = df.drop(['Wskaźnik', 'Czas uśredniania'])

    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df.index = pd.to_datetime(df.index)

    df.index = [
        idx - pd.Timedelta(seconds=1) if idx.hour == 0 else idx
        for idx in df.index
    ]

    df.index.name = "Data poboru danych"
    return df


# --------------------------------------------------
# Metadane
# --------------------------------------------------
def download_metadata():
    # Pobieranie z URL:
    # response = requests.get(META_URL)
    # response.raise_for_status()
    # with open("metadane.xlsx", "wb") as f:
    #     f.write(response.content)

    metadane = pd.read_excel("metadane.xlsx")

    cols = list(metadane.columns)
    cols[4] = 'Stary kod'
    metadane.columns = cols

    metadane = metadane.dropna(subset=['Stary kod'])
    metadane['Stary kod'] = metadane['Stary kod'].str.split(', ')
    metadane = metadane.explode('Stary kod')

    return metadane



# Mapowanie kodów stacji
def map_station_codes(df, mapping_dict):
    df.columns = df.columns.map(lambda x: mapping_dict.get(x, x))
    return df



# Pobranie + czyszczenie + mapowanie (wszystkie lata)
def download_all(years):
    metadane = download_metadata()
    mapping_dict = dict(zip(metadane['Stary kod'], metadane['Kod stacji']))

    data = {}

    for year in years:
        df = download_gios_archive(year)
        df = clean_data(df)
        df = map_station_codes(df, mapping_dict)
        data[year] = df

    return data



# Wspólne stacje
def get_common_stations(data):
    dfs = list(data.values())
    common = dfs[0].columns

    for df in dfs[1:]:
        common = common.intersection(df.columns)

    return common



# MultiIndex (Kod stacji, Miejscowość)
def make_multi_index(metadane, common_stations):
    filtered = metadane[metadane['Kod stacji'].isin(common_stations)]
    filtered_unique = filtered.drop_duplicates(subset=['Kod stacji'])

    mapping_dict = dict(
        zip(filtered_unique['Kod stacji'], filtered_unique['Miejscowość'])
    )

    station_city = [
        (st_code, mapping_dict.get(st_code, "Nieznana"))
        for st_code in common_stations
    ]

    return pd.MultiIndex.from_tuples(
        station_city,
        names=['Kod stacji', 'Miejscowość']
    )



# Finalne dane do analizy
def prepare_common_data(years):
    metadane = download_metadata()
    data = download_all(years)

    common_stations = get_common_stations(data)
    multi_index = make_multi_index(metadane, common_stations)

    dfs_common = []
    for year in years:
        df = data[year][common_stations]
        df.columns = multi_index
        dfs_common.append(df)

    return pd.concat(dfs_common)

  
