import pandas as pd
import requests
import zipfile
import io


# Stałe
META_URL = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/622"
GIOS_ARCHIVE_URL = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/"

GIOS_ID = {
    2015: '236', 
    2018: '603', 
    2021: '486', 
    2024: '582'}


GIOS_PM25_FILE = {
    2015: '2015_PM25_1g.xlsx', 
    2018: '2018_PM25_1g.xlsx', 
    2021: '2021_PM25_1g.xlsx', 
    2024: '2024_PM25_1g.xlsx'}


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
def clean_data(df,year):
    df = df.set_index(0)
    
    if year==2015:
        df = df.drop(['Wskaźnik', 'Czas uśredniania'])
    
    elif year==2018:
        df = df.drop(['Nr','Wskaźnik','Czas uśredniania', 'Jednostka', 'Czas pomiaru'],axis=0)
    else: 
        df = df.drop(['Nr','Wskaźnik','Czas uśredniania', 'Jednostka', 'Kod stanowiska'],axis=0)
    
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    df.index = pd.to_datetime(df.index)
    
    # Zaokrąglenie do sekundy, aby uniknąć problemów z precyzją (drift w milisekundach)
    df.index = df.index.round('s')

    df.index = [
        idx - pd.Timedelta(seconds=1) if idx.hour == 0 else idx
        for idx in df.index
    ]

    df.index.name = "Data poboru danych"
    return df

# Metadane
def download_metadata():
    # Pobieranie z URL:
    response = requests.get(META_URL)
    response.raise_for_status()
    with open("metadane_new.xlsx", "wb") as f:
        f.write(response.content)

    metadane = pd.read_excel("metadane_new.xlsx")

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
        df = clean_data(df,year)
        df = map_station_codes(df, mapping_dict)
        data[year] = df

    return data

# MultiIndex (Kod stacji, Miejscowość)
def make_multi_index(metadane, common_stations):
    filtered = metadane[metadane['Kod stacji'].isin(common_stations)]
    mapping_dict = dict(
        zip(filtered['Kod stacji'], filtered['Miejscowość'])
    )

    station_city = [
        (st_code, mapping_dict.get(st_code, "Nieznana"))
        for st_code in common_stations
    ]

    return pd.MultiIndex.from_tuples(
        station_city,
        names=['Kod stacji', 'Miejscowość']
    )

def prepare_common_data(years):
    metadane = download_metadata()
    data = download_all(years)

    dfs = [data[year] for year in years]
    df_all = pd.concat(dfs, join="inner")

    multi_index = make_multi_index(metadane, df_all.columns)
    df_all.columns = multi_index
    
    # Konwersja na typ numeryczny dla wszystkich kolumn danych
    # Obsługa przecinków jako separatorów dziesiętnych oraz usuwanie znaków < >
    def robust_to_numeric(s):
        if s.dtype == object:
            s = s.astype(str).str.replace(',', '.', regex=False)
            s = s.str.replace('<', '', regex=False).str.replace('>', '', regex=False)
        return pd.to_numeric(s, errors='coerce')

    df_all = df_all.apply(robust_to_numeric)

    # Odfiltrowanie danych tylko dla żądanych lat (usuwa artefakty przesunięcia czasu)
    df_all = df_all[df_all.index.year.isin(years)]

    # Zapis DataFrame do pliku
    lata = "_".join(map(str, years))
    tytul = f"data_{lata}.csv"
    df_all.to_csv(tytul, index=True)

    print(f"Zapisano do pliku: {tytul}")

    return df_all