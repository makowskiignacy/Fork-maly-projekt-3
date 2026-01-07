# Maly-projekt-3

## Dokumentacja

### Całokształt

W tym repozytorium znajduje się plik ipynb - projekt_3.ipynb, który jest zmodyfikowaną wersją małego projektu nr 1.
Najważniejszą zmianą było przeniesienie części: pobierania danych, analizy danych, wizualizacji danych do odrębnych plików .py (w tej samej kolejności: DownloadClean.py, Analysis.py, Visualization.py) z których funkcje są importowane i po kolei odpalane w notatniku.
Obecne są równierz testy dla każdego z plików .py, znajdujące się w folderze tests. 
Są to: DownloadClean_test.py, Analysis_test.py, Visualization_test.py . 
Znajduje się również plik .gitignore, README.md zawierający dokumentację, plik z metadanymi - metadane_new.xlsx oraz oryginalny notatnik dla małego projektu nr 1 - projekt_1_student.ipynb

### Pliki .py

DownloadClean.py zawiera:
- stałe: URL dla metadanych i archiwów, ID i nazwy plików dla poszczególnych lat.
- funkcję download_gios_archive(year) -> df ; pobiera archiwum GIOŚ dla danego roku.
- funckję clean_data(df,year) -> df ; oczyszcza df z danymi w zależności od roku, shiftuje czas dla godziny 00:00:00 do 23:59:59 .
- funkcję download_metadata() -> df ; pobiera metadane ze strony GIOŚ.
- funkcję map_station_codes(df, mapping_dict) -> df ; mapuje kody stacji dla danego df.
- funkcję download_all(years) -> dict[year, df] ; przy pomocy powyższych funkcji pobiera, czyści i mapuje dane dla wszystkich lat znajdujących się w liście years, tworzy także mapping_dict używany przez funkcję map_stattion_codes.
- funkcję make_multi_index(metadane, common_stations) -> pd.MultiIndex ; Tworzy multiindex - indeks składający się z kodu stacji i miejscowości, jeżeli miasto jest nieznane - indeks tworzy kod stacji i "nieznane"
- funkcję prepare_common_data(years) -> df ; dla listy lat tworzy ostateczny df z multiindeksem - z połączonych df dla poszczególnych lat, który zapisuje do pliku .csv . Wykorzystuje do tego powyższe funkcje.

Analysis.py zawiera:
- stałą globalną: PM25_NORM=15 .
- funckję monthly_mean(df,years) -> df ; która grupując po latach i miesiącach oblicza średnie miesięczne dla lat-years, wyniki zwraca w tabeli gdzie rok i miesiąc są indeksami a wartością średnia.
- funkcję days_above_norm(df, norm) -> df ; kopiuje dane a następnie wyciąga datę bez godziny i umieszcza ją w nowej kolumnie. Grupując po nowej kolumnie oblicza średnią dzienną. Zwraca zmodyfikowaną o średnią kopię oryginalnego df.
- funkcję days_above_norm(df, norm) -> df ; dzięki średnim dobowym i grupowaniu po latach zlicza wystąpenia dni w których norma została przekroczona dla poszczególnych lat.

    
    
