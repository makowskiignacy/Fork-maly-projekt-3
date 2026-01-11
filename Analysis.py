import pandas as pd

PM25_NORM=15 # µg/m3 (dobowa)


def monthly_mean(df,years):
    df_month = (
    df
    .groupby([df.index.year, df.index.month])
    .mean(numeric_only=True))
    df_years=df_month.loc[years]
    df_years.index.names = ["Rok", "Miesiąc"]

    return df_years

def daily_mean(df):
    df_copy = df.copy()
    
    # Dodaj kolumnę z datą
    df_copy['data'] = df_copy.index.date
    
    df_daily = df_copy.groupby('data').mean(numeric_only=True)
    
    return df_daily



def days_above_norm(df, norm=PM25_NORM):
    
    df_daily = daily_mean(df)
    
    # Sprawdzenie przekroczeń
    exceed = df_daily > norm
    
    # Grupowanie po roku (tylko istniejące lata)
    days_exceeded= exceed.groupby(pd.DatetimeIndex(df_daily.index).year).sum()
    days_exceeded.index.name = 'Rok'

    return days_exceeded



