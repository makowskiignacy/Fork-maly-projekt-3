import pandas as pd
import pytest

from Analysis import *

def test_monthly_mean_jan(): # test dla średnich miesięcznych - styczeń

    idx = pd.date_range('2021-01-01', periods=4, freq='15D')
    df = pd.DataFrame({
        'S1': [10, 20, 30, 40],
        'S2': [15, 30, 45, 60]
    }, index=idx)
    
    result = monthly_mean(df, [2021])
    
    # sprawdzenie wartości dla stycznia
    # Jan: 10+20+30=60/3=20
    assert result.loc[(2021, 1), 'S1'] == 20
    assert result.loc[(2021, 1), 'S2'] == 30


def test_daily_mean(): # test dla średnich dobowych

    idx = pd.date_range('2021-01-01 01:00', periods = 8, freq = '4h')

    df = pd.DataFrame({
        'S1': [i*10 for i in range(1, 9)]
    }, index=idx)

    result = daily_mean(df)

    # sprawdzenie liczby dni
    assert len(result) == 2

    # średnia dla 2021-01-01 = 35
    # średnia dla 2021-01-02 = 75
    assert result.loc[pd.to_datetime('2021-01-01').date(), 'S1'] == 35
    assert result.loc[pd.to_datetime('2021-01-02').date(), 'S1'] == 75


def test_days_above_norm(): # test dla dni powyżej normy

    idx = pd.date_range('2021-01-01', periods=6, freq='D')
    df = pd.DataFrame({
        'S1': [10, 20, 16, 14, 18, 22]
    }, index=idx)

    result = days_above_norm(df, norm=15)

    # dni powyżej normy - 4
    assert result.loc[2021, 'S1'] == 4
