from datetime import date
import pandas as pd


def _identify_retaliatory_wine_tariffs():
    current_year = date.today().year
    current_month = date.today().month

    months = [
        f'{year}-{month:02d}'
        for year in range(1900, current_year+1)
        for month in range(1, 13)
        if year < current_year or (year == current_year and month <= current_month)
    ]
    is_active = [1 if month >= '2019-10' and month <= '2021-06' else 0 for month in months]
    print(months[0:5])
    months = pd.to_datetime(months, format='%Y-%m')

    return {'date': months, 'retaliatory_wine_tariffs': is_active}

def get_retaliatory_wine_tariff_months():
    return pd.DataFrame(_identify_retaliatory_wine_tariffs())
