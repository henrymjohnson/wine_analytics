import pandas as pd
import requests
from decouple import config

fred_api_key = config('FRED_API_KEY')


def _get_fred_series_data(series_id):
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={fred_api_key}&file_type=json'

    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()

    date = []
    value = []
    for i in range(len(data['observations'])):
        date.append(data['observations'][i]['date'])
        value.append(data['observations'][i]['value'])
    
    df = pd.DataFrame({'date': date, 'value': value})
    return df


# Average Price of Wine, Not Seasonally Adjusted
average_wine_price_series_id = 'APU0000720311' 
def get_average_wine_price_data(series_id=average_wine_price_series_id):
    df = _get_fred_series_data(series_id)
    df['value'] = df['value'].astype(float)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df.rename(columns={'value': 'average_wine_price_us'}, inplace=True)
    return df

# Producer Price Index by Commodity: Wine, Not Seasonally Adjusted
ppi_series_id = 'PCU3121303121300' 
def get_ppi_data(series_id=ppi_series_id):
    df = _get_fred_series_data(series_id)
    df['value'] = df['value'].astype(float)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df.rename(columns={'value': 'producer_price_index_us'}, inplace=True)
    return df

# Population, Not Seasonally Adjusted, in Thousands of Persons
pop_series_id = 'POPTHM' 
def get_population_data(series_id=pop_series_id):
    df = _get_fred_series_data(series_id)

    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['value'] = df['value'].astype(float)

    df.rename(columns={'value': 'population_size'}, inplace=True)
    return df

# Disposable Personal Income, Seasonally Adjusted, in Billions of Chained 2012 Dollars
disp_inc_series_id = 'DSPIC96'
def get_disposable_income_data(series_id=disp_inc_series_id):
    df = _get_fred_series_data(series_id)
    df['value'] = df['value'].astype(float)
    df.rename(columns={'value': 'disposable_income_amount'}, inplace=True)
    return df
