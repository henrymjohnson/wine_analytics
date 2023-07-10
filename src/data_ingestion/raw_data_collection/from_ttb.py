import pandas as pd
import numpy as np
import datetime
import openpyxl
import logging

# from src.data_ingestion.db_creation import update_timeseries_table


logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

ttb_dir_path = '/app/data/TTB/'


# process the most recent data we have from the TTB
def process_recent_data(ttb_data_path=ttb_dir_path):
    # first we need to loop through the excel workbook that
    # contains the monthly production data from the TTB
    # and store off the monthly data from there.
    try:
        wb = openpyxl.load_workbook(ttb_data_path + 'Wine_National_Report_10_MAR_2023.xlsx')
    except Exception as e:
        logging.error(f'An error occurred while loading the workbook: {e}')

    production_data = []

    # loop through each sheet
    for sheetname in wb.sheetnames:
        sheet = wb[sheetname]
        
        # headers start on the 8th row.
        df = pd.DataFrame(sheet.values)
        df.columns = df.iloc[7]
        df = df.iloc[8:]
        
        prod_row = df[df['Units: Wine Gallons'] == 'Production']
        
        for col in prod_row.columns[1:]:
            if not col.startswith('Annual Total'):
                date_string = f"{col}"
                date_string = date_string.replace('\n', ' ')
                date_string = ' '.join(date_string.split())
                date_object = datetime.datetime.strptime(date_string, "%B %Y")
                formatted_date = date_object.strftime("%Y-%m-%d")
                
                # monthly wine production in gallons
                production_value = prod_row[col].values[0]
                production_data.append((formatted_date, production_value))

    # convert the list to a DataFrame
    production_df = pd.DataFrame(production_data, columns=['month', 'production'])

    # fill the old datapoints with any missing values
    try:
        old_df = pd.read_excel(ttb_data_path + 'Monthly Production from TTB Wine Statistics 2008 to Present.xlsx')
    except Exception as e:
        logging.error(f'An error occurred while loading the old data: {e}')

    old_df['month'] = pd.to_datetime(old_df['month']).dt.strftime('%Y-%m-%d')

    result = pd.merge(production_df, old_df, how='outer', on='month')

    result['production'] = result['production'].fillna(result['national_production'])
    result['production'] = result['production'].astype(pd.Int64Dtype())

    result = result.drop(columns=['national_production'])
    result['month'] = pd.to_datetime(result['month'], format='%Y-%m-%d')
    result = result.sort_values(by='month', ascending=False)
    result.set_index('month', inplace=True)

    result['date'] = result.index.date  # add the 'date' column

    return result


# since wine production observations only go back to 2012 from the TTB,
# we'll need to pull in the archived pdfs I'd stowed away previously
# There are PDFs in the data/raw folder that go back to 2008
# The PDF values also contain the prior years' values
# Since those will contain any revisions, we'll use the prior years' values
# I'd previously extracted the data from the PDFs and saved them to a csv
# in data_acquisition.ipynb
# Before 2008, there was a different formatting, so we'll pull that separately
archived_data_from_2007_to_2010 = pd.read_csv(ttb_dir_path + 'wine_production_from_archived_pdfs.csv')


# lastly, pull in the archived data that was before 2008
archived_wine_production_before_2008 = pd.read_csv(ttb_dir_path + 'archived_wine_production.csv')


# validate datetime column and index
#  * all kwargs dataframes to make sure they have the month column 
#  * that the month column is a datetime
#  * that the index is the month column
def _format_wine_production_df(**kwargs):
    for df_name, df in kwargs.items():
        if 'month' not in df.columns and df.index.name != 'month':
            raise ValueError(f'{df_name} does not contain a month column or index')

        if 'month' in df.columns and df['month'].dtype != 'datetime64[ns]':
            df['month'] = pd.to_datetime(df['month'], format='%Y-%m-%d')
            df.set_index('month', inplace=True)
            df.sort_index(ascending=False, inplace=True)
            
        elif df.index.name == 'month' and df.index.dtype != 'datetime64[ns]':
            df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
            df.sort_index(ascending=False, inplace=True)
            
        kwargs[df_name] = df

    return kwargs

# get the range min and max dates from all dataframes
# return an array of months filling that range
def _get_months(**kwargs):
    all_months = []
    for df_name, df in kwargs.items():
        all_months.append(df.index.min())
        all_months.append(df.index.max())
        # replace smallest if necessary
        all_months.append(df.index.min() if all_months[0] > df.index.min() else all_months[0])
        all_months.append(df.index.max() if all_months[1] < df.index.max() else all_months[1])

    all_months = pd.to_datetime(all_months, format='%Y-%m-%d')
    all_months = pd.date_range(start=all_months.min(), end=all_months.max(), freq='MS')
    all_months = all_months.sort_values(ascending=False)
    
    return all_months

# merge the dataframes together
def combine_multiple_wine_production_series(**kwargs):

    kwargs = _format_wine_production_df(**kwargs)
    months = _get_months(**kwargs)

    # create a dataframe with one column, month, and another column production and fill it with null values
    wine_production = pd.DataFrame(months, columns=['month'])
    wine_production['month'] = pd.to_datetime(wine_production['month'], format='%Y-%m-%d')
    wine_production = wine_production.sort_values(by=['month'], ascending=False)
    wine_production.set_index('month', inplace=True)
    # make production values a nullable integer (instead of float)
    wine_production['production'] = pd.array([np.nan] * len(wine_production), dtype=pd.Int64Dtype())

    # combine each dataframe in turn
    for df_name, df in kwargs.items():
        logging.info(f'merging {df_name}')
        wine_production['production'] = wine_production['production'].combine_first(df['production'])

    return wine_production


# combine the dataframes (order matters, newest first in the list)
wine_production = combine_multiple_wine_production_series(df1=process_recent_data(), df2=archived_data_from_2007_to_2010, df3=archived_wine_production_before_2008)


# get wine production data
def get_wine_production():
    wine_production = combine_multiple_wine_production_series(df1=process_recent_data(), df2=archived_data_from_2007_to_2010, df3=archived_wine_production_before_2008)
    result = wine_production.reset_index()
    result.rename(columns={'production': 'wine_production_us', 'month': 'date'}, inplace=True)
    return result


# update the wine production table
# update_timeseries_table('production', get_wine_production(), ['wine_production_us'])
