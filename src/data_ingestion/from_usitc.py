import pandas as pd
import re


# read the excel file into a dictionary of dataframes
# file_path = '/app/data/USITC/dataweb-queryExport-20230609.xlsx'

# convert wide format to long format
def _wide_to_long(df, year):
    wide_df = df.copy()
    if 'Country' not in wide_df.columns:
        wide_df['Country'] = 'all'
    if 'Special Import Program' not in wide_df.columns:
        wide_df = wide_df.melt(id_vars=['Data Type', 'Country', 'Quantity Description'], var_name='month', value_name='value')
        wide_df['date'] = pd.to_datetime(wide_df['month'] + f'{year}', format='%b%Y')
        wide_df = wide_df.pivot_table(index=['date', 'Country', 'Quantity Description'], columns='Data Type', values='value').reset_index()
    else:
        wide_df = wide_df.melt(id_vars=['Data Type', 'Country', 'Special Import Program', 'Quantity Description'], var_name='month', value_name='value')
        wide_df['date'] = pd.to_datetime(wide_df['month'] + f'{year}', format='%b%Y')
        wide_df = wide_df.pivot_table(index=['date', 'Country', 'Special Import Program', 'Quantity Description'], columns='Data Type', values='value').reset_index()

    return wide_df

# get imports data from the excel file
def get_usitc_data(file_path):

    sheets_dict = pd.read_excel(file_path, sheet_name=None, header=None)

    query_results_data_dict = {}
    years = set()

    # set the file's header row length and the index of the next header row
    header_row_length = 2
    next_header_start_index = 0

    while True:
        # read the query results header
        query_results_header = sheets_dict['Query Results'].iloc[next_header_start_index:next_header_start_index+2, :]
        table_and_year_row = query_results_header.iloc[0, 0]
        
        # get the table name and year from the header
        match = re.search(r"(.+) \| Monthly data for (\d{4})", table_and_year_row)
        table_name = match.group(1) if match else None
        year = match.group(2) if match else None
        
        # set the number of observations for the year
        num_observations = query_results_header.iloc[1, 1]
        
        # get the table of results for the year, country, import program, and values per month
        query_results_data = sheets_dict['Query Results'].iloc[next_header_start_index+2:next_header_start_index+num_observations+3, :]
        query_results_data.columns = query_results_data.iloc[0]
        query_results_data = query_results_data.iloc[1:]
        
        # append the new table to the existing dictionary
        if year not in years:
            query_results_data_dict[year] = query_results_data
            years.add(year)
        else:
            query_results_data_dict[year] = pd.concat([query_results_data_dict[year], query_results_data], ignore_index=True)
        
        next_header_start_index += num_observations + header_row_length + 4
        
        # check if there's more data available
        if next_header_start_index >= len(sheets_dict['Query Results']):
            break

    # create a set of import programs
    for year in query_results_data_dict.keys():
        # convert the dataframe from wide to long
        query_results_data_dict[year] = _wide_to_long(query_results_data_dict[year], year)

    # build up the final dataframe
    final_df = pd.concat(query_results_data_dict.values(), ignore_index=True)
    final_df.rename(columns={'date': 'month'}, inplace=True)
    final_df.drop(columns=['Quantity Description'], inplace=True)
    
    return table_name, final_df
