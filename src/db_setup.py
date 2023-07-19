import logging
from src.data_ingestion.db_creation import update_timeseries_table, update_imports_panel_data, update_exports_panel_data
from src.data_ingestion.raw_data_collection.from_fred import get_population_data, get_disposable_income_data, get_average_wine_price_data, get_ppi_data
from src.data_ingestion.raw_data_collection.from_ttb import get_wine_production
from src.data_ingestion.raw_data_collection.from_usitc import get_usitc_data
from src.data_ingestion.raw_data_collection.from_thin_air import get_retaliatory_wine_tariff_months

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_db():    
    logging.info('Starting wine production update...')
    wine_production = get_wine_production()
    update_timeseries_table('production', wine_production, ['wine_production_us'])
    logging.info('Wine production update finished.')

    logging.info('Starting population update...')
    population = get_population_data()
    update_timeseries_table('demographics', population, ['population_size'])
    logging.info('Population update finished.')

    logging.info('Starting disposable income update...')
    disposable_income = get_disposable_income_data()
    update_timeseries_table('demographics', disposable_income, ['disposable_income_amount'])
    logging.info('Disposable income update finished.')

    logging.info('Starting average wine prices update...')
    average_wine_prices = get_average_wine_price_data()
    update_timeseries_table('prices', average_wine_prices, ['average_wine_price_us'])
    logging.info('Average wine prices update finished.')

    logging.info('Starting producer price index update...')
    ppi = get_ppi_data()
    update_timeseries_table('prices', ppi, ['producer_price_index_us'])
    logging.info('Producer price index update finished.')

    logging.info('Starting imports panel data update...')
    imports_panel_data = get_usitc_data('/app/data/USITC/dataweb-queryExport-20230609.xlsx')
    update_imports_panel_data(imports_panel_data[1])
    logging.info('Imports panel data update finished.')

    logging.info('Starting imports panel data (all countries) update...')
    imports_panel_data_all_countries = get_usitc_data('/app/data/USITC/dataweb-queryExport-20230611.xlsx')
    update_imports_panel_data(imports_panel_data_all_countries[1])
    logging.info('Imports panel data (all countries) update finished.')

    logging.info('Starting exports panel data (all countries) update...')
    exports_panel_data_all_countries = get_usitc_data('/app/data/USITC/dataweb-queryExport-20230611-exports.xlsx')
    update_exports_panel_data(exports_panel_data_all_countries[1])
    logging.info('Exports panel data (all countries) update finished.')

    logging.info('Starting retaliatory tariffs time series update...')
    retaliatory_tariffs_time_series = get_retaliatory_wine_tariff_months()
    update_timeseries_table('prices', retaliatory_tariffs_time_series, ['retaliatory_wine_tariffs'])
    logging.info('Retaliatory tariffs time series update finished.')


if __name__ == '__main__':
    setup_db()
