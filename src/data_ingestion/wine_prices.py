from src.data_ingestion.raw_data_collection.from_fred import get_average_wine_price_data, get_ppi_data
from src.data_ingestion.db_changes import update_timeseries_table


average_wine_prices = get_average_wine_price_data()
update_timeseries_table('prices', average_wine_prices, ['average_wine_price_us'])

ppi = get_ppi_data()
update_timeseries_table('prices', ppi, ['producer_price_index_us'])
