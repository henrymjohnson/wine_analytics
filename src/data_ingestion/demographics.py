from src.data_ingestion.raw_data_collection.from_fred import get_population_data, get_disposable_income_data
from src.data_ingestion.db_creation import update_timeseries_table


population = get_population_data()
update_timeseries_table('demographics', population, ['population_size'])

disposable_income = get_disposable_income_data()
update_timeseries_table('demographics', disposable_income, ['disposable_income_amount'])
