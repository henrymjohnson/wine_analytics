from src.data_ingestion.from_ttb import get_wine_production
from src.data_ingestion.db_changes import update_timeseries_table


wine_production = get_wine_production()
update_timeseries_table('production', wine_production, ['wine_production_us'])
