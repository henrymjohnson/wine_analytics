from src.data_ingestion.raw_data_collection.from_thin_air import get_retaliatory_wine_tariff_months
from src.data_ingestion.raw_data_collection.from_usitc import get_usitc_data
from src.data_ingestion.db_changes import update_imports_panel_data, update_exports_panel_data, update_retaliatory_wine_tariffs


imports_panel_data = get_usitc_data('/app/data/USITC/dataweb-queryExport-20230609.xlsx')
update_imports_panel_data(imports_panel_data[1])

imports_panel_data_all_countries = get_usitc_data('/app/data/USITC/dataweb-queryExport-20230611.xlsx')
update_imports_panel_data(imports_panel_data_all_countries[1])

exports_panel_data_all_countries = get_usitc_data('/app/data/USITC/dataweb-queryExport-20230611-exports.xlsx')
update_exports_panel_data(exports_panel_data_all_countries[1])

retaliatory_tariffs_time_series = get_retaliatory_wine_tariff_months()
update_retaliatory_wine_tariffs()
