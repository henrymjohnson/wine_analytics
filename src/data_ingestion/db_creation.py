import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import register_adapter, AsIs
from decouple import config

from src.data_ingestion.raw_data_collection.from_thin_air import get_retaliatory_wine_tariff_months


# create timeseries table if it doesn't already exist
def update_timeseries_table(table_name, df, columns):
    # establish db connection
    connection = psycopg2.connect(
        database=config('POSTGRES_DB'),
        user=config('POSTGRES_USER'),
        password=config('POSTGRES_PASSWORD'),
        host=config('POSTGRES_HOST'),
        port=5432,
    )
    cursor = connection.cursor()

    # register adapter function for numpy.int64 to handle integer types
    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)
    register_adapter(np.int64, addapt_numpy_int64)

    # create schema if it doesn't exist
    cursor.execute("CREATE SCHEMA IF NOT EXISTS time_series_features")
    connection.commit()

    # create the table if it doesn't exist
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS time_series_features.{table_name} (
            month DATE PRIMARY KEY
        )
    """)
    connection.commit()

    # add missing columns to the table
    for column in columns:
        data_type = df[column].dtype

        if pd.api.types.is_numeric_dtype(data_type):
            cursor.execute(f"""
                ALTER TABLE time_series_features.{table_name}
                ADD COLUMN IF NOT EXISTS {column} NUMERIC
            """)
        elif np.issubdtype(data_type, np.datetime64):
            cursor.execute(f"""
                ALTER TABLE time_series_features.{table_name}
                ADD COLUMN IF NOT EXISTS {column} DATE
            """)
        else:
            cursor.execute(f"""
                ALTER TABLE time_series_features.{table_name}
                ADD COLUMN IF NOT EXISTS {column} VARCHAR
            """)
        connection.commit()

    # upsert rows into the table
    for i in range(len(df)):
        month = df['date'][i]

        # check if the row already exists
        cursor.execute(f"""
            SELECT *
            FROM time_series_features.{table_name}
            WHERE month = %s
        """, (month,))
        row_exists = cursor.fetchone() is not None

        # construct the query dynamically to update or insert the row
        if row_exists:
            query = f"UPDATE time_series_features.{table_name} SET "
            for column in columns:
                query += f"{column} = %s, "
            query = query[:-2] + " WHERE month = %s"
            values = [df[column][i] for column in columns] + [month]
        else:
            query = f"INSERT INTO time_series_features.{table_name} (month, "
            for column in columns:
                query += f"{column}, "
            query = query[:-2] + ") VALUES (%s, "
            for _ in columns:
                query += "%s, "
            query = query[:-2] + ")"
            values = [month] + [df[column][i] for column in columns]

        # execute query with corresponding values
        cursor.execute(query, tuple(values))
        connection.commit()

    # close db connection
    connection.close()


# create panel data if it doesn't already exist
def _create_panel_data_schema(connection):
    cursor = connection.cursor()   
 
    # create schema if it doesn't exist
    cursor.execute("CREATE SCHEMA IF NOT EXISTS panel_data")
    connection.commit()

def _create_panel_data_tables(connection):
    cursor = connection.cursor()

    # create the table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS panel_data.regions (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(255) NOT NULL,
            CONSTRAINT unique_region_name UNIQUE (name, type)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS panel_data.series (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(255),
            source VARCHAR(255),
            link VARCHAR(255)
        )
    """)

    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS unique_name_type_combo
        ON panel_data.series (name, type)
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS panel_data.import_programs (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            CONSTRAINT unique_import_program_name UNIQUE (name)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS panel_data.imports_for_consumption (
            month DATE NOT NULL,
            region_id INT REFERENCES panel_data.regions(id),
            import_program_id INT REFERENCES panel_data.import_programs(id),
            series_id INT REFERENCES panel_data.series(id),
            value NUMERIC NOT NULL,
            PRIMARY KEY (month, region_id, import_program_id, series_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS panel_data.total_exports (
            month DATE NOT NULL,
            region_id INT REFERENCES panel_data.regions(id),
            series_id INT REFERENCES panel_data.series(id),
            value NUMERIC NOT NULL,
            PRIMARY KEY (month, region_id, series_id)
        )
    """)


def update_imports_panel_data(df):
    # establish db connection
    connection = psycopg2.connect(
        database=config('POSTGRES_DB'),
        user=config('POSTGRES_USER'),
        password=config('POSTGRES_PASSWORD'),
        host=config('POSTGRES_HOST'),
        port=5432,
    )
    # create schema and tables if they don't exist
    _create_panel_data_schema(connection)
    _create_panel_data_tables(connection)

    cursor = connection.cursor()

    # update the import_programs table to have the import_programs_df values from the df of unique values in Special Import Program column
    import_programs = set()
    import_programs.update(df['Special Import Program'].unique())
    for import_program in import_programs:
        cursor.execute("""
            INSERT INTO panel_data.import_programs (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """, (import_program,))
        connection.commit()

    # update the regions table to have the regions_df values from the dict
    regions = set()
    regions.update(df['Country'].unique())
    for region in regions:
        cursor.execute("""
            INSERT INTO panel_data.regions (name, type)
            VALUES (%s, %s)
            ON CONFLICT (name, type) DO NOTHING
        """, (region, 'country'))
        connection.commit()


    # melt the df to have the columns: month, Country, Special Import Program, Series Name, Value
    df = df.melt(id_vars=['month', 'Country', 'Special Import Program'], var_name='Series Name', value_name='Value')
    df.columns = ['month', 'region', 'import_program_name', 'series_name', 'value']

    # update the series table to have the series_df values from the dict
    series = set()
    series.update(df['series_name'].unique())
    for series in series:
        cursor.execute("""
            INSERT INTO panel_data.series (name, type, source, link)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name, type) DO NOTHING
        """, (series, 'import', 'usitc', 'https://dataweb.usitc.gov/'))
        connection.commit()


    # upsert rows into the imports_for_consumption table
    for _, row in df.iterrows():
        month = row['month']
        region = row['region']
        import_program = row['import_program_name']
        series = row['series_name']
        value = row['value']

        # get the region_id
        cursor.execute("""
            SELECT id
            FROM panel_data.regions
            WHERE name = %s
        """, (region,))
        region_id = cursor.fetchone()[0]

        # get the import_program_id
        cursor.execute("""
            SELECT id
            FROM panel_data.import_programs
            WHERE name = %s
        """, (import_program,))
        import_program_id = cursor.fetchone()[0]

        # get the series_id
        cursor.execute("""
            SELECT id
            FROM panel_data.series
            WHERE name = %s
        """, (series,))
        series_id = cursor.fetchone()[0]

        # check if the row already exists
        cursor.execute("""
            SELECT *
            FROM panel_data.imports_for_consumption
            WHERE month = %s
                AND region_id = %s
                AND import_program_id = %s
                AND series_id = %s
        """, (month, region_id, import_program_id, series_id))
        row_exists = cursor.fetchone()

        if row_exists:
            # update the row with the new value if it already exists
            cursor.execute("""
                UPDATE panel_data.imports_for_consumption
                SET value = %s
                WHERE month = %s
                    AND region_id = %s
                    AND import_program_id = %s
                    AND series_id = %s
            """, (value, month, region_id, import_program_id, series_id))
            connection.commit()
        else:
            # insert the row if it doesn't exist
            cursor.execute("""
                INSERT INTO panel_data.imports_for_consumption (month, region_id, import_program_id, series_id, value)
                VALUES (%s, %s, %s, %s, %s)
            """, (month, region_id, import_program_id, series_id, value))
            connection.commit()

    connection.close()


def update_exports_panel_data(df):
    # establish db connection
    connection = psycopg2.connect(
        database=config('POSTGRES_DB'),
        user=config('POSTGRES_USER'),
        password=config('POSTGRES_PASSWORD'),
        host=config('POSTGRES_HOST'),
        port=5432,
    )
    # create schema and tables if they don't exist
    _create_panel_data_schema(connection)
    _create_panel_data_tables(connection)

    cursor = connection.cursor()

    # update the regions table to have the regions_df values from the dict
    regions = set()
    regions.update(df['Country'].unique())
    for region in regions:
        cursor.execute("""
            INSERT INTO panel_data.regions (name, type)
            VALUES (%s, %s)
            ON CONFLICT (name, type) DO NOTHING
        """, (region, 'country'))
        connection.commit()

    # melt the df to have the columns: month, Country, Special Import Program, Series Name, Value
    df = df.melt(id_vars=['month', 'Country'], var_name='Series Name', value_name='Value')
    df.columns = ['month', 'region', 'series_name', 'value']

    # update the series table to have the series_df values from the dict
    series = set()
    series.update(df['series_name'].unique())
    for series in series:
        cursor.execute("""
            INSERT INTO panel_data.series (name, type, source, link)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name, type) DO NOTHING
        """, (series, 'export', 'usitc', 'https://dataweb.usitc.gov/'))
        connection.commit()


    # upsert rows into the imports_for_consumption table
    for _, row in df.iterrows():
        month = row['month']
        region = row['region']
        series = row['series_name']
        value = row['value']

        # get the region_id
        cursor.execute("""
            SELECT id
            FROM panel_data.regions
            WHERE name = %s
                AND type = 'country'
        """, (region,))
        region_id = cursor.fetchone()[0]

        # get the series_id
        cursor.execute("""
            SELECT id
            FROM panel_data.series
            WHERE name = %s
        """, (series,))
        series_id = cursor.fetchone()[0]

        # check if the row already exists
        cursor.execute("""
            SELECT *
            FROM panel_data.total_exports
            WHERE month = %s
                AND region_id = %s
                AND series_id = %s
        """, (month, region_id, series_id))
        row_exists = cursor.fetchone()

        if row_exists:
            # update the row with the new value if it already exists
            cursor.execute("""
                UPDATE panel_data.total_exports
                SET value = %s
                WHERE month = %s
                    AND region_id = %s
                    AND series_id = %s
            """, (value, month, region_id, series_id))
            connection.commit()
        else:
            # insert the row if it doesn't exist
            cursor.execute("""
                INSERT INTO panel_data.total_exports (month, region_id, series_id, value)
                VALUES (%s, %s, %s, %s)
            """, (month, region_id, series_id, value))
            connection.commit()

    connection.close()


def update_retaliatory_wine_tariffs():
    update_timeseries_table('prices', get_retaliatory_wine_tariff_months(), ['retaliatory_wine_tariffs'])
