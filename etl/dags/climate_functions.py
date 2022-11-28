import requests
import logging
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

DATA_SOURCES = {
    "climate_by_country.csv": "https://query.data.world/s/euo44nk2cizrjavkfqtogk7eiwwv4u",
    "climate_by_state.csv": "https://query.data.world/s/epcm3iauwcborpfhda7en6yeqdgsux",
    "geo_climate.geojson": "http://cecgis-caenergy.opendata.arcgis.com/datasets/549017ee96e341d2bbb3dd0c291a9112_0.geojson"
}


def extract_climate():
    pass
    # for filename, url in DATA_SOURCES.items():
    #     try:
    #         response = requests.get(url)
    #         with open(filename, "wb") as file:
    #             file.write(response.content)
    #             logging.info(f"Create {filename} file")
    #     except requests.exceptions.RequestException as e:
    #         logging.critical(f"ERROR when download climate data: {e}")


def transform_climate():
    climate_by_state = pd.read_csv("climate_by_state.csv")
    climate_by_state_usa = climate_by_state[climate_by_state['Country'] == 'United States'].drop('Country', axis=1)
    climate_by_state_usa["dt"] = pd.to_datetime(climate_by_state_usa["dt"])
    climate_by_state_usa['day'] = climate_by_state_usa["dt"].dt.day
    climate_by_state_usa['month'] = climate_by_state_usa["dt"].dt.month
    climate_by_state_usa['year'] = climate_by_state_usa["dt"].dt.year
    climate_by_state_usa.drop('dt', axis=1, inplace=True)
    climate_by_state_usa.fillna(-1).to_csv("climate_by_state1.csv", sep='\t', header=False)


def load_climate():
    climate_by_state = pd.read_csv("climate_by_state1.csv")
    print(climate_by_state)
    print(climate_by_state.columns)
    print(climate_by_state.isna().sum())
    hook = PostgresHook("climate")
    table_name = "climate3"
    # AverageTemperature\tAverageTemperatureUncertainty\tState\tday\tmonth\tyear
    request = f"CREATE TABLE if not exists {table_name} " \
              f"(unique_key VARCHAR(255) PRIMARY KEY, " \
              f"AverageTemperature NUMERIC," \
              f"AverageTemperatureUncertainty NUMERIC," \
              f"State VARCHAR(255)," \
              f"day INTEGER," \
              f"month INTEGER," \
              f"year INTEGER);"

    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    connection.commit()
    cursor.close()
    connection.close()

    hook.bulk_load(table_name, "climate_by_state1.csv")
    return table_name
