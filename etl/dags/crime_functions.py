import requests
import logging
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

DATA_SOURCES = {
    "crime_data.xlsx": "https://query.data.world/s/ba5mxc3usfabmquytf3uuo7oruwkev.xlsx"
}


def extract_crime():
    for filename, url in DATA_SOURCES.items():
        try:
            response = requests.get(url)
            with open(filename, "wb") as file:
                file.write(response.content)
                logging.info(f"Create {filename} file")
        except requests.exceptions.RequestException as e:
            logging.critical(f"ERROR when download climate data: {e}")


def transform_crime():
    crime_data = pd.read_excel("crime_data.xlsx")
    print(crime_data)


def load_crime():
    print("LOAD")
    # hook = PostgresHook("climate")
    # print(pd.read_csv("climate_by_state.csv"))
    # hook.bulk_load(CLIMATE_TABLE_NAME, "climate_by_state.csv")
    # return CLIMATE_TABLE_NAME
