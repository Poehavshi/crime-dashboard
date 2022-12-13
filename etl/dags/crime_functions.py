import requests
import logging
import pandas as pd
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
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


def convert_crime_to_csv():
    data_excel_crime = pd.read_excel("crime_data.xlsx", sheet_name='ViolentCrime')
    data_excel_crime = data_excel_crime.drop(['ind_definition', 'strata_name', 'version'], axis=1)
    data_excel_crime.to_csv("crime_data.csv")


def transform_crime():
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("crimeData") \
        .getOrCreate()
    crime_data = spark.read.csv("crime_data.csv", header=True)
    crime_data.select(col("reportyear").cast(IntegerType()))

    print(spark.sparkContext.getConf().getAll())


def load_crime():
    print("LOAD")
    # hook = PostgresHook("climate")
    # print(pd.read_csv("climate_by_state.csv"))
    # hook.bulk_load(CLIMATE_TABLE_NAME, "climate_by_state.csv")
    # return CLIMATE_TABLE_NAME
