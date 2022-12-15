import requests
import logging
import pandas as pd
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from airflow.hooks.postgres_hook import PostgresHook
from config import CRIME_TABLE_NAME
from utils import download

DATA_SOURCES = {
    "crime_data.xlsx": "https://query.data.world/s/ba5mxc3usfabmquytf3uuo7oruwkev.xlsx"
}


def extract_crime():
    for filename, url in DATA_SOURCES.items():
        download(filename, url)


def convert_crime_to_csv():
    data_excel_crime = pd.read_excel("crime_data.xlsx", sheet_name='ViolentCrime')
    data_excel_crime = data_excel_crime[['reportyear', 'geotype', 'geotypevalue', 'geoname', 'region_code',
                                         'region_name', 'strata_level_name_code', 'rate', 'dof_population']]
    data_excel_crime.to_csv("crime.csv")


def transform_crime():
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("crimeData") \
        .getOrCreate()
    crime_data = spark.read.csv("crime.csv", header=True)
    crime_data = crime_data.where(crime_data.strata_level_name_code == 5.0) \
        .where(crime_data.geotype == "CO") \
        .drop(col("strata_level_name_code"))\
        .withColumn("reportyear", col("reportyear").cast("Integer"))\
        .withColumn("geotypevalue", col("geotypevalue").cast("Integer")) \
        .withColumn("region_code", col("region_code").cast("Integer")) \
        .withColumn("dof_population", col("dof_population").cast("Integer"))
    crime_data.show()
    print(spark.sparkContext.getConf().getAll())

    crime_data.toPandas().to_csv("crime.csv", header=False, index=False, sep='\t')


def load_crime():
    hook = PostgresHook("climate")
    print(pd.read_csv("crime.csv"))
    hook.bulk_load(CRIME_TABLE_NAME, "crime.csv")
    return CRIME_TABLE_NAME


def drop_old_crime_table():
    with PostgresHook("climate").get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {CRIME_TABLE_NAME}")


def create_crime_table():
    with PostgresHook("climate").get_conn() as conn:
        request = f"CREATE TABLE {CRIME_TABLE_NAME} " \
                  f"(_c0 VARCHAR(255) PRIMARY KEY, " \
                  f"reportyear INTEGER," \
                  f"geotype VARCHAR(15)," \
                  f"geotypevalue INTEGER," \
                  f"geoname VARCHAR(255)," \
                  f"region_code INTEGER," \
                  f"region_name VARCHAR(255)," \
                  f"rate NUMERIC," \
                  f"dof_population INTEGER);"
        cursor = conn.cursor()
        cursor.execute(request)
        conn.commit()

