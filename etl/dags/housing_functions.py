import requests
import logging
import pandas as pd
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from airflow.hooks.postgres_hook import PostgresHook
from config import HOUSING_TABLE_NAME
from utils import download

DATA_SOURCES = {
    "housing_data.csv": "https://files.zillowstatic.com/research/public_csvs/zhvi/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1664451572"
}


def extract_housing():
    for filename, url in DATA_SOURCES.items():
        download(filename, url)


def convert_housing_to_csv():
    pass


def transform_housing():
    pass


def load_housing():
    pass


def drop_old_housing_table():
    with PostgresHook("climate").get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {HOUSING_TABLE_NAME}")


def create_housing_table():
    pass

