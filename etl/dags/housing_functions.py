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


def select_year(housing_df, year_number):
    year_columns = list(filter(lambda x: str(year_number) in x, housing_df.columns))
    rename_year_columns = {column_name: "month"+column_name[5:7] for column_name in year_columns}
    specific_year_df = housing_df[['RegionName']+year_columns]
    specific_year_df['reportyear'] = year_number
    specific_year_df = specific_year_df[['reportyear', 'RegionName'] + year_columns]
    specific_year_df = specific_year_df.rename(rename_year_columns, axis=1)
    specific_year_df = specific_year_df.dropna()
    return specific_year_df


def transform_housing():
    housing_data = pd.read_csv("housing_data.csv").drop(["SizeRank", "RegionType", "RegionID"], axis=1)
    housing_data = housing_data[housing_data["StateName"] == "CA"]
    housing_data['RegionName'] = housing_data.RegionName.str[:-4]
    housing_to_concat = []
    for year_number in range(2000, 2015):
        housing_to_concat.append(select_year(housing_data, year_number))
    housing_data = pd.concat(housing_to_concat).reset_index().drop('index', axis=1)
    print(housing_data)
    housing_data.to_csv("housing_data.csv", header=False, index=False, sep='\t')


def load_housing():
    hook = PostgresHook("climate")
    print(pd.read_csv("housing_data.csv"))
    hook.bulk_load(HOUSING_TABLE_NAME, "housing_data.csv")
    return HOUSING_TABLE_NAME


def drop_old_housing_table():
    with PostgresHook("climate").get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {HOUSING_TABLE_NAME}")


def create_housing_table():
    with PostgresHook("climate").get_conn() as conn:
        request = f"CREATE TABLE {HOUSING_TABLE_NAME} " \
                  f"(reportyear INTEGER," \
                  f"RegionName VARCHAR(255)," \
                  f"month1 NUMERIC," \
                  f"month2 NUMERIC," \
                  f"month3 NUMERIC," \
                  f"month4 NUMERIC," \
                  f"month5 NUMERIC," \
                  f"month6 NUMERIC," \
                  f"month7 NUMERIC," \
                  f"month8 NUMERIC," \
                  f"month9 NUMERIC," \
                  f"month10 NUMERIC," \
                  f"month11 NUMERIC," \
                  f"month12 NUMERIC);"
        cursor = conn.cursor()
        cursor.execute(request)
        conn.commit()


def health_check():
    with PostgresHook("climate").get_conn() as conn:
        request = f"SELECT * FROM {HOUSING_TABLE_NAME};"
        cursor = conn.cursor()
        print(cursor.execute(request).fetchall())
