# test dag here

import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# default arguments
default_args = {
    'owner': 'dw',
    'start_date': datetime(2021, 5, 9),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

# * timetable DAG
dag_timetable = DAG('timetable',
                    description='Fetch timetable data hourly', catchup=False, schedule_interval="@hourly",
                    default_args=default_args)
