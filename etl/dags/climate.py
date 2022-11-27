# simple dag - extract into local file system as staging area,
# transform with pandas or spark
# load into postgres


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from extract_functions import extract_climate, show_climate_data


# default arguments
default_args = {
    'owner': 'arkadiy',
    'start_date': datetime.today(),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

# * dataset DAG
dag_climate = DAG('climate',
                  description='Fetch climate data',
                  catchup=False,
                  schedule_interval="@weekly",
                  default_args=default_args)


t1 = PythonOperator(
    task_id='download_file',
    python_callable=extract_climate,
    provide_context=True,
    dag=dag_climate
)

t2 = PythonOperator(
    task_id='show_climate',
    python_callable=show_climate_data,
    provide_context=True,
    dag=dag_climate
)

t1 >> t2
