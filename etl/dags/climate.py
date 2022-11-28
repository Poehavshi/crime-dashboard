# simple dag - extract into local file system as staging area,
# transform with pandas or spark
# load into postgres


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from climate_functions import extract_climate, transform_climate, load_climate


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
    task_id='transform_climate',
    python_callable=transform_climate,
    provide_context=True,
    dag=dag_climate
)

t3 = PythonOperator(
    task_id='load_climate',
    python_callable=load_climate,
    provide_context=True,
    dag=dag_climate
)

t1 >> t2
t2 >> t3
