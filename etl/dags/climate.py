# simple dag - extract into local file system as staging area,
# transform with pandas or spark
# load into postgres


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from climate_functions import extract_climate, transform_climate, load_climate, drop_old_table, create_climate_table
from merging_functions import merge_crime_and_climate
from crime_functions import extract_crime, convert_crime_to_csv, load_crime, transform_crime


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


t11 = PythonOperator(
    task_id='download_file',
    python_callable=extract_climate,
    provide_context=True,
    dag=dag_climate
)

t21 = PythonOperator(
    task_id='transform_climate',
    python_callable=transform_climate,
    provide_context=True,
    dag=dag_climate
)

t31 = PythonOperator(
    task_id='drop_old',
    python_callable=drop_old_table,
    provide_context=True,
    dag=dag_climate
)


t41 = PythonOperator(
    task_id='create_new',
    python_callable=create_climate_table,
    provide_context=True,
    dag=dag_climate
)

t51 = PythonOperator(
    task_id='load_climate',
    python_callable=load_climate,
    provide_context=True,
    dag=dag_climate
)

t12 = PythonOperator(
    task_id="extract_crime",
    python_callable=extract_crime,
    provide_context=True,
    dag=dag_climate
)

t22 = PythonOperator(
    task_id="convert_to_csv",
    python_callable=convert_crime_to_csv,
    provide_context=True,
    dag=dag_climate
)

t32 = PythonOperator(
    task_id="transform_crime",
    python_callable=transform_crime,
    provide_context=True,
    dag=dag_climate
)

t42 = PythonOperator(
    task_id="load_crime",
    python_callable=load_crime,
    provide_context=True,
    dag=dag_climate
)

t6 = PythonOperator(
    task_id="merge",
    python_callable=merge_crime_and_climate,
    provide_context=True,
    dag=dag_climate
)

t11 >> t21
t21 >> t31
t31 >> t41
t41 >> t51
t51 >> t6

t12 >> t22
t22 >> t32
t32 >> t6
