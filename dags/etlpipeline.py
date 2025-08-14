from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator, SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.hooks.postgres.hooks.postgres import PostgresHook
import json
from airflow.utils.dates import days_ago

with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule_intervla='@daily',
    catchup=False
)as dag:
    # step 1 : Create the table if it does not exist

    # step 2: Extract the NASA API data(APOD)