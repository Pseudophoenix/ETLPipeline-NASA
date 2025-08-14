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
    @task
    def create_table():
        ## initialize the PostgresHook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table 
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explaination TEXT,
            url TEXT,
            data DATE,
            media_type VARCHAR(50)
        )
        """



    # step 2: Extract the NASA API data(APOD)-Astronomy Picture of the Day(Extract pipeline)



    # step 3: Transform the data(Pick the information that i need to save)


    # step 4: Load the data into the Postgres SQL


    # step 5: Verify the data DBViewer


    # step 6: Define the task dependencies