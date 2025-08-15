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
        postgres_hook.run(create_table_query)
    # step 2: Extract the NASA API data(APOD)-Astronomy Picture of the Day(Extract pipeline)
    # https://api.nasa.gov/planetary/apod?api_key=jM6YqwU5QLoXr5olAVFEKO8v4VvWm1ibmfsjgxYg
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id="nasa_api",# connection id defined in airflow for NASA API
        endpoint='planetary/apod', # NASA API endpoint for APOD
        methd='GET',
        data={'api_key':"{{conn.nasa_api.extra_dejson.api__key}}"},
        # data={'api_key':"jM6YqwU5QLoXr5olAVFEKO8v4VvWm1ibmfsjgxYg"},
        response_filter=lambda response:response.json(), ## Convert response to json
    )
    # step 3: Transform the data(Pick the information that i need to save)
    @task 
    def trasnform_apod_data(response):
        apod_data={
            'title':response.get('title',''),
            'explanation':response.get('explanation',''),
            'url':response.get('url',''),
            'data':response.get('date',''),
            'media_type':response.get('media_type','')
        }
        return apod_data
    # step 4: Load the data into the Postgres SQL
    @task 
    def load_data_to_postgres(apod_data):
        # Initialize the PostgresHook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")
        # Define  the SQL Insert Query
        insert_query="""
            INSERT INTO apod_data (title,explanation,url,date,media_type) VALUES(%s,%s,%s,%s,%s);
        """
        # Execute the SQL Query
        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))





    # step 5: Verify the data DBViewer


    # step 6: Define the task dependencies
    # Extract
    create_table()>>extract_apod # Ensure the table is created before extraction
    api_response=extract_apod.output
    # Transform
    trasnform_data=trasnform_apod_data(api_response)
    # Load
    load_data_to_postgres(trasnform_data)
    