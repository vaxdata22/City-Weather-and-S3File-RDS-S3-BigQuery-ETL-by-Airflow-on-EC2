
from airflow import DAG    # Imports the DAG (Directed Acyclic Graph) class, which is the foundation of an Airflow workflow. A DAG defines the sequence and dependencies of tasks.
from datetime import timedelta, datetime    # Used for defining time intervals (e.g., setting schedule intervals for DAG runs). Helps in handling timestamps, execution dates, and task scheduling.
from airflow.operators.dummy_operator import DummyOperator    # A placeholder operator that does nothing but can be used to define dependencies between tasks.
from airflow.utils.task_group import TaskGroup    # Helps in organizing tasks within a DAG by grouping them logically. Improves DAG readability by collapsing related tasks into a single visual block in the UI.
from airflow.providers.http.sensors.http import HttpSensor    # Monitors an HTTP endpoint (e.g., an API) and waits until it responds with a successful status before proceeding.
import json    # Used for handling JSON data (e.g., parsing API responses or preparing JSON payloads for requests).
from airflow.operators.python import PythonOperator    # Runs custom Python functions as part of the workflow.
import requests    # Used to make HTTP requests (e.g., calling an API to fetch weather data) along with PythonOperator in place of SimpleHttpOperator
import pandas as pd    # Used for data manipulation and transformation, such as converting API responses into DataFrames.
import io    # Used for in-memory file operations, such as handling CSV/JSON data as file-like objects.
from airflow.models import Variable    # For retrieving configuratio variables dynamically
from google.cloud import bigquery    # To copy data from AWS S3 to BigQuery without involving Google Cloud Storage (GCS), BigQueryHook, and AWS Connection.
from google.oauth2 import service_account    # To handle authentication to BigQuery without setting up Google Auth
import psycopg2    # Provides a connection to PostgreSQL databases, allowing Python scripts to interact with the database.
from psycopg2 import sql    # Used to execute SQL queries in a PostgreSQL database, such as creating tables or inserting data.
from psycopg2.extras import execute_values    # Used to execute bulk SQL Commands in a Postgres database


# Credentials retrieved securely
API_KEY = Variable.get("openweather_api_key")
RDS_HOST = Variable.get("rds_host")
RDS_PASSWORD = Variable.get("rds_password")


# Define useful variables
s3_bucket = "us-city-weather-data-etl-pipeline-data-lake"
s3_key_raw = "raw_data/us_cities.csv"
s3_key_processed = "processed_data/final_weather_data.csv"
ENDPOINT = "https://api.openweathermap.org"
SERVICE_ACCOUNT_JSON_FILE = "/home/ubuntu/us-city-weather-data-warehouse.json"
BIGQUERY_PROJECT_ID = "us-city-weather-data-warehouse"
BIGQUERY_TABLE = "us-city-weather-data-warehouse.weather_datawarehouse.final_weather_data"


# RDS Database connection parameters
conn_params = {
    "host": RDS_HOST,
    "database": "us_city_weather_db",
    "user": "postgres",
    "password": RDS_PASSWORD,
    "port": "5432"   # default Postgres port
}


# Function to convert Kelvin to Fahrenheit
def kelvin_to_fahrenheit(temp):
    return round((temp - 273.15) * (9/5) + 32, 3)


def fetch_weather_data():
    response = requests.get(f"{ENDPOINT}/data/2.5/weather?q=houston&appid={API_KEY}")
    return response.json()


# Transform Houston weather data to be loaded into RDS
def transform_load_weather_data_rds(task_instance):
    data = task_instance.xcom_pull(task_ids="fetch_weather_data")
    
    transformed_data = {
        "city": data["name"],
        "description": data["weather"][0]["description"],
        "temperature_fahrenheit": kelvin_to_fahrenheit(data["main"]["temp"]),
        "feels_like_fahrenheit": kelvin_to_fahrenheit(data["main"]["feels_like"]),
        "min_temperature_fahrenheit": kelvin_to_fahrenheit(data["main"]["temp_min"]),
        "max_temperature_fahrenheit": kelvin_to_fahrenheit(data["main"]["temp_max"]),
        "pressure": data["main"]["pressure"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "time_of_record": datetime.utcfromtimestamp(data["dt"] + data["timezone"]),
        "sunrise": datetime.utcfromtimestamp(data["sys"]["sunrise"] + data["timezone"]),
        "sunset": datetime.utcfromtimestamp(data["sys"]["sunset"] + data["timezone"])
    }

    df = pd.DataFrame([transformed_data])
        
    # Establish connection
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO houston_weather_data (
        city, 
        description, 
        temperature_fahrenheit, 
        feels_like_fahrenheit, 
        min_temperature_fahrenheit, 
        max_temperature_fahrenheit, 
        pressure, 
        humidity, 
        wind_speed, 
        time_of_record, 
        sunrise, 
        sunset
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    cursor.execute(insert_query, (
        transformed_data["city"], 
        transformed_data["description"],
        transformed_data["temperature_fahrenheit"], 
        transformed_data["feels_like_fahrenheit"],
        transformed_data["min_temperature_fahrenheit"], 
        transformed_data["max_temperature_fahrenheit"],
        transformed_data["pressure"], 
        transformed_data["humidity"],
        transformed_data["wind_speed"], 
        transformed_data["time_of_record"],
        transformed_data["sunrise"], 
        transformed_data["sunset"]
    ))

    conn.commit()
    cursor.close()
    conn.close()


# Load the S3 CSV file into RDS
def load_s3_to_rds():    
    # Read the CSV file from S3 into a Pandas DataFrame
    s3_file_path = f"s3://{s3_bucket}/{s3_key_raw}"

    # Ensure you have the necessary IAM role attached to your EC2 instance
    df = pd.read_csv(s3_file_path)

    # Connect to RDS database
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # Define insert query
        insert_query = sql.SQL("""
            INSERT INTO city_look_up (city, state, census_2020, land_area_sq_mile_2020)
            VALUES %s
        """)

        # Convert dataframe to list of tuples
        data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Execute batch insert for efficiency
        execute_values(cursor, insert_query, data_tuples)

        # Commit and close
        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully inserted into city_look_up table.")

    except Exception as e:
        print(f"Error: {e}")


# Function to join tables in the RDS Postgres DB
def join_tables():
    # Establish a connection to the RDS database
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    # SQL query to join tables
    join_query = """
        SELECT 
            w.city, 
            w.description, 
            w.temperature_fahrenheit, 
            w.feels_like_fahrenheit, 
            w.min_temperature_fahrenheit, 
            w.max_temperature_fahrenheit, 
            w.pressure, 
            w.humidity, 
            w.wind_speed, 
            w.time_of_record, 
            w.sunrise, 
            w.sunset, 
            c.state, 
            c.census_2020, 
            c.land_area_sq_mile_2020 
        FROM houston_weather_data w
        INNER JOIN city_look_up c
        ON w.city = c.city;
    """

    cursor.execute(join_query)
    data = cursor.fetchall()  # Fetch results as list of tuples

    cursor.close()
    conn.close()

    return data  # This must be pushed to XCom


# Save joined data as CSV and upload to S3
def save_joined_data_s3(task_instance):
    # Pull data from XCom
    data = task_instance.xcom_pull(task_ids="tsk_join_tables_in_rds")

    if not data:
        raise ValueError("No data received from XCom. Ensure tsk_join_tables returns data.")

    # Convert the list of tuples into a Pandas DataFrame
    df = pd.DataFrame(data, columns=[
        "city", 
        "description", 
        "temperature_fahrenheit", 
        "feels_like_fahrenheit", 
        "min_temperature_fahrenheit", 
        "max_temperature_fahrenheit", 
        "pressure", 
        "humidity", 
        "wind_speed", 
        "time_of_record", 
        "sunrise", 
        "sunset", 
        "state", 
        "census_2020", 
        "land_area_sq_mile_2020"
    ])

    # Define the S3 path
    s3_path = f"s3://{s3_bucket}/{s3_key_processed}"

    # Save DataFrame directly to S3 using s3fs
    df.to_csv(s3_path, index=False, storage_options={"anon": False})  

    print(f"Successfully uploaded {len(df)} rows to {s3_path}")


# Function to load S3 data to BigQuery
def load_s3_data_to_bigquery():
    # Load credentials from JSON file (without setting global auth)
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_JSON_FILE
    )

    # Define S3 bucket and file
    s3_path = f"s3://{s3_bucket}/{s3_key_processed}"

    # Read CSV from S3 using pandas and s3fs (ensure s3fs is installed)
    df = pd.read_csv(s3_path, storage_options={"anon": False})

    # Convert datetime columns to pandas datetime type
    df["time_of_record"] = pd.to_datetime(df["time_of_record"], errors="coerce")
    df["sunrise"] = pd.to_datetime(df["sunrise"], errors="coerce")
    df["sunset"] = pd.to_datetime(df["sunset"], errors="coerce")

    # Initialize BigQuery client with explicit credentials
    client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)

    # Define BigQuery table
    table_id = BIGQUERY_TABLE

    # Load DataFrame into BigQuery
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for job completion

    print(f"Successfully loaded {len(df)} rows into {table_id}")


# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 16),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=3)
}


# The DAG orchestration
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Begin the data pipeline
    start_pipeline = DummyOperator(task_id="start_pipeline")

    # Check if the API is available to communicate
    is_weather_api_ready = HttpSensor(
        task_id="tsk_check_weather_api",
        http_conn_id="weather_api_new_conn",
        endpoint=f"/data/2.5/weather?q=houston&appid={API_KEY}"
    )

    # Extract Houston Weather Data
    extract_houston_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
        dag=dag
    )

    # Load Houston weather table and city lookup tables in parallel
    with TaskGroup(group_id="group_parallel_load") as parallel_tasks:
        # From weather API to houston_weather_data table in RDS
        load_weather_rds = PythonOperator(
            task_id="tsk_load_weather_rds",
            python_callable=transform_load_weather_data_rds,
            provide_context=True  # Ensures task_instance is available
        )
        # From S3 bucket to city_look_up table in RDS
        load_s3_to_rds_task = PythonOperator(
            task_id="tsk_load_s3_to_rds",
            python_callable=load_s3_to_rds
        )

    # Join tables in RDS Postgres database
    join_tables = PythonOperator(
        task_id="tsk_join_tables_in_rds",
        python_callable=join_tables
    )

    # Save the joined data to S3 as CSV file
    save_to_s3 = PythonOperator(
        task_id="tsk_save_joined_data_s3",
        python_callable=save_joined_data_s3
    )

    # Load the new CSV file from S3 into BigQuery DW
    load_to_bigquery = PythonOperator(
        task_id="tsk_load_s3_to_bigquery",
        python_callable=load_s3_data_to_bigquery
    )

    # Terminate the data pipeline
    end_pipeline = DummyOperator(task_id="end_pipeline")

    # DAG Sequence
    start_pipeline >> is_weather_api_ready >> extract_houston_weather >> parallel_tasks >> join_tables >> save_to_s3 >> load_to_bigquery >> end_pipeline
