# Airflow DAG with Snowflake and GCP Integration

```sql

# Airflow DAG with Snowflake and GCP Integration

This project demonstrates the creation of an Airflow DAG that:
1. Fetches news articles from a public API (e.g., NewsAPI).
2. Saves the fetched data as a Parquet file in a Google Cloud Storage (GCS) bucket.
3. Loads the data into Snowflake for further processing.

## Project Structure

```bash
.
├── dags/
│   ├── dag_snowflake.py   # Main DAG definition
├── fetch_news_data.py     # Script for fetching news data and uploading to GCP
├── README.md              # This file
└── requirements.txt       # Airflow and dependencies
```
## Requirements
Google Cloud Storage: This is used to store Parquet files with the news data fetched from an API.
Snowflake: Used to store the fetched data after it is copied from GCS.
Airflow: Orchestrates the entire workflow.

## Prerequisites
- Google Cloud Storage:
Create a GCP bucket for storing Parquet files.
Set up authentication using a service account.

- Snowflake:
Create a database and table (if not already created).
Ensure the necessary stages (GCS stages) and file formats (Parquet format) are created in Snowflake.

- Airflow:
Install and configure Airflow with the necessary connections to GCP and Snowflake.
Python Dependencies: Add the required dependencies in requirements.txt:

```sql
apache-airflow
apache-airflow-providers-snowflake
apache-airflow-providers-google
pandas
pyarrow
snowflake-connector-python
google-cloud-storage
requests
```

- Snowflake Connection:
In Airflow, configure a Snowflake connection named airflow_snowflake with the necessary details (account, warehouse, database, etc.).

## Workflow Steps
- fetch_news_data.py: This script fetches data from the NewsAPI and saves it as a Parquet file in a GCS bucket.
```sql
import pandas as pd
import datetime
from datetime import date
import os
from google.cloud import storage
import requests

def gcp_upload(bucket_name, local_file_name, destination_blob_name):
    # Create a storage client using the service account
    storage_client = storage.Client()
    
    # Get the bucket object by passing the bucket name
    bucket = storage_client.get_bucket(bucket_name)
    
    # Create a blob object (this represents the file to be uploaded)
    blob = bucket.blob(destination_blob_name)
    
    # Upload the file to the specified bucket
    blob.upload_from_filename(local_file_name)
    
    print(f"File {local_file_name} uploaded to {destination_blob_name}.")

def fetch_data():
    api_key = "your_news_api_key"
    
    today = date.today()
    end_date = str(today - datetime.timedelta(days=1))
    start_date = str(today)
    query = 'apple'
    
    url = f"https://newsapi.org/v2/everything?q={query}&from={start_date}&to={end_date}&sortBy=popularity&apiKey={api_key}"
    
    # Make the request to the API
    response = requests.get(url)
    results = response.json()

    articles_data = []
    df = pd.DataFrame(columns=['Title', 'Author', 'Source', 'Published_Date'])
    
    for article in results['articles']:
        title = article['title']
        author = article['author']
        source = article['source']['name']
        published_date = article['publishedAt']

        articles_data.append({
            'Title': title,
            'Author': author,
            'Source': source,
            'Published_Date': published_date
        })

    # Create DataFrame from articles data
    df = pd.DataFrame(articles_data)

    # Filename formatting
    current_date = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"run_{current_date}.parquet"

    # Save the DataFrame as a parquet file
    df.to_parquet(filename)
    
    # Upload to GCP
    gcp_upload('your_bucket_name', filename, f"news_data_analysis/parquet_files/{filename}")
    
    # Clean up the local file
    os.remove(filename)
```

- Airflow DAG (dag_snowflake.py): The main Airflow DAG that orchestrates fetching data and loading into Snowflake.

 ```sql
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from fetch_news_data import fetch_data

# Define default arguments
default_args = {
    'owner': 'geonpeter',
    'start_date': datetime(2024, 9, 1),
    'retries': 0
}

# Define the DAG
dag = DAG(
    'dag_snowflake',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define the Python task to fetch data from the API and upload to GCP
api_gcp = PythonOperator(
    task_id='api_gcp',
    python_callable=fetch_data,
    dag=dag
)

# Snowflake task to create the table if it doesn't exist
snowflake_create_table = SnowflakeOperator(
    task_id='creation_snowflake_table',
    sql="""
    CREATE TABLE IF NOT EXISTS news_api_data USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            (INFER_SCHEMA(
                LOCATION => '@news_api.PUBLIC.gcs_raw_data_stage', 
                FILE_FORMAT => 'parquet_format'
            ))
        )
    )
    """,
    snowflake_conn_id='airflow_snowflake',
    warehouse='COMPUTE_WH',
    database='news_api',
    role='ACCOUNTADMIN',
    dag=dag
)

# Snowflake task to copy data from the GCS stage to the Snowflake table
snowflake_copy_from_stage = SnowflakeOperator(
    task_id='copy_from_stage',
    sql="""
    COPY INTO news_api.PUBLIC.news_api_data
    FROM @news_api.PUBLIC.gcs_raw_data_stage
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    FILE_FORMAT=(FORMAT_NAME='parquet_format')
    ON_ERROR='SKIP_FILE'
    """,
    snowflake_conn_id='airflow_snowflake',
    warehouse='COMPUTE_WH',
    database='news_api',
    role='ACCOUNTADMIN',
    dag=dag
)

# Define task dependencies
api_gcp >> snowflake_create_table >> snowflake_copy_from_stage
```
## Configuration
- Google Cloud Storage:

Ensure that the GCS bucket is correctly configured with a valid service account.
- Snowflake:

Configure the Snowflake connection (airflow_snowflake) in Airflow.
Ensure the GCS stage is correctly set up in Snowflake with access to the Parquet files.
- Airflow:

Add necessary connection configurations to point to Snowflake and GCP.

![Dag-Workflow]()
