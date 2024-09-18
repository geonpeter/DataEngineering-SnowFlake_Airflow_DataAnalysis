from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator  # New Operator
from datetime import datetime, timedelta
import os
from fetch_news_data import fetch_data

# Define default arguments
default_args = {
    'owner': 'geonpeter',
    'start_date': datetime(2024, 9, 1),  # Changed from date to datetime
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

# Snowflake task to create table if it does not exist
snowflake_create_table = SQLExecuteQueryOperator(
    task_id='creation_snowflake_table',
    sql="""
    CREATE TABLE IF NOT EXISTS news_api_data AS
    SELECT * FROM TABLE(
      INFER_SCHEMA(LOCATION => '@news_api.PUBLIC.gcs_raw_data_stage', FILE_FORMAT => 'parquet_format')
    )
    """,
    conn_id='airflow_snowflake',  # Ensure this connection is properly configured
    hook_params={
        'warehouse': 'COMPUTE_WH',
        'database': 'news_api',
        'role': 'ACCOUNTADMIN',
    },
    dag=dag
)

# Snowflake task to copy data from the GCS stage to the Snowflake table
snowflake_copy_from_stage = SQLExecuteQueryOperator(
    task_id='copy_from_stage',
    sql="""
    COPY INTO news_api.PUBLIC.news_api_data
    FROM @news_api.PUBLIC.gcs_raw_data_stage
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    FILE_FORMAT=(FORMAT_NAME='parquet_format')
    ON_ERROR='SKIP_FILE'
    """,
    conn_id='airflow_snowflake',
    hook_params={
        'warehouse': 'COMPUTE_WH',
        'database': 'news_api',
        'role': 'ACCOUNTADMIN',
    },
    dag=dag
)

# Define task dependencies
api_gcp >> snowflake_create_table >> snowflake_copy_from_stage
