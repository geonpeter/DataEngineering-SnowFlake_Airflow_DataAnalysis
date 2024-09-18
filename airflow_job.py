from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime,timedelta
import datetime
import os
from fetch_news_data import fetch_data

default_args = {
    'owner' : 'geonpeter',
    'start_date' : datetime(2024,9,1),
    'retries':0
}

dag = DAG(
    'dag_snowflake',
    default_args= default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

api_gcp = PythonOperator(task_id='api_gcp',python_callable=fetch_data,dag=dag)

snowflake_create_table = SnowflakeOperator(task_id='creation_snowflake_table',sql="""CREATE TABLE IF NOT EXISTS news_api_data
                                           USING TEMPLATE(
                                            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                                                FROM TABLE(
                                                    (INFER_SCHEMA(
                                                        LOCATION=> '@news_api.PUBLIC.gcs_raw_data_stage', FILE_FORMAT => 'parquet_format'))
                                           )
                                           )""",
                                           snowflake_conn_id='airflow_snowflake')


snowflake_copy_from_stage = SnowflakeOperator(task_id='copy_from_stage',sql=""" COPY INTO news_api.PUBLIC.news_api_data
                                              FROM @@news_api.PUBLIC.gcs_raw_data_stage
                                              MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                                              FILE_FORMAT=(FORMAT_NAME='parquet_format')
                                              ON_ERROR='SKIP_FILE' """,
                                              snowflake_conn_id='airflow_snowflake')


fetch_data >> snowflake_create_table >> snowflake_copy_from_stage