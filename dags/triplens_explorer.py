import os
from datetime import datetime, timedelta
from airflow import DAG
from include.extract import extract_data
from include.load_to_bucket import load_to_bucket
from include.load_to_snowflake import transfer_minio_data_to_snowflake
from airflow.sdk import task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


default_args = {
    'owner': 'Triplens_Global',
    'start_time': datetime(2026, 8, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

DBT_EXECUTABLE_PATH = f'{os.environ["AIRFLOW_HOME"]}/dbt_venv/bin/dbt'

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

DBT_PROJECT_PATH = f'{os.environ["AIRFLOW_HOME"]}/dbt/triplens'

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    manifest_path=f'{DBT_PROJECT_PATH}/target/manifest.json'
)

profile_config = ProfileConfig(
    profile_name='default',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_conn',
        profile_args={'database': 'triplens', 'schema': 'raw'}
    )
)

@task
def extract_data_from_api():
    api_response = extract_data()

    return api_response

@task
def load_data_to_s3(api_response):
    load_to_bucket(api_response)

@task
def transfer_to_snowflake():
    transfer_minio_data_to_snowflake(
        bucket='triplens',
        file_key='raw/triplens_global.json',
        target_table='COUNTRIES_RAW'
    )

with DAG(
    dag_id='Triplens-Global',
    default_args=default_args,
    schedule='@hourly',
    tags=['triplens', 'explorer', 'tourism']
) as dag:

    transform_data = DbtTaskGroup(
    group_id='transform_data',
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    default_args=default_args
)

    api_response = extract_data_from_api()

    (
        api_response
        >> load_data_to_s3(api_response)
        >> transfer_to_snowflake()
        >> transform_data
    )