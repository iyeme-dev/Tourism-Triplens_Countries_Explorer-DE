import snowflake.connector
from pathlib import Path
import boto3
import os
from dotenv import load_dotenv


load_dotenv()


user = os.getenv('SNOW_USER')
password = os.getenv('SNOW_PASSWORD')
account = os.getenv('SNOW_ACCOUNT')


access_key = os.getenv('MINIO_ROOT_USER')
secret_key = os.getenv('MINIO_ROOT_PASSWORD')


snow_auth = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse='COMPUTE_WH',
    database='TRIPLENS',
    schema='RAW'
)

snow_cursor = snow_auth.cursor()


client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False
)


def transfer_minio_data_to_snowflake(bucket, file_key, target_table):

    current_dir = Path(__file__).resolve().parent

    filename = os.path.basename(file_key)

    local_temp_path = current_dir / filename

    client.download_file(
        bucket,
        file_key,
        local_temp_path
    )

    try:
        snow_cursor.execute("""
            CREATE OR REPLACE TABLE TRIPLENS.RAW.COUNTRIES_RAW(
                ingestion_time TIMESTAMP_NTZ,
                src_file STRING,
                payload VARIANT
            );
        """)

        snow_cursor.execute(f'PUT file://{local_temp_path} @TRIPLENS.RAW.TRIPLENS_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE')

        snow_cursor.execute(
            f"TRUNCATE TABLE {target_table};"
        )

        snow_cursor.execute(f"""
            COPY INTO {target_table} (
                ingestion_time,
                src_file,
                payload
            )
            FROM (
                SELECT
                    CURRENT_TIMESTAMP(),
                    METADATA$FILENAME,
                    $1
                FROM @TRIPLENS_STAGE
            )
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        print(
            f'Successfully loaded {file_key} '
            f'into {target_table}'
        )

    except Exception as e:
        print(e)

    finally:
        if os.path.exists(local_temp_path):
            os.remove(local_temp_path)