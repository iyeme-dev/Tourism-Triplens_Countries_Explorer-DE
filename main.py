from extract import extract_data
from load_to_bucket import load_to_bucket
from load_to_snowflake import transfer_minio_data_to_snowflake
import time


api_response = extract_data()

load_to_bucket(api_response)

time.sleep(2)

transfer_minio_data_to_snowflake(
    'triplens',
    'raw/triplens_global.json',
    'COUNTRIES_RAW'
)