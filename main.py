from extract import extract_data
from load_to_bucket import load_to_bucket


api_response = extract_data()

load_to_bucket(api_response)