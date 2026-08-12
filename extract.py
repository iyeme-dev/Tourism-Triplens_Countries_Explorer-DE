import requests
import os
from dotenv import load_dotenv

load_dotenv()

attributes = (
    'names.common,names.official,names.native,capitals,region,'
    'subregion,continents,landlocked,borders,population,timezones,'
    'languages,currencies,calling_codes,car.driving_sides'
)

key = os.getenv('API_KEY')

response = requests.get(
    f'https://api.restcountries.com/countries/v5?response_fields={attributes}&limit=5',
    headers={'Authorization': f'{key}'}
)

response = response.json()

print(response['data']['objects'])