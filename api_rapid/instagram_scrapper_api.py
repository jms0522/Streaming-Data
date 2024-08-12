import os
import requests
from dotenv import load_dotenv

# .env 파일 사용함.
load_dotenv()

api_key = os.getenv("RAPIDAPI_KEY")

url = "https://instagram-scraper-api2.p.rapidapi.com/v1/likes"
querystring = {"code_or_id_or_url":"CxYQJO8xuC6"}

headers = {
    "x-rapidapi-key": api_key,  
    "x-rapidapi-host": "instagram-scraper-api2.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())