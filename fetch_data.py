<<<<<<< HEAD
import pandas as pd
import datetime
from datetime import date
import os
from google.cloud import storage
import requests
from dotenv import load_dotenv
import json

def gcp_upload(bucket_name, source_path, destination_path):
    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(source_path)
    print(f"File '{source_path}' successfully uploaded to '{destination_path}' in bucket '{bucket_name}'.")

def fetch_data():
    # Get API_Key from .env file
    load_dotenv()
    api_key = os.getenv('api_key')
    
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
    
    # Print the current working directory and filename
    current_wd = os.getcwd()
    print(current_wd)
    
    source_path = os.path.join(current_wd, filename)  # Correct file path
    destination_path = f"news_data_analysis/parquet_files/{filename}"  # Save the file with the same name in the bucket
    bucket_name = 'input_bucket_geonpeter24'
    
    # Call the upload function with the correct variable values
    gcp_upload(bucket_name, source_path, destination_path)

fetch_data()
=======
import pandas as pd
import datetime
from datetime import date
import os
from google.cloud import storage
import requests
from dotenv import load_dotenv
import json

def gcp_upload(bucket_name, source_path, destination_path):
    storage_client = storage.Client.from_service_account_json('service_account.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_path)
    blob.upload_from_filename(source_path)
    print(f"File '{source_path}' successfully uploaded to '{destination_path}' in bucket '{bucket_name}'.")

def fetch_data():
    # Get API_Key from .env file
    load_dotenv()
    api_key = os.getenv('api_key')
    
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
    
    # Print the current working directory and filename
    current_wd = os.getcwd()
    print(current_wd)
    
    source_path = os.path.join(current_wd, filename)  # Correct file path
    destination_path = f"news_data_analysis/parquet_files/{filename}"  # Save the file with the same name in the bucket
    bucket_name = 'input_bucket_geonpeter24'
    
    # Call the upload function with the correct variable values
    gcp_upload(bucket_name, source_path, destination_path)

fetch_data()
>>>>>>> f1b97ffe96cb8c37ad2622968bfbd4433e0f064f
