import pandas as pd
import datetime
from datetime import date
import os
from google.cloud import storage
import requests
import json

def gcp_upload(bucket_name, local_file_name, destination_blob_name):
    # Create a storage client using the service account
    storage_client = storage.Client()  # Initialize the client
    
    # Get the bucket object by passing the bucket name
    bucket = storage_client.bucket(bucket_name)
    
    # Create a blob object (this represents the file to be uploaded)
    blob = bucket.blob(destination_blob_name)
    
    # Upload the file to the specified bucket
    blob.upload_from_filename(local_file_name)
    
    print(f"File {local_file_name} uploaded to {destination_blob_name}.")

def fetch_data():
    # Get API_Key from .env file (if needed)
    # load_dotenv()
    # api_key = os.getenv('api_key')
    api_key = "cdaec870836545abafa97258fd77bf88"  # For testing purposes, define the API key directly here
    
    today = date.today()
    end_date = str(today)
    start_date = str(today - datetime.timedelta(days=1))
    query = 'apple'
    url = f"https://newsapi.org/v2/everything?q={query}&from={start_date}&to={end_date}&sortBy=popularity&apiKey={api_key}"
    
    # Make the request to the API
    response = requests.get(url)
    
    # Check if the response was successful
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}")
        return
    
    results = response.json()
    
    if 'articles' not in results:
        print("No articles found in the API response.")
        return

    articles_data = []
    
    for article in results['articles']:
        title = article['title']
        author = article.get('author', 'Unknown')  # Handle missing author
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
    print(f"File saved to: {current_wd}/{filename}")
    
    source_path = os.path.join(current_wd, filename)  # Correct file path
    destination_path = f"news_data_analysis/parquet_files/{filename}"  # Save the file with the same name in the bucket
    bucket_name = 'input_bucket_geonpeter24'
    
    # Call the upload function with the correct variable values
    gcp_upload(bucket_name, source_path, destination_path)
    
    # Remove the file after upload
    os.remove(filename)
    print(f"Local file {filename} removed after upload.")

# Run the fetch_data function
fetch_data()
