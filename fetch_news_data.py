import pandas as pd
from datetime import datetime, timedelta, date
import requests
from dotenv import load_dotenv
import os

def fetch_data():
    # Load the .env file
    load_dotenv()
    api_key = os.getenv('API_KEY')  # Ensure the key name matches the one in your .env file
    
    if not api_key:
        print("API key is missing. Please check your .env file.")
        return

    query = 'apple'  # Example query term
    end_date = str(date.today())
    start_date = str(date.today() - timedelta(days=1))  # Fetch articles from yesterday to today

    url = f"https://newsapi.org/v2/everything?q={query}&from={start_date}&to={end_date}&sortBy=popularity&apiKey={api_key}"
    print(f"Fetching data from URL: {url}")

    # Make the request to the API
    response = requests.get(url)

    # Check for a successful request
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")
        return
    
    results = response.json()

    # Collect rows for the DataFrame in a list
    articles_data = []

    # Populate the data list if the results contain articles
    if 'articles' in results:
        for article in results['articles']:
            title = article['title']
            author = article.get('author', 'Unknown')  # Use 'Unknown' if author is None
            source = article['source'].get('name', 'Unknown')  # Use 'Unknown' if source name is None
            
             # Parse and format the published date
            published_date = article['publishedAt'][:10]
            
            
            # Append article data to the list
            articles_data.append({
                'Title': title,
                'Author': author,
                'Source': source,
                'Published_Date': published_date
            })

        # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(articles_data)

        # Display the resulting DataFrame
        print(df)
    else:
        print("No articles found.")
    
fetch_data()
