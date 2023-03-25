import os
import requests
import pymongo
import json
import datetime
import logging
from multiprocessing import Pool
from dotenv import load_dotenv

load_dotenv()

# Facebook Graph API credentials
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
API_VERSION = os.getenv('API_VERSION')

# MongoDB credentials
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')

# Topic to search for
TOPIC = 'the death of President Jacques Chirac'

# Facebook Graph API endpoint
endpoint = f"https://graph.facebook.com/{API_VERSION}/"

# MongoDB client and collection
client = pymongo.MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLLECTION]

# Logging configuration
logging.basicConfig(filename='app.log', level=logging.INFO)

# Function to handle errors
def handle_error(response):
    """
    Handle errors returned by the Facebook Graph API.

    Args:
        response (dict): Response returned by the Facebook Graph API.

    Raises:
        Exception: If the response contains an error message.
    """
    if 'error' in response:
        error_message = response['error']['message']
        error_code = response['error']['code']
        raise Exception(f"Error {error_code}: {error_message}")

# Function to get posts and their comments for a given Facebook page
def get_posts(page_id, since_date, until_date):
    """
    Get all posts and their comments for a given Facebook page within a specific date range.

    Args:
        page_id (str): ID of the Facebook page to get posts for.
        since_date (datetime): Start date of the date range to get posts for.
        until_date (datetime): End date of the date range to get posts for.
    """
    try:
        posts = []
        url = f"{endpoint}{page_id}/posts"
        params = {
            'access_token': ACCESS_TOKEN,
            'fields': 'id,created_time,message,full_picture,comments{id,message,created_time}'
        }
        while True:
            response = requests.get(url, params=params)
            data = json.loads(response.text)
            if 'data' not in data:
                handle_error(data)
            posts += data['data']
            if 'paging' not in data or 'next' not in data['paging']:
                break
            url = data['paging']['next']
        for post in posts:
            created_time = datetime.datetime.strptime(post['created_time'], '%Y-%m-%dT%H:%M:%S%z')
            if since_date <= created_time <= until_date:
                post['topic'] = TOPIC
        if posts:
            result = collection.insert_many(posts)
            logging.info(f"Inserted {len(result.inserted_ids)} posts for page {page_id}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"RequestException: {e}")
    except Exception as e:
        logging.error(f"Exception: {e}")

# Search for Facebook pages related to the topic
url = f"{endpoint}search"
params = {
    'access_token': ACCESS_TOKEN,
    'q': TOPIC,
    'type': 'page',
    'fields': 'id,name'
}
response = requests.get(url, params=params)
data = json.loads(response.text)
if 'data' not in data:
    handle_error(data)
pages = data['data']

# Get posts and comments for each page
if __name__ == '__main__':
    pool = Pool(processes=4)
    for page in pages:
        page_id = page['id']
        since_date = datetime.datetime(2019, 1, 1, tzinfo=datetime.timezone.utc)
        until_date = datetime.datetime(2022, 12, 31, tzinfo=datetime.timezone.utc)
        pool.apply_async(get_posts, args=(page_id, since_date, until_date))
    pool.close()
    pool.join()

print('Posts and comments collected and stored in MongoDB.')
