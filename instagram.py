import time
import pymongo
from pymongo import InsertOne
from instagrapi import Client
from instagrapi.exceptions import ClientError

# Define Instagram API credentials
username = ""
password = ""
# I used my personal credentials , therefore iI removed them before Submitting

api = Client(username, password)

# Define the topic you want to search for
topic = "the death of President Jacques Chirac"

# Set up MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["instagram"]
collection = db["posts"]
collection.create_index([("id", pymongo.ASCENDING)], unique=True)

# Set up rate limiting variables
MAX_REQUESTS = 100
REQUESTS_WINDOW = 60  # in seconds

def fetch_comments(post_id):
    REQUESTS_COUNT = 0
    while True:
        try:
            comments = api.media_comments(post_id)
            break
        except ClientError as e:
            print(f"Error fetching comments for post {post_id}: {e}")
            REQUESTS_COUNT += 1
            if REQUESTS_COUNT > MAX_REQUESTS:
                print(f"Reached rate limit, sleeping for {REQUESTS_WINDOW} seconds...")
                time.sleep(REQUESTS_WINDOW)
                REQUESTS_COUNT = 0
    comment_docs = []
    for comment in comments:
        comment_id = comment["id"]
        comment_text = comment["text"]
        comment_created_at = comment["created_at"]
        comment_user = comment.get("user", {}).get("username", "")
        comment_doc = {"id": comment_id, "text": comment_text, "created_at": comment_created_at, "user": comment_user}
        comment_docs.append(InsertOne(comment_doc))
    try:
        collection.bulk_write(comment_docs)
    except pymongo.errors.BulkWriteError as e:
        print(f"Error inserting comments for post {post_id}: {e}")
            
def fetch_posts(topic):
    REQUESTS_COUNT = 0
    while True:
        try:
            results = api.feed_tag(topic)
            break
        except ClientError as e:
            print(f"Error fetching posts for topic {topic}: {e}")
            REQUESTS_COUNT += 1
            if REQUESTS_COUNT > MAX_REQUESTS:
                print(f"Reached rate limit, sleeping for {REQUESTS_WINDOW} seconds...")
                time.sleep(REQUESTS_WINDOW)
                REQUESTS_COUNT = 0
    post_docs = []
    for item in results:
        post_id = item["id"]
        post_text = item.get("caption", {}).get("text", "")
        post_image_url = item.get("image_versions2", {}).get("candidates", [])[0].get("url", "")
        post = {"id": post_id, "text": post_text, "image_url": post_image_url, "comments": []}
        post_docs.append(InsertOne(post))
        fetch_comments(post_id)
        REQUESTS_COUNT += 1
        if REQUESTS_COUNT > MAX_REQUESTS:
            print(f"Reached rate limit, sleeping for {REQUESTS_WINDOW} seconds...")
            time.sleep(REQUESTS_WINDOW)
            REQUESTS_COUNT = 0
        time.sleep(1)
    try:
        collection.bulk_write(post_docs)
    except pymongo.errors.BulkWriteError as e:
        print(f"Error inserting posts for topic {topic}: {e}")

fetch_posts(topic)
