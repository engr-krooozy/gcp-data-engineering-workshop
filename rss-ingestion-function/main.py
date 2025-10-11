import functions_framework
from google.cloud import pubsub_v1
import feedparser
import json
import os
import hashlib
from datetime import datetime

# --- Environment Variables ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
ARTICLES_TOPIC = os.environ.get("ARTICLES_TOPIC")
RSS_FEED_URL = "https://www.investing.com/rss/news_25.rss"

@functions_framework.cloud_event
def fetch_and_publish_rss(cloud_event):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, ARTICLES_TOPIC)

    feed = feedparser.parse(RSS_FEED_URL)

    for entry in feed.entries:
        # Use a hash of the article link as a unique ID
        article_id = hashlib.md5(entry.link.encode()).hexdigest()

        message_data = {
            "article_id": article_id,
            "headline": entry.title,
            "full_text": entry.summary,
            "link": entry.link
        }

        future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
        future.result()

    return "OK"
