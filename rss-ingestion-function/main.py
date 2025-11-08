import functions_framework
from google.cloud import pubsub_v1
import feedparser
import json
import os
import hashlib
import logging

# Configure basic logging to view output in Cloud Logging
logging.basicConfig(level=logging.INFO)

@functions_framework.cloud_event
def fetch_and_publish_rss(cloud_event):
    """
    Triggered by a Pub/Sub message to fetch an RSS feed and publish each article
    to another Pub/Sub topic.
    """
    logging.info("Cloud Function 'fetch_and_publish_rss' invoked.")

    # --- 1. Configuration and Validation ---
    gcp_project_id = os.environ.get("GCP_PROJECT_ID")
    articles_topic = os.environ.get("ARTICLES_TOPIC")
    rss_feed_url = "https://www.investing.com/rss/news_25.rss"

    if not gcp_project_id or not articles_topic:
        logging.error("FATAL: Missing required environment variables: GCP_PROJECT_ID or ARTICLES_TOPIC.")
        return "Configuration Error", 500

    logging.info(f"Project ID: {gcp_project_id}, Articles Topic: {articles_topic}")

    try:
        # --- 2. Fetch RSS Feed ---
        logging.info(f"Fetching RSS feed from: {rss_feed_url}")
        feed = feedparser.parse(rss_feed_url)

        if not feed.entries:
            logging.warning("No new articles found in the RSS feed.")
            return "OK - No new articles", 200

        logging.info(f"Found {len(feed.entries)} articles in the RSS feed.")

        # --- 3. Publish to Pub/Sub ---
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(gcp_project_id, articles_topic)

        published_count = 0
        for entry in feed.entries:
            try:
                article_id = hashlib.md5(entry.link.encode()).hexdigest()

                # Safely get article content, falling back from 'summary' to 'description'
                full_text = entry.get('summary', entry.get('description', ''))

                message_data = {
                    "article_id": article_id,
                    "headline": entry.title,
                    "full_text": full_text,
                    "link": entry.link
                }

                message_bytes = json.dumps(message_data).encode("utf-8")
                future = publisher.publish(topic_path, message_bytes)
                future.result()  # Wait for the publish to complete
                published_count += 1
                logging.info(f"Successfully published article ID: {article_id}")

            except Exception as e:
                logging.error(f"Failed to process or publish article: {entry.get('title', 'N/A')}. Error: {e}", exc_info=True)

        logging.info(f"Function complete. Successfully published {published_count} of {len(feed.entries)} articles.")
        return "OK", 200

    except Exception as e:
        logging.error(f"An unexpected error occurred in the function: {e}", exc_info=True)
        return "Internal Server Error", 500
