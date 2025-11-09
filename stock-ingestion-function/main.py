import functions_framework
from google.cloud import pubsub_v1
import yfinance as yf
import json
import os
import logging
from datetime import datetime
import pandas as pd

# Configure basic logging
logging.basicConfig(level=logging.INFO)

# Tickers for major tech companies
TICKERS = ["GOOGL", "AAPL", "MSFT", "AMZN", "NVDA", "META"]

@functions_framework.cloud_event
def fetch_and_publish_stock_data(cloud_event):
    """
    Fetches real-time stock data from Yahoo Finance and publishes it to Pub/Sub.
    """
    logging.info("Function 'fetch_and_publish_stock_data' invoked.")

    gcp_project_id = os.environ.get("GCP_PROJECT_ID")
    data_topic = os.environ.get("DATA_TOPIC")

    if not gcp_project_id or not data_topic:
        logging.error("FATAL: Missing GCP_PROJECT_ID or DATA_TOPIC env variables.")
        return "Configuration Error", 500

    logging.info(f"Project ID: {gcp_project_id}, Target Topic: {data_topic}")

    try:
        logging.info(f"Fetching latest market data for: {', '.join(TICKERS)}")
        data = yf.download(tickers=TICKERS, period="1m", interval="1m")

        if data.empty:
            logging.warning("No data returned from yfinance.")
            return "OK - No data", 200

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(gcp_project_id, data_topic)

        published_count = 0
        for ticker in TICKERS:
            # yfinance returns a multi-level column index; we need to handle it carefully
            if ('Close', ticker) in data.columns and not data['Close'][ticker].empty:
                price = data['Close'][ticker].iloc[-1]
                volume = data['Volume'][ticker].iloc[-1]

                message_data = {
                    "ticker": ticker,
                    "price": price,
                    "volume": int(volume),
                    "timestamp": datetime.now().isoformat()
                }

                message_bytes = json.dumps(message_data).encode("utf-8")
                future = publisher.publish(topic_path, message_bytes)
                future.result()
                published_count += 1

        logging.info(f"Published {published_count} stock data messages.")
        return "OK", 200

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        return "Internal Server Error", 500
