import functions_framework
from google.cloud import pubsub_v1
import yfinance as yf
import json
import os
import logging
from datetime import datetime
import pandas as pd
import time

# Configure basic logging
logging.basicConfig(level=logging.INFO)

# Tickers for major tech companies
TICKERS = ["GOOGL", "AAPL", "MSFT", "AMZN", "NVDA", "META"]

@functions_framework.cloud_event
def fetch_and_publish_stock_data(cloud_event):
    """
    Fetches historical stock data from Yahoo Finance and replays it as if it were live.
    It selects the most recent full trading day and loops through it minute-by-minute
    based on the current time.
    """
    logging.info("Function 'fetch_and_publish_stock_data' invoked.")

    gcp_project_id = os.environ.get("GCP_PROJECT_ID")
    data_topic = os.environ.get("DATA_TOPIC")

    if not gcp_project_id or not data_topic:
        logging.error("FATAL: Missing GCP_PROJECT_ID or DATA_TOPIC env variables.")
        return "Configuration Error", 500

    logging.info(f"Project ID: {gcp_project_id}, Target Topic: {data_topic}")

    try:
        logging.info(f"Fetching historical market data for: {', '.join(TICKERS)}")
        # Fetch 5 days of data to ensure we capture at least one full trading day
        data = yf.download(tickers=TICKERS, period="5d", interval="1m")

        if data.empty:
            logging.warning("No data returned from yfinance for any tickers.")
            return "OK - No data", 200

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(gcp_project_id, data_topic)

        published_count = 0

        # Determine the most recent full trading day
        # We look at the index (DatetimeIndex) to find unique dates
        unique_dates = sorted(list(set(data.index.date)))
        
        if not unique_dates:
             logging.warning("No dates found in data.")
             return "OK - No data", 200
             
        # Pick the last available date (likely the most recent full trading day)
        target_date = unique_dates[-1]
        logging.info(f"Replaying data from: {target_date}")

        # Filter data for that specific date
        # Note: yfinance returns a DatetimeIndex which is timezone-aware.
        # We need to handle the slicing carefully.
        # Converting to string for robust slicing usually works well with pandas.
        target_date_str = str(target_date)
        day_data = data.loc[target_date_str]

        if day_data.empty:
             logging.warning(f"No data found for target date {target_date_str}")
             return "OK - No data", 200

        # Calculate the replay index based on the current time
        # We want to loop through the day's minutes.
        # Total minutes in a trading day (9:30 - 16:00) is 390.
        # But yfinance might return pre/post market data too, or fewer rows.
        # We'll just take the total number of rows available for that day and modulo.
        total_rows = len(day_data)
        current_timestamp = int(time.time())
        # Change the index every minute (60 seconds)
        replay_index = (current_timestamp // 60) % total_rows
        
        logging.info(f"Replay Index: {replay_index} / {total_rows}")

        # Extract the specific row for this minute
        current_row = day_data.iloc[replay_index]

        # Handle MultiIndex vs Single Index
        if isinstance(data.columns, pd.MultiIndex):
            for ticker in TICKERS:
                try:
                    # Access the data for this ticker from the current row
                    # The row is a Series with a MultiIndex if we selected a single row from a MultiIndex DataFrame
                    # Structure: (Price, Ticker)
                    
                    # We need to extract Close and Volume for this specific ticker
                    # current_row is a Series. Index is MultiIndex (Price, Ticker) or (Ticker, Price) depending on yfinance version?
                    # Actually, day_data is a DataFrame. current_row is a Series corresponding to one timestamp.
                    # The index of current_row is the columns of the DataFrame.
                    
                    price = current_row.get(('Close', ticker))
                    volume = current_row.get(('Volume', ticker))

                    if pd.isna(price) or pd.isna(volume):
                        logging.debug(f"NaN data for {ticker} at index {replay_index}")
                        continue

                    message_data = { "ticker": ticker, "price": float(price), "volume": int(volume), "timestamp": datetime.now().isoformat() }
                    message_bytes = json.dumps(message_data).encode("utf-8")
                    future = publisher.publish(topic_path, message_bytes)
                    future.result()
                    published_count += 1
                except Exception as e:
                    logging.warning(f"Could not process data for ticker '{ticker}'. Error: {e}")
                    continue
        else: # Handle the single-ticker case
            if not day_data.empty:
                # For single ticker, columns are just ['Open', 'High', 'Low', 'Close', 'Volume', ...]
                price = current_row['Close']
                volume = current_row['Volume']
                ticker = TICKERS[0]
                
                if not pd.isna(price) and not pd.isna(volume):
                    message_data = { "ticker": ticker, "price": float(price), "volume": int(volume), "timestamp": datetime.now().isoformat() }
                    message_bytes = json.dumps(message_data).encode("utf-8")
                    future = publisher.publish(topic_path, message_bytes)
                    future.result()
                    published_count += 1

        logging.info(f"Successfully published {published_count} stock data messages (Replay Mode).")
        return "OK", 200

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        return "Internal Server Error", 500
