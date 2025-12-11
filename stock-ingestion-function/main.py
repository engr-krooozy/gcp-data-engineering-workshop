import functions_framework
import logging
import os

# NOTE: Do not add any top-level logic or heavy imports here.
# The container must start and listen on port 8080 immediately.
# All initialization must happen inside the function handler.

@functions_framework.cloud_event
def fetch_and_publish_stock_data(cloud_event):
    """
    Cloud Function entry point.
    Fetches stock data and publishes it to Pub/Sub with AI insights.
    """
    # 1. Setup Logging (safe to do here)
    logging.basicConfig(level=logging.INFO)
    logging.info("Function 'fetch_and_publish_stock_data' started.")

    try:
        # 2. Lazy Import Heavy Dependencies
        # This prevents cold-start crashes if imports fail or take too long.
        import json
        import time
        from datetime import datetime
        import yfinance as yf
        import pandas as pd
        from google.cloud import pubsub_v1
        import google.generativeai as genai

        # 3. Configuration
        PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
        TOPIC_ID = os.environ.get("DATA_TOPIC")
        API_KEY = os.environ.get("GOOGLE_API_KEY")

        if not PROJECT_ID or not TOPIC_ID:
            logging.error("Missing required environment variables: GCP_PROJECT_ID or DATA_TOPIC")
            return

        # 4. Configure yfinance cache
        try:
            yf.set_tz_cache_location("/tmp/yf_cache")
        except Exception as e:
            logging.warning(f"Could not set yfinance cache: {e}")

        # 5. Fetch Data
        TICKERS = ["GOOGL", "AAPL", "MSFT", "AMZN", "NVDA", "META", "TSLA"]
        logging.info(f"Fetching data for: {TICKERS}")

        # Fetch 5 days to ensure we get the last trading day
        df = yf.download(TICKERS, period="5d", interval="1m", progress=False)
        
        if df.empty:
            logging.warning("No data received from yfinance.")
            return

        # Get the last available date
        unique_dates = sorted(list(set(df.index.date)))
        if not unique_dates:
            logging.warning("No dates found in data.")
            return
            
        target_date = unique_dates[-1]
        target_date_str = str(target_date)
        day_data = df.loc[target_date_str]
        
        if day_data.empty:
            logging.warning(f"No data for date {target_date_str}")
            return

        # Replay logic: pick a minute based on current time
        total_minutes = len(day_data)
        current_minute_index = (int(time.time()) // 60) % total_minutes
        
        current_row = day_data.iloc[current_minute_index]
        logging.info(f"Replaying minute {current_minute_index}/{total_minutes} from {target_date_str}")

        # 6. Prepare Data for Batch Processing
        batch_data = {}
        is_multi_index = isinstance(df.columns, pd.MultiIndex)

        for ticker in TICKERS:
            try:
                if is_multi_index:
                    price = current_row.get(('Close', ticker))
                    volume = current_row.get(('Volume', ticker))
                else:
                    price = current_row['Close']
                    volume = current_row['Volume']

                if pd.isna(price) or pd.isna(volume):
                    continue
                
                batch_data[ticker] = {
                    "price": float(price),
                    "volume": int(volume)
                }
            except Exception as e:
                logging.warning(f"Error extracting data for {ticker}: {e}")

        if not batch_data:
            logging.warning("No valid data found for any ticker.")
            return

        # 7. Generate Batch AI Insights
        ai_results = {}
        if API_KEY:
            try:
                genai.configure(api_key=API_KEY)
                model = genai.GenerativeModel("models/gemini-3-pro-preview")
                
                # Construct a single prompt for all tickers with technical analysis focus
                prompt_lines = [
                    "Role: Act as a Senior Technical Analyst for the technology sector.",
                    "Input: I will provide the current price and volume data for the magnificent 7 tech companies (Tech Giants).",
                    "Task: Analyze the price action, volatility, and recent momentum for each ticker.",
                    "",
                    "Output Requirements:",
                    "1. Technical Insight: Identify the current chart pattern or trend (e.g., 'breaking out,' 'consolidating,' or 'hitting resistance'). Do not simply state the price change. Mention key support/resistance levels if visible.",
                    "2. Trend Score: Assign a 'Technical Strength Score' from -1 (Strong Bearish Trend) to 1 (Strong Bullish Trend).",
                    "",
                    "Return a JSON object where keys are tickers and values have 'summary' (technical insight) and 'sentiment' (trend score).",
                    "",
                    "Data:"
                ]
                for t, d in batch_data.items():
                    prompt_lines.append(f"- {t}: Price ${d['price']:.2f}, Volume {d['volume']:,}")
                
                full_prompt = "\n".join(prompt_lines)
                
                response = model.generate_content(
                    full_prompt,
                    generation_config=genai.GenerationConfig(
                        temperature=0.2,
                        response_mime_type="application/json",
                        max_output_tokens=8192
                    ),
                    safety_settings=[
                        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                    ]
                )
                ai_results = json.loads(response.text)
                logging.info("Batch AI insights generated successfully.")
            except Exception as ai_err:
                logging.error(f"Batch AI generation failed: {ai_err}")
        else:
            logging.warning("GOOGLE_API_KEY not set. Skipping AI generation.")

        # 8. Publish to Pub/Sub
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

        for ticker, data in batch_data.items():
            try:
                # Get AI result for this ticker, or default
                ticker_ai = ai_results.get(ticker, {})
                sentiment = ticker_ai.get("sentiment", 0.0)
                summary = ticker_ai.get("summary", "No AI insight.")

                message_json = {
                    "ticker": ticker,
                    "price": data['price'],
                    "volume": data['volume'],
                    "timestamp": datetime.now().isoformat(),
                    "ai_sentiment": sentiment,
                    "ai_summary": summary
                }
                
                data_str = json.dumps(message_json)
                future = publisher.publish(topic_path, data_str.encode("utf-8"))
                future.result()
                
            except Exception as pub_err:
                logging.error(f"Error publishing {ticker}: {pub_err}")

        logging.info(f"Published {len(batch_data)} messages.")

    except Exception as e:
        logging.error(f"Fatal error in execution: {e}")
