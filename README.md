# Building an AI-Powered Real-Time Stock Analysis Pipeline on Google Cloud

## Welcome to the Workshop!

Welcome! In this hands-on workshop, you'll build a high-throughput, real-time data engineering pipeline on Google Cloud. This project is designed for a **fintech** use case, where live stock market data is ingested, processed using advanced analytics, and stored in a data warehouse for real-time dashboarding.

This workshop is designed to be completed in approximately **1 hour**.

---

### What You'll Learn

*   How to set up a Google Cloud environment using Cloud Shell.
*   How to build a modern, event-driven data pipeline from scratch.
*   How to use **Cloud Scheduler** to trigger a pipeline on a recurring schedule.
*   How to write a Python **Cloud Function** to ingest live financial data using an external API.
*   How to use **Pub/Sub** as a scalable, durable message bus.
*   How to build and deploy a streaming **Dataflow** pipeline that uses advanced features like **windowing** and **stateful processing**.
*   How to calculate real-time **technical indicators** (e.g., Simple Moving Average).
*   How to perform **anomaly detection** on a live data stream.
*   How to store and query the final structured results in **BigQuery**.

### Our Fintech Architecture

We will build a real-time stock analysis pipeline using a scheduled, event-driven architecture.

![Architecture Diagram](https://storage.googleapis.com/gweb-cloudblog-publish/images/Event-driven_data_processing_rev2.max-2600x2600.png)

The workflow is:
1.  **Schedule:** A **Cloud Scheduler** job runs every minute, sending a trigger message to a Pub/Sub topic.
2.  **Ingest:** A **Cloud Function**, subscribed to the trigger topic, activates. It fetches the latest stock prices for major tech companies from the Yahoo Finance API.
3.  **Publish:** The function publishes the data for each stock as a distinct message to a second Pub/Sub topic.
4.  **Process & Analyze:** A streaming **Dataflow** pipeline, subscribed to the data topic, performs a series of real-time analyses:
    *   **1-Minute Aggregations:** It calculates the highest price, **total traded value**, and total trading volume within 1-minute fixed windows.
    *   **5-Minute Moving Average:** It calculates the 5-minute Simple Moving Average (SMA) of the price using a 5-minute sliding window.
    *   **Volume Spike Detection:** It uses stateful processing to detect anomalous spikes in trading volume by comparing the current minute's volume to the 10-minute historical average.
    *   **System Latency:** It tracks the end-to-end latency of the pipeline to measure performance.
5.  **Store:** The final, enriched data—including the latest price, technical indicators, and anomaly flags—is streamed into a **BigQuery** table for analysis.

---

## Section 1: Preparing Your Google Cloud Environment (Approx. 15 mins)

First, let's get your Google Cloud project and Cloud Shell ready.

> **Prerequisite:** You need a Google Cloud account with billing enabled.

### 1.1. Activate Cloud Shell & Open Editor

*   **Action:** In the Google Cloud Console, click the **Activate Cloud Shell** button (`>_`).
*   **Action:** In the Cloud Shell terminal, click the **Open Editor** button.

### 1.2. Configure Your Project and Region

```bash
# 1. Set your Project ID
#Replace `your-project-id` with your actual project ID on Google Cloud
gcloud config set project your-project-id
echo "Project configured."

# 2. Store variables for easy use
export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-central1"

# 3. Confirm your settings
echo "Using Project ID: $PROJECT_ID in Region: $REGION"
```

```
 
 ### 1.3. Configure Your Google AI Studio API Key
 
 To access the Gemini 3.0 model, you need an API key from Google AI Studio.
 
 1.  Go to [Google AI Studio](https://aistudio.google.com/).
 2.  Click on **Get API key**.
 3.  Click **Create API key**.
 4.  Copy the key and run the following command in Cloud Shell:
 
 ```bash
 export GOOGLE_API_KEY="YOUR_API_KEY_HERE"
 echo "Google API Key configured."
 ```
 
 ### 1.4. Enable Required Google Cloud APIs

```bash
gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  cloudscheduler.googleapis.com \
  aiplatform.googleapis.com

echo "APIs enabled successfully."
```

### 1.5. Grant Permissions to the Dataflow Service Account

By default, Dataflow jobs use your project's Compute Engine service account to run. To allow this service account to read from Pub/Sub and write to BigQuery, we need to grant it the necessary IAM roles.

```bash
# Get your project number
export PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

# Identify the default Compute Engine service account
export GCE_SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Grant the Pub/Sub Subscriber role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GCE_SERVICE_ACCOUNT}" \
    --role="roles/pubsub.subscriber"

# Grant the BigQuery Data Editor role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GCE_SERVICE_ACCOUNT}" \
    --role="roles/bigquery.dataEditor"

echo "IAM roles granted to Dataflow's service account."
```

---

## Section 2: Create Your Cloud Resources (Approx. 10 mins)

### 2.1. Create a Cloud Storage Staging Bucket

```bash
export STAGING_BUCKET_NAME="fintech-workshop-staging-${PROJECT_ID}"
gsutil mb -p $PROJECT_ID -l $REGION gs://$STAGING_BUCKET_NAME
echo "Created GCS Staging Bucket."
```

### 2.2. Create a BigQuery Dataset and Table

```bash
export BQ_DATASET="stock_market_dataset"
export BQ_TABLE="realtime_analysis"

bq --location=$REGION mk --dataset ${PROJECT_ID}:${BQ_DATASET}

bq mk --table ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE} \
    ticker:STRING,window_timestamp:TIMESTAMP,latest_price:FLOAT,high_price_1m:FLOAT,total_volume_1m:INTEGER,total_value_1m:FLOAT,sma_5m:FLOAT,is_volume_spike:BOOLEAN,system_latency:FLOAT,ai_sentiment:FLOAT,ai_summary:STRING

echo "Created BigQuery Dataset and Table."
```

### 2.3. Create Pub/Sub Topics and Subscription

```bash
export TRIGGER_TOPIC="stock-ingestion-trigger"
export DATA_TOPIC="stock-data-enriched"
export DATA_SUB="stock-data-enriched-sub"

gcloud pubsub topics create $TRIGGER_TOPIC
gcloud pubsub topics create $DATA_TOPIC
gcloud pubsub subscriptions create $DATA_SUB --topic=$DATA_TOPIC

echo "Created 2 Pub/Sub Topics and 1 Subscription."
```

---

## Section 3: Create the Ingestion Cloud Function (Approx. 10 mins)

### 3.1. Create the Function's Source Code

```bash
mkdir -p stock-ingestion-function

# Create main.py
cat > stock-ingestion-function/main.py << EOF
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
        TICKERS = ["GOOGL", "AAPL", "MSFT", "AMZN", "NVDA", "META"]
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
                
                # Construct a single prompt for all tickers
                prompt_lines = ["Analyze the following stock data and provide a 1-sentence summary and sentiment score (-1 to 1) for EACH ticker."]
                prompt_lines.append("Return a JSON object where keys are tickers and values have 'summary' and 'sentiment'.")
                prompt_lines.append("Data:")
                for t, d in batch_data.items():
                    prompt_lines.append(f"- {t}: Price ${d['price']:.2f}, Volume {d['volume']}")
                
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
EOF

# Create requirements.txt
cat > stock-ingestion-function/requirements.txt << EOF
functions-framework>=3.0.0
google-cloud-pubsub>=2.13.0
yfinance>=0.2.37
pandas>=2.2.0
google-generativeai>=0.3.0
EOF
```

### 3.2. Deploy the Cloud Function

```bash
cd stock-ingestion-function

gcloud functions deploy fetch-and-publish-stock-data \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=fetch_and_publish_stock_data \
  --trigger-topic=$TRIGGER_TOPIC \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,DATA_TOPIC=$DATA_TOPIC,GOOGLE_API_KEY=$GOOGLE_API_KEY \
  --memory=2Gi \
  --timeout=300s

cd ..
echo "Cloud Function deployment initiated."
```

---

## Section 4: Create the Dataflow Analysis Pipeline (Approx. 15 mins)

### 4.1. Create the Dataflow Pipeline's Source Code

```bash
mkdir -p analysis-dataflow-pipeline

# Create pipeline.py
cat > analysis-dataflow-pipeline/pipeline.py << EOF
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import logging
import json
from datetime import datetime
from apache_beam.transforms.window import FixedWindows, SlidingWindows, TimestampedValue
from apache_beam.transforms.combiners import Mean
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.coders import VarIntCoder

# --- Custom Pipeline Options ---
class StockAnalysisPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', required=True)
        parser.add_argument('--output_table', required=True)

# --- Stateful DoFn for Anomaly Detection ---
class DetectVolumeSpike(beam.DoFn):
    VOLUME_HISTORY = BagStateSpec('volume_history', VarIntCoder())

    def process(self, element, volume_history=beam.DoFn.StateParam(VOLUME_HISTORY)):
        ticker, data = element
        current_volume = data.get('total_volume_1m', 0)

        # Read the list of historical volumes
        historical_volumes = list(volume_history.read())

        # Calculate the average of the historical volumes
        avg_volume_10m = sum(historical_volumes) / len(historical_volumes) if historical_volumes else 0

        # Determine if the current volume is a spike
        is_spike = current_volume > (avg_volume_10m * 2) and avg_volume_10m > 0

        # Add the current volume to the history and keep the list at a max of 10 items
        historical_volumes.append(current_volume)
        volume_history.clear()
        for vol in historical_volumes[-10:]:
            volume_history.add(vol)

        yield (ticker, {**data, 'is_volume_spike': is_spike})

# --- DoFn for Formatting Output ---
class FormatOutput(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        ticker, data = element
        
        # Calculate System Latency
        # Window end time is when the window closes. Processing time is now.
        window_end_time = window.end.to_utc_datetime()
        processing_time = datetime.utcnow()
        system_latency = (processing_time - window_end_time).total_seconds()

        output_row = {
            'ticker': ticker,
            'window_timestamp': window_end_time.isoformat(),
            'latest_price': data['latest_price'],
            'high_price_1m': data['high_price_1m'],
            'total_volume_1m': data['total_volume_1m'],
            'total_value_1m': data['total_value_1m'], # New Metric
            'sma_5m': data['sma_5m'],
            'is_volume_spike': data.get('is_volume_spike', False),
            'system_latency': system_latency, # New Metric
            'ai_sentiment': data.get('ai_sentiment', 0.0), # New AI Metric
            'ai_summary': data.get('ai_summary', 'No summary') # New AI Metric
        }
        yield output_row

def run():
    pipeline_options = PipelineOptions(streaming=True)
    custom_options = pipeline_options.view_as(StockAnalysisPipelineOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        events = (p
                  | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=custom_options.input_subscription)
                  | 'Decode JSON' >> beam.Map(lambda x: json.loads(x.decode('utf-8'))))

        timestamped_events = (events
                              | 'Add Timestamps' >> beam.Map(lambda e: TimestampedValue(e, datetime.fromisoformat(e['timestamp']).timestamp())))

        keyed_by_ticker = (timestamped_events | 'Key by Ticker' >> beam.Map(lambda e: (e['ticker'], e)))

        # 1-minute aggregations
        agg_1m = (keyed_by_ticker
                  | '1-Min Window' >> beam.WindowInto(FixedWindows(60))
                  | 'Group by Ticker' >> beam.GroupByKey()
                  | 'Calculate 1-Min Aggs' >> beam.Map(lambda kv: (kv[0], {
                      'latest_price': max(kv[1], key=lambda x: datetime.fromisoformat(x['timestamp']).timestamp())['price'],
                      'high_price_1m': max(item['price'] for item in kv[1]),
                      'total_volume_1m': sum(item['volume'] for item in kv[1]),
                      'total_value_1m': sum(item['price'] * item['volume'] for item in kv[1]), # New Metric
                      # Pass through the AI fields from the latest event in the window
                      'ai_sentiment': max(kv[1], key=lambda x: datetime.fromisoformat(x['timestamp']).timestamp()).get('ai_sentiment', 0.0),
                      'ai_summary': max(kv[1], key=lambda x: datetime.fromisoformat(x['timestamp']).timestamp()).get('ai_summary', 'No summary')
                  })))

        # 5-minute SMA
        # Note: We re-window into FixedWindows(60) before joining to ensure alignment with agg_1m.
        # The SlidingWindow value emitted is the one valid for that window.
        sma_5m = (keyed_by_ticker
                  | 'Map to Price' >> beam.Map(lambda kv: (kv[0], kv[1]['price']))
                  | '5-Min Sliding Window' >> beam.WindowInto(SlidingWindows(300, 60))
                  | 'Calculate 5-Min SMA' >> Mean.PerKey()
                  | 'Format SMA' >> beam.Map(lambda kv: (kv[0], {'sma_5m': kv[1]}))
                  | 'Re-Window SMA' >> beam.WindowInto(FixedWindows(60)))

        # Join all metrics
        def merge_metrics(kv):
            # This function safely merges the two streams
            if kv[1]['agg_1m'] and kv[1]['sma_5m']:
                # Take the first element from each (should be only one per window per key)
                yield (kv[0], {**kv[1]['agg_1m'][0], **kv[1]['sma_5m'][0]})

        joined_data = (
            {'agg_1m': agg_1m, 'sma_5m': sma_5m}
            | 'Join Metrics' >> beam.CoGroupByKey()
            | 'Merge Metrics' >> beam.FlatMap(merge_metrics))

        # Detect anomalies
        anomaly_data = (joined_data | 'Detect Volume Spikes' >> beam.ParDo(DetectVolumeSpike()))

        # Format and write to BigQuery
        (anomaly_data
         | 'Format for BigQuery' >> beam.ParDo(FormatOutput())
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             custom_options.output_table,
             schema='ticker:STRING,window_timestamp:TIMESTAMP,latest_price:FLOAT,high_price_1m:FLOAT,total_volume_1m:INTEGER,total_value_1m:FLOAT,sma_5m:FLOAT,is_volume_spike:BOOLEAN,system_latency:FLOAT,ai_sentiment:FLOAT,ai_summary:STRING',
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
           )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
EOF

# Create requirements.txt
cat > analysis-dataflow-pipeline/requirements.txt << EOF
apache-beam[gcp]>=2.40.0
google-cloud-storage>=2.0.0
EOF

# Create metadata.json for the Flex Template
cat > analysis-dataflow-pipeline/metadata.json << EOF
{
    "name": "Real-Time Stock Analysis",
    "description": "A Dataflow pipeline that performs real-time analysis on stock market data.",
    "parameters": [
        {
            "name": "input_subscription",
            "label": "Input Pub/Sub subscription",
            "helpText": "The Pub/Sub subscription to read stock data from. Format: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_ID>",
            "paramType": "TEXT"
        },
        {
            "name": "output_table",
            "label": "Output BigQuery table",
            "helpText": "The BigQuery table to write analysis results to. Format: <PROJECT_ID>:<DATASET_ID>.<TABLE_ID>",
            "paramType": "TEXT"
        }
    ]
}
EOF

# Create Dockerfile for the Flex Template
cat > analysis-dataflow-pipeline/Dockerfile << EOF
# Dockerfile for Dataflow Flex Template
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base
WORKDIR /template
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py .
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/pipeline.py
EOF
```

### 4.2. Deploy the Dataflow Pipeline

```bash
export TEMPLATE_IMAGE="gcr.io/${PROJECT_ID}/dataflow/stock-analysis:latest"
export TEMPLATE_PATH="gs://${STAGING_BUCKET_NAME}/templates/stock_analysis_template.json"

# Build the Docker image for the Flex Template
gcloud builds submit --tag $TEMPLATE_IMAGE analysis-dataflow-pipeline

# Create the Flex Template spec file
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image $TEMPLATE_IMAGE \
  --sdk-language PYTHON \
  --metadata-file analysis-dataflow-pipeline/metadata.json

# Run the Flex Template to start the streaming job
gcloud dataflow flex-template run "stock-market-analysis-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location $TEMPLATE_PATH \
    --project $PROJECT_ID \
    --region $REGION \
    --parameters input_subscription=projects/$PROJECT_ID/subscriptions/$DATA_SUB \
    --parameters output_table=$PROJECT_ID:$BQ_DATASET.$BQ_TABLE
```

---

## Section 5: Schedule and Verify the Pipeline (Approx. 10 mins)

### 5.1. Schedule the Ingestion Job

Create a Cloud Scheduler job to run the pipeline automatically every minute.

```bash
gcloud scheduler jobs create pubsub trigger-stock-ingestion-job \
    --schedule "* * * * *" \
    --topic $TRIGGER_TOPIC \
    --message-body "Run" \
    --location $REGION
```

### 5.2. Manually Trigger the Pipeline (Optional)

```bash
gcloud scheduler jobs run trigger-stock-ingestion-job --location $REGION
```

### 5.3. Verify the Results in BigQuery

Wait a few minutes for data to flow through the pipeline. Then, query your BigQuery table.

```bash
bq query "SELECT * FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` ORDER BY window_timestamp DESC LIMIT 10"
```

You should see real-time analysis results for each stock, including the latest price, moving average, and volume spike alerts!

---

## Section 6: Analyze the Data with SQL

Now that your pipeline is running, let's use BigQuery to uncover insights from the streaming data. Run these queries in the **BigQuery Studio** console.

### 6.1. Real-Time Dashboard View
Get the latest snapshot of every stock, ranked by the most recent update.

```sql
SELECT 
    ticker,
    latest_price,
    total_volume_1m,
    total_value_1m,
    sma_5m,
    is_volume_spike,
    window_timestamp
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY window_timestamp DESC) = 1
ORDER BY total_value_1m DESC;
```

### 6.2. Trend Analysis (Buy/Sell Signals)
Identify stocks where the price is crossing above the 5-minute Moving Average (a potential "Buy" signal).

```sql
SELECT 
    ticker,
    latest_price,
    sma_5m,
    (latest_price - sma_5m) as price_diff,
    window_timestamp
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
WHERE latest_price > sma_5m
ORDER BY window_timestamp DESC
LIMIT 10;
```

### 6.3. Whale Watching 🐳
Find the specific minutes with the highest traded value across all stocks.

```sql
SELECT 
    ticker,
    total_value_1m,
    total_volume_1m,
    latest_price,
    window_timestamp
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
ORDER BY total_value_1m DESC
LIMIT 5;
```

### 6.4. Pipeline Health Check
Analyze the system latency to see how "real-time" your pipeline truly is.

```sql
SELECT 
    AVG(system_latency) as avg_latency_seconds,
    MAX(system_latency) as max_latency_seconds,
    MIN(system_latency) as min_latency_seconds,
    COUNT(*) as total_records
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
WHERE window_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

### 6.5. Volatility Analysis 📉
Identify which stocks are most volatile by calculating the standard deviation of their price over the last 10 minutes.

```sql
SELECT
    ticker,
    STDDEV(latest_price) as price_stddev,
    AVG(latest_price) as avg_price,
    (STDDEV(latest_price) / AVG(latest_price)) * 100 as volatility_pct
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
WHERE window_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
GROUP BY ticker
ORDER BY volatility_pct DESC;
```

### 6.6. Momentum Ranking 🚀
Rank stocks by their current momentum, defined as the percentage difference between the latest price and the 5-minute SMA.

```sql
SELECT
    ticker,
    latest_price,
    sma_5m,
    ((latest_price - sma_5m) / sma_5m) * 100 as momentum_pct,
    window_timestamp
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY window_timestamp DESC) = 1
ORDER BY momentum_pct DESC;
```

### 6.7. Latency Debugging 🐢
Find specific minutes where the system latency spiked above 30 seconds, helping you debug performance bottlenecks.

```sql
SELECT
    window_timestamp,
    system_latency,
    ticker,
    total_volume_1m
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
WHERE system_latency > 30
ORDER BY system_latency DESC;
```

### 6.8. AI Sentiment Analysis 🤖
Analyze the correlation between the AI-generated sentiment score and the actual price movement.

```sql
SELECT
    ticker,
    AVG(ai_sentiment) as avg_sentiment,
    AVG(latest_price) as avg_price,
    COUNT(*) as data_points,
    -- Check if positive sentiment correlates with higher prices (simplified)
    CORR(ai_sentiment, latest_price) as sentiment_price_correlation
FROM `YOUR_PROJECT_ID.stock_market_dataset.realtime_analysis`
WHERE window_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY ticker
ORDER BY avg_sentiment DESC;
```

---

## Section 7: Cleanup

To avoid ongoing charges, run these commands to delete the resources you created.

```bash
# ---- Cleanup Commands ----
# 1. Stop the Dataflow Job
export JOB_ID=$(gcloud dataflow jobs list --region=$REGION --filter="name:stock-market-analysis" --format="get(id)")
gcloud dataflow jobs drain $JOB_ID --region=$REGION --quiet

# 2. Delete the Cloud Scheduler Job
gcloud scheduler jobs delete trigger-stock-ingestion-job --location $REGION --quiet

# 3. Delete the Cloud Function
gcloud functions delete fetch-and-publish-stock-data --region=$REGION --gen2 --quiet

# 4. Delete the GCS Bucket
gsutil rm -r -f gs://$STAGING_BUCKET_NAME

# 5. Delete the BigQuery Dataset
bq rm -r -f --dataset ${PROJECT_ID}:${BQ_DATASET}

# 6. Delete the Pub/Sub Topics and Subscription
gcloud pubsub subscriptions delete $DATA_SUB --project=$PROJECT_ID
gcloud pubsub topics delete $TRIGGER_TOPIC --project=$PROJECT_ID
gcloud pubsub topics delete $DATA_TOPIC --project=$PROJECT_ID

echo "Cleanup complete."
```
