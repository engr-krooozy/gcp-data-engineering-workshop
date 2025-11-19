# Real-Time Stock Analysis Pipeline on Google Cloud

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
*   How to build a **live dashboard** in **Looker Studio**.

### Our Fintech Architecture

We will build a real-time stock analysis pipeline using a scheduled, event-driven architecture.

![Architecture Diagram](https://storage.googleapis.com/gweb-cloudblog-publish/images/Event-driven_data_processing_rev2.max-2600x2600.png)

The workflow is:
1.  **Schedule:** A **Cloud Scheduler** job runs every minute, sending a trigger message to a Pub/Sub topic.
2.  **Ingest:** A **Cloud Function**, subscribed to the trigger topic, activates. It fetches the latest stock prices for major tech companies from the Yahoo Finance API.
3.  **Publish:** The function publishes the data for each stock as a distinct message to a second Pub/Sub topic.
4.  **Process & Analyze:** A streaming **Dataflow** pipeline, subscribed to the data topic, performs a series of real-time analyses:
    *   **1-Minute Aggregations:** It calculates the highest price and total trading volume within 1-minute fixed windows.
    *   **5-Minute Moving Average:** It calculates the 5-minute Simple Moving Average (SMA) of the price using a 5-minute sliding window.
    *   **Volume Spike Detection:** It uses stateful processing to detect anomalous spikes in trading volume by comparing the current minute's volume to the 10-minute historical average.
5.  **Store:** The final, enriched data—including the latest price, technical indicators, and anomaly flags—is streamed into a **BigQuery** table for analysis.
6.  **Visualize:** The BigQuery table is connected to a **Looker Studio** dashboard to provide a live, interactive view of the market data.

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
gcloud config set project your-project-id
echo "Project configured."

# 2. Store variables for easy use
export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-central1"

# 3. Confirm your settings
echo "Using Project ID: $PROJECT_ID in Region: $REGION"
```

### 1.3. Enable Required Google Cloud APIs

```bash
gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  cloudscheduler.googleapis.com

echo "APIs enabled successfully."
```

### 1.4. Grant Permissions to the Dataflow Service Account

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
    ticker:STRING,window_timestamp:TIMESTAMP,latest_price:FLOAT,high_price_1m:FLOAT,total_volume_1m:INTEGER,sma_5m:FLOAT,is_volume_spike:BOOLEAN

echo "Created BigQuery Dataset and Table."
```

### 2.3. Create Pub/Sub Topics and Subscription

```bash
export TRIGGER_TOPIC="stock-ingestion-trigger"
export DATA_TOPIC="stock-data-for-analysis"
export DATA_SUB="stock-data-for-analysis-sub"

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
        data = yf.download(tickers=TICKERS, period="1d", interval="1m")

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
EOF

# Create requirements.txt
cat > stock-ingestion-function/requirements.txt << EOF
functions-framework>=3.0.0
google-cloud-pubsub>=2.13.0
yfinance>=0.2.37
pandas>=2.2.0
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
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,DATA_TOPIC=$DATA_TOPIC \
  --memory=512Mi \
  --timeout=120s

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
                      'latest_price': kv[1][-1]['price'],
                      'high_price_1m': max(item['price'] for item in kv[1]),
                      'total_volume_1m': sum(item['volume'] for item in kv[1])
                  })))

        # 5-minute SMA
        sma_5m = (keyed_by_ticker
                  | 'Map to Price' >> beam.Map(lambda kv: (kv[0], kv[1]['price']))
                  | '5-Min Sliding Window' >> beam.WindowInto(SlidingWindows(300, 60))
                  | 'Calculate 5-Min SMA' >> Mean.PerKey()
                  | 'Format SMA' >> beam.Map(lambda kv: (kv[0], {'sma_5m': kv[1]})))

        # Join all metrics
        def merge_metrics(kv):
            # This function safely merges the two streams, ignoring windows where one stream is empty
            if kv[1]['agg_1m'] and kv[1]['sma_5m']:
                yield (kv[0], {**kv[1]['agg_1m'][0], **kv[1]['sma_5m'][0]})

        joined_data = (
            {'agg_1m': agg_1m, 'sma_5m': sma_5m}
            | 'Join Metrics' >> beam.CoGroupByKey()
            | 'Merge Metrics' >> beam.FlatMap(merge_metrics))

        # Detect anomalies
        anomaly_data = (joined_data | 'Detect Volume Spikes' >> beam.ParDo(DetectVolumeSpike()))

        # Format and write to BigQuery
        (anomaly_data
         | 'Format for BigQuery' >> beam.Map(lambda kv: {
             'ticker': kv[0],
             'window_timestamp': datetime.utcnow().isoformat(),
             'latest_price': kv[1]['latest_price'],
             'high_price_1m': kv[1]['high_price_1m'],
             'total_volume_1m': kv[1]['total_volume_1m'],
             'sma_5m': kv[1]['sma_5m'],
             'is_volume_spike': kv[1].get('is_volume_spike', False)
         })
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             custom_options.output_table,
             schema='ticker:STRING,window_timestamp:TIMESTAMP,latest_price:FLOAT,high_price_1m:FLOAT,total_volume_1m:INTEGER,sma_5m:FLOAT,is_volume_spike:BOOLEAN',
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

## Section 6: Visualize the Results in Looker Studio

The final step is to create a live dashboard to monitor your analysis.

1.  **Open Looker Studio:** Navigate to [lookerstudio.google.com](https://lookerstudio.google.com).
2.  **Create a Data Source:**
    *   Click **Create** > **Data Source**.
    *   Select the **BigQuery** connector.
    *   Authorize the connector if prompted.
    *   In the project hierarchy, select your **Project**, then the **`stock_market_dataset`**, and finally the **`realtime_analysis`** table.
    *   Click **Connect** in the top right.
3.  **Create a Report (Dashboard):**
    *   After connecting, click **Create Report**.
    *   Use the tools to add charts to your dashboard. Here are some ideas:
        *   **Time Series Chart:** Plot the `latest_price` and `sma_5m` over time, using `window_timestamp` as the dimension.
        *   **Bar Chart:** Show the `total_volume_1m` for each `ticker`.
        *   **Table with Conditional Formatting:** Display all fields, but highlight rows where `is_volume_spike` is `True`.

Now you have a live dashboard for your real-time stock analysis pipeline!

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
