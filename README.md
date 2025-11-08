# The Fintech News Analysis Pipeline: From Live RSS to AI-Powered Insights

## Welcome to the Workshop!

Welcome! In this hands-on workshop, you'll build a sophisticated, real-time data engineering pipeline on Google Cloud. This project is designed for a **fintech** use case, where raw financial news is automatically ingested, analyzed by AI, and transformed into structured, actionable intelligence.

This workshop is designed to be completed in approximately **1 hour**.

---

### What You'll Learn

*   How to set up a Google Cloud environment using Cloud Shell.
*   How to build a modern, event-driven data pipeline from scratch.
*   How to use **Cloud Scheduler** to trigger a pipeline on a recurring schedule.
*   How to write a Python **Cloud Function** to ingest and parse a live, public **RSS feed**.
*   How to use **Pub/Sub** to decouple ingestion from processing.
*   How to build and deploy a streaming **Dataflow** pipeline for scalable data transformation.
*   How to use **Vertex AI (Gemini)** for advanced NLP tasks like summarization and sentiment analysis.
*   How to use **Vertex AI (Imagen)** to generate visual representations of data.
*   How to store and query the final structured results in **BigQuery**.

### Our Fintech Architecture

We will build a real-time news analysis pipeline using a scheduled, event-driven architecture.

![Architecture Diagram](https://storage.googleapis.com/gweb-cloudblog-publish/images/Event-driven_data_processing_rev2.max-2600x2600.png)

The workflow is:
1.  **Schedule:** A **Cloud Scheduler** job runs every 15 minutes, sending a trigger message to a Pub/Sub topic.
2.  **Ingest:** A **Cloud Function**, subscribed to the trigger topic, activates. It fetches the latest articles from the Investing.com "Stock Market News" RSS feed.
3.  **Publish:** The function parses the XML from the RSS feed and publishes each news article as a distinct message to a second Pub/Sub topic.
4.  **Process:** A streaming **Dataflow** pipeline, subscribed to the article topic, processes each article in parallel.
5.  **Analyze & Visualize:** For each article, the Dataflow pipeline uses **Vertex AI** to:
    *   **Summarize** the article text with the Gemini model.
    *   **Determine the market sentiment** (Positive, Negative, or Neutral) with Gemini.
    *   **Generate a sentiment chart icon** with the Imagen model (e.g., a green up-arrow for Positive).
6.  **Store:** The final, enriched data—including the summary, sentiment, and image URL—is stored in a **BigQuery** table for analysis.

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
  aiplatform.googleapis.com \
  iam.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  cloudscheduler.googleapis.com

echo "APIs enabled successfully."
```

---

## Section 2: Create Your Cloud Resources (Approx. 10 mins)

### 2.1. Create Cloud Storage Buckets

```bash
export STAGING_BUCKET_NAME="fintech-workshop-staging-${PROJECT_ID}"
export SENTIMENT_IMAGES_BUCKET_NAME="fintech-workshop-images-${PROJECT_ID}"

gsutil mb -p $PROJECT_ID -l $REGION gs://$STAGING_BUCKET_NAME
gsutil mb -p $PROJECT_ID -l $REGION gs://$SENTIMENT_IMAGES_BUCKET_NAME

# Make the image bucket public
gsutil iam ch allUsers:objectViewer gs://$SENTIMENT_IMAGES_BUCKET_NAME

echo "Created 2 GCS Buckets."
```

### 2.2. Create a BigQuery Dataset and Table

```bash
export BQ_DATASET="market_news_dataset"
export BQ_TABLE="article_analysis"

bq --location=$REGION mk --dataset ${PROJECT_ID}:${BQ_DATASET}

bq mk --table ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE} \
    article_id:STRING,headline:STRING,summary:STRING,sentiment:STRING,sentiment_chart_url:STRING,processed_at:TIMESTAMP

echo "Created BigQuery Dataset and Table."
```

### 2.3. Create Pub/Sub Topics and Subscription

We need two topics and one subscription for the Dataflow pipeline.

```bash
export TRIGGER_TOPIC="rss-ingestion-trigger"
export ARTICLES_TOPIC="articles-for-analysis"
export ARTICLES_SUB="articles-for-analysis-sub"

gcloud pubsub topics create $TRIGGER_TOPIC
gcloud pubsub topics create $ARTICLES_TOPIC
gcloud pubsub subscriptions create $ARTICLES_SUB --topic=$ARTICLES_TOPIC

echo "Created 2 Pub/Sub Topics and 1 Subscription."
```

---

## Section 3: Create the Ingestion Cloud Function (Approx. 10 mins)

### 3.1. Create the Function's Source Code

```bash
mkdir -p rss-ingestion-function

# Create main.py
cat > rss-ingestion-function/main.py << EOF
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
EOF

# Create requirements.txt
cat > rss-ingestion-function/requirements.txt << EOF
functions-framework>=3.0.0
google-cloud-pubsub>=2.13.0
feedparser>=6.0.0
EOF
```

### 3.2. Deploy the Cloud Function

```bash
cd rss-ingestion-function

gcloud functions deploy fetch-and-publish-rss \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=fetch_and_publish_rss \
  --trigger-topic=$TRIGGER_TOPIC \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,ARTICLES_TOPIC=$ARTICLES_TOPIC \
  --memory=2Gi \
  --timeout=3600s

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
import io

from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
from vertexai.preview.vision_models import ImageGenerationModel

# --- Custom Pipeline Options ---
class MarketNewsPipelineOptions(PipelineOptions):
    """Custom options for the Market News Analysis pipeline."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', required=True, help='Pub/Sub subscription to read from. Format: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_ID>')
        parser.add_argument('--output_table', required=True, help='BigQuery table to write to. Format: <PROJECT_ID>:<DATASET_ID>.<TABLE_ID>')
        parser.add_argument('--sentiment_images_bucket_name', required=True, help='GCS bucket to store generated sentiment chart icons.')

# This DoFn class encapsulates the logic to call the Vertex AI APIs
class AnalyzeArticle(beam.DoFn):
    def __init__(self, project_id, region, sentiment_images_bucket_name):
        self.project_id = project_id
        self.region = region
        self.sentiment_images_bucket_name = sentiment_images_bucket_name

    def setup(self):
        # Initialize the Vertex AI clients. This is done once per worker.
        vertexai.init(project=self.project_id, location=self.region)
        self.text_model = GenerativeModel("gemini-2.5-pro", safety_settings={
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        })
        self.image_model = ImageGenerationModel.from_pretrained("imagegeneration@006")
        self.storage_client = storage.Client()

    def process(self, element):
        try:
            # --- 1. Summarization ---
            summary_prompt = f"You are a financial analyst. Summarize the key points of this article for a busy trader in three bullet points: '{element['full_text']}'"
            summary_response = self.text_model.generate_content(summary_prompt)
            summary = summary_response.text.strip() if hasattr(summary_response, 'text') else "Error: Could not generate summary."

            # --- 2. Sentiment Analysis ---
            sentiment_prompt = f"Based on the following headline and summary, what is the market sentiment? Respond with only one word: Positive, Negative, or Neutral. Headline: '{element['headline']}' Summary: '{summary}'"
            sentiment_response = self.text_model.generate_content(sentiment_prompt)
            sentiment = sentiment_response.text.strip() if hasattr(sentiment_response, 'text') else "Neutral"
            # Basic validation to ensure the sentiment is one of the three expected values
            if sentiment not in ["Positive", "Negative", "Neutral"]:
                sentiment = "Neutral"

            # --- 3. Image Generation ---
            image_prompt = f"Create a simple, abstract stock market chart icon that visually represents a '{sentiment}' trend."
            images = self.image_model.generate_images(prompt=image_prompt, number_of_images=1)

            image_bytes = images[0]._image_bytes
            image_blob_name = f"{element['article_id']}_{sentiment.lower()}.png"

            image_bucket = self.storage_client.bucket(self.sentiment_images_bucket_name)
            image_blob = image_bucket.blob(image_blob_name)
            image_blob.upload_from_file(io.BytesIO(image_bytes), content_type="image/png")
            sentiment_chart_url = image_blob.public_url

            # Yield the final, enriched record
            yield {
                "article_id": element['article_id'],
                "headline": element['headline'],
                "summary": summary,
                "sentiment": sentiment,
                "sentiment_chart_url": sentiment_chart_url,
                "processed_at": datetime.utcnow().isoformat()
            }
        except Exception as e:
            logging.error(f"Failed to process article {element['article_id']}: {e}", exc_info=True)
            # Optionally, you could output to a dead-letter queue here
            pass

def run():
    """Defines and runs the Dataflow pipeline."""
    pipeline_options = PipelineOptions(streaming=True)
    custom_options = pipeline_options.view_as(MarketNewsPipelineOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)

    # This is necessary for Dataflow to pickle the main session
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=custom_options.input_subscription)
         | 'Decode JSON' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
         | 'Analyze Article' >> beam.ParDo(AnalyzeArticle(
             project_id=gcp_options.project,
             region=gcp_options.region,
             sentiment_images_bucket_name=custom_options.sentiment_images_bucket_name
           ))
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
             custom_options.output_table,
             schema='article_id:STRING,headline:STRING,summary:STRING,sentiment:STRING,sentiment_chart_url:STRING,processed_at:TIMESTAMP',
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
google-cloud-aiplatform[generative_models]>=1.38.0
google-cloud-storage>=2.0.0
EOF

# Create metadata.json for the Flex Template
cat > analysis-dataflow-pipeline/metadata.json << EOF
{
    "name": "Market News Analysis",
    "description": "A Dataflow pipeline that analyzes financial news with Vertex AI.",
    "parameters": [
        {
            "name": "input_subscription",
            "label": "Input Pub/Sub subscription",
            "helpText": "The Pub/Sub subscription to read messages from. Format: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_ID>",
            "paramType": "TEXT"
        },
        {
            "name": "output_table",
            "label": "Output BigQuery table",
            "helpText": "The BigQuery table to write results to. Format: <PROJECT_ID>:<DATASET_ID>.<TABLE_ID>",
            "paramType": "TEXT"
        },
        {
            "name": "sentiment_images_bucket_name",
            "label": "Sentiment images bucket name",
            "helpText": "The GCS bucket to store generated sentiment chart icons in.",
            "paramType": "TEXT"
        }
    ]
}
EOF

# Create Dockerfile for the Flex Template
cat > analysis-dataflow-pipeline/Dockerfile << EOF
# Dockerfile for Dataflow Flex Template
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Set the working directory
WORKDIR /template

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pipeline source code
COPY pipeline.py .

# Set the entrypoint for the template launcher
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/pipeline.py
EOF
```

### 4.2. Deploy the Dataflow Pipeline

```bash
export TEMPLATE_IMAGE="gcr.io/${PROJECT_ID}/dataflow/fintech-analysis:latest"
export TEMPLATE_PATH="gs://${STAGING_BUCKET_NAME}/templates/fintech_analysis_template.json"

# Build the Docker image for the Flex Template
gcloud builds submit --tag $TEMPLATE_IMAGE analysis-dataflow-pipeline

# Create the Flex Template spec file
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image $TEMPLATE_IMAGE \
  --sdk-language PYTHON \
  --metadata-file analysis-dataflow-pipeline/metadata.json

# Run the Flex Template to start the streaming job
gcloud dataflow flex-template run "fintech-news-analysis-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location $TEMPLATE_PATH \
    --project $PROJECT_ID \
    --region $REGION \
    --parameters input_subscription=projects/$PROJECT_ID/subscriptions/$ARTICLES_SUB \
    --parameters output_table=$PROJECT_ID:$BQ_DATASET.$BQ_TABLE \
    --parameters sentiment_images_bucket_name=$SENTIMENT_IMAGES_BUCKET_NAME
```

---

## Section 5: Schedule and Verify the Pipeline (Approx. 5 mins)

### 5.1. Schedule the Ingestion Job

Create the Cloud Scheduler job to run the pipeline automatically every 15 minutes.

```bash
gcloud scheduler jobs create pubsub trigger-rss-ingestion-job \
    --schedule "*/15 * * * *" \
    --topic $TRIGGER_TOPIC \
    --message-body "Run" \
    --location $REGION
```

### 5.2. Manually Trigger the Pipeline (Optional)

You can wait for the scheduler, or trigger it manually to see results faster.

```bash
gcloud scheduler jobs run trigger-rss-ingestion-job --location $REGION
```

### 5.3. Verify the Results in BigQuery

Wait a few minutes for the data to flow through the pipeline. Then, query your BigQuery table.

```bash
bq query "SELECT headline, summary, sentiment, sentiment_chart_url FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` ORDER BY processed_at DESC LIMIT 10"
```

You should see the latest financial news headlines, along with the AI-generated summary, sentiment, and a link to the sentiment chart icon!

---

## Section 6: Cleanup

To avoid ongoing charges, run these commands to delete the resources you created.

```bash
# ---- Cleanup Commands ----
# 1. Stop the Dataflow Job
export JOB_ID=$(gcloud dataflow jobs list --region=$REGION --filter="name:fintech-news-analysis" --format="get(id)")
gcloud dataflow jobs drain $JOB_ID --region=$REGION --quiet

# 2. Delete the Cloud Scheduler Job
gcloud scheduler jobs delete trigger-rss-ingestion-job --location $REGION --quiet

# 3. Delete the Cloud Function
gcloud functions delete fetch-and-publish-rss --region=$REGION --gen2 --quiet

# 4. Delete the GCS Buckets
gsutil rm -r -f gs://$STAGING_BUCKET_NAME gs://$SENTIMENT_IMAGES_BUCKET_NAME

# 5. Delete the BigQuery Dataset
bq rm -r -f --dataset ${PROJECT_ID}:${BQ_DATASET}

# 6. Delete the Pub/Sub Topics and Subscription
gcloud pubsub subscriptions delete $ARTICLES_SUB --project=$PROJECT_ID
gcloud pubsub topics delete $TRIGGER_TOPIC --project=$PROJECT_ID
gcloud pubsub topics delete $ARTICLES_TOPIC --project=$PROJECT_ID

echo "Cleanup complete."
```
