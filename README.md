# The Modern Data Engineering Factory: From CSV to Content with Google Cloud

## Welcome to the Workshop!

Welcome! In this hands-on workshop, you'll build a powerful, resilient, and scalable data pipeline on Google Cloud. You will build an end-to-end automated content factory that ingests product data from a CSV file, uses a serverless **Dataflow** pipeline to generate marketing content with **Vertex AI**, and stores the results in **BigQuery**.

This workshop is designed to be completed in approximately **1 hour**.

---

### What You'll Learn

*   How to set up a Google Cloud environment using Cloud Shell.
*   The fundamentals of a scalable, event-driven data pipeline.
*   How to decouple services using **Pub/Sub**.
*   How to write and deploy a lightweight Python **Cloud Function** to publish messages.
*   How to build, deploy, and monitor a streaming **Dataflow** pipeline for parallel data processing.
*   How to call and chain multiple **Vertex AI** models (**Gemini** for text and **Imagen** for images) from a Dataflow pipeline.
*   How to implement content moderation and robust error handling.
*   How to use **Cloud Storage** to trigger events and stage data.
*   How to store and query structured results in **BigQuery**.

### Our New Architecture

We will build a resilient, multi-modal system using a modern, data-centric architecture that is more scalable and robust than a single monolithic function.

![Architecture Diagram](https://storage.googleapis.com/gweb-cloudblog-publish/images/Event-driven_data_processing_rev2.max-2600x2600.png)

The workflow is:
1.  **Upload:** A user uploads a `products.csv` file to a **Cloud Storage** bucket.
2.  **Trigger:** The upload triggers a lightweight **Cloud Function**.
3.  **Publish:** The Cloud Function reads the CSV and publishes each row as a message to a **Pub/Sub** topic.
4.  **Process:** A streaming **Dataflow** pipeline subscribes to the Pub/Sub topic, processing each message in parallel.
5.  **Generate & Store:** For each message, the Dataflow pipeline:
    *   Calls **Vertex AI (Gemini)** to generate marketing text.
    *   Calls **Vertex AI (Imagen)** to generate a product image.
    *   Stores the image in a separate **Cloud Storage** bucket.
    *   Writes the final results to a **BigQuery** table.

---

## Section 1: Preparing Your Google Cloud Environment (Approx. 15 mins)

First, let's get your Google Cloud project and Cloud Shell ready.

> **Prerequisite:** You need a Google Cloud account with billing enabled.

### 1.1. Activate Cloud Shell

*   **Action:** In the Google Cloud Console, click the **Activate Cloud Shell** button (`>_`) in the top-right corner.

### 1.2. Configure Your Project and Region

Run the following commands in the Cloud Shell terminal.

```bash
# 1. Set your Project ID. Replace "your-project-id" with your actual GCP Project ID.
gcloud config set project your-project-id
echo "Project configured."

# 2. Store your Project ID and Region in variables for easy use.
export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-central1" # A good default region

# 3. Confirm your settings.
echo "----------------------------------"
echo "Using Project ID: $PROJECT_ID"
echo "Using Region:     $REGION"
echo "----------------------------------"
```

### 1.3. Enable Required Google Cloud APIs

This command activates the APIs for the services we'll be using.

```bash
# --- Enable APIs Command ---
echo "Enabling necessary GCP APIs... (This may take a few minutes)"
gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  aiplatform.googleapis.com \
  iam.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com

echo "APIs enabled successfully."
```

### 1.4. Grant Permissions for Cloud Storage Triggers

To allow Cloud Storage to trigger our Cloud Function, its service account needs permission to publish events.

```bash
# 1. Get the special Cloud Storage service account email address.
export GCS_SERVICE_ACCOUNT=$(gcloud storage service-agent --format 'get(email)')

# 2. Grant the Pub/Sub Publisher role to that service account.
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GCS_SERVICE_ACCOUNT}" \
    --role="roles/pubsub.publisher"

echo "Permissions granted to Cloud Storage service account."
```

---

## Section 2: Create Your Cloud Resources (Approx. 10 mins)

Next, let's create the storage buckets, Pub/Sub topic, and BigQuery table.

### 2.1. Create Cloud Storage Buckets

We need three buckets: one for input, one for failed files, and one for the generated images.

```bash
# --- Create GCS Buckets ---
export BUCKET_NAME="ai-content-workshop-${PROJECT_ID}"
export FAILED_BUCKET_NAME="ai-content-workshop-failed-${PROJECT_ID}"
export PRODUCT_IMAGES_BUCKET_NAME="ai-content-workshop-images-${PROJECT_ID}"

gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME
gsutil mb -p $PROJECT_ID -l $REGION gs://$FAILED_BUCKET_NAME
gsutil mb -p $PROJECT_ID -l $REGION gs://$PRODUCT_IMAGES_BUCKET_NAME

# Make the image bucket public so the URLs are accessible
gsutil iam ch allUsers:objectViewer gs://$PRODUCT_IMAGES_BUCKET_NAME

echo "Created 3 GCS Buckets."
```

### 2.2. Create a BigQuery Dataset and Table

```bash
# --- Create BigQuery Dataset and Table ---
export BQ_DATASET="ai_workshop_dataset"
export BQ_TABLE="generated_content"

bq --location=$REGION mk --dataset ${PROJECT_ID}:${BQ_DATASET}

bq mk --table ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE} \
    product_name:STRING,keywords:STRING,generated_content:STRING,generated_image_url:STRING,source_file:STRING,processed_at:TIMESTAMP

echo "Created BigQuery Dataset and Table."
```

### 2.3. Create a Pub/Sub Topic and Subscription

Create a Pub/Sub topic to decouple the Cloud Function from the Dataflow pipeline, and a subscription for Dataflow to read from.

```bash
# --- Create Pub/Sub Topic and Subscription ---
export PUBSUB_TOPIC="csv-processing"
export PUBSUB_SUBSCRIPTION="csv-processing-sub"

gcloud pubsub topics create $PUBSUB_TOPIC
gcloud pubsub subscriptions create $PUBSUB_SUBSCRIPTION --topic=$PUBSUB_TOPIC

echo "Created Pub/Sub Topic and Subscription."
```

---

## Section 3: Create the Cloud Function (Approx. 10 mins)

The Cloud Function's only job is to read the CSV and publish each row to Pub/Sub.

### 3.1. Create the Function's Source Code

**Action:** Run the following commands to create the function's directory and source files.

```bash
# Create the directory for the function's code
mkdir -p content-generator-function
```
```bash
# Create the main.py file
cat > content-generator-function/main.py << EOF
import functions_framework
from google.cloud import pubsub_v1, storage
import csv
import json
import logging
import os
from io import StringIO

# --- Environment Variables ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")
FAILED_BUCKET_NAME = os.environ.get("FAILED_BUCKET_NAME")

def move_to_failed_bucket(storage_client, bucket_name, file_name):
    """Moves a file to the designated 'failed' bucket."""
    if not FAILED_BUCKET_NAME:
        logging.error("FAILED_BUCKET_NAME not set. Cannot move file.")
        return
    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket = storage_client.bucket(FAILED_BUCKET_NAME)
    source_blob = source_bucket.blob(file_name)
    try:
        destination_blob_name = f"{file_name}_failed_{datetime.utcnow().isoformat()}"
        source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
        source_blob.delete()
        logging.info(f"Moved '{file_name}' to failed bucket.")
    except Exception as e:
        logging.error(f"Failed to move '{file_name}' to failed bucket: {e}", exc_info=True)

@functions_framework.cloud_event
def publish_csv_rows_to_pubsub(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    storage_client = storage.Client()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC)

    logging.info(f"Processing file: gs://{bucket_name}/{file_name}")

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        csv_content = blob.download_as_text(encoding="utf-8")

        reader = csv.reader(StringIO(csv_content))
        header = next(reader)  # Skip header row

        for row in reader:
            if len(row) != 2:
                continue

            message_data = {
                "product_name": row[0].strip(),
                "keywords": row[1].strip(),
                "source_file": f"gs://{bucket_name}/{file_name}"
            }

            future = publisher.publish(topic_path, json.dumps(message_data).encode("utf-8"))
            future.result() # Wait for publish to complete

        logging.info(f"Successfully published all rows from '{file_name}' to Pub/Sub.")

    except Exception as e:
        logging.error(f"Critical error processing file '{file_name}': {e}", exc_info=True)
        move_to_failed_bucket(storage_client, bucket_name, file_name)

    return "OK"
EOF
```
```bash
# Create the requirements.txt file
cat > content-generator-function/requirements.txt << EOF
functions-framework>=3.0.0
google-cloud-storage>=2.0.0
google-cloud-pubsub>=2.13.0
EOF
```

### 3.2. Deploy the Cloud Function

```bash
# --- Deploy Cloud Function ---
cd content-generator-function

gcloud functions deploy publish-csv-rows-to-pubsub \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=publish_csv_rows_to_pubsub \
  --trigger-bucket=$BUCKET_NAME \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,PUBSUB_TOPIC=$PUBSUB_TOPIC,FAILED_BUCKET_NAME=$FAILED_BUCKET_NAME \
  --memory=512Mi \
  --timeout=120s

cd ..
echo "Cloud Function deployment initiated."
```

---

## Section 4: Create the Dataflow Pipeline (Approx. 20 mins)

This is the core of our data engineering workshop. The Dataflow pipeline will read from Pub/Sub, call the Vertex AI APIs, and write to BigQuery and GCS.

### 4.1. The Dataflow Pipeline's Logic

Our pipeline performs these steps for each message:
1.  **Generate Text:** It constructs a prompt and calls the **Gemini** model.
    *   **Content Moderation:** We have enabled built-in safety filters. If Gemini blocks the response, the pipeline logs it and writes "Content blocked by safety filter" to BigQuery.
2.  **Generate Image:** If text generation was successful, it uses that text to create a prompt for the **Imagen** model.
    *   **Image Storage:** The generated image is uploaded as a `.png` file to the public images bucket.
3.  **Write to BigQuery:** The final record, including the public image URL, is written to the results table.

### 4.2. Create the Dataflow Pipeline's Source Code

The code for the Dataflow pipeline is already in the `dataflow` directory. You can explore the `pipeline.py` file to see how it works.

### 4.3. Deploy the Dataflow Pipeline

We will use the Dataflow Flex Templates feature to run our pipeline. This command packages our code, uploads it to Cloud Storage, and starts the streaming job on the Dataflow managed service.

```bash
# --- Deploy Dataflow Pipeline ---
export TEMPLATE_IMAGE="gcr.io/${PROJECT_ID}/dataflow/csv-to-ai-content:latest"
export TEMPLATE_PATH="gs://${BUCKET_NAME}/dataflow_templates/csv_to_ai_content.json"

# Build the Docker image for the Flex Template
gcloud builds submit --tag $TEMPLATE_IMAGE dataflow

# Create the Flex Template spec file
gcloud dataflow flex-template build $TEMPLATE_PATH \
  --image $TEMPLATE_IMAGE \
  --sdk-language PYTHON \
  --metadata-file dataflow/metadata.json

# Run the Flex Template
gcloud dataflow flex-template run "csv-to-ai-content-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location $TEMPLATE_PATH \
    --project $PROJECT_ID \
    --region $REGION \
    --parameters input_subscription=projects/$PROJECT_ID/subscriptions/$PUBSUB_SUBSCRIPTION \
    --parameters output_table=$PROJECT_ID:$BQ_DATASET.$BQ_TABLE \
    --parameters product_images_bucket_name=$PRODUCT_IMAGES_BUCKET_NAME \
    --parameters project_id=$PROJECT_ID \
    --parameters region=$REGION
```

---

## Section 5: Test Your AI Content Generator (Approx. 5 mins)

Let's see our creation in action!

### 5.1. Create and Upload a Sample CSV File

*   **Action:** In the Cloud Shell Editor, create a new file named `products.csv` and paste in the following data.

```csv
product_name,keywords
"TerraTrek Hiking Boots","waterproof, breathable, all-terrain grip"
"PowerStack Go","portable charger, 20000mAh, fast-charging, multi-device"
"FitTrack Pro Watch","heart rate monitor, GPS, sleep tracking"
"Gourmet Grind Coffee Maker","built-in burr grinder, programmable, thermal carafe"
```

*   **Action:** Upload the file to your input bucket to trigger the pipeline.

```bash
# --- Upload CSV to Trigger the Function ---
gsutil cp products.csv gs://$BUCKET_NAME/
```

### 5.2. Monitor the Pipeline and Verify the Results

1.  **Cloud Function Logs:** View the function's logs to see it process the file.
2.  **Dataflow Job:** Go to the Dataflow section in the Google Cloud Console to see your running pipeline and watch the data flow through the steps.
3.  **BigQuery Results:** Wait a few minutes for the data to be processed, then query your BigQuery table.

```bash
# --- Query BigQuery to See Results ---
bq query "SELECT product_name, generated_content, generated_image_url FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` ORDER BY processed_at DESC"
```

You should see your product names, the unique marketing descriptions from Gemini, and public URLs to the images created by Imagen!

---

## Section 6: Orchestration with Cloud Composer (For Further Exploration)

This workshop uses an event-driven architecture, which is excellent for real-time processing. In many enterprise scenarios, however, you need more control, such as scheduling, backfills, and complex dependency management. This is where **Cloud Composer** comes in.

Cloud Composer is a fully managed workflow orchestration service built on Apache Airflow.

### How Would Cloud Composer Fit In?

Instead of a streaming Dataflow job, you could use a **batch** Dataflow job and orchestrate it with Cloud Composer. A typical pattern would be:

1.  **Schedule:** A DAG is scheduled to run daily.
2.  **Data Validation:** A task checks that the daily input CSV has arrived in GCS.
3.  **Start Dataflow Job:** A `DataflowStartFlexTemplateOperator` launches your batch pipeline, pointing it to the specific CSV file for that day.
4.  **Quality Checks:** After the Dataflow job succeeds, a `BigQueryCheckOperator` runs SQL queries to ensure the output data is valid.
5.  **Notifications:** A final task sends an email or Slack notification about the pipeline's status.

This approach provides more operational control and is a natural next step for productionizing the concepts you've learned.

---

## Section 7: Conclusion & Cleanup

Congratulations! You've successfully built a modern, scalable data pipeline on Google Cloud.

### 7.1. Cleanup

To avoid ongoing charges, run these commands to delete the resources you created.

```bash
# ---- Cleanup Commands ----
# 1. Stop the Dataflow Job
export JOB_ID=$(gcloud dataflow jobs list --region=$REGION --filter="name:csv-to-ai-content" --format="get(id)")
gcloud dataflow jobs drain $JOB_ID --region=$REGION

# 2. Delete the Cloud Function
gcloud functions delete publish-csv-rows-to-pubsub --region=$REGION --gen2 --quiet

# 3. Delete the GCS Buckets
gsutil rm -r -f gs://$BUCKET_NAME gs://$FAILED_BUCKET_NAME gs://$PRODUCT_IMAGES_BUCKET_NAME

# 4. Delete the BigQuery Dataset
bq rm -r -f --dataset ${PROJECT_ID}:${BQ_DATASET}

# 5. Delete the Pub/Sub Subscription and Topic
gcloud pubsub subscriptions delete $PUBSUB_SUBSCRIPTION --project=$PROJECT_ID
gcloud pubsub topics delete $PUBSUB_TOPIC --project=$PROJECT_ID

echo "Cleanup complete."
```
