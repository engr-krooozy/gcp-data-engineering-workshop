import functions_framework
from google.cloud import pubsub_v1, storage
import csv
import json
import logging
import os
from io import StringIO
from datetime import datetime

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
