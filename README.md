# The Serverless AI Factory: From CSV to Content with Google Cloud

## Welcome to the Workshop!

Welcome! In this hands-on workshop, you'll build a powerful, resilient, multi-modal, serverless AI application on Google Cloud. Forget complex pipelines and long deployments. Today, we focus on speed, simplicity, and the magic of chaining generative AI models.

You will build an automated content factory that reads product information from a CSV file, uses Google's **Gemini 2.5 Pro** model to write compelling marketing descriptions, and then calls the **Imagen** model to generate a unique product image to match. The entire process is protected by content moderation filters and includes robust error handling, with all results stored in **BigQuery**, Google's serverless data warehouse.

---

### What You'll Learn

*   How to set up a Google Cloud environment using Cloud Shell.
*   The fundamentals of a serverless, event-driven architecture.
*   How to write, configure, and deploy a production-ready Python **Cloud Function**.
*   How to call and chain multiple **Vertex AI** models (**Gemini** for text and **Imagen** for images).
*   How to implement content moderation using built-in safety filters.
*   How to build a resilient pipeline with robust error handling.
*   How to use **Cloud Storage** to trigger events and store multiple types of artifacts.
*   How to store and query structured results in **BigQuery**.

### Our Architecture

We will build a resilient, multi-modal system with several Google Cloud services. The primary workflow is:

`[User uploads CSV]` -> `Cloud Storage (Input)` -> `(triggers)` -> `Cloud Function`

The Cloud Function then performs several actions:
1.  Calls **Vertex AI (Gemini)** to generate text.
2.  Calls **Vertex AI (Imagen)** to generate an image from the text.
3.  Stores the generated text and image URL in **BigQuery**.
4.  Stores the generated image file in a separate **Cloud Storage (Image) Bucket**.

If any file fails during processing, it is automatically moved to a **Cloud Storage (Failed) Bucket**.

---

## Section 1: Preparing Your Google Cloud Environment (Approx. 15 mins)

First, let's get your Google Cloud project and Cloud Shell ready.

> **Prerequisite:** You need a Google Cloud account with billing enabled. If you're new, you can sign up for the [Google Cloud Free Tier](https://cloud.google.com/free), which includes a 90-day, $300 free trial.

### 1.1. Activate Cloud Shell

Cloud Shell is a browser-based command line with all the tools you need.

*   **Action:** In the Google Cloud Console, click the **Activate Cloud Shell** button (`>_`) in the top-right corner. A terminal pane will open at the bottom of your browser.
*   **Action:** In the Cloud Shell terminal, click the **Open Editor** button (it looks like a pencil) to open the code editor.

### 1.2. Configure Your Project and Region

Run the following commands in the Cloud Shell terminal to set up your environment.

```bash
# --- Configuration Commands ---

# 1. Set your Project ID. Replace "your-project-id" with your actual GCP Project ID.
gcloud config set project your-project-id
echo "Project configured."
```

```bash
# 2. Store your Project ID and Region in variables for easy use.
# You can change the region, but make sure it's one where all services are available.
export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-central1" # A good default region
```

```bash

# 3. Confirm your settings.
echo "----------------------------------"
echo "Using Project ID: $PROJECT_ID"
echo "Using Region:     $REGION"
echo "----------------------------------"
```

### 1.3. Enable Required Google Cloud APIs

This command activates the APIs for the services we'll be using. It might take a minute or two.

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
  iam.googleapis.com

echo "APIs enabled successfully."
```

### 1.4. Grant Permissions for Cloud Storage Triggers

To allow Cloud Storage to trigger our Cloud Function, we need to give its service account permission to publish events. This is a crucial step for event-driven functions.

```bash
# --- Grant Pub/Sub Publisher Role to GCS Service Account ---

# 1. Get the special Cloud Storage service account email address.
export GCS_SERVICE_ACCOUNT=$(gcloud storage service-agent --format 'get(email)')
```
```bash
# 2. Grant the Pub/Sub Publisher role to that service account.
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GCS_SERVICE_ACCOUNT}" \
    --role="roles/pubsub.publisher"

echo "Permissions granted to Cloud Storage service account."
```

---

## Section 2: Create Your Cloud Resources (Approx. 10 mins)

Next, let's create the storage bucket and BigQuery table that our application will use.

### 2.1. Create a Cloud Storage Bucket

Your bucket needs a globally unique name. We'll use your Project ID to help ensure it's unique.

```bash
# --- Create GCS Bucket ---
# The bucket will be used to upload the CSV files that trigger the process.
export BUCKET_NAME="ai-content-workshop-${PROJECT_ID}"
```

```bash
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME

echo "Created GCS Bucket: gs://$BUCKET_NAME"

# Create a bucket for files that cause processing errors
export FAILED_BUCKET_NAME="ai-content-workshop-failed-${PROJECT_ID}"
gsutil mb -p $PROJECT_ID -l $REGION gs://$FAILED_BUCKET_NAME

echo "Created GCS Bucket for failed files: gs://$FAILED_BUCKET_NAME"

# Create a bucket for the generated product images
export PRODUCT_IMAGES_BUCKET_NAME="ai-content-workshop-images-${PROJECT_ID}"
gsutil mb -p $PROJECT_ID -l $REGION gs://$PRODUCT_IMAGES_BUCKET_NAME

# Make the new image bucket public so the URLs work
gsutil iam ch allUsers:objectViewer gs://$PRODUCT_IMAGES_BUCKET_NAME

echo "Created public GCS Bucket for product images: gs://$PRODUCT_IMAGES_BUCKET_NAME"
# You can view your buckets here: https://console.cloud.google.com/storage/browser
```

### 2.2. Create a BigQuery Dataset and Table

Now, create a home for our data in BigQuery.

```bash
# --- Create BigQuery Dataset and Table ---
export BQ_DATASET="ai_workshop_dataset"
export BQ_TABLE="generated_content"
```
```bash
# Create the dataset
bq --location=$REGION mk --dataset \
    --description="Dataset for the AI Content Generator Workshop" \
    ${PROJECT_ID}:${BQ_DATASET}
```
```bash
# Create the table with a defined schema, now including a column for the image URL
bq mk --table \
    --description="Stores product info, AI-generated marketing content, and image URLs" \
    ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE} \
    product_name:STRING,keywords:STRING,generated_content:STRING,generated_image_url:STRING,source_file:STRING,processed_at:TIMESTAMP

echo "Created BigQuery Dataset '${BQ_DATASET}' and Table '${BQ_TABLE}'"
# You can view your table here: https://console.cloud.google.com/bigquery
```

---

## Section 3: Create the Cloud Function (Approx. 15 mins)

This is the core of our workshop. We'll write the Python code that does all the work and tell Google Cloud what libraries it needs.

### 3.1. Create the Function's Source Code

Instead of manually creating files using a text editor, you can run the following commands in your Cloud Shell terminal. This will create the directory and the necessary source files (`main.py` and `requirements.txt`) for you.

**Action:** Run the following commands to create the function directory and the `main.py` file.

```bash
# Create the directory for the function's code
mkdir -p content-generator-function
```
```bash
# Create the main.py file using a "here document"
cat > content-generator-function/main.py << EOF
import functions_framework
import vertexai

from google.cloud import bigquery
from google.cloud import storage
from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
from vertexai.preview.vision_models import ImageGenerationModel, Image

import csv
import logging
import os
from datetime import datetime
from io import StringIO
import io

# --- Environment Variables ---
# These are read once when the function instance starts.
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_REGION = os.environ.get("GCP_REGION")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")
FAILED_BUCKET_NAME = os.environ.get("FAILED_BUCKET_NAME")
PRODUCT_IMAGES_BUCKET_NAME = os.environ.get("PRODUCT_IMAGES_BUCKET_NAME")

def move_to_failed_bucket(storage_client, bucket_name, file_name):
    """Moves a file to the designated 'failed' bucket."""
    if not FAILED_BUCKET_NAME:
        logging.error("FAILED_BUCKET_NAME environment variable not set. Cannot move file.")
        return

    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket = storage_client.bucket(FAILED_BUCKET_NAME)
    source_blob = source_bucket.blob(file_name)

    # To move, we copy the file and then delete the original
    try:
        destination_blob = source_bucket.copy_blob(source_blob, destination_bucket, file_name)
        source_blob.delete()
        logging.info(f"Moved '{file_name}' to failed bucket: gs://{FAILED_BUCKET_NAME}/{destination_blob.name}")
    except Exception as e:
        logging.error(f"Failed to move '{file_name}' to failed bucket: {e}", exc_info=True)

@functions_framework.cloud_event
def process_csv_and_generate_content(cloud_event):
    # --- FIX: Initialize Clients within the function handler ---
    # This is the core of the fix. It ensures that for every invocation,
    # we get a new, valid client connection, preventing issues with
    # stale or broken connections on "warm starts".
    try:
        storage_client = storage.Client()
        bq_client = bigquery.Client()
        vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)
        # Add safety settings to the model to block harmful content.
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        }
        text_model = GenerativeModel("gemini-2.5-pro", safety_settings=safety_settings)
        image_model = ImageGenerationModel.from_pretrained("imagegeneration@006")
        logging.info("Successfully initialized clients and Vertex AI models for this invocation.")
    except Exception as e:
        logging.error(f"CRITICAL: Failed to initialize clients or models: {e}", exc_info=True)
        # If clients fail, we can't proceed.
        return "Initialization failed", 500

    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    logging.info(f"Triggered by file: gs://{bucket_name}/{file_name}")

    # --- File Download and Processing ---
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        csv_content = blob.download_as_text(encoding="utf-8")
        logging.info(f"Successfully downloaded '{file_name}'.")

        rows_to_insert = []
        reader = csv.reader(StringIO(csv_content))

        try:
            header = next(reader)
            logging.info(f"CSV Header: {header}")
        except StopIteration:
            raise ValueError(f"CSV file '{file_name}' is empty or has no header.")

        # --- Process Rows and Generate Content ---
        for i, row in enumerate(reader):
            if len(row) != 2:
                logging.warning(f"Skipping malformed row #{i+2} in '{file_name}': {row}")
                continue

            product_name, keywords = row[0].strip(), row[1].strip()

            if not product_name and not keywords:
                logging.info(f"Skipping empty row #{i+2} in '{file_name}'.")
                continue

            generated_image_url = None
            try:
                # 1. Generate Text
                prompt = f"Write a short, exciting marketing description for a product named '{product_name}' that is '{keywords}'. The description should be one paragraph."
                response = text_model.generate_content(prompt)

                if not response.candidates:
                    generated_text = "Content blocked by safety filter."
                    logging.warning(f"Content generation for '{product_name}' was blocked by safety filters.")
                elif hasattr(response.candidates[0].content, 'text'):
                    generated_text = response.candidates[0].content.text.strip()
                    logging.info(f"Generated content for '{product_name}'.")
                else:
                    generated_text = "Error: No content generated by model."
                    logging.warning(f"Model response for '{product_name}' lacked a 'text' attribute.")

                # 2. Generate Image (only if text was successful)
                if "Error:" not in generated_text and "Content blocked" not in generated_text:
                    try:
                        image_prompt = f"A professional, high-resolution marketing photo, studio lighting, of: {generated_text}"
                        images = image_model.generate_images(
                            prompt=image_prompt,
                            number_of_images=1,
                        )

                        # Add a check to ensure images were actually generated before proceeding.
                        if images:
                            image_bytes = images[0]._image_bytes
                            image_blob_name = f"{product_name.replace(' ', '_').lower()}_{int(datetime.utcnow().timestamp())}.png"

                            image_bucket = storage_client.bucket(PRODUCT_IMAGES_BUCKET_NAME)
                            image_blob = image_bucket.blob(image_blob_name)
                            # Use io.BytesIO to upload from memory
                            image_blob.upload_from_file(io.BytesIO(image_bytes), content_type="image/png")

                            generated_image_url = image_blob.public_url
                            logging.info(f"Successfully generated and uploaded image for '{product_name}' to {generated_image_url}")
                        else:
                            logging.warning(f"Image generation for '{product_name}' returned no images, possibly due to safety filters.")
                            generated_image_url = "Error: No image generated by model."

                    except Exception as img_e:
                        logging.error(f"Failed to generate image for '{product_name}': {img_e}", exc_info=True)
                        generated_image_url = "Error: Could not generate image."

            except Exception as e:
                logging.error(f"Failed to generate content for '{product_name}': {e}", exc_info=True)
                generated_text = "Error: Could not generate content due to an exception."
                generated_image_url = "Error: Could not generate image."

            rows_to_insert.append({
                "product_name": product_name,
                "keywords": keywords,
                "generated_content": generated_text,
                "generated_image_url": generated_image_url,
                "source_file": f"gs://{bucket_name}/{file_name}",
                "processed_at": datetime.utcnow().isoformat()
            })

        if not rows_to_insert:
            raise ValueError(f"No valid data rows were processed from '{file_name}'.")

    except Exception as e:
        # This is the main error handler. If anything goes wrong during download or processing,
        # log the error and move the file to the failed bucket.
        logging.error(f"Critical error processing file '{file_name}': {e}", exc_info=True)
        move_to_failed_bucket(storage_client, bucket_name, file_name)
        return # Stop execution for this file

    # --- Insert into BigQuery ---
    try:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if not errors:
            logging.info(f"Successfully inserted {len(rows_to_insert)} rows into BigQuery from '{file_name}'.")
        else:
            # Improved error logging for BigQuery
            error_details = [str(e) for e in errors]
            logging.error(f"Encountered errors inserting into BigQuery: {'; '.join(error_details)}")
    except Exception as e:
        logging.error(f"Failed to insert rows into BigQuery: {e}", exc_info=True)

    return "OK"
EOF
```

**Action:** Run the following command to create the `requirements.txt` file.

```bash
# Create the requirements.txt file
cat > content-generator-function/requirements.txt << EOF
# This file lists the Python packages required by the Cloud Function.
# The Google Cloud Functions environment will automatically install these
# dependencies when the function is deployed.
functions-framework>=3.0.0
google-cloud-storage>=2.0.0
google-cloud-bigquery>=3.0.0
google-cloud-aiplatform[generative_models]>=1.38.0
EOF

echo "Cloud Function source files created successfully."
```

### 3.3. Deploy the Cloud Function

Now, run this command in the Cloud Shell terminal to deploy your function. This step will take a few minutes.

```bash
# --- Deploy Cloud Function ---
# First, navigate into your function's directory
cd content-generator-function
```
```bash
# Now, deploy the function
gcloud functions deploy generate-content-from-csv \
  --gen2 \
  --runtime=python311 \
  --region=$REGION \
  --source=. \
  --entry-point=process_csv_and_generate_content \
  --trigger-bucket=$BUCKET_NAME \
  --set-env-vars=GCP_PROJECT_ID=$PROJECT_ID,GCP_REGION=$REGION,BQ_DATASET=$BQ_DATASET,BQ_TABLE=$BQ_TABLE,FAILED_BUCKET_NAME=$FAILED_BUCKET_NAME,PRODUCT_IMAGES_BUCKET_NAME=$PRODUCT_IMAGES_BUCKET_NAME \
  --memory=2Gi \
  --timeout=540s


echo "Cloud Function deployment initiated."
# Return to the parent directory
cd ..
```

---

## Section 4: Test Your AI Content Generator (Approx. 10 mins)

Let's see our creation in action!

### 4.1. Create a Sample CSV File

*   **Action:** In the Cloud Shell Editor, create a new file in your root directory named `products.csv`.
*   **Action:** Copy and paste the following sample data into `products.csv`. Feel free to add your own products!

```csv
product_name,keywords
"TerraTrek Hiking Boots","waterproof, breathable, all-terrain grip"
"PowerStack Go","portable charger, 20000mAh, fast-charging, multi-device"
"FitTrack Pro Watch","heart rate monitor, GPS, sleep tracking"
"Gourmet Grind Coffee Maker","built-in burr grinder, programmable, thermal carafe"
"RoboVac Plus","robotic vacuum, smart mapping, self-emptying"
"Voyager Travel Backpack","anti-theft, laptop compartment, water-resistant"
"PureAir Purifier","HEPA filter, quiet operation, smart sensor"
"FlexiDesk Stand","adjustable height, ergonomic, standing desk converter"
```

### 4.2. Upload the File to Cloud Storage

This is the trigger for our whole process.

```bash
# --- Upload CSV to Trigger the Function ---
gsutil cp products.csv gs://$BUCKET_NAME/

echo "Uploaded products.csv to gs://$BUCKET_NAME to trigger the function."
```

### 4.3. Verify the Results in BigQuery

Wait about a minute for the function to trigger and process the file. Then, run this query in the Cloud Shell to see the results.

```bash
# --- Query BigQuery to See Results ---
bq query --project_id=$PROJECT_ID "SELECT product_name, generated_content FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` ORDER BY processed_at DESC LIMIT 10"
```

You should see a table with your product names and the unique marketing descriptions generated by Gemini!

---

## Section 5: Implemented Feature: Content Moderation

In any application that generates content, it's crucial to ensure the output is safe and appropriate. The Vertex AI Gemini API includes built-in safety filters to help you moderate content. The Cloud Function has been automatically updated to enable these filters, preventing the generation of harmful text.

When Gemini blocks content due to a safety policy, the function will now log a warning and write a placeholder message, "Content blocked by safety filter," to BigQuery.

The `main.py` file created in Section 3 now includes this moderation logic. The key changes were:
1.  **Importing `HarmCategory` and `HarmBlockThreshold`** to define the safety rules.
2.  **Initializing the `GenerativeModel` with `safety_settings`**, which instructs the model to block content that reaches a medium or high probability of being harmful across several categories.
3.  **Checking `response.candidates`**: If this list is empty after a generation call, it confirms the content was blocked, and the function handles it gracefully.

You can now proceed to the next section to test the full pipeline, now with enhanced safety features.

---

## Section 6: Implemented Feature: Robust Error Handling

To make our pipeline more resilient, the Cloud Function has been updated to handle processing failures gracefully. If a file is uploaded that is empty, malformed, or causes any other critical error during processing, the function will now automatically move it to a separate "failed files" bucket for you to inspect later.

This prevents a single bad file from blocking the entire content generation process.

The key changes were:
1.  **Creating a `FAILED_BUCKET_NAME`**: A second Cloud Storage bucket is now created to isolate problematic files.
2.  **Passing the Bucket Name as an Environment Variable**: The deploy command was updated to pass the new bucket's name to the function.
3.  **Implementing `move_to_failed_bucket`**: A helper function was added to handle the logic of moving a file from the main bucket to the failed bucket.
4.  **Adding a Global `try...except` Block**: The core file processing logic is now wrapped in an error handler. If any exception occurs, it's caught, the error is logged, and the file is moved, ensuring the function exits cleanly.

---

## Section 7: Implemented Feature: AI-Powered Image Generation

The pipeline is now a truly multi-modal content factory. In addition to generating marketing text, the Cloud Function now calls a second powerful Vertex AI model, **Imagen**, to create a unique product image based on the text description it just wrote.

This showcases the power of chaining different AI models together to build sophisticated, automated workflows.

The key changes to implement this were:
1.  **Creating a Public Image Bucket**: A new, publicly-accessible Cloud Storage bucket is created to store the generated images so their URLs can be shared.
2.  **Adding an Image URL Column to BigQuery**: The BigQuery table schema was altered to include a `generated_image_url` column.
3.  **Initializing the `ImageGenerationModel`**: The function now initializes the `imagegeneration@006` model in addition to the Gemini text model.
4.  **Implementing the Image Generation Flow**:
    *   After text is generated, a new prompt is created for the image model.
    *   The model generates the image, which is returned as raw bytes.
    *   The function uploads these bytes to the public image bucket, creating a `.png` file.
    *   The public URL of this new image is saved to the `generated_image_url` column in BigQuery.

---

## Section 8: Conclusion & Cleanup

Congratulations! You've successfully built and tested a serverless AI application on Google Cloud. You learned how to connect several powerful services to create an intelligent, automated workflow.

### 8.1. Cleanup

To avoid ongoing charges, run these commands to delete the resources you created.

```bash
# ---- Cleanup Commands ----

# 1. Delete the Cloud Function
gcloud functions delete generate-content-from-csv --region=$REGION --gen2 --quiet

# 2. Delete the GCS Bucket (the -r and -f flags remove all contents first)
gsutil rm -r -f gs://$BUCKET_NAME

# 3. Delete the BigQuery Dataset (the -r and -f flags remove all tables first)
bq rm -r -f --dataset ${PROJECT_ID}:${BQ_DATASET}

echo "Cleanup complete. All resources have been deleted."
```

**Thank you for participating in the workshop!**
