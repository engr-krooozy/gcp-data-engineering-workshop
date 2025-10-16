import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import argparse
import logging
import json
from datetime import datetime
import io

from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
from vertexai.preview.vision_models import ImageGenerationModel

# This DoFn class encapsulates the logic to call the Vertex AI APIs
class AnalyzeArticle(beam.DoFn):
    def __init__(self, project_id, region, sentiment_images_bucket_name):
        self.project_id = project_id
        self.region = region
        self.sentiment_images_bucket_name = sentiment_images_bucket_name

    def setup(self):
        # Initialize the Vertex AI clients. This is done once per worker.
        vertexai.init(project=self.project_id, location=self.region)
        self.text_model = GenerativeModel("gemini-1.0-pro", safety_settings={
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
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True, help='Pub/Sub subscription to read from.')
    parser.add_argument('--output_table', required=True, help='BigQuery table to write to.')
    parser.add_argument('--sentiment_images_bucket_name', required=True, help='GCS bucket for sentiment images.')

    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)

    # Define and run the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (p | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
           | 'Decode JSON' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
           | 'Analyze Article' >> beam.ParDo(AnalyzeArticle(
               project_id=gcp_options.project,
               region=gcp_options.region,
               sentiment_images_bucket_name=known_args.sentiment_images_bucket_name
            ))
           | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
               known_args.output_table,
               schema='article_id:STRING,headline:STRING,summary:STRING,sentiment:STRING,sentiment_chart_url:STRING,processed_at:TIMESTAMP',
               write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
               create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
           )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
