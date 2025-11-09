import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import logging
import json
from datetime import datetime
from apache_beam.transforms.window import FixedWindows, SlidingWindows, TimestampedValue
from apache_beam.transforms.combiners import Mean
from apache_beam.transforms.userstate import BagStateSpec

# --- Custom Pipeline Options ---
class StockAnalysisPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_subscription', required=True)
        parser.add_argument('--output_table', required=True)

# --- Stateful DoFn for Anomaly Detection ---
class DetectVolumeSpike(beam.DoFn):
    VOLUME_HISTORY = BagStateSpec('volume_history')

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
                  | 'Calculate 5-Min SMA' >> beam.CombinePerKey(Mean.Globally())
                  | 'Format SMA' >> beam.Map(lambda kv: (kv[0], {'sma_5m': kv[1]})))

        # Join all metrics
        joined_data = (
            {'agg_1m': agg_1m, 'sma_5m': sma_5m}
            | 'Join Metrics' >> beam.CoGroupByKey()
            | 'Merge Metrics' >> beam.Map(lambda kv: (kv[0], {**kv[1]['agg_1m'][0], **kv[1]['sma_5m'][0]})))

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
