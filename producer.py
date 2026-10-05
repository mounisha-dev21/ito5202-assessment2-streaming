"""
Kafka producer for ITO5202 Assessment 2.

Reads the held-out parquet data and sends it to Kafka in small batches
to imitate new order items arriving over time.

Example:
    python producer.py --batch-size 500
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer


# Time to wait between each batch
BATCH_INTERVAL_SECONDS = 5

# These columns can contain nulls (want to keep them nullable)
NULLABLE_INT_COLUMNS = [
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]


# Basic console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)

log = logging.getLogger("producer")


def parse_args():
    # Read options passed in from the command line
    parser = argparse.ArgumentParser(
        description="Send the saved streaming data to Kafka."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of records sent in each batch"
    )

    parser.add_argument(
        "--topic",
        default="events",
        help="Kafka topic to send records to"
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka broker address"
    )

    parser.add_argument(
        "--input",
        default="data/stream_data.parquet",
        help="Path to the streaming parquet data"
    )

    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Optional limit on the number of batches sent"
    )

    return parser.parse_args()


def load_stream_data(path):
    # Load the held-out records
    df = pd.read_parquet(path)

    # Put orders back into purchase-time order
    df = df.sort_values(
        [
            "order_purchase_timestamp",
            "order_id",
            "order_item_id"
        ]
    )

    df = df.reset_index(drop=True)

    # Fix integer columns that pandas may read as floats
    for column in NULLABLE_INT_COLUMNS:
        df[column] = df[column].astype("Int64")

    return df


def main():
    args = parse_args()

    # Load the records that will be replayed
    df = load_stream_data(args.input)

    total_records = len(df)

    # Work out how many batches are needed
    total_batches = total_records // args.batch_size

    if total_records % args.batch_size != 0:
        total_batches += 1

    # Optionally stop early for testing
    if args.max_batches is not None:
        total_batches = min(
            total_batches,
            args.max_batches
        )

    log.info(
        "Loaded %d records from %s",
        total_records,
        args.input
    )

    log.info(
        "Topic: %s | Batch size: %d | Batches: %d | Interval: %ds",
        args.topic,
        args.batch_size,
        total_batches,
        BATCH_INTERVAL_SECONDS
    )


    # Connect to Kafka
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers
    )

    sent = 0


    try:
        # Send one batch at a time
        for batch_num in range(total_batches):

            start = batch_num * args.batch_size
            end = start + args.batch_size

            batch = df.iloc[start:end].copy()


            # Add the time this batch is published
            event_time = datetime.now(
                timezone.utc
            ).isoformat(timespec="milliseconds")

            batch["event_timestamp"] = event_time


            # Convert the batch to JSON
            json_text = batch.to_json(
                orient="records",
                date_format="iso"
            )

            payload = json_text.encode("utf-8")


            # Send the whole batch as one Kafka message
            producer.send(
                args.topic,
                value=payload
            )

            producer.flush()

            sent += len(batch)


            # Print progress after each batch
            log.info(
                "Batch %d/%d | records: %d | event_timestamp: %s | size: %.1f KB | total sent: %d",
                batch_num + 1,
                total_batches,
                len(batch),
                event_time,
                len(payload) / 1024,
                sent
            )


            # Wait before sending the next batch
            if batch_num < total_batches - 1:
                time.sleep(BATCH_INTERVAL_SECONDS)


    except KeyboardInterrupt:
        log.warning(
            "Stopped by user after %d records",
            sent
        )


    finally:
        # Close the Kafka connection cleanly
        producer.close()

        log.info(
            "Producer closed. Total records sent: %d",
            sent
        )


if __name__ == "__main__":
    main()