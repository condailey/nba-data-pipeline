"""AWS Lambda handler — transforms and loads NBA data from S3 into Postgres."""

import boto3
import logging
from transform import transform
from load import load

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')

BUCKET = 'nba-data-pipeline-raw'
BATCH_SIZE = 100


def lambda_handler(event, context):
    """List all games in S3, then transform and load in batches."""
    s3 = boto3.client('s3')

    # Collect all game file keys from S3
    game_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                game_keys.append(obj['Key'])

    logging.info(f"Found {len(game_keys)} games to process")

    # Full refresh: truncate once, then load each batch
    load(truncate=True)

    for i in range(0, len(game_keys), BATCH_SIZE):
        batch = game_keys[i:i + BATCH_SIZE]
        logging.info(f"Processing batch {i // BATCH_SIZE + 1} ({len(batch)} games)")
        transform(batch)
        load(truncate=False)

    logging.info("Done")
