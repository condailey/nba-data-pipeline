"""Transform raw play-by-play JSON from S3 into a cleaned CSV."""

import boto3
import pandas as pd
import json

s3_client = boto3.client('s3')

BUCKET = 'nba-data-pipeline-raw'
OUTPUT_PATH = '/tmp/output.csv'
COLUMNS = [
    'gameId', 'clock', 'period', 'teamId', 'personId', 'playerName',
    'xLegacy', 'yLegacy', 'shotDistance', 'shotResult', 'isFieldGoal',
    'scoreHome', 'scoreAway', 'pointsTotal', 'location', 'description',
    'actionType', 'shotValue', 'actionId'
]


def transform(game_keys):
    """Read raw JSON from S3, clean the data, and write to CSV."""
    df_list = []

    for key in game_keys:
        obj = s3_client.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(obj['Body'].read())
        df = pd.DataFrame(data['game']['actions'])
        df['gameId'] = data['game']['gameId']
        df_list.append(df)

    final_df = pd.concat(df_list, ignore_index=True)
    cleaned = final_df[COLUMNS].copy()

    # Fill missing actionType for blocks and steals (API leaves these blank)
    cleaned.loc[(cleaned['actionType'] == '') & (cleaned['description'].str.contains('BLOCK')), 'actionType'] = 'Block'
    cleaned.loc[(cleaned['actionType'] == '') & (cleaned['description'].str.contains('STEAL')), 'actionType'] = 'Steal'

    # Convert clock from ISO duration (PT12M00.00S) to readable format (12:00.00)
    cleaned['clock'] = cleaned['clock'].str.replace('PT', '').str.replace('M', ':').str.replace('S', '')

    # Forward-fill scores so every row has the current score
    cleaned[['scoreHome', 'scoreAway']] = cleaned[['scoreHome', 'scoreAway']].replace('', pd.NA)
    cleaned[['scoreHome', 'scoreAway']] = cleaned.groupby('gameId')[['scoreHome', 'scoreAway']].ffill()
    cleaned[['scoreHome', 'scoreAway']] = cleaned[['scoreHome', 'scoreAway']].fillna('0').astype(int)

    cleaned.to_csv(OUTPUT_PATH, index=False)
