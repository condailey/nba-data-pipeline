import boto3
import pandas as pd
import json

s3_client = boto3.client('s3')

# List all raw JSON files in the 2024-25 season folder
response = s3_client.list_objects_v2(Bucket='nba-data-pipeline-raw', Prefix='2024-25/')

obj_keys = []

for obj in response['Contents']:
    obj_keys.append(obj['Key'])

while response['IsTruncated'] == True:
    response = s3_client.list_objects_v2(Bucket='nba-data-pipeline-raw', Prefix='2024-25/', ContinuationToken=response['NextContinuationToken'])
    for obj in response['Contents']:                                                    
      obj_keys.append(obj['Key']) 

# Read each JSON file from S3 and extract the play-by-play actions into DataFrames
df_list = []

for key in obj_keys:
    obj = s3_client.get_object(
            Bucket="nba-data-pipeline-raw",
            Key=key,
    )
    json_data = obj['Body'].read()
    data = json.loads(json_data)
    temp_df = pd.DataFrame(data['game']['actions'])
    temp_df['gameId'] = data['game']['gameId']
    df_list.append(temp_df)

# Combine all games into a single DataFrame
final_df = pd.concat(df_list, ignore_index=True)

# Select only the columns needed for analysis
cleaned_columns = final_df[['gameId', 'clock', 'period', 'teamId', 'personId', 'playerName', 'xLegacy', 'yLegacy', 'shotDistance', 'shotResult',
                       'isFieldGoal', 'scoreHome', 'scoreAway', 'pointsTotal', 'location', 'description', 'actionType', 'shotValue', 'actionId']].copy()

# Fill empty actionType for blocks and steals (API leaves these blank)
cleaned_columns.loc[(cleaned_columns['actionType'] == '') & (cleaned_columns['description'].str.contains('BLOCK')), 'actionType'] = 'Block'
cleaned_columns.loc[(cleaned_columns['actionType'] == '') & (cleaned_columns['description'].str.contains('STEAL')), 'actionType'] = 'Steal'

# Convert clock from ISO duration (PT12M00.00S) to readable format (12:00.00)
cleaned_columns['clock'] = cleaned_columns['clock'].str.replace('PT', '').str.replace('M', ':').str.replace('S', '')

# Forward-fill scores within each game so every row shows the current score
# First convert empty strings to NaN so ffill works, then fill remaining NaN with 0
cleaned_columns[['scoreHome', 'scoreAway']] = cleaned_columns[['scoreHome',
  'scoreAway']].replace('', pd.NA)
cleaned_columns[['scoreHome', 'scoreAway']] = cleaned_columns.groupby('gameId')[['scoreHome', 'scoreAway']].ffill()
cleaned_columns[['scoreHome', 'scoreAway']] = cleaned_columns[['scoreHome','scoreAway']].fillna('0')

# Convert scores to integers for Postgres
cleaned_columns[['scoreHome', 'scoreAway']] = cleaned_columns[['scoreHome',
  'scoreAway']].astype(int)

# Export cleaned data to CSV for loading into Postgres
cleaned_columns.to_csv('output.csv', index=False)