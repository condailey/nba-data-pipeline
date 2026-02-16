from nba_api.stats.endpoints import playbyplayv3, leaguegamefinder
import boto3
import time

# Query all regular season games for 2024-25 (league_id '00' = NBA)
gamefinder = leaguegamefinder.LeagueGameFinder(
    season_nullable='2024-25',
    league_id_nullable='00',
    season_type_nullable='Regular Season'
)

# Returns one row per team per game, so each game_id appears twice
games = gamefinder.get_data_frames()[0]

sorted_games_id = games.sort_values("GAME_ID")

# Deduplicate so each game_id appears once
unique_sorted_games_id = sorted_games_id["GAME_ID"].unique()

# Limit to 5 games for testing
unique_sorted_five_games_id = unique_sorted_games_id[0:5]

s3 = boto3.client('s3')

# Pull play-by-play for each game and upload raw JSON to S3
for game in unique_sorted_five_games_id:
    pbp = playbyplayv3.PlayByPlayV3(game_id=f'{game}')
    pbpJSON = pbp.get_json()
    s3.put_object(
        Bucket="nba-data-pipeline-raw",
        Key=f'2024-25/{game}.json',
        Body=pbpJSON
    )
    time.sleep(1)  # Rate limit