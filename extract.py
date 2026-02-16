from nba_api.stats.endpoints import playbyplayv3, leaguegamefinder
import boto3
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')

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

s3 = boto3.client('s3')

while True:

    success = 0
    failure = 0

    # Pull play-by-play for each game and upload raw JSON to S3
    for i, game in enumerate(unique_sorted_games_id):
        try:
            s3.head_object(Bucket="nba-data-pipeline-raw", Key=f'2024-25/{game}.json')
            continue
        except s3.exceptions.ClientError:
            pass
        try:
            pbp = playbyplayv3.PlayByPlayV3(game_id=f'{game}')
            pbpJSON = pbp.get_json()
            s3.put_object(
                Bucket="nba-data-pipeline-raw",
                Key=f'2024-25/{game}.json',
                Body=pbpJSON
            )
            logging.info(f"Extracted game {i + 1}/{len(unique_sorted_games_id)} ({game})")
            success += 1
            failure = 0
            time.sleep(2)  # Rate limit
        except Exception as e:
            logging.error(f"Failed to fetch data on game {game}: {e}")
            failure += 1
            if failure >= 10:
                logging.error(f"Reached {failure} consecutive failures. Killing script.")
                break
    else:
        break

    logging.info("Cooling down for 5 minutes before retrying")
    time.sleep(300)