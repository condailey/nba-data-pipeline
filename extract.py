"""Extract NBA play-by-play data from the NBA API and upload to S3."""

from nba_api.stats.endpoints import playbyplayv3, leaguegamefinder
import boto3
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')

BUCKET = 'nba-data-pipeline-raw'
SEASONS = ['2024-25', '2025-26']


def extract():
    """Pull play-by-play JSON for each game and upload to S3. Skips games already in S3."""
    s3 = boto3.client('s3')
    new_game_list = []

    for season in SEASONS:
        game_ids = _get_game_ids(season)
        logging.info(f"Found {len(game_ids)} games for {season}")

        for i, game_id in enumerate(game_ids):
            # Skip games already extracted
            try:
                s3.head_object(Bucket=BUCKET, Key=f'{season}/{game_id}.json')
                continue
            except s3.exceptions.ClientError:
                pass

            try:
                pbp = playbyplayv3.PlayByPlayV3(game_id=game_id)
                s3.put_object(
                    Bucket=BUCKET,
                    Key=f'{season}/{game_id}.json',
                    Body=pbp.get_json()
                )
                new_game_list.append(f'{season}/{game_id}.json')
                logging.info(f"Extracted game {i + 1}/{len(game_ids)} ({game_id})")
                time.sleep(1)
            except Exception as e:
                logging.error(f"Failed to extract game {game_id}: {e}")

    return new_game_list


def _get_game_ids(season):
    """Fetch unique game IDs for a given NBA season."""
    gamefinder = leaguegamefinder.LeagueGameFinder(
        season_nullable=season,
        league_id_nullable='00',
        season_type_nullable='Regular Season'
    )
    games = gamefinder.get_data_frames()[0]
    return sorted(games['GAME_ID'].unique())


if __name__ == '__main__':
    extract()
