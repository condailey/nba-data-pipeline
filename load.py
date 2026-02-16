import psycopg2
import pandas as pd
import os
import dotenv

dotenv.load_dotenv()

# Load database credentials from .env
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# Connect to Postgres on RDS
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, password=DB_PASSWORD, port=DB_PORT)
cur = conn.cursor()

# Read transformed CSV and insert each row into the nba_data table
df = pd.read_csv('output.csv')
for index, row in df.iterrows():
    cur.execute("""
                INSERT INTO NBA_DATA (gameId, clock, period, teamId, personId, playerName, xLegacy, yLegacy, shotDistance, shotResult, isFieldGoal, scoreHome, scoreAway, pointsTotal, location, description, actionType, shotValue, actionId)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,(row['gameId'], row['clock'], row['period'], row['teamId'], row['personId'], row['playerName'],
                     row['xLegacy'], row['yLegacy'], row['shotDistance'], row['shotResult'], row['isFieldGoal'],
                     row['scoreHome'], row['scoreAway'], row['pointsTotal'], row['location'], row['description'],
                     row['actionType'], row['shotValue'], row['actionId']))

conn.commit()
cur.close()
conn.close()