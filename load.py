import psycopg2
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

# Truncate table and bulk load CSV using Postgres COPY (faster than row-by-row inserts)
cur.execute("TRUNCATE nba_data;")

with open('output.csv', 'r') as f:
    cur.copy_expert("COPY nba_data FROM STDIN WITH CSV HEADER", f)

conn.commit()                                                                                         
cur.close()     
conn.close()