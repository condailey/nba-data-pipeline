"""Load transformed CSV data into PostgreSQL on RDS."""

import psycopg2
import os

OUTPUT_PATH = '/tmp/output.csv'


def load(truncate=False):
    """Load CSV into the nba_data table. Optionally truncate the table first."""
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        host=os.environ['DB_HOST'],
        password=os.environ['DB_PASSWORD'],
        port=os.environ['DB_PORT']
    )
    cur = conn.cursor()

    if truncate:
        cur.execute("TRUNCATE nba_data")
        conn.commit()
        cur.close()
        conn.close()
        return

    with open(OUTPUT_PATH, 'r') as f:
        cur.copy_expert("COPY nba_data FROM STDIN WITH CSV HEADER", f)

    conn.commit()
    cur.close()
    conn.close()
