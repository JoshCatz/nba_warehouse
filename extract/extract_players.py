from nba_api.stats.static import players
import pandas as pd
import psycopg2, os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    conn = None

    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_DATABASE'),
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT')
        )
        print(f"CONNECTED TO DATABASE: {os.getenv('DB_DATABASE')}")
    except Exception as e:
        print(f"Error connecting to database: {e}")

    return conn, conn.cursor()

playerDF = pd.DataFrame(players.get_players())

players_list = playerDF[['id', 'full_name', 'is_active']]

records = [tuple(x) for x in players_list.to_numpy()]

conn, cur= get_connection()

query = """
    INSERT INTO bronze.raw_players (player_id, player_name, is_active)
    VALUES (%s, %s, %s)
"""

cur.executemany(query, records)
conn.commit()

cur.close()
conn.close()


