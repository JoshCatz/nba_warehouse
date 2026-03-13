from nba_api.stats.static import teams
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

teamDF = pd.DataFrame(teams.get_teams())
print(teamDF)

team_list = teamDF[['id', 'full_name', 'abbreviation']]

records = [tuple(x) for x in team_list.to_numpy()]

conn, cur= get_connection()

query = """
    INSERT INTO bronze.raw_teams (team_id, team_name, abbreviation)
    VALUES (%s, %s, %s)
"""

cur.executemany(query, records)
conn.commit()

cur.close()
conn.close()





