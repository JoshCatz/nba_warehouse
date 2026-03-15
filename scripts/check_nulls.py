import pandas as pd
import psycopg2
import os
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

conn, cur = get_connection()

df = pd.read_sql("SELECT * FROM silver.team_game_stats", conn)

print(df.isnull().sum())