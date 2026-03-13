from nba_api.stats.endpoints import leaguegamefinder
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

conn, cur = get_connection()

seasons = ['2019-20', '2020-21', '2021-22', '2022-23', '2024-25', '2025-26']

all_games = []
for season in seasons:
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    season_games = gamefinder.get_data_frames()[0]
    all_games.append(season_games)

games = pd.concat(all_games, ignore_index=True)

games_list = games[['SEASON_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL', 'MIN', 'PTS', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PLUS_MINUS']]
records = [tuple(x) for x in games_list.to_numpy()]

query = """
    INSERT INTO bronze.raw_team_game_logs (season_id, team_id, team_abbreviation, team_name, game_id, game_date, matchup, wl, minutes, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, plus_minus)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

cur.executemany(query, records)
conn.commit()

cur.close()
conn.close()
