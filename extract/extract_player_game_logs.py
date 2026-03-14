from nba_api.stats.endpoints import playergamelogs
import pandas as pd
import psycopg2, os
from dotenv import load_dotenv
import time

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
        try:
            player_game = playergamelogs.PlayerGameLogs(
                season_nullable=season,
                season_type_nullable='Regular Season'
            )

            df = player_game.get_data_frames()[0]

            if not df.empty:
                all_games.append(df)

        except Exception as e:
            print(f"Failed for season {season}: {e}")

        time.sleep(0.6)

if all_games:
    gamesDF = pd.concat(all_games, ignore_index=True)

    games_list = gamesDF[
        [
            'SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'WL',
            'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT',
            'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF',
            'PTS', 'PLUS_MINUS'
            ]
    ]

    records = [tuple(x) for x in games_list.to_numpy()]

    query = """
        INSERT INTO bronze.raw_player_game_logs (
            season_year, player_id, player_name, team_id, team_abbreviation, team_name, game_id, game_date, matchup, wl, minutes,
            fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast,
            stl, blk, tov, pf, pts, plus_minus
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.executemany(query, records)
    conn.commit()

cur.close()
conn.close()