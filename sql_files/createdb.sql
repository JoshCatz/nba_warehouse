-- CREATE DATABASE nba_warehouse;

DROP TABLE IF EXISTS fact_player_game_stats;
DROP TABLE IF EXISTS dim_game;
DROP TABLE IF EXISTS dim_season;
DROP TABLE IF EXISTS dim_team;
DROP TABLE IF EXISTS dim_player;

CREATE TABLE dim_player (
    player_sk SERIAL PRIMARY KEY,
    player_id INT NOT NULL UNIQUE,
    player_name VARCHAR(64) NOT NULL,
    is_active INT
);

CREATE TABLE dim_team (
    team_sk SERIAL PRIMARY KEY,
    team_id INT NOT NULL UNIQUE,
    team_name VARCHAR(64) NOT NULL,
    team_abbr CHAR(3) NOT NULL
);

CREATE TABLE dim_season (
    season_sk SERIAL PRIMARY KEY,
    season_id INT NOT NULL UNIQUE,
    season CHAR(7) NOT NULL
);

CREATE TABLE dim_game (
    game_sk SERIAL PRIMARY KEY,
    game_id INT NOT NULL UNIQUE,
    season_sk INT NOT NULL REFERENCES dim_season(season_sk),
    home_team_sk INT NOT NULL REFERENCES dim_team(team_sk),
    away_team_sk INT NOT NULL REFERENCES dim_team(team_sk),
    game_date DATE NOT NULL,
    home_score INT NOT NULL,
    away_score INT NOT NULL
);

CREATE TABLE fact_player_game_stats (
    id SERIAL PRIMARY KEY,
    game_sk INT NOT NULL REFERENCES dim_game(game_sk),
    player_sk INT NOT NULL REFERENCES dim_player(player_sk),
    team_sk INT NOT NULL REFERENCES dim_team(team_sk),
    pts INT NOT NULL DEFAULT 0,
    ast INT NOT NULL DEFAULT 0,
    reb INT NOT NULL DEFAULT 0,
    off_reb INT NOT NULL DEFAULT 0,
    def_reb INT NOT NULL DEFAULT 0,
    mins numeric NOT NULL DEFAULT 0,
    fg2a INT NOT NULL DEFAULT 0,
    fg2m INT NOT NULL DEFAULT 0,
    fg3a INT NOT NULL DEFAULT 0,
    fg3m INT NOT NULL DEFAULT 0,
    fga INT NOT NULL DEFAULT 0,
    fgm INT NOT NULL DEFAULT 0,
    turnover INT NOT NULL DEFAULT 0,
    stl INT NOT NULL DEFAULT 0,
    blk INT NOT NULL DEFAULT 0,
    pf INT NOT NULL DEFAULT 0,
    plus_minus INT NOT NULL DEFAULT 0,
    UNIQUE (game_sk, player_sk)
);