DROP TABLE IF EXISTS bronze.raw_players;
DROP TABLE IF EXISTS bronze.raw_teams;
DROP TABLE IF EXISTS bronze.raw_team_game_logs;
DROP TABLE IF EXISTS bronze.raw_player_game_logs;

CREATE TABLE bronze.raw_players (
    player_id INT,
    player_name VARCHAR(64),
    is_active BOOLEAN,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bronze.raw_teams (
    team_id INT,
    team_name VARCHAR(64),
    abbreviation CHAR(3),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bronze.raw_team_game_logs (
    season_id CHAR(5),
    team_id INT,
    team_abbreviation CHAR(3),
    team_name VARCHAR (64),
    game_id CHAR(10),
    game_date CHAR(10),
    matchup VARCHAR(16),
    wl VARCHAR(10),
    minutes INT,
    pts INT,
    fgm INT,
    fga INT,
    fg_pct NUMERIC(5,3),
    fg3m INT,
    fg3a INT,
    fg3_pct NUMERIC(5,3),
    ftm INT,
    fta INT,
    ft_pct NUMERIC(5,3),
    oreb INT,
    dreb INT,
    reb INT,
    ast INT,
    stl INT,
    blk INT,
    tov INT,
    pf INT,
    plus_minus NUMERIC(5,3),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bronze.raw_player_game_logs (
    season_year VARCHAR(16),
    player_id INT,
    player_name VARCHAR(64),
    team_id INT,
    team_abbreviation CHAR(3),
    team_name VARCHAR(64),
    game_id CHAR(10),
    game_date DATE,
    matchup VARCHAR(16),
    wl VARCHAR(10),
    minutes INT,
    fgm INT,
    fga INT,
    fg_pct NUMERIC(5,3),
    fg3m INT,
    fg3a INT,
    fg3_pct NUMERIC(5,3),
    ftm INT,
    fta INT,
    ft_pct NUMERIC(5,3),
    oreb INT,
    dreb INT,
    reb INT,
    ast INT,
    stl INT,
    blk INT,
    tov INT,
    pf INT,
    pts INT,
    plus_minus NUMERIC(5,3),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (player_id, game_id)
);