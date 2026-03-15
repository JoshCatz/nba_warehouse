DROP TABLE IF EXISTS silver.players;
DROP TABLE IF EXISTS silver.games;
DROP TABLE IF EXISTS silver.team_game_stats;
DROP TABLE IF EXISTS silver.player_game_stats;

CREATE TABLE silver.players AS
    SELECT DISTINCT 
        rp.player_id, 
        rp.player_name, 
        rp.is_active
    FROM bronze.raw_players rp
    JOIN bronze.raw_player_game_logs rpgl
    ON rp.player_id = rpgl.player_id;


CREATE TABLE silver.games AS 
    SELECT DISTINCT
        game_id,
        game_date,
        season_id
    FROM bronze.raw_team_game_logs;


CREATE TABLE silver.team_game_stats AS 
    SELECT 
        game_id,
        team_id,
        team_abbreviation,
        team_name,
        game_date,
        matchup,
        wl, 
        pts,
        fgm, fga, fg_pct,
        fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct,
        oreb, dreb, reb,
        ast,
        stl,
        blk,
        tov, 
        pf,
        plus_minus
    FROM bronze.raw_team_game_logs;

CREATE TABLE silver.player_game_stats AS
    SELECT 
        season_year::CHAR(7) AS season_year, 
        player_id, 
        player_name,
        team_id, 
        team_abbreviation,
        team_name,
        game_id,
        game_date::DATE AS game_date,
        matchup,
        wl,
        minutes,
        pts,
        fgm, fga, fg_pct,
        fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct,
        oreb, dreb, reb,
        ast,
        stl,
        blk, 
        tov,
        plus_minus
    FROM bronze.raw_player_game_logs;

ALTER TABLE silver.players ADD PRIMARY KEY (player_id);
ALTER TABLE silver.games ADD PRIMARY KEY (game_id);
ALTER TABLE silver.team_game_stats ADD PRIMARY KEY (game_id, team_id);
ALTER TABLE silver.player_game_stats ADD PRIMARY KEY (player_id, game_id);