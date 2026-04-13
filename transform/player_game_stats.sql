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
FROM bronze.raw_player_game_logs
WHERE player_name LIKE '%Wembanyama%'
ORDER BY game_date, game_id, team_name
LIMIT 100;