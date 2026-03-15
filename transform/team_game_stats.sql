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
FROM bronze.raw_team_game_logs
ORDER BY game_date
LIMIT 10;