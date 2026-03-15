SELECT *
FROM silver.team_game_stats
WHERE
    fg_pct = 'NaN'::numeric
    OR fg3_pct = 'NaN'::numeric
    OR ft_pct = 'NaN'::numeric;