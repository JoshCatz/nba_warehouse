SELECT * 
FROM bronze.raw_player_game_logs 
WHERE player_name LIKE 'Victor%'
    AND season_year = '2025-26'
ORDER BY pts DESC
LIMIT 10;