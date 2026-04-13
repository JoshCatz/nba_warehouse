-- SELECT DISTINCT
--     game_id,
--     game_date,
--     season_id
-- FROM bronze.raw_team_game_logs;

SELECT DISTINCT season_id
FROM bronze.raw_team_game_logs
ORDER BY season_id;
