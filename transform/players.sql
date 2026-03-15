SELECT DISTINCT 
    rp.player_id, 
    rp.player_name, 
    rp.is_active
FROM bronze.raw_players rp
JOIN bronze.raw_player_game_logs rpgl
ON rp.player_id = rpgl.player_id;
