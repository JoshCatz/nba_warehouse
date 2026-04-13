CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.player_season_summary;
DROP TABLE IF EXISTS gold.team_season_summary;
DROP TABLE IF EXISTS gold.player_recent_form;

CREATE TABLE gold.player_season_summary AS
SELECT
    pgs.season_year,
    pgs.player_id,
    pgs.player_name,
    COUNT(*) AS games_played,

    ROUND(AVG(pgs.minutes), 2) AS avg_minutes,
    ROUND(AVG(pgs.pts), 2) AS avg_pts,
    ROUND(AVG(pgs.reb), 2) AS avg_reb,
    ROUND(AVG(pgs.ast), 2) AS avg_ast,
    ROUND(AVG(pgs.stl), 2) AS avg_stl,
    ROUND(AVG(pgs.blk), 2) AS avg_blk,
    ROUND(AVG(pgs.tov), 2) AS avg_tov,
    ROUND(AVG(pgs.plus_minus), 2) AS avg_plus_minus,

    SUM(pgs.pts) AS total_pts,
    SUM(pgs.reb) AS total_reb,
    SUM(pgs.ast) AS total_ast,
    SUM(pgs.stl) AS total_stl,
    SUM(pgs.blk) AS total_blk,
    SUM(pgs.tov) AS total_tov,

    SUM(pgs.fgm) AS total_fgm,
    SUM(pgs.fga) AS total_fga,
    CASE
        WHEN SUM(pgs.fga) = 0 THEN NULL
        ELSE ROUND(SUM(pgs.fgm)::NUMERIC / SUM(pgs.fga), 4)
    END AS season_fg_pct,

    SUM(pgs.fg3m) AS total_fg3m,
    SUM(pgs.fg3a) AS total_fg3a,
    CASE
        WHEN SUM(pgs.fg3a) = 0 THEN NULL
        ELSE ROUND(SUM(pgs.fg3m)::NUMERIC / SUM(pgs.fg3a), 4)
    END AS season_fg3_pct,

    SUM(pgs.ftm) AS total_ftm,
    SUM(pgs.fta) AS total_fta,
    CASE
        WHEN SUM(pgs.fta) = 0 THEN NULL
        ELSE ROUND(SUM(pgs.ftm)::NUMERIC / SUM(pgs.fta), 4)
    END AS season_ft_pct

FROM silver.player_game_stats pgs
GROUP BY
    pgs.season_year,
    pgs.player_id,
    pgs.player_name;

ALTER TABLE gold.player_season_summary
    ADD PRIMARY KEY (season_year, player_id);


CREATE TABLE gold.team_season_summary AS
SELECT
    tgs.season_year,
    tgs.team_id,
    tgs.team_name,
    tgs.team_abbreviation,

    COUNT(*) AS games_played,
    SUM(CASE WHEN tgs.wl = 'W' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN tgs.wl = 'L' THEN 1 ELSE 0 END) AS losses,

    CASE
        WHEN COUNT(*) = 0 THEN NULL
        ELSE ROUND(SUM(CASE WHEN tgs.wl = 'W' THEN 1 ELSE 0 END)::NUMERIC / COUNT(*), 4)
    END AS win_pct,

    ROUND(AVG(tgs.pts), 2) AS avg_pts,
    ROUND(AVG(tgs.reb), 2) AS avg_reb,
    ROUND(AVG(tgs.ast), 2) AS avg_ast,
    ROUND(AVG(tgs.stl), 2) AS avg_stl,
    ROUND(AVG(tgs.blk), 2) AS avg_blk,
    ROUND(AVG(tgs.tov), 2) AS avg_tov,
    ROUND(AVG(tgs.plus_minus), 2) AS avg_plus_minus,

    SUM(tgs.fgm) AS total_fgm,
    SUM(tgs.fga) AS total_fga,
    CASE
        WHEN SUM(tgs.fga) = 0 THEN NULL
        ELSE ROUND(SUM(tgs.fgm)::NUMERIC / SUM(tgs.fga), 4)
    END AS season_fg_pct,

    SUM(tgs.fg3m) AS total_fg3m,
    SUM(tgs.fg3a) AS total_fg3a,
    CASE
        WHEN SUM(tgs.fg3a) = 0 THEN NULL
        ELSE ROUND(SUM(tgs.fg3m)::NUMERIC / SUM(tgs.fg3a), 4)
    END AS season_fg3_pct,

    SUM(tgs.ftm) AS total_ftm,
    SUM(tgs.fta) AS total_fta,
    CASE
        WHEN SUM(tgs.fta) = 0 THEN NULL
        ELSE ROUND(SUM(tgs.ftm)::NUMERIC / SUM(tgs.fta), 4)
    END AS season_ft_pct

FROM silver.team_game_stats tgs
GROUP BY
    tgs.season_year,
    tgs.team_id,
    tgs.team_name,
    tgs.team_abbreviation;

ALTER TABLE gold.team_season_summary
    ADD PRIMARY KEY (season_year, team_id);


CREATE TABLE gold.player_recent_form AS
WITH ranked_games AS (
    SELECT
        pgs.season_year,
        pgs.player_id,
        pgs.player_name,
        pgs.game_date,
        pgs.pts,
        pgs.reb,
        pgs.ast,
        ROW_NUMBER() OVER (
            PARTITION BY pgs.season_year, pgs.player_id
            ORDER BY pgs.game_date DESC, pgs.game_id DESC
        ) AS rn
    FROM silver.player_game_stats pgs
),
last_5 AS (
    SELECT
        season_year,
        player_id,
        player_name,
        MAX(game_date) AS last_game_date,
        ROUND(AVG(pts), 2) AS last_5_avg_pts,
        ROUND(AVG(reb), 2) AS last_5_avg_reb,
        ROUND(AVG(ast), 2) AS last_5_avg_ast
    FROM ranked_games
    WHERE rn <= 5
    GROUP BY season_year, player_id, player_name
),
last_10 AS (
    SELECT
        season_year,
        player_id,
        player_name,
        ROUND(AVG(pts), 2) AS last_10_avg_pts,
        ROUND(AVG(reb), 2) AS last_10_avg_reb,
        ROUND(AVG(ast), 2) AS last_10_avg_ast
    FROM ranked_games
    WHERE rn <= 10
    GROUP BY season_year, player_id, player_name
)
SELECT
    l5.season_year,
    l5.player_id,
    l5.player_name,
    l5.last_game_date,
    l5.last_5_avg_pts,
    l5.last_5_avg_reb,
    l5.last_5_avg_ast,
    l10.last_10_avg_pts,
    l10.last_10_avg_reb,
    l10.last_10_avg_ast
FROM last_5 l5
LEFT JOIN last_10 l10
    ON l5.season_year = l10.season_year
   AND l5.player_id = l10.player_id;

ALTER TABLE gold.player_recent_form
    ADD PRIMARY KEY (season_year, player_id);

CREATE INDEX IF NOT EXISTS idx_gold_player_season_summary_player
    ON gold.player_season_summary (player_id);

CREATE INDEX IF NOT EXISTS idx_gold_team_season_summary_team
    ON gold.team_season_summary (team_id);

CREATE INDEX IF NOT EXISTS idx_gold_player_recent_form_player
    ON gold.player_recent_form (player_id);