-- ============================================================
-- Defensive Stats View for baseball.computer
-- ============================================================
-- This SQL creates a comprehensive defensive statistics view
-- showing player fielding stats by season, team, and position.
--
-- Run this with: duckdb baseball.duckdb < create_defensive_stats_view.sql
-- Or interactively: duckdb baseball.duckdb
--   > .read create_defensive_stats_view.sql
--   > SELECT * FROM defensive_stats LIMIT 10;
-- ============================================================

-- Drop existing view/table if exists
DROP TABLE IF EXISTS defensive_stats;
DROP VIEW IF EXISTS defensive_stats;

-- ============================================================
-- Create the defensive_stats table with comprehensive fielding data
-- ============================================================
-- This aggregates data from game_fielding_appearances to get games
-- played at each position, plus calculates innings from appearances
-- ============================================================

CREATE TABLE defensive_stats AS
WITH

-- Get game-level fielding appearances to count games
game_appearances AS (
    SELECT
        -- Extract season from game_id (format: ATL202304080 where 2023 is the year)
        SUBSTRING(fa.game_id, 4, 4)::INT AS season,
        SUBSTRING(fa.game_id, 1, 3) AS team_id,
        fa.player_id,
        fa.fielding_position,
        COUNT(DISTINCT fa.game_id) AS games,
        -- Calculate outs played (approximate based on appearance duration)
        SUM(
            CASE
                WHEN fa.end_event_id = 0 THEN 54 -- Full game (~18 outs, stored as outs*3)
                WHEN fa.end_event_id > fa.start_event_id
                    THEN CAST((fa.end_event_id - fa.start_event_id) AS SMALLINT) * 3
                ELSE 27
            END
        )::INT AS outs_played
    FROM game.game_fielding_appearances fa
    GROUP BY
        SUBSTRING(fa.game_id, 4, 4),
        SUBSTRING(fa.game_id, 1, 3),
        fa.player_id,
        fa.fielding_position
),

-- Add position names and player info
with_positions AS (
    SELECT
        ga.season,
        ga.team_id,
        ga.player_id,
        ga.fielding_position,
        ga.games,
        ga.outs_played,
        CASE ga.fielding_position
            WHEN 1 THEN 'P'
            WHEN 2 THEN 'C'
            WHEN 3 THEN '1B'
            WHEN 4 THEN '2B'
            WHEN 5 THEN '3B'
            WHEN 6 THEN 'SS'
            WHEN 7 THEN 'LF'
            WHEN 8 THEN 'CF'
            WHEN 9 THEN 'RF'
            WHEN 10 THEN 'DH'
            ELSE 'Unknown'
        END AS position_name,
        CASE
            WHEN ga.fielding_position BETWEEN 3 AND 6 THEN 'IF'
            WHEN ga.fielding_position BETWEEN 7 AND 9 THEN 'OF'
            WHEN ga.fielding_position = 1 THEN 'P'
            WHEN ga.fielding_position = 2 THEN 'C'
            ELSE 'Other'
        END AS position_category,
        p.last_name,
        p.first_name
    FROM game_appearances ga
    LEFT JOIN dim.players p ON ga.player_id = p.player_id
)

-- Final output with all defensive stats
SELECT
    season,
    team_id,
    player_id,
    first_name,
    last_name,
    fielding_position,
    position_name,
    position_category,
    games,
    outs_played,
    ROUND(outs_played / 3.0, 1) AS innings,
    -- Traditional counting stats (0 for now - these come from event data)
    0 AS putouts,
    0 AS assists,
    0 AS errors,
    0.0 AS fielding_percentage,
    0.0 AS range_factor,
    -- Catcher-specific
    0 AS passed_balls,
    0 AS caught_stealing,
    -- Advanced (placeholder - would need event-level aggregation)
    0 AS double_plays,
    0 AS triple_plays
FROM with_positions
WHERE games > 0
ORDER BY season, team_id, player_id, fielding_position;

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_defensive_season_player ON defensive_stats(season, player_id);
CREATE INDEX IF NOT EXISTS idx_defensive_season_position ON defensive_stats(season, fielding_position);

-- ============================================================
-- Sample queries
-- ============================================================
-- Get all defensive stats for a specific season:
--   SELECT * FROM defensive_stats WHERE season = 2023 ORDER BY player_id, fielding_position;
--
-- Get a player's career defensive stats:
--   SELECT * FROM defensive_stats WHERE player_id = 'judga001' ORDER BY season;
--
-- Get top games played at each position for a season:
--   SELECT position_name, player_id, last_name, first_name, games, innings
--   FROM defensive_stats
--   WHERE season = 2023 AND fielding_position != 10
--   ORDER BY fielding_position, games DESC
--   LIMIT 10;
-- ============================================================

-- Show summary
SELECT 'Defensive stats table created!' AS status;
SELECT COUNT(*) AS total_records FROM defensive_stats;
SELECT season, COUNT(*) AS players, SUM(games) AS total_games
FROM defensive_stats
GROUP BY season
ORDER BY season DESC
LIMIT 10;
