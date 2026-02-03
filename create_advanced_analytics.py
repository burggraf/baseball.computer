#!/usr/bin/env python3
"""
Create advanced analytics tables directly from Retrosheet event data.
Bypasses dbt to work with our imported data structure.
"""

import duckdb
from pathlib import Path

DB_PATH = Path("/Users/markb/dev/baseball.computer/baseball.duckdb")

con = duckdb.connect(str(DB_PATH), read_only=False)

print("="*70)
print("Creating Advanced Analytics Tables")
print("="*70)

# ============================================================
# 1. Player Season League Offense
# ============================================================
print("\n1. Creating metrics_player_season_league_offense...")

con.execute("DROP TABLE IF EXISTS metrics_player_season_league_offense")

con.execute("""
    CREATE TABLE metrics_player_season_league_offense AS
    WITH event_stats AS (
        SELECT
            SUBSTRING(e.game_id, 4, 4)::INT AS season,
            g.away_team_id AS team_id,
            e.batter_id AS player_id,
            COUNT(*) AS plate_appearances,
            SUM(CASE WHEN e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun', 'InsideTheParkHomeRun', 'GroundRuleDouble', 'InPlayOut', 'FieldersChoice', 'ReachedOnError') THEN 1 ELSE 0 END) AS at_bats,
            SUM(CASE WHEN e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun', 'InsideTheParkHomeRun', 'GroundRuleDouble') THEN 1 ELSE 0 END) AS hits,
            SUM(CASE WHEN e.plate_appearance_result = 'Double' THEN 1 ELSE 0 END) AS doubles,
            SUM(CASE WHEN e.plate_appearance_result = 'Triple' THEN 1 ELSE 0 END) AS triples,
            SUM(CASE WHEN e.plate_appearance_result IN ('HomeRun', 'InsideTheParkHomeRun') THEN 1 ELSE 0 END) AS home_runs,
            SUM(CASE WHEN e.plate_appearance_result = 'Walk' THEN 1 ELSE 0 END) AS walks,
            SUM(CASE WHEN e.plate_appearance_result = 'IntentionalWalk' THEN 1 ELSE 0 END) AS intentional_walks,
            SUM(CASE WHEN e.plate_appearance_result = 'HitByPitch' THEN 1 ELSE 0 END) AS hit_by_pitch,
            SUM(CASE WHEN e.plate_appearance_result = 'SacrificeHit' THEN 1 ELSE 0 END) AS sacrifice_bunts,
            SUM(CASE WHEN e.plate_appearance_result = 'SacrificeFly' THEN 1 ELSE 0 END) AS sacrifice_flies,
            SUM(CASE WHEN e.plate_appearance_result = 'StrikeOut' THEN 1 ELSE 0 END) AS strikeouts,
            SUM(e.runs_batted_in) AS runs_batted_in,
            SUM(e.runs_on_play) AS runs
        FROM event.events e
        LEFT JOIN game.games g ON e.game_id = g.game_id
        WHERE e.batter_id IS NOT NULL
        GROUP BY season, team_id, player_id
    ),
    calculated AS (
        SELECT *,
            hits + walks + hit_by_pitch AS times_on_base,
            at_bats + walks + hit_by_pitch + sacrifice_flies AS plate_appearances_for_obp,
            hits - doubles - triples - home_runs AS singles,
            doubles + 2*triples + 3*home_runs AS total_bases
        FROM event_stats
    )
    SELECT
        c.season,
        c.team_id,
        c.player_id,
        p.first_name,
        p.last_name,
        c.plate_appearances,
        c.at_bats,
        c.runs,
        c.hits,
        c.doubles,
        c.triples,
        c.home_runs,
        c.runs_batted_in,
        c.walks,
        c.intentional_walks,
        c.hit_by_pitch,
        c.sacrifice_bunts,
        c.sacrifice_flies,
        c.strikeouts,
        c.singles,
        c.total_bases,
        ROUND(CAST(c.hits AS DOUBLE) / NULLIF(c.at_bats, 0), 3) AS batting_average,
        ROUND(CAST(c.times_on_base AS DOUBLE) / NULLIF(c.plate_appearances_for_obp, 0), 3) AS on_base_percentage,
        ROUND(CAST(c.total_bases AS DOUBLE) / NULLIF(c.at_bats, 0), 3) AS slugging_percentage,
        ROUND(CAST(c.times_on_base AS DOUBLE) / NULLIF(c.plate_appearances_for_obp, 0) + CAST(c.total_bases AS DOUBLE) / NULLIF(c.at_bats, 0), 3) AS ops
    FROM calculated c
    LEFT JOIN dim.players p ON c.player_id = p.player_id
""")

count = con.execute("SELECT COUNT(*) FROM metrics_player_season_league_offense").fetchone()[0]
print(f"   Created: {count:,} player-season offensive records")

# ============================================================
# 2. Player Season League Pitching
# ============================================================
print("\n2. Creating metrics_player_season_league_pitching...")

con.execute("DROP TABLE IF EXISTS metrics_player_season_league_pitching")

con.execute("""
    CREATE TABLE metrics_player_season_league_pitching AS
    WITH event_stats AS (
        SELECT
            SUBSTRING(e.game_id, 4, 4)::INT AS season,
            g.home_team_id AS team_id,
            e.pitcher_id AS player_id,
            COUNT(*) AS batters_faced,
            SUM(CASE WHEN e.plate_appearance_result NOT IN ('SacrificeHit', 'SacrificeFly', 'IntentionalWalk', 'HitByPitch', 'Interference') THEN 1 ELSE 0 END) AS at_bats_against,
            SUM(CASE WHEN e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun', 'InsideTheParkHomeRun', 'GroundRuleDouble') THEN 1 ELSE 0 END) AS hits,
            SUM(CASE WHEN e.plate_appearance_result IN ('Walk', 'IntentionalWalk') THEN 1 ELSE 0 END) AS walks,
            SUM(CASE WHEN e.plate_appearance_result = 'IntentionalWalk' THEN 1 ELSE 0 END) AS intentional_walks,
            SUM(CASE WHEN e.plate_appearance_result = 'HitByPitch' THEN 1 ELSE 0 END) AS hit_by_pitch,
            SUM(CASE WHEN e.plate_appearance_result = 'StrikeOut' THEN 1 ELSE 0 END) AS strikeouts,
            SUM(CASE WHEN e.plate_appearance_result IN ('HomeRun', 'InsideTheParkHomeRun') THEN 1 ELSE 0 END) AS home_runs,
            SUM(e.runs_on_play) AS runs,
            SUM(e.team_unearned_runs) AS unearned_runs
        FROM event.events e
        LEFT JOIN game.games g ON e.game_id = g.game_id
        WHERE e.pitcher_id IS NOT NULL
        GROUP BY season, team_id, player_id
    ),
    calculated AS (
        SELECT *,
            at_bats_against - hits - walks - hit_by_pitch AS outs_recorded,
            hits + walks - hit_by_pitch AS base_on_balls,
            at_bats_against AS batters_faced_for_whip
        FROM event_stats
    )
    SELECT
        c.season,
        c.team_id,
        c.player_id,
        p.first_name,
        p.last_name,
        c.batters_faced,
        c.at_bats_against,
        c.outs_recorded,
        ROUND(c.outs_recorded / 3.0, 1) AS innings_pitched,
        c.hits,
        c.walks,
        c.intentional_walks,
        c.hit_by_pitch,
        c.strikeouts,
        c.home_runs,
        c.runs,
        c.unearned_runs,
        c.runs - c.unearned_runs AS earned_runs,
        ROUND(CAST(c.base_on_balls AS DOUBLE) / NULLIF(c.batters_faced_for_whip, 0), 3) AS whip,
        ROUND(CAST((c.runs - c.unearned_runs) * 9 AS DOUBLE) / NULLIF(c.outs_recorded, 0), 2) AS era,
        ROUND(CAST(c.strikeouts * 9 AS DOUBLE) / NULLIF(c.outs_recorded, 0), 1) AS k_per_9,
        ROUND(CAST(c.walks * 9 AS DOUBLE) / NULLIF(c.outs_recorded, 0), 1) AS bb_per_9,
        ROUND(CAST(c.home_runs * 9 AS DOUBLE) / NULLIF(c.outs_recorded, 0), 1) AS hr_per_9,
        ROUND(CAST(c.strikeouts AS DOUBLE) / NULLIF(c.batters_faced, 0), 3) AS k_rate,
        ROUND(CAST(c.walks AS DOUBLE) / NULLIF(c.batters_faced, 0), 3) AS bb_rate
    FROM calculated c
    LEFT JOIN dim.players p ON c.player_id = p.player_id
""")

count = con.execute("SELECT COUNT(*) FROM metrics_player_season_league_pitching").fetchone()[0]
print(f"   Created: {count:,} player-season pitching records")

# ============================================================
# 3. Park Factors
# ============================================================
print("\n3. Creating park_factors...")

con.execute("DROP TABLE IF EXISTS park_factors")

con.execute("""
    CREATE TABLE park_factors AS
    WITH home_games AS (
        SELECT
            SUBSTRING(e.game_id, 4, 4)::INT AS season,
            g.park_id,
            g.home_team_id,
            SUM(CASE WHEN e.plate_appearance_result = 'StrikeOut' OR e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun', 'InsideTheParkHomeRun', 'GroundRuleDouble', 'InPlayOut', 'FieldersChoice', 'ReachedOnError') THEN 1 ELSE 0 END) AS outs,
            SUM(CASE WHEN e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun', 'InsideTheParkHomeRun', 'GroundRuleDouble') THEN 1 ELSE 0 END) AS hits,
            SUM(CASE WHEN e.plate_appearance_result IN ('HomeRun', 'InsideTheParkHomeRun') THEN 1 ELSE 0 END) AS home_runs,
            SUM(e.runs_on_play) AS runs
        FROM event.events e
        LEFT JOIN game.games g ON e.game_id = g.game_id
        WHERE e.batter_id IS NOT NULL
        GROUP BY season, g.park_id, g.home_team_id
    ),
    league_avg AS (
        SELECT
            season,
            SUM(outs) AS total_outs,
            SUM(hits) AS total_hits,
            SUM(home_runs) AS total_home_runs,
            SUM(runs) AS total_runs
        FROM home_games
        GROUP BY season
    ),
    park_factors_calc AS (
        SELECT
            hg.season,
            hg.park_id,
            p.name AS park_name,
            p.city AS park_city,
            COUNT(*) AS games,
            SUM(hg.outs) AS outs,
            SUM(hg.hits) AS hits,
            SUM(hg.home_runs) AS home_runs,
            SUM(hg.runs) AS runs,
            CAST(SUM(hg.hits) AS DOUBLE) / NULLIF(SUM(hg.outs), 0) AS h_per_out,
            CAST(SUM(hg.home_runs) AS DOUBLE) / NULLIF(SUM(hg.outs), 0) AS hr_per_out,
            CAST(SUM(hg.runs) AS DOUBLE) / NULLIF(SUM(hg.outs), 0) AS r_per_out
        FROM home_games hg
        LEFT JOIN league_avg la ON hg.season = la.season
        LEFT JOIN dim.parks p ON hg.park_id = p.park_id
        GROUP BY hg.season, hg.park_id, p.name, p.city
    ),
    with_league_avg AS (
        SELECT
            pfc.*,
            (CAST(la.total_hits AS DOUBLE) / NULLIF(la.total_outs, 0)) AS league_h_per_out,
            (CAST(la.total_home_runs AS DOUBLE) / NULLIF(la.total_outs, 0)) AS league_hr_per_out,
            (CAST(la.total_runs AS DOUBLE) / NULLIF(la.total_outs, 0)) AS league_r_per_out
        FROM park_factors_calc pfc
        LEFT JOIN league_avg la ON pfc.season = la.season
    )
    SELECT
        season,
        park_id,
        park_name,
        park_city,
        games,
        ROUND(h_per_out / NULLIF(league_h_per_out, 0), 3) AS park_factor_hits,
        ROUND(hr_per_out / NULLIF(league_hr_per_out, 0), 3) AS park_factor_home_runs,
        ROUND(r_per_out / NULLIF(league_r_per_out, 0), 3) AS park_factor_runs,
        ROUND((h_per_out / NULLIF(league_h_per_out, 0) + hr_per_out / NULLIF(league_hr_per_out, 0) + r_per_out / NULLIF(league_r_per_out, 0)) / 3, 3) AS park_factor_overall
    FROM with_league_avg
""")

count = con.execute("SELECT COUNT(*) FROM park_factors").fetchone()[0]
print(f"   Created: {count:,} park-season records")

# ============================================================
# 4. Event Baserunning Stats
# ============================================================
print("\n4. Creating event_baserunning_stats...")

con.execute("DROP TABLE IF EXISTS event_baserunning_stats")

con.execute("""
    CREATE TABLE event_baserunning_stats AS
    SELECT
        e.game_id,
        e.event_id,
        SUBSTRING(e.game_id, 4, 4)::INT AS season,
        b.runner_id AS baserunner_id,
        b.baserunner AS baserunner_position,
        b.attempted_advance_to_base,
        b.baserunning_play_type,
        b.is_out AS out_recorded_on_play,
        b.run_scored_flag AS runs_scored_on_play,
        b.rbi_flag AS is_rbi
    FROM event.events e
    LEFT JOIN event.event_baserunners b ON e.event_id = b.event_id AND e.event_key = b.event_key
    WHERE b.runner_id IS NOT NULL
""")

count = con.execute("SELECT COUNT(*) FROM event_baserunning_stats").fetchone()[0]
print(f"   Created: {count:,} baserunning event records")

# ============================================================
# 5. Calc Batted Ball Type
# ============================================================
print("\n5. Creating calc_batted_ball_type...")

con.execute("DROP TABLE IF EXISTS calc_batted_ball_type")

con.execute("""
    CREATE TABLE calc_batted_ball_type AS
    SELECT
        e.game_id,
        e.event_id,
        SUBSTRING(e.game_id, 4, 4)::INT AS season,
        e.batter_id,
        e.pitcher_id,
        e.batted_trajectory,
        CASE
            WHEN e.batted_trajectory IN ('fly_ball', 'popup') THEN 'fly_ball'
            WHEN e.batted_trajectory = 'ground_ball' THEN 'ground_ball'
            WHEN e.batted_trajectory = 'line_drive' THEN 'line_drive'
            WHEN e.batted_trajectory IS NULL AND e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun') THEN 'unknown'
            ELSE NULL
        END AS batted_ball_type,
        e.batted_location_general,
        e.batted_location_depth,
        e.plate_appearance_result
    FROM event.events e
    WHERE e.batted_trajectory IS NOT NULL OR e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun')
""")

count = con.execute("SELECT COUNT(*) FROM calc_batted_ball_type").fetchone()[0]
print(f"   Created: {count:,} batted ball classifications")

# ============================================================
# 6. Event Batted Ball Stats
# ============================================================
print("\n6. Creating event_batted_ball_stats...")

con.execute("DROP TABLE IF EXISTS event_batted_ball_stats")

con.execute("""
    CREATE TABLE event_batted_ball_stats AS
    WITH batted_types AS (
        SELECT
            e.game_id,
            e.event_id,
            SUBSTRING(e.game_id, 4, 4)::INT AS season,
            e.batter_id,
            e.pitcher_id,
            e.batted_trajectory,
            CASE
                WHEN e.batted_trajectory IN ('fly_ball', 'popup') THEN 'fly_ball'
                WHEN e.batted_trajectory = 'ground_ball' THEN 'ground_ball'
                WHEN e.batted_trajectory = 'line_drive' THEN 'line_drive'
                WHEN e.batted_trajectory IS NULL AND e.plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun') THEN 'unknown'
                ELSE NULL
            END AS batted_ball_type,
            e.plate_appearance_result,
            e.batted_location_general,
            e.outs_on_play
        FROM event.events e
    )
    SELECT
        season,
        batter_id,
        pitcher_id,
        batted_ball_type,
        batted_trajectory,
        COUNT(*) AS balls_in_play,
        SUM(CASE WHEN plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun') THEN 1 ELSE 0 END) AS hits,
        SUM(CASE WHEN outs_on_play > 0 THEN 1 ELSE 0 END) AS outs,
        ROUND(CAST(SUM(CASE WHEN plate_appearance_result IN ('Single', 'Double', 'Triple', 'HomeRun') THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0), 3) AS batting_average
    FROM batted_types
    WHERE batted_ball_type IS NOT NULL
    GROUP BY season, batter_id, pitcher_id, batted_ball_type, batted_trajectory
""")

count = con.execute("SELECT COUNT(*) FROM event_batted_ball_stats").fetchone()[0]
print(f"   Created: {count:,} batted ball stat records")

con.close()

print("\n" + "="*70)
print("Advanced Analytics Tables Created Successfully!")
print("="*70)

# Show summary
con = duckdb.connect(str(DB_PATH), read_only=True)

print("\nTable Summary:")
tables = [
    ("metrics_player_season_league_offense", "Player-season batting stats"),
    ("metrics_player_season_league_pitching", "Player-season pitching stats"),
    ("park_factors", "Park factors by season"),
    ("event_baserunning_stats", "Baserunning event data"),
    ("calc_batted_ball_type", "Batted ball classifications"),
    ("event_batted_ball_stats", "Batted ball statistics"),
]

for table, description in tables:
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} records - {description}")
    except Exception as e:
        print(f"  {table}: Error - {e}")

con.close()

print("\nExample queries:")
print("  -- Get top hitters by OPS in 2023")
print("  SELECT * FROM metrics_player_season_league_offense")
print("  WHERE season = 2023 AND at_bats >= 300")
print("  ORDER BY ops DESC LIMIT 10;")
