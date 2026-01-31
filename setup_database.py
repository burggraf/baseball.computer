#!/usr/bin/env python3
"""
Create the DuckDB database with proper schema for baseball data.
Run this once on a fresh clone before running process_historical.py
"""

import duckdb
from pathlib import Path

DB_PATH = Path("/Users/markb/dev/baseball.computer/baseball.duckdb")

print(f"Creating database at: {DB_PATH}")

# Remove existing database if it exists
if DB_PATH.exists():
    print(f"  Removing existing database...")
    DB_PATH.unlink()

con = duckdb.connect(str(DB_PATH))

# Create schemas
print("  Creating schemas...")
con.execute("CREATE SCHEMA event")
con.execute("CREATE SCHEMA game")
con.execute("CREATE SCHEMA info")
con.execute("CREATE SCHEMA box_score")
con.execute("CREATE SCHEMA dim")

# Create custom types
print("  Creating custom types...")
con.execute("CREATE TYPE SIDE_ENUM AS ENUM ('top', 'bottom')")
con.execute("CREATE TYPE HAND_ENUM AS ENUM ('L', 'R', 'B')")
con.execute("CREATE TYPE FRAME_ENUM AS ENUM ('first', 'second', 'third', 'home')")
con.execute("CREATE TYPE BASE_ENUM AS ENUM ('first', 'second', 'third', 'home')")
con.execute("CREATE TYPE BASERUNNER_ENUM AS ENUM ('first', 'second', 'third')")

# Create empty event table with proper schema
print("  Creating event.events table...")
con.execute("""
    CREATE TABLE event.events (
        game_id VARCHAR,
        event_id BIGINT,
        event_key BIGINT,
        inning BIGINT,
        frame VARCHAR,
        batter_lineup_position BIGINT,
        batter_id VARCHAR,
        pitcher_id VARCHAR,
        batting_team_id VARCHAR,
        fielding_team_id VARCHAR,
        outs BIGINT,
        base_state BIGINT,
        count_balls BIGINT,
        count_strikes BIGINT,
        specified_batter_hand VARCHAR,
        specified_pitcher_hand VARCHAR,
        strikeout_responsible_batter_id VARCHAR,
        walk_responsible_pitcher_id VARCHAR,
        plate_appearance_result VARCHAR,
        batted_trajectory VARCHAR,
        batted_to_fielder BIGINT,
        batted_location_general VARCHAR,
        batted_location_depth VARCHAR,
        batted_location_angle VARCHAR,
        batted_contact_strength VARCHAR,
        outs_on_play BIGINT,
        runs_on_play BIGINT,
        runs_batted_in BIGINT,
        team_unearned_runs BIGINT,
        no_play_flag BOOLEAN,
        side SIDE_ENUM
    )
""")

# Other event tables will be created automatically during import
print("  Skipping other event tables (will be created during import)...")

# Create game tables
print("  Creating game.game_lineup_appearances table...")
con.execute("""
    CREATE TABLE game.game_lineup_appearances (
        game_id VARCHAR,
        player_id VARCHAR,
        side SIDE_ENUM,
        lineup_position UTINYINT,
        entered_game_as VARCHAR,
        start_event_id UTINYINT,
        end_event_id UTINYINT
    )
""")

print("  Creating game.game_fielding_appearances table...")
con.execute("""
    CREATE TABLE game.game_fielding_appearances (
        game_id VARCHAR,
        player_id VARCHAR,
        side SIDE_ENUM,
        fielding_position UTINYINT,
        start_event_id UTINYINT,
        end_event_id UTINYINT
    )
""")

print("  Creating game.game_earned_runs table...")
con.execute("""
    CREATE TABLE game.game_earned_runs (
        game_id VARCHAR,
        player_id VARCHAR,
        earned_runs UTINYINT
    )
""")

print("  Creating game.games table...")
con.execute("""
    CREATE TABLE game.games (
        game_id VARCHAR,
        date DATE,
        start_time TIMESTAMP,
        doubleheader_status VARCHAR,
        time_of_day VARCHAR,
        game_type VARCHAR,
        bat_first_side VARCHAR,
        sky VARCHAR,
        field_condition VARCHAR,
        precipitation VARCHAR,
        wind_direction VARCHAR,
        park_id VARCHAR,
        temperature_fahrenheit BIGINT,
        attendance BIGINT,
        wind_speed_mph BIGINT,
        use_dh BOOLEAN,
        winning_pitcher VARCHAR,
        losing_pitcher VARCHAR,
        save_pitcher VARCHAR,
        game_winning_rbi VARCHAR,
        time_of_game_minutes BIGINT,
        protest_info VARCHAR,
        completion_info VARCHAR,
        scorer VARCHAR,
        scoring_method VARCHAR,
        inputter VARCHAR,
        translator VARCHAR,
        date_inputted TIMESTAMP,
        date_edited VARCHAR,
        account_type VARCHAR,
        filename VARCHAR,
        game_key BIGINT,
        away_team_id VARCHAR,
        home_team_id VARCHAR,
        umpire_home_id VARCHAR,
        umpire_first_id VARCHAR,
        umpire_second_id VARCHAR,
        umpire_third_id VARCHAR,
        umpire_left_id VARCHAR,
        umpire_right_id VARCHAR
    )
""")

# box_score tables will be created automatically during import (for pre-1900 games)
print("  Skipping box_score tables (will be created during import)...")

# Create dim (dimension) tables for players, teams, parks
print("  Creating dim.teams...")
con.execute("""
    CREATE TABLE dim.teams (
        team_id VARCHAR PRIMARY KEY,
        city VARCHAR,
        name VARCHAR,
        nickname VARCHAR,
        league VARCHAR,
        division VARCHAR
    )
""")

print("  Creating dim.parks...")
con.execute("""
    CREATE TABLE dim.parks (
        park_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        city VARCHAR,
        state VARCHAR,
        country VARCHAR
    )
""")

print("  Creating dim.players...")
con.execute("""
    CREATE TABLE dim.players (
        player_id VARCHAR PRIMARY KEY,
        last_name VARCHAR,
        first_name VARCHAR,
        bats VARCHAR,
        throws VARCHAR,
        teams_played VARCHAR[]
    )
""")

con.close()

print("\nDatabase created successfully!")
print(f"Location: {DB_PATH}")
print("\nTo add player, team, and park data:")
print("  python3 add_reference_data.py")
print("\nYou can now run: python3 process_historical.py")
