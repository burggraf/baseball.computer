#!/usr/bin/env python3
"""
Add reference data (players, teams, parks) to the database.
Imports roster files from Retrosheet and downloads team/park reference data.
"""

import duckdb
from pathlib import Path
import csv
import subprocess
import tempfile
import shutil

# Use script directory for portability
BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = BASE_DIR / "baseball.duckdb"
RETROSHEET_DIR = BASE_DIR / "retrosheet"
RETROSHEET_URL = "https://www.retrosheet.org"

print(f"Database: {DB_PATH}")
print(f"Retrosheet dir: {RETROSHEET_DIR}")

con = duckdb.connect(str(DB_PATH))

# ============================================================
# CREATE TABLES
# ============================================================

print("\nCreating reference tables...")

# Players table
print("  Creating dim.players...")
con.execute("""
    CREATE SCHEMA IF NOT EXISTS dim
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS dim.players (
        player_id VARCHAR PRIMARY KEY,
        last_name VARCHAR,
        first_name VARCHAR,
        bats VARCHAR,
        throws VARCHAR,
        teams_played VARCHAR[]  -- Array of team_ids this player played for
    )
""")

# Teams table - already created by setup_database.py
# Parks table - already created by setup_database.py
# Skipping table creation since setup_database.py handles this

# ============================================================
# DOWNLOAD AND IMPORT REFERENCE DATA
# ============================================================

print("\nDownloading reference data from Retrosheet...")

with tempfile.TemporaryDirectory() as temp_dir:
    temp_path = Path(temp_dir)

    # Download teams.csv
    print("  Downloading teams.csv...")
    teams_zip = temp_path / "teams.zip"
    subprocess.run(
        ["curl", "-s", "-o", str(teams_zip), f"{RETROSHEET_URL}/teams.zip"],
        check=True
    )
    subprocess.run(
        ["unzip", "-q", "-o", str(teams_zip), "-d", str(temp_path)],
        check=True
    )

    # Download ballparks.csv
    print("  Downloading ballparks.csv...")
    parks_zip = temp_path / "ballparks.zip"
    subprocess.run(
        ["curl", "-s", "-o", str(parks_zip), f"{RETROSHEET_URL}/ballparks.zip"],
        check=True
    )
    subprocess.run(
        ["unzip", "-q", "-o", str(parks_zip), "-d", str(temp_path)],
        check=True
    )

    # Import teams
    print("\nImporting teams...")
    teams_file = temp_path / "teams.csv"
    with open(teams_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        team_count = 0
        for row in reader:
            if len(row) >= 6:
                team_id, league, city, nickname, first_year, last_year = row[:6]
                # Map to the schema: team_id, city, name, nickname, league, division
                # Using city as name, nickname stays as is, league as is
                con.execute("""
                    INSERT OR REPLACE INTO dim.teams (team_id, city, name, nickname, league)
                    VALUES (?, ?, ?, ?, ?)
                """, [team_id, city, city, nickname, league])
                team_count += 1
    print(f"  Imported {team_count:,} teams")

    # Import parks
    print("Importing parks...")
    parks_file = temp_path / "ballparks.csv"
    with open(parks_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        park_count = 0
        for row in reader:
            if len(row) >= 9:
                park_id, name, aka, city, state, start, end, league, notes = row[:9]
                # Map to schema: park_id, name, city, state, country
                con.execute("""
                    INSERT OR REPLACE INTO dim.parks (park_id, name, city, state)
                    VALUES (?, ?, ?, ?)
                """, [park_id, name, city, state])
                park_count += 1
    print(f"  Imported {park_count:,} parks")

# ============================================================
# IMPORT PLAYER DATA FROM ROSTER FILES
# ============================================================

print("\nImporting player data from roster files...")
roster_files = sorted(RETROSHEET_DIR.glob("*.ROS"))

players_data = {}
for roster_file in roster_files:
    team_id = roster_file.stem[:3]  # e.g., "BOS" from "BOS2024.ROS"

    with open(roster_file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 7:
                player_id, last_name, first_name, bats, throws, team, pos = row[:7]

                if player_id not in players_data:
                    players_data[player_id] = {
                        'player_id': player_id,
                        'last_name': last_name,
                        'first_name': first_name,
                        'bats': bats,
                        'throws': throws,
                        'teams_played': []
                    }

                if team not in players_data[player_id]['teams_played']:
                    players_data[player_id]['teams_played'].append(team)

# Insert players
print(f"  Found {len(players_data):,} unique players")
for player_id, data in players_data.items():
    con.execute("""
        INSERT OR REPLACE INTO dim.players (player_id, last_name, first_name, bats, throws, teams_played)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [data['player_id'], data['last_name'], data['first_name'],
          data['bats'], data['throws'], data['teams_played']])

# ============================================================
# SHOW RESULTS
# ============================================================

print("\n" + "="*60)
print("SUMMARY")
print("="*60)

player_count = con.execute("SELECT COUNT(*) FROM dim.players").fetchone()[0]
team_count = con.execute("SELECT COUNT(*) FROM dim.teams").fetchone()[0]
park_count = con.execute("SELECT COUNT(*) FROM dim.parks").fetchone()[0]

print(f"Players: {player_count:,}")
print(f"Teams: {team_count:,}")
print(f"Parks: {park_count:,}")

print("\nSample players:")
for row in con.execute("SELECT * FROM dim.players LIMIT 5").fetchall():
    print(f"  {row[2]} {row[1]} ({row[0]}) - bats: {row[3]}, throws: {row[4]}")

print("\nSample teams:")
for row in con.execute("SELECT * FROM dim.teams LIMIT 5").fetchall():
    print(f"  {row[2]} {row[3]} ({row[0]}) - {row[1]} ({row[4]}-{row[5]})")

print("\nSample parks:")
for row in con.execute("SELECT * FROM dim.parks LIMIT 5").fetchall():
    print(f"  {row[3]}: {row[1]} ({row[4]})")

con.close()
print("\nDone!")
