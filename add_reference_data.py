#!/usr/bin/env python3
"""
Add reference data (players, teams, parks) to the database.
Imports roster files from Retrosheet and adds team/park mappings.
"""

import duckdb
from pathlib import Path
import csv
import sys

DB_PATH = Path("/Users/markb/dev/baseball.computer/baseball.duckdb")
RETROSHEET_DIR = Path("/Users/markb/dev/baseball.computer/retrosheet")

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

# Teams table (static mapping of team codes to names)
print("  Creating dim.teams...")
con.execute("""
    CREATE TABLE IF NOT EXISTS dim.teams (
        team_id VARCHAR PRIMARY KEY,
        city VARCHAR,
        name VARCHAR,
        nickname VARCHAR,
        league VARCHAR,
        division VARCHAR
    )
""")

# Parks table (static mapping of park codes to names)
print("  Creating dim.parks...")
con.execute("""
    CREATE TABLE IF NOT EXISTS dim.parks (
        park_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        city VARCHAR,
        state VARCHAR,
        country VARCHAR
    )
""")

# ============================================================
# IMPORT DATA
# ============================================================

print("\nImporting data...")

# Import roster files to build players table
print("  Importing player data from roster files...")
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
print(f"  Found {len(players_data)} unique players")
for player_id, data in players_data.items():
    con.execute("""
        INSERT INTO dim.players (player_id, last_name, first_name, bats, throws, teams_played)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [data['player_id'], data['last_name'], data['first_name'], 
          data['bats'], data['throws'], data['teams_played']])

# Insert team data (MLB teams - static mapping)
print("  Inserting team data...")
teams = [
    ('ANA', 'Los Angeles', 'Angels', 'Angels', 'AL', 'West'),
    ('ARI', 'Arizona', 'Diamondbacks', 'D-backs', 'NL', 'West'),
    ('ATL', 'Atlanta', 'Braves', 'Braves', 'NL', 'East'),
    ('BAL', 'Baltimore', 'Orioles', 'Orioles', 'AL', 'East'),
    ('BOS', 'Boston', 'Red Sox', 'Red Sox', 'AL', 'East'),
    ('CHA', 'Chicago', 'White Sox', 'White Sox', 'AL', 'Central'),
    ('CHN', 'Chicago', 'Cubs', 'Cubs', 'NL', 'Central'),
    ('CIN', 'Cincinnati', 'Reds', 'Reds', 'NL', 'Central'),
    ('CLE', 'Cleveland', 'Guardians', 'Guardians', 'AL', 'Central'),
    ('COL', 'Colorado', 'Rockies', 'Rockies', 'NL', 'West'),
    ('DET', 'Detroit', 'Tigers', 'Tigers', 'AL', 'Central'),
    ('HOU', 'Houston', 'Astros', 'Astros', 'AL', 'West'),
    ('KCA', 'Kansas City', 'Royals', 'Royals', 'AL', 'Central'),
    ('LAN', 'Los Angeles', 'Dodgers', 'Dodgers', 'NL', 'West'),
    ('MIA', 'Miami', 'Marlins', 'Marlins', 'NL', 'East'),
    ('MIL', 'Milwaukee', 'Brewers', 'Brewers', 'NL', 'Central'),
    ('MIN', 'Minnesota', 'Twins', 'Twins', 'AL', 'Central'),
    ('NYA', 'New York', 'Yankees', 'Yankees', 'AL', 'East'),
    ('NYN', 'New York', 'Mets', 'Mets', 'NL', 'East'),
    ('OAK', 'Oakland', 'Athletics', 'Athletics', 'AL', 'West'),
    ('PHI', 'Philadelphia', 'Phillies', 'Phillies', 'NL', 'East'),
    ('PIT', 'Pittsburgh', 'Pirates', 'Pirates', 'NL', 'Central'),
    ('SDN', 'San Diego', 'Padres', 'Padres', 'NL', 'West'),
    ('SEA', 'Seattle', 'Mariners', 'Mariners', 'AL', 'West'),
    ('SFN', 'San Francisco', 'Giants', 'Giants', 'NL', 'West'),
    ('SLN', 'St. Louis', 'Cardinals', 'Cardinals', 'NL', 'Central'),
    ('TBA', 'Tampa Bay', 'Rays', 'Rays', 'AL', 'East'),
    ('TEX', 'Texas', 'Rangers', 'Rangers', 'AL', 'West'),
    ('TOR', 'Toronto', 'Blue Jays', 'Blue Jays', 'AL', 'East'),
    ('WAS', 'Washington', 'Nationals', 'Nationals', 'NL', 'East'),
]

for team_id, city, name, nickname, league, division in teams:
    con.execute("""
        INSERT INTO dim.teams (team_id, city, name, nickname, league, division)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [team_id, city, name, nickname, league, division])

print(f"  Inserted {len(teams)} teams")

# Insert park data (major league parks)
print("  Inserting park data...")
parks = [
    ('ANA01', 'Angel Stadium', 'Anaheim', 'CA', 'USA'),
    ('ARI01', 'Chase Field', 'Phoenix', 'AZ', 'USA'),
    ('ATL02', 'Truist Park', 'Atlanta', 'GA', 'USA'),
    ('BAL12', 'Oriole Park at Camden Yards', 'Baltimore', 'MD', 'USA'),
    ('BOS02', 'Fenway Park', 'Boston', 'MA', 'USA'),
    ('CHA08', 'Guaranteed Rate Field', 'Chicago', 'IL', 'USA'),
    ('CHN02', 'Wrigley Field', 'Chicago', 'IL', 'USA'),
    ('CIN02', 'Great American Ball Park', 'Cincinnati', 'OH', 'USA'),
    ('CLE03', 'Progressive Field', 'Cleveland', 'OH', 'USA'),
    ('COL02', 'Coors Field', 'Denver', 'CO', 'USA'),
    ('DET02', 'Comerica Park', 'Detroit', 'MI', 'USA'),
    ('HOU03', 'Minute Maid Park', 'Houston', 'TX', 'USA'),
    ('KCA01', 'Kauffman Stadium', 'Kansas City', 'MO', 'USA'),
    ('LAN02', 'Dodger Stadium', 'Los Angeles', 'CA', 'USA'),
    ('MIA02', 'LoanDepot Park', 'Miami', 'FL', 'USA'),
    ('MIL01', 'American Family Field', 'Milwaukee', 'WI', 'USA'),
    ('MIN01', 'Target Field', 'Minneapolis', 'MN', 'USA'),
    ('NYA31', 'Yankee Stadium', 'New York', 'NY', 'USA'),
    ('NYN02', 'Citi Field', 'New York', 'NY', 'USA'),
    ('OAK01', 'Oakland Coliseum', 'Oakland', 'CA', 'USA'),
    ('PHI13', 'Citizens Bank Park', 'Philadelphia', 'PA', 'USA'),
    ('PIT02', 'PNC Park', 'Pittsburgh', 'PA', 'USA'),
    ('SDN02', 'Petco Park', 'San Diego', 'CA', 'USA'),
    ('SEA02', 'T-Mobile Park', 'Seattle', 'WA', 'USA'),
    ('SFN01', 'Oracle Park', 'San Francisco', 'CA', 'USA'),
    ('SLN01', 'Busch Stadium', 'St. Louis', 'MO', 'USA'),
    ('TBA02', 'Tropicana Field', 'Tampa Bay', 'FL', 'USA'),
    ('TEX02', 'Globe Life Field', 'Arlington', 'TX', 'USA'),
    ('TOR02', 'Rogers Centre', 'Toronto', 'ON', 'Canada'),
    ('WAS11', 'Nationals Park', 'Washington', 'DC', 'USA'),
]

for park_id, name, city, state, country in parks:
    con.execute("""
        INSERT INTO dim.parks (park_id, name, city, state, country)
        VALUES (?, ?, ?, ?, ?)
    """, [park_id, name, city, state, country])

print(f"  Inserted {len(parks)} parks")

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
print(f"Teams: {team_count}")
print(f"Parks: {park_count}")

print("\nSample players:")
for row in con.execute("SELECT * FROM dim.players LIMIT 5").fetchall():
    print(f"  {row[1]} {row[2]} ({row[0]}) - bats: {row[3]}, throws: {row[4]}")

print("\nSample teams:")
for row in con.execute("SELECT * FROM dim.teams LIMIT 5").fetchall():
    print(f"  {row[1]} {row[2]} ({row[0]}) - {row[4]} {row[5]}")

con.close()
print("\nDone!")
