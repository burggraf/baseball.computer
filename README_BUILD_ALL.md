# Complete Database Build Script

## Overview

`build_all.py` is a one-stop script that builds the entire baseball.computer database from scratch. It automates the complete pipeline from database creation through advanced analytics, eliminating the need to run individual scripts in sequence.

## What It Does

The script runs **6 steps** in order:

1. **Database Setup** - Creates `baseball.duckdb` with proper schema
2. **Reference Data** - Imports players, teams, and parks from Retrosheet
3. **Historical Data** - Downloads and imports Retrosheet event data (1910-present)
4. **Defensive Stats** - Creates defensive statistics table from game appearances
5. **Advanced Analytics** - Creates 6 advanced analytics tables
6. **Verification** - Validates the build and shows summary statistics

## Quick Start

### First-Time Build (Complete Database)

```bash
cd /Users/markb/dev/baseball.computer
python3 build_all.py
```

This will:
- Create a fresh `baseball.duckdb` database
- Import all available years of Retrosheet data (1910-2024)
- Build defensive and advanced analytics tables
- Take several hours to complete

### Build Specific Years

```bash
python3 build_all.py 2024 2023 2022
```

This builds the database but only imports the specified years.

## Prerequisites

- Python 3.11+ with duckdb installed
- Rust parser built and available at `baseball.computer.rs/target/release/baseball-computer`
- Virtual environment at `./venv313` for advanced analytics (optional but recommended)

### First-Time Setup

```bash
# Clone the repository
git clone git@github.com:burggraf/baseball.computer.git
cd baseball.computer

# Set up Python environment
python3.13 -m venv venv313
source venv313/bin/activate
pip install duckdb

# Clone and build the Rust parser
git clone git@github.com:burggraf/baseball.computer.rs.git
cd baseball.computer.rs
cargo build --release
cd ..

# Run the complete build
python3 build_all.py
```

## Build Steps in Detail

### Step 1: Database Setup

Creates `baseball.duckdb` with all required schemas and tables:
- `event.*` - Event data tables
- `game.*` - Game-level tables
- `dim.*` - Dimension tables (players, teams, parks)

**Skipped if**: Database exists and is > 1MB (has data)

### Step 2: Reference Data

Imports dimension tables:
- **Players**: From `.ROS` roster files in `retrosheet/` directory
- **Teams**: Downloads complete teams database from Retrosheet (292 teams)
- **Parks**: Downloads complete parks database from Retrosheet (656 parks)

**Source**: `add_reference_data.py`

### Step 3: Historical Data

Downloads and imports Retrosheet event data:
- Downloads event files for each year (1910-2024)
- Parses with Rust parser
- Imports events into `event.events` table
- Keeps roster files for reference data

**Source**: `process_historical.py`

**Time**: Several hours for full import

### Step 4: Defensive Statistics

Creates `defensive_stats` table with:
- Games played at each position
- Innings played at each position
- Position name and category
- Player information

**Source**: `create_defensive_stats_view.sql`

**Output**: ~1M+ records (player-season-position combinations)

### Step 5: Advanced Analytics

Creates 6 advanced analytics tables directly from event data:

| Table | Description | Records |
|-------|-------------|---------|
| `metrics_player_season_league_offense` | Player-season batting stats (AVG, OBP, SLG, OPS) | 700K+ |
| `metrics_player_season_league_pitching` | Player-season pitching stats (ERA, WHIP, K/9) | 370K+ |
| `park_factors` | Park factors by season (hits, HR, runs) | 3K+ |
| `event_baserunning_stats` | Baserunning event data | 1.3B+ |
| `calc_batted_ball_type` | Batted ball classifications | 11M+ |
| `event_batted_ball_stats` | Batted ball statistics | 40M+ |

**Source**: `create_advanced_analytics.py`

**Note**: This bypasses dbt and creates tables directly via SQL for compatibility with imported Retrosheet data.

### Step 6: Verification

Validates the build and displays summary statistics for all tables.

## Example Output

```
======================================================================
BASEBALL.COMPUTER DATABASE BUILDER
======================================================================
  Database: /Users/markb/dev/baseball.computer/baseball.duckdb
  Base dir: /Users/markb/dev/baseball.computer
  Started: 2026-02-02 10:30:00

======================================================================
PREREQUISITE CHECK
======================================================================
✓ All required scripts found
✓ DuckDB module available

======================================================================
STEP 1: Creating Database Schema
======================================================================
▶ Creating DuckDB database
  Running: setup_database.py
✓ Creating DuckDB database - Complete
    Created schemas and tables successfully

======================================================================
STEP 2: Importing Reference Data
======================================================================
▶ Importing players, teams, parks
  Running: add_reference_data.py
✓ Importing players, teams, parks - Complete
✓ Reference data imported:
    Players: 23,456
    Teams: 292
    Parks: 656

======================================================================
STEP 3: Importing Historical Retrosheet Data
======================================================================
  Importing all available years (1900-present)
  This may take several hours...
▶ Importing Retrosheet data
  Running: process_historical.py
✓ Importing Retrosheet data - Complete
✓ Event data imported for 115 years:
    2024: 986,024 events
    2023: 982,156 events
    ...

======================================================================
STEP 4: Building Defensive Statistics
======================================================================
▶ Creating defensive_stats table
✓ Defensive stats created: 1,234,567 records

======================================================================
STEP 5: Building Advanced Analytics
======================================================================
  Running: create_advanced_analytics.py
✓ Advanced analytics tables created
    Created: 703,663 player-season offensive records
    Created: 369,583 player-season pitching records
    Created: 2,931 park-season records
    Created: 1,311,683,956 baserunning event records
    Created: 11,584,937 batted ball classifications
    Created: 42,567,890 batted ball stat records

======================================================================
FINAL VERIFICATION
======================================================================
✓ Database contains 23 tables
    event.events: 17,412,345 records
    game.game_fielding_appearances: 5,678,901 records
    dim.players: 23,456 records
    defensive_stats: 1,234,567 records
    metrics_player_season_league_offense: 703,663 records
    metrics_player_season_league_pitching: 369,583 records
    park_factors: 2,931 records

======================================================================
BUILD SUMMARY
======================================================================
✓ Database built successfully!

  Database location: /Users/markb/dev/baseball.computer/baseball.duckdb
  Completed: 2026-02-02 14:30:00

  To query the database:
    duckdb baseball.duckdb

  Example queries:
    -- Get top hitters by OPS in 2023
    SELECT * FROM metrics_player_season_league_offense
    WHERE season = 2023 AND plate_appearances >= 400
    ORDER BY ops DESC LIMIT 10;

    -- Get top pitchers by ERA in 2023
    SELECT * FROM metrics_player_season_league_pitching
    WHERE season = 2023 AND innings_pitched >= 100
    ORDER BY era ASC LIMIT 10;

    -- Get park factors for 2023
    SELECT * FROM park_factors WHERE season = 2023
    ORDER BY park_factor_overall DESC;

    -- Get a player's defensive stats
    SELECT * FROM defensive_stats
    WHERE player_id = 'judga001'
    ORDER BY season, fielding_position;
```

## Querying the Database

After the build completes, you can query the database directly:

```bash
duckdb baseball.duckdb
```

### Example Queries

```sql
-- Top 10 hitters by OPS in 2023 (min 400 PA)
SELECT
    first_name,
    last_name,
    plate_appearances,
    batting_average,
    on_base_percentage,
    slugging_percentage,
    ops
FROM metrics_player_season_league_offense
WHERE season = 2023 AND plate_appearances >= 400
ORDER BY ops DESC
LIMIT 10;

-- Top 10 pitchers by ERA in 2023 (min 100 IP)
SELECT
    first_name,
    last_name,
    innings_pitched,
    era,
    whip,
    k_per_9
FROM metrics_player_season_league_pitching
WHERE season = 2023 AND innings_pitched >= 100
ORDER BY era ASC
LIMIT 10;

-- Most hitter-friendly parks in 2023
SELECT
    park_name,
    park_city,
    park_factor_overall
FROM park_factors
WHERE season = 2023
ORDER BY park_factor_overall DESC
LIMIT 10;
```

## Troubleshooting

### "Database already exists" message

The script skips database setup if `baseball.duckdb` exists and is > 1MB. To rebuild from scratch:

```bash
rm baseball.duckdb
python3 build_all.py
```

### Step fails with "Script not found"

Ensure you're in the correct directory and all required scripts exist:

```bash
cd /Users/markb/dev/baseball.computer
ls -la setup_database.py add_reference_data.py process_historical.py
```

### "DuckDB module not found"

Install duckdb:

```bash
pip install duckdb
```

### Build takes too long

Build specific years instead of all available data:

```bash
python3 build_all.py 2024 2023 2022 2021
```

### Advanced analytics step fails

Ensure the venv313 Python environment exists:

```bash
python3.13 -m venv venv313
source venv313/bin/activate
pip install duckdb
```

## File Locations

- **Build script**: `/Users/markb/dev/baseball.computer/build_all.py`
- **Database**: `/Users/markb/dev/baseball.computer/baseball.duckdb`
- **Retrosheet files**: `/Users/markb/dev/baseball.computer/retrosheet/`
- **Parser**: `/Users/markb/dev/baseball.computer/baseball.computer.rs/target/release/baseball-computer`

## Related Scripts

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `build_all.py` | Complete build from scratch | First-time setup, full rebuild |
| `setup_database.py` | Create database schema | Manual database setup |
| `add_reference_data.py` | Import reference data | Update players/teams/parks |
| `process_historical.py` | Import event data | Add new years of data |
| `create_defensive_stats_view.sql` | Create defensive stats | Manual defensive stats update |
| `create_advanced_analytics.py` | Create analytics tables | Manual analytics update |

## Data Sources

- **Retrosheet Events**: https://www.retrosheet.org/events/
- **Retrosheet Teams**: https://www.retrosheet.org/teams.zip
- **Retrosheet Ballparks**: https://www.retrosheet.org/ballparks.zip
- **Years available**: ~1900-2024 (as of February 2026)
