# Historical Baseball Data Processing

## Overview

This script downloads and processes Retrosheet baseball data year by year, importing it into the DuckDB database. It processes years backwards from 2024, automatically skipping years that are already in the database.

## First-Time Setup

**On a fresh clone, you MUST create the database first:**

```bash
cd /Users/markb/dev/baseball.computer
source venv/bin/activate
python3 setup_database.py
```

This creates `baseball.duckdb` with the proper schema.

## How It Determines Where to Start

The script **automatically detects which years to process** by:

1. **Querying the database** for existing years in `event.events` table
2. **Starting from 2024** and going backwards
3. **Skipping years** that already exist in the database
4. **Processing only new years**

For example:
- If database has 2023, 2024 → script starts at 2022
- If database has 2020-2024 → script starts at 2019
- If database is empty → script starts at 2024 and goes back to ~1900

## Running the Script

### Prerequisites

- Python 3.13+ with duckdb installed
- Rust parser built and available at `baseball.computer.rs/target/release/baseball-computer`
- DuckDB database created with `setup_database.py`

### Commands

```bash
# Navigate to the baseball.computer directory
cd /Users/markb/dev/baseball.computer

# Activate the Python virtual environment
source venv/bin/activate

# Run the import script (downloads and imports event data)
python3 process_historical.py

# IMPORTANT: After importing event data, run this to update reference tables
python3 add_reference_data.py
```

### What Happens

**`process_historical.py`:**
1. **Scans database** for existing years
2. **Downloads** Retrosheet data for next missing year
3. **Parses** event files with Rust parser
4. **Imports** events into DuckDB database
5. **Keeps** roster files (.ROS) for reference data import
6. **Repeats** for next year

**`add_reference_data.py`:**
1. **Downloads** complete teams and parks database from Retrosheet (292 teams, 656 parks)
2. **Reads** all roster files (.ROS) in retrosheet directory
3. **Imports** players, teams, and parks into dimension tables

### Why Run `add_reference_data.py` After Import?

- **Players**: Only imported from `.ROS` files you have. As you import more years, run this again to get new players.
- **Teams/Parks**: Downloaded from Retrosheet's complete database (1871-2025). Includes historical teams, defunct franchises, Negro leagues, etc.
- **Safe to run multiple times**: Uses `INSERT OR REPLACE` - no duplicates created.

## Stopping the Script

Press `Ctrl+C` at any time to stop. The script will:

1. **Finish current operation** (download/parse/import step)
2. **Remove all data** for the incomplete year from database
3. **Clean up** any downloaded files for that year
4. **Exit cleanly**

**No partial data is left in the database.**

## Fresh Clone Instructions

```bash
# Clone the repository
git clone git@github.com:burggraf/baseball.computer.git
cd baseball.computer

# Set up Python environment
python3.13 -m venv venv
source venv/bin/activate
pip install duckdb

# Clone and build the Rust parser
git clone git@github.com:burggraf/baseball.computer.rs.git
cd baseball.computer.rs
cargo build --release
cd ..

# Create the database
python3 setup_database.py

# Run the import script (let it run for as many years as you want)
python3 process_historical.py

# After import completes (or you stop it), update reference data
python3 add_reference_data.py
```

## Example Output

```
============================================================
Retrosheet Historical Data Processor
============================================================
Database: /Users/markb/dev/baseball.computer/baseball.duckdb
Retrosheet dir: /Users/markb/dev/baseball.computer/retrosheet
Parser: /Users/markb/dev/baseball.computer/baseball.computer.rs

Press Ctrl+C to stop (will clean up current year)
============================================================

Years to process: 125
Starting from: 2024

============================================================
Processing Year: 2024
============================================================
  Downloading 2024 data...
  Extracting 2024 files...
  Parsing 2024 event files...
  Parsed 986,024 events for 2024
  Importing 2024 into database...
  Imported 248,740 events for 2024
  Cleaning up 2024 source files...
  Year 2024 completed successfully!
```

After import completes, run `add_reference_data.py`:

```
Downloading reference data from Retrosheet...
  Downloading teams.csv...
  Downloading ballparks.csv...

Importing teams...
  Imported 292 teams

Importing parks...
  Imported 656 parks

Importing player data from roster files...
  Found 5,079 unique players

============================================================
SUMMARY
============================================================
Players: 5,079
Teams: 292
Parks: 656
```

## Understanding the Reference Data

| Table | Count | Source | Years covered |
|-------|-------|--------|---------------|
| **Players** | Grows as you import | `.ROS` files in retrosheet/ | Only years you've imported |
| **Teams** | 292 | Retrosheet teams.csv | 1871-2025 (complete) |
| **Parks** | 656 | Retrosheet ballparks.csv | All historical (complete) |

- **Teams and Parks** are complete - includes all historical franchises, Negro leagues, defunct teams
- **Players** will grow as you import more years - re-run `add_reference_data.py` after each import session

## Troubleshooting

### "Table does not exist" error

You forgot to run `setup_database.py` first. Run it and try again.

### Script fails with "Parser error"

Check that the Rust parser is built:
```bash
cd baseball.computer.rs
cargo build --release
```

### Script fails with "Database locked"

Another process is using the database. Close any other DuckDB connections and retry.

### Script stops with "ERROR processing year"

The error will be displayed, and all data for that year is removed from the database. Fix the issue and rerun - the script will retry that year.

### CSV parsing errors with commas in quoted values

The script now uses `ignore_errors=True` for CSV imports to handle edge cases like:
```
"RelayToFielderWithNoOutMade([Catcher, Shortstop])"
```

## File Locations

- **Scripts**: `/Users/markb/dev/baseball.computer/process_historical.py`, `setup_database.py`, `add_reference_data.py`
- **Database**: `/Users/markb/dev/baseball.computer/baseball.duckdb`
- **Retrosheet files**: `/Users/markb/dev/baseball.computer/retrosheet/`
- **Parser**: `/Users/markb/dev/baseball.computer/baseball.computer.rs/target/release/baseball-computer`

## Data Sources

- **Retrosheet Events**: https://www.retrosheet.org/events/
- **Retrosheet Teams**: https://www.retrosheet.org/teams.zip
- **Retrosheet Ballparks**: https://www.retrosheet.org/ballparks.zip
- **Years available**: ~1900-2024 (as of January 2026)
