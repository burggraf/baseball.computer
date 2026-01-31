# Historical Baseball Data Processing

## Overview

This script downloads and processes Retrosheet baseball data year by year, importing it into the DuckDB database. It processes years backwards from 2022, automatically skipping years that are already in the database.

## How It Determines Where to Start

The script **automatically detects which years to process** by:

1. **Querying the database** for existing years in `event.events` table
2. **Starting from 2022** and going backwards
3. **Skipping years** that already exist in the database
4. **Processing only new years**

For example:
- If database has 2023, 2024 → script starts at 2022
- If database has 2020-2024 → script starts at 2019
- If database is empty → script starts at 2022 and goes back to ~1900

## Running the Script

### Prerequisites

- Python 3.13+ with duckdb installed
- Rust parser built and available at `baseball.computer.rs/target/release/baseball-computer`
- DuckDB database at `baseball.duckdb`

### Commands

```bash
# Navigate to the baseball.computer directory
cd /Users/markb/dev/baseball.computer

# Activate the Python virtual environment
source venv/bin/activate

# Run the script
python3 process_historical.py
```

### What Happens

1. **Scans database** for existing years
2. **Downloads** Retrosheet data for next missing year
3. **Parses** event files with Rust parser
4. **Imports** into DuckDB database
5. **Cleans up** source files after successful import
6. **Repeats** for next year

## Stopping the Script

Press `Ctrl+C` at any time to stop. The script will:

1. **Finish current operation** (download/parse/import step)
2. **Remove all data** for the incomplete year from database
3. **Clean up** any downloaded files for that year
4. **Exit cleanly**

**No partial data is left in the database.**

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

Years to process: 123
Starting from: 2022

============================================================
Processing Year: 2022
============================================================
  Downloading 2022 data...
  Extracting 2022 files...
  Parsing 2022 event files...
  Parsed 1,637,162 events for 2022
  Importing 2022 into database...
  Imported 1,637,162 events for 2022
  Cleaning up 2022 source files...
  Year 2022 completed successfully!

============================================================
Processing Year: 2021
============================================================
  Downloading 2021 data...
  ...
```

## Troubleshooting

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

## File Locations

- **Script**: `/Users/markb/dev/baseball.computer/process_historical.py`
- **Database**: `/Users/markb/dev/baseball.computer/baseball.duckdb`
- **Retrosheet files**: `/Users/markb/dev/baseball.computer/retrosheet/`
- **Parser**: `/Users/markb/dev/baseball.computer/baseball.computer.rs/target/release/baseball-computer`

## Data Sources

- **Retrosheet**: https://www.retrosheet.org/events/
- **Years available**: ~1900-2024 (as of January 2026)
