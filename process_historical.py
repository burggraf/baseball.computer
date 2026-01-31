#!/usr/bin/env python3
"""
Process Retrosheet data year by year, importing to DuckDB.
Can be interrupted safely - incomplete years are removed.
"""

import os
import sys
import signal
import subprocess
import tempfile
import shutil
from pathlib import Path
import duckdb

# Configuration
BASE_DIR = Path("/Users/markb/dev/baseball.computer")
RETROSHEET_DIR = BASE_DIR / "retrosheet"
PARSER_DIR = BASE_DIR / "baseball.computer.rs"
DB_PATH = BASE_DIR / "baseball.duckdb"
RETROSHEET_URL = "https://www.retrosheet.org/events"

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    global shutdown_requested
    print("\n\nShutdown requested. Finishing current year and cleaning up...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def check_interrupted():
    """Check if shutdown was requested"""
    return shutdown_requested

def get_years_to_process():
    """Get list of years to process (2022 and earlier)"""
    # Check what years already exist in database
    con = duckdb.connect(str(DB_PATH))
    try:
        result = con.execute("""
            SELECT DISTINCT SUBSTRING(game_id, 4, 4) as year
            FROM event.events
            ORDER BY year DESC
        """).fetchall()
        existing_years = {row[0] for row in result}
    except:
        existing_years = set()
    finally:
        con.close()
    
    # Start from 2024 and go backwards
    start_year = 2024
    end_year = 1900  # Retrosheet goes back to ~1900
    
    years_to_process = []
    for year in range(start_year, end_year - 1, -1):
        if str(year) not in existing_years:
            years_to_process.append(year)
        else:
            print(f"Year {year} already in database, skipping...")
    
    return years_to_process

def download_year(year, temp_dir):
    """Download Retrosheet data for a specific year"""
    print(f"  Downloading {year} data...")
    
    zip_file = temp_dir / f"{year}eve.zip"
    
    # Download the zip file
    result = subprocess.run(
        ["curl", "-s", "-o", str(zip_file), f"{RETROSHEET_URL}/{year}eve.zip"],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0 or zip_file.stat().st_size < 1000:
        raise Exception(f"Failed to download {year} data")
    
    # Extract files
    print(f"  Extracting {year} files...")
    subprocess.run(
        ["unzip", "-q", "-o", str(zip_file), "-d", str(temp_dir)],
        check=True
    )
    
    # Move files to retrosheet directory
    for ev_file in temp_dir.glob(f"{year}*.EV?"):
        shutil.move(str(ev_file), RETROSHEET_DIR / ev_file.name)
    
    for ros_file in temp_dir.glob(f"*{year}.ROS"):
        shutil.move(str(ros_file), RETROSHEET_DIR / ros_file.name)
    
    # Clean up zip
    zip_file.unlink()
    
    return True

def parse_year(year):
    """Parse Retrosheet data for a specific year using the Rust parser"""
    print(f"  Parsing {year} event files...")
    
    output_dir = PARSER_DIR / f"parser_output_{year}"
    
    # Clean any existing output for this year
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    
    # Run parser
    result = subprocess.run(
        [
            str(PARSER_DIR / "target" / "release" / "baseball-computer"),
            "--input", str(RETROSHEET_DIR),
            "--output-dir", str(output_dir)
        ],
        capture_output=True,
        text=True,
        cwd=str(PARSER_DIR)
    )
    
    if result.returncode != 0:
        raise Exception(f"Parser failed for {year}: {result.stderr}")
    
    # Verify we got data for this year
    events_file = output_dir / "events.csv"
    if not events_file.exists():
        raise Exception(f"No events.csv found for {year}")
    
    # Count events for this year
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE events AS SELECT * FROM read_csv_auto('{events_file}', ignore_errors=True)")
    count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    con.close()
    
    print(f"  Parsed {count:,} events for {year}")
    
    return output_dir, count

def import_year_to_db(year, parser_output_dir):
    """Import parsed data for a year into DuckDB"""
    print(f"  Importing {year} into database...")

    con = duckdb.connect(str(DB_PATH))

    # Get current schema to map columns correctly
    # Parser outputs batting_side, but DB has side
    events_file = parser_output_dir / "events.csv"

    # Load CSV data and transform batting_side to side
    con.execute(f"""
        CREATE TEMP TABLE tmp_events AS
        SELECT *,
            CASE WHEN batting_side = 'bottom' THEN 'bottom' ELSE 'top' END as side
        FROM read_csv_auto('{events_file}', ignore_errors=True)
        WHERE SUBSTRING(game_id, 4, 4) = '{year}'
    """)

    # Drop batting_side column and insert
    con.execute("ALTER TABLE tmp_events DROP COLUMN batting_side")

    # Get columns in temp table and target table
    target_cols = [row[0] for row in con.execute("DESCRIBE SELECT * FROM event.events").fetchall()]
    temp_cols = [row[0] for row in con.execute("DESCRIBE SELECT * FROM tmp_events").fetchall()]

    # Build column list (intersection of both)
    common_cols = [c for c in temp_cols if c in target_cols]

    # Insert using common columns
    cols_str = ", ".join(common_cols)
    con.execute(f"""
        INSERT INTO event.events ({cols_str})
        SELECT {cols_str} FROM tmp_events
    """)

    con.execute("DROP TABLE tmp_events")
    
    # Import other tables similarly
    tables_to_import = [
        ("event_audit", "event"),
        ("event_baserunners", "event"),
        ("event_comments", "event"),
        ("event_fielding_play", "event"),
        ("event_flags", "event"),
        ("event_pitch_sequences", "event"),
        ("game_lineup_appearances", "game"),
        ("game_fielding_appearances", "game"),
        ("game_earned_runs", "game"),
        ("games", "game"),
    ]
    
    for table_name, schema in tables_to_import:
        table_file = parser_output_dir / f"{table_name}.csv"
        if table_file.exists():
            if table_name == "game_lineup_appearances":
                con.execute(f"""
                    INSERT INTO {schema}.{table_name}
                    SELECT
                        game_id, player_id,
                        CASE WHEN side = 'bottom' THEN 'bottom' ELSE 'top' END as side,
                        CAST(lineup_position AS UTINYINT) as lineup_position,
                        entered_game_as,
                        CAST(start_event_id AS UTINYINT) as start_event_id,
                        CAST(end_event_id AS UTINYINT) as end_event_id
                    FROM read_csv_auto('{table_file}', ignore_errors=True)
                    WHERE SUBSTRING(game_id, 4, 4) = '{year}'
                """)
            elif table_name == "game_fielding_appearances":
                con.execute(f"""
                    INSERT INTO {schema}.{table_name}
                    SELECT
                        game_id, player_id,
                        CASE WHEN side = 'bottom' THEN 'bottom' ELSE 'top' END as side,
                        CAST(fielding_position AS UTINYINT) as fielding_position,
                        CAST(start_event_id AS UTINYINT) as start_event_id,
                        CAST(end_event_id AS UTINYINT) as end_event_id
                    FROM read_csv_auto('{table_file}', ignore_errors=True)
                    WHERE SUBSTRING(game_id, 4, 4) = '{year}'
                """)
            elif table_name == "game_earned_runs":
                con.execute(f"""
                    INSERT INTO {schema}.{table_name}
                    SELECT game_id, player_id, CAST(earned_runs AS UTINYINT)
                    FROM read_csv_auto('{table_file}', ignore_errors=True)
                    WHERE SUBSTRING(game_id, 4, 4) = '{year}'
                    GROUP BY game_id, player_id, earned_runs
                """)
            else:
                # Load to temp table first to avoid column resolution issues
                temp_table = f"tmp_import_{table_name}"
                con.execute(f"DROP TABLE IF EXISTS {temp_table}")
                con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM read_csv_auto('{table_file}', ignore_errors=True)")

                # Check if target table exists
                target_exists = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                """).fetchone()[0] > 0

                # Check if temp table has game_id column
                has_game_id = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_name = '{temp_table}' AND column_name = 'game_id'
                """).fetchone()[0] > 0

                if not target_exists:
                    # Create the table from temp table data
                    if has_game_id:
                        con.execute(f"""
                            CREATE TABLE {schema}.{table_name} AS
                            SELECT * FROM {temp_table}
                            WHERE SUBSTRING(game_id, 4, 4) = '{year}'
                        """)
                    else:
                        con.execute(f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM {temp_table}")
                else:
                    # Table exists, insert into it
                    if has_game_id:
                        con.execute(f"""
                            INSERT INTO {schema}.{table_name}
                            SELECT * FROM {temp_table}
                            WHERE SUBSTRING(game_id, 4, 4) = '{year}'
                        """)
                    else:
                        # Tables without game_id - import all (they link via event_id)
                        con.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM {temp_table}")

                con.execute(f"DROP TABLE {temp_table}")
    
    # Verify import
    count = con.execute(f"""
        SELECT COUNT(*) FROM event.events 
        WHERE SUBSTRING(game_id, 4, 4) = '{year}'
    """).fetchone()[0]
    
    con.close()
    
    print(f"  Imported {count:,} events for {year}")
    return count

def cleanup_year_files(year):
    """Remove downloaded files for a year (after successful import)"""
    print(f"  Cleaning up {year} source files...")

    # Remove only event files - keep ROS files for player reference data
    for ev_file in RETROSHEET_DIR.glob(f"{year}*.EV?"):
        ev_file.unlink()

def remove_year_from_db(year):
    """Remove a year's data from the database (for cleanup on failure)"""
    print(f"  Removing {year} data from database...")

    try:
        # Try to connect with a timeout for the lock
        con = duckdb.connect(str(DB_PATH), read_only=False)

        # Delete from all tables
        for schema in ["event", "game"]:
            tables = con.execute(f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{schema}'
            """).fetchall()

            for (table_name,) in tables:
                try:
                    # First check if table has game_id column
                    has_game_id = con.execute(f"""
                        SELECT COUNT(*) FROM information_schema.columns
                        WHERE table_schema = '{schema}' AND table_name = '{table_name}' AND column_name = 'game_id'
                    """).fetchone()[0] > 0

                    if has_game_id:
                        con.execute(f"""
                            DELETE FROM {schema}.{table_name}
                            WHERE SUBSTRING(game_id, 4, 4) = '{year}'
                        """)
                except Exception as e:
                    # Table might not have game_id or other error - log and continue
                    pass

        con.close()
    except Exception as e:
        print(f"  Warning: Could not remove {year} data: {e}")
        # Don't raise - we want to continue cleanup even if this fails

def process_year(year):
    """Process a single year: download, parse, import"""
    print(f"\n{'='*60}")
    print(f"Processing Year: {year}")
    print(f"{'='*60}")
    
    temp_dir = Path(tempfile.mkdtemp())
    parser_output_dir = None
    
    try:
        # Download
        if check_interrupted():
            return False
        download_year(year, temp_dir)
        
        # Parse
        if check_interrupted():
            remove_year_from_db(year)
            return False
        parser_output_dir, event_count = parse_year(year)
        
        if event_count == 0:
            print(f"  Warning: No events found for {year}")
            return True  # Skip but don't fail
        
        # Import
        if check_interrupted():
            remove_year_from_db(year)
            if parser_output_dir:
                shutil.rmtree(parser_output_dir)
            return False
        import_year_to_db(year, parser_output_dir)
        
        # Cleanup
        cleanup_year_files(year)
        if parser_output_dir:
            shutil.rmtree(parser_output_dir)
        
        print(f"  Year {year} completed successfully!")
        return True
        
    except Exception as e:
        print(f"  ERROR processing {year}: {e}")
        # Cleanup incomplete data
        remove_year_from_db(year)
        if parser_output_dir and parser_output_dir.exists():
            shutil.rmtree(parser_output_dir)
        # Also cleanup files
        for ev_file in RETROSHEET_DIR.glob(f"{year}*.EV?"):
            ev_file.unlink()
        for ros_file in RETROSHEET_DIR.glob(f"*{year}.ROS"):
            ros_file.unlink()
        return False
    
    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir)

def main():
    """Main entry point"""
    print("="*60)
    print("Retrosheet Historical Data Processor")
    print("="*60)
    print(f"Database: {DB_PATH}")
    print(f"Retrosheet dir: {RETROSHEET_DIR}")
    print(f"Parser: {PARSER_DIR}")
    print()
    print("Press Ctrl+C to stop (will clean up current year)")
    print("="*60)
    
    # Get years to process
    years = get_years_to_process()
    
    if not years:
        print("\nNo new years to process!")
        return
    
    print(f"\nYears to process: {len(years)}")
    print(f"Starting from: {years[0]}")
    print()
    
    # Process each year
    successful = []
    failed = []
    
    for year in years:
        if process_year(year):
            successful.append(year)
        else:
            failed.append(year)
            if check_interrupted():
                print("\nStopped by user request")
                break
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Successfully processed: {len(successful)} years")
    if successful:
        print(f"  {', '.join(map(str, successful))}")
    print(f"Failed: {len(failed)} years")
    if failed:
        print(f"  {', '.join(map(str, failed))}")
    
    # Show current database state
    con = duckdb.connect(str(DB_PATH))
    result = con.execute("""
        SELECT 
            SUBSTRING(game_id, 4, 4) as year,
            COUNT(*) as event_count
        FROM event.events
        GROUP BY year
        ORDER BY year
    """).fetchall()
    con.close()
    
    print(f"\nDatabase now contains {len(result)} years:")
    for year, count in result:
        print(f"  {year}: {count:,} events")

if __name__ == "__main__":
    main()
