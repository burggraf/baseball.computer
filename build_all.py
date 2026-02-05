#!/usr/bin/env python3
"""
Complete Baseball Database Build Script

This script builds the entire baseball.computer database from scratch,
including:
1. DuckDB database setup
2. Reference data (players, teams, parks) from Retrosheet
3. Historical Retrosheet event data (1900-present)
4. Defensive statistics table

Requirements:
- Python 3.11+ (for dbt compatibility)
- duckdb Python package
- Virtual environment at ./venv313 for dbt

Usage:
    python3 build_all.py [years]

    If no years specified, imports all available years (1900-present)
    To import specific years: python3 build_all.py 2023 2022 2021
"""

import os
import sys
import shutil
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

# Use script directory as BASE_DIR for portability across machines
BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = BASE_DIR / "baseball.duckdb"
RETROSHEET_DIR = BASE_DIR / "retrosheet"
BC_DIR = BASE_DIR / "bc"
VENV_DIR = BASE_DIR / "venv313"
VENV_PYTHON = VENV_DIR / "bin" / "python3"

# Colors for output
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"

def print_header(text):
    print(f"\n{'='*70}")
    print(f"{BLUE}{text}{RESET}")
    print('='*70)

def print_success(text):
    print(f"{GREEN}✓ {text}{RESET}")

def print_warning(text):
    print(f"{YELLOW}⚠ {text}{RESET}")

def print_error(text):
    print(f"{RED}✗ {text}{RESET}")

def print_step(text):
    print(f"\n{BLUE}▶ {text}{RESET}")

def run_script(script_path, description, timeout=600000):
    """Run a Python script and return success status."""
    print_step(description)

    if not script_path.exists():
        print_error(f"Script not found: {script_path}")
        return False

    # Use venv python if available
    python_exe = str(VENV_PYTHON) if VENV_PYTHON.exists() else "python3"

    print(f"  Running: {script_path.name}")
    result = subprocess.run(
        [python_exe, str(script_path)],
        capture_output=True,
        text=True,
        timeout=timeout
    )

    if result.returncode == 0:
        print_success(f"{description} - Complete")
        # Print last few lines of output for context
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            for line in lines[-5:]:
                if line.strip():
                    print(f"    {line}")
        return True
    else:
        print_error(f"{description} - Failed")
        if result.stderr:
            print(f"    Error: {result.stderr[-500:]}")
        return False

def setup_database():
    """Step 1: Create the database schema."""
    print_header("STEP 1: Creating Database Schema")

    # Check if database already exists and has data
    if DB_PATH.exists() and DB_PATH.stat().st_size > 1000000:  # > 1MB means likely has data
        print_warning(f"Database already exists at {DB_PATH}")
        print("  Skipping setup. To rebuild from scratch, delete the database first.")
        return True

    # Run setup_database.py
    setup_script = BASE_DIR / "setup_database.py"
    if not run_script(setup_script, "Creating DuckDB database"):
        return False

    print_success("Database schema created")
    return True

def add_reference_data():
    """Step 2: Import reference data (players, teams, parks)."""
    print_header("STEP 2: Importing Reference Data")

    ref_script = BASE_DIR / "add_reference_data.py"
    if not run_script(ref_script, "Importing players, teams, parks"):
        return False

    # Show summary
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=False)
        player_count = con.execute("SELECT COUNT(*) FROM dim.players").fetchone()[0]
        team_count = con.execute("SELECT COUNT(*) FROM dim.teams").fetchone()[0]
        park_count = con.execute("SELECT COUNT(*) FROM dim.parks").fetchone()[0]
        con.close()

        print_success(f"Reference data imported:")
        print(f"    Players: {player_count:,}")
        print(f"    Teams: {team_count:,}")
        print(f"    Parks: {park_count:,}")
    except Exception as e:
        print_warning(f"Could not verify reference data: {e}")

    return True

def import_historical_data(years=None):
    """Step 3: Import Retrosheet event data."""
    print_header("STEP 3: Importing Historical Retrosheet Data")

    if years:
        print(f"  Importing specific years: {', '.join(map(str, years))}")
    else:
        print("  Importing all available years (1900-present)")
        print("  This may take several hours...")

    # Run process_historical.py
    hist_script = BASE_DIR / "process_historical.py"
    if not run_script(hist_script, f"Importing Retrosheet data", timeout=3600):
        return False

    # Show summary
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=False)
        result = con.execute("""
            SELECT
                SUBSTRING(game_id, 4, 4) as year,
                COUNT(*) as event_count
            FROM event.events
            GROUP BY year
            ORDER BY year DESC
        """).fetchall()
        con.close()

        if result:
            print_success(f"Event data imported for {len(result)} years:")
            for year, count in result[:10]:
                print(f"    {year}: {count:,} events")
            if len(result) > 10:
                print(f"    ... and {len(result) - 10} more years")
    except Exception as e:
        print_warning(f"Could not verify event data: {e}")

    # Verify critical tables exist
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=False)

        # Critical tables required for advanced analytics
        critical_tables = [
            ('event', 'events'),
            ('event', 'event_baserunners'),
            ('event', 'event_pitch_sequences'),
        ]

        missing_tables = []
        for schema, table in critical_tables:
            result = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_name = '{table}'
            """).fetchone()[0]
            if result == 0:
                missing_tables.append(f'{schema}.{table}')

        con.close()

        if missing_tables:
            print_error(f"Critical tables missing after import: {', '.join(missing_tables)}")
            print("\n  This usually means the Rust parser failed to create CSV files.")
            print("  Check that:")
            print("  1. The Rust parser was built: cd baseball.computer.rs && cargo build --release")
            print("  2. Retrosheet event files exist in the retrosheet/ directory")
            print("  3. The parser ran successfully (check process_historical.py output)")
            return False

        print_success("All critical tables verified")

    except Exception as e:
        print_warning(f"Could not verify critical tables: {e}")

    return True

def build_defensive_stats():
    """Step 4: Create defensive statistics table."""
    print_header("STEP 4: Building Defensive Statistics")

    sql_script = BASE_DIR / "create_defensive_stats_view.sql"
    if not sql_script.exists():
        print_error("SQL script not found. Skipping defensive stats.")
        return True

    print_step("Creating defensive_stats table")

    # Read and execute SQL
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=False)

        with open(sql_script, 'r') as f:
            sql = f.read()

        # Split and execute statements
        # Remove comments first
        sql_lines = []
        for line in sql.split('\n'):
            if '--' in line and not line.strip().startswith('--'):
                sql_lines.append(line.split('--')[0])
            elif not line.strip().startswith('--'):
                sql_lines.append(line)

        sql = '\n'.join(sql_lines)

        # Execute the main table creation
        con.execute(sql)

        # Verify
        result = con.execute("SELECT COUNT(*) FROM defensive_stats").fetchone()[0]
        con.close()

        print_success(f"Defensive stats created: {result:,} records")
        return True

    except Exception as e:
        print_error(f"Failed to create defensive stats: {e}")
        return False

def build_dbt_models():
    """Step 5: Build advanced analytics tables (bypassing dbt, using direct SQL)."""
    print_header("STEP 5: Building Advanced Analytics")

    # Run the advanced analytics script
    analytics_script = BASE_DIR / "create_advanced_analytics.py"

    if not analytics_script.exists():
        print_error(f"Analytics script not found: {analytics_script}")
        return False

    print(f"  Running: {analytics_script.name}")

    result = subprocess.run(
        [str(VENV_PYTHON), str(analytics_script)],
        capture_output=True,
        text=True,
        timeout=1800  # 30 minutes
    )

    if result.returncode == 0:
        print_success("Advanced analytics tables created")

        # Show summary from output
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            for line in lines[-20:]:
                if 'Created:' in line or 'records' in line:
                    print(f"    {line.strip()}")

        return True
    else:
        print_error("Failed to create advanced analytics tables")
        if result.stderr:
            print(f"    Error: {result.stderr[-500:]}")
        return False

def add_lahman_validation():
    """Step 6: Import Lahman Baseball Database as validation tables."""
    print_header("STEP 6: Importing Lahman Validation Data")

    lahman_script = BASE_DIR / "add_lahman_validation_data.py"

    # Check if Lahman CSV directory exists
    lahman_dir = BASE_DIR / "lahman_1871-2025_csv"
    if not lahman_dir.exists():
        print_warning(f"Lahman CSV directory not found: {lahman_dir}")
        print("  Skipping Lahman validation data import.")
        print("  To enable: Download Lahman database from SABR and extract to lahman_1871-2025_csv/")
        return True  # Not a failure - optional step

    if not lahman_script.exists():
        print_warning(f"Lahman import script not found: {lahman_script}")
        return True  # Not a failure - optional step

    if not run_script(lahman_script, "Importing Lahman validation data"):
        return False

    print_success("Lahman validation data imported to 'validation' schema")
    return True

def verify_database():
    """Final verification step."""
    print_header("FINAL VERIFICATION")

    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)

        # Check tables
        tables = con.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('system', 'pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
        """).fetchall()

        print_success(f"Database contains {len(tables)} tables")

        # Show key tables
        key_tables = [
            ('event', 'events'),
            ('game', 'game_fielding_appearances'),
            ('dim', 'players'),
            (None, 'defensive_stats'),
            # advanced analytics models
            (None, 'metrics_player_season_league_offense'),
            (None, 'metrics_player_season_league_pitching'),
            (None, 'park_factors'),
            (None, 'event_baserunning_stats'),
            (None, 'calc_batted_ball_type'),
            (None, 'event_batted_ball_stats'),
            # validation tables
            ('validation', 'lahman_people'),
            ('validation', 'lahman_batting'),
            ('validation', 'lahman_pitching'),
            ('validation', 'lahman_teams'),
        ]

        for schema, table in key_tables:
            if schema:
                full_name = f"{schema}.{table}"
            else:
                full_name = table

            try:
                count = con.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
                print(f"    {full_name}: {count:,} records")
            except:
                print(f"    {full_name}: (empty or not found)")

        con.close()
        return True

    except Exception as e:
        print_error(f"Verification failed: {e}")
        return False

def main():
    print_header("BASEBALL.COMPUTER DATABASE BUILDER")
    print(f"  Database: {DB_PATH}")
    print(f"  Base dir: {BASE_DIR}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Check prerequisites
    print_header("PREREQUISITE CHECK")

    # Check for required scripts
    required_scripts = [
        "setup_database.py",
        "add_reference_data.py",
        "process_historical.py"
    ]

    for script in required_scripts:
        if not (BASE_DIR / script).exists():
            print_error(f"Required script not found: {script}")
            return 1

    print_success("All required scripts found")

    # Check for duckdb
    try:
        import duckdb
        print_success("DuckDB module available")
    except ImportError:
        print_error("DuckDB module not found. Install with: pip install duckdb")
        return 1

    # Check for Rust parser
    parser_binary = BASE_DIR / "baseball.computer.rs" / "target" / "release" / "baseball-computer"
    if not parser_binary.exists():
        print_error("Rust parser binary not found!")
        print("\n  The Rust parser is required for importing Retrosheet data.")
        print("  Please clone and build it:")
        print()
        print("    git clone https://github.com/droher/baseball.computer.rs.git")
        print("    cd baseball.computer.rs")
        print("    cargo build --release")
        print()
        return 1
    print_success("Rust parser binary found")

    # Parse command line args for specific years
    years = None
    if len(sys.argv) > 1:
        try:
            years = [int(arg) for arg in sys.argv[1:]]
            print_success(f"Will import specific years: {years}")
        except ValueError:
            print_warning("Invalid year arguments. Importing all available years.")

    # Build steps
    steps = [
        ("Database Setup", setup_database),
        ("Reference Data", add_reference_data),
        ("Historical Data", lambda: import_historical_data(years)),
        ("Defensive Stats", build_defensive_stats),
        ("Advanced Analytics", build_dbt_models),
        ("Lahman Validation Data", add_lahman_validation),
        ("Verification", verify_database),
    ]

    failed_steps = []

    for step_name, step_func in steps:
        try:
            if not step_func():
                failed_steps.append(step_name)
                print_error(f"Step '{step_name}' failed. Stopping.")
                break
        except KeyboardInterrupt:
            print_warning("\nBuild interrupted by user")
            return 1
        except Exception as e:
            print_error(f"Step '{step_name}' encountered an error: {e}")
            failed_steps.append(step_name)
            break

    # Final summary
    print_header("BUILD SUMMARY")

    if not failed_steps:
        print_success("Database built successfully!")
        print(f"\n  Database location: {DB_PATH}")
        print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n  To query the database:")
        print(f"    duckdb {DB_PATH}")
        print("\n  Example queries:")
        print("    -- Get top hitters by OPS in 2023")
        print("    SELECT * FROM metrics_player_season_league_offense")
        print("    WHERE season = 2023 AND plate_appearances >= 400")
        print("    ORDER BY ops DESC LIMIT 10;")
        print()
        print("    -- Get top pitchers by ERA in 2023")
        print("    SELECT * FROM metrics_player_season_league_pitching")
        print("    WHERE season = 2023 AND innings_pitched >= 100")
        print("    ORDER BY era ASC LIMIT 10;")
        print()
        print("    -- Get park factors for 2023")
        print("    SELECT * FROM park_factors WHERE season = 2023")
        print("    ORDER BY park_factor_overall DESC;")
        print()
        print("    -- Get a player's defensive stats")
        print("    SELECT * FROM defensive_stats")
        print("    WHERE player_id = 'judga001'")
        print("    ORDER BY season, fielding_position;")
        print()
        print("    -- Validate our metrics against Lahman data")
        print("    SELECT o.season, o.player_id, p.first_name, p.last_name,")
        print("           o.batting_average AS our_ba, l.batting_average AS lahman_ba")
        print("    FROM metrics_player_season_league_offense o")
        print("    JOIN validation.lahman_batting_season_agg l")
        print("      ON o.player_id = l.player_id AND o.season = l.season")
        print("    JOIN dim.players p ON o.player_id = p.player_id")
        print("    WHERE o.season = 2023 AND o.at_bats >= 400")
        print("    ORDER BY o.ops DESC LIMIT 10;")
        return 0
    else:
        print_error(f"Build failed at step(s): {', '.join(failed_steps)}")
        print("\nTroubleshooting:")
        print("1. Check error messages above")
        print("2. Ensure Retrosheet directory exists and has .ROS files")
        print("3. Try re-running from the failed step")
        return 1

if __name__ == "__main__":
    sys.exit(main())
