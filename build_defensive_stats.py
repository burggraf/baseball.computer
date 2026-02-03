#!/usr/bin/env python3
"""
Build defensive stats tables using dbt.

This script runs dbt to create comprehensive defensive statistics tables
from the Retrosheet event data already imported into baseball.duckdb.

Defensive stats include:
- Counting stats: games, putouts, assists, errors, double plays, etc.
- Rate stats: fielding percentage, range factor, innings played
- Advanced metrics: balls hit to, double plays started, etc.
"""

import subprocess
import sys
from pathlib import Path

# Configuration
BASE_DIR = Path("/Users/markb/dev/baseball.computer")
DBT_DIR = BASE_DIR / "bc"
DB_PATH = BASE_DIR / "baseball.duckdb"
VENV_DIR = BASE_DIR / "venv"

# Colors for output
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"

def print_header(text):
    print(f"\n{'='*70}")
    print(f"{text}")
    print('='*70)

def print_success(text):
    print(f"{GREEN}✓ {text}{RESET}")

def print_warning(text):
    print(f"{YELLOW}⚠ {text}{RESET}")

def print_error(text):
    print(f"{RED}✗ {text}{RESET}")

def run_command(cmd, cwd=None, use_venv=False):
    """Run a command and stream output."""
    print(f"\nRunning: {' '.join(cmd)}")
    print("-" * 70)

    # If using venv, prefix with the activation
    if use_venv and VENV_DIR.exists():
        venv_bin = VENV_DIR / "bin"
        # Use the venv's python/dbt directly
        if cmd[0] == "dbt":
            cmd[0] = str(venv_bin / "dbt")
        elif cmd[0] == "duckdb":
            cmd[0] = str(venv_bin / "duckdb")

    result = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        stdout=None,  # Stream output directly to terminal
        stderr=None
    )

    return result.returncode == 0

def main():
    print_header("Defensive Stats Builder")

    # Verify database exists
    if not DB_PATH.exists():
        print_error(f"Database not found at {DB_PATH}")
        print("Please run process_historical.py first to import Retrosheet data.")
        return 1

    print_success(f"Database found: {DB_PATH}")

    # Check database has data
    duckdb_cmd = ["duckdb", str(DB_PATH), "readonly", "-c",
                  "SELECT COUNT(*) FROM event.events LIMIT 1"]

    result = subprocess.run(
        duckdb_cmd,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print_warning("No event data found. Some tables may be empty.")
        print("The build will still run, using Baseball Databank data where available.")

    print_success("Event data verified")

    # Options for dbt build
    print_header("Build Options")
    print("1. Build only fielding metrics (fastest)")
    print("2. Build all defensive stats tables (recommended)")
    print("3. Build entire dbt project (slowest, includes all stats)")

    choice = input("\nSelect option (1-3) or press Enter for default [2]: ").strip()
    if not choice:
        choice = "2"

    if choice == "1":
        # Build just the final metrics table
        selector = "+metrics_player_season_league_fielding"
        description = "final fielding metrics table"
    elif choice == "2":
        # Build all fielding-related models
        selector = "*fielding*"
        description = "all fielding stats tables"
    elif choice == "3":
        # Build entire project
        selector = ""  # No selector = build everything
        description = "entire dbt project"
    else:
        print_error("Invalid choice")
        return 1

    print_header(f"Building {description}")

    # Build dbt command
    dbt_cmd = ["dbt", "build"]
    if selector:
        dbt_cmd.extend(["--select", selector])

    # Run dbt build (use venv if available)
    if run_command(dbt_cmd, cwd=DBT_DIR, use_venv=True):
        print_header("Build Complete!")
        print_success("Defensive stats tables have been created.")

        # Show what tables were created
        print("\nTables created/updated:")
        if choice == "1":
            print("  - metrics_player_season_league_fielding")
        elif choice == "2":
            print("  - player_position_game_fielding_stats")
            print("  - player_position_team_season_fielding_stats")
            print("  - metrics_player_season_league_fielding")
            print("  - (plus all upstream staging tables)")
        else:
            print("  - All models in the dbt project")

        print("\nTo query the data:")
        print(f"  duckdb {DB_PATH}")

        if choice in ("1", "2"):
            print("\nExample query:")
            print("  SELECT player_id, season, fielding_position,")
            print("         games, putouts, assists, errors,")
            print("         fielding_percentage, range_factor")
            print("  FROM metrics_player_season_league_fielding")
            print("  WHERE season = 2023")
            print("  ORDER BY season, player_id, fielding_position")

        return 0
    else:
        print_header("Build Failed")
        print_error("dbt build encountered errors.")
        print("\nTroubleshooting:")
        print("1. Ensure dbt is installed: pip install dbt-duckdb")
        print("2. Ensure dbt packages are installed: cd bc && dbt deps")
        print("3. Check the error messages above for details")

        return 1

if __name__ == "__main__":
    sys.exit(main())
