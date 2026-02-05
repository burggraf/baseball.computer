#!/usr/bin/env python3
"""
Import Baseball Databank (Lahman) data as validation tables.
This data covers 1871-2025 and can be used to validate our metrics.
"""

import duckdb
from pathlib import Path

# Use script directory for portability
BASE_DIR = Path(__file__).parent.resolve()
DB_PATH = BASE_DIR / "baseball.duckdb"
LAHMAN_DIR = BASE_DIR / "lahman_1871-2025_csv"

con = duckdb.connect(str(DB_PATH), read_only=False)

# Create validation schema
con.execute("CREATE SCHEMA IF NOT EXISTS validation")

print("="*70)
print("Importing Lahman Baseball Database (1871-2025)")
print("="*70)

# ============================================================
# 1. Import People
# ============================================================
print("\n1. Creating validation.lahman_people...")

con.execute("DROP TABLE IF EXISTS validation.lahman_people")

con.execute(f"""
    CREATE TABLE validation.lahman_people AS
    SELECT
        playerID AS player_id,
        nameFirst AS first_name,
        nameLast AS last_name,
        birthYear AS birth_year,
        birthMonth AS birth_month,
        birthDay AS birth_day,
        birthCountry AS birth_country,
        birthState AS birth_state,
        birthCity AS birth_city,
        deathYear AS death_year,
        deathMonth AS death_month,
        deathDay AS death_day,
        deathCountry AS death_country,
        deathState AS death_state,
        deathCity AS death_city,
        nameGiven AS given_name,
        weight,
        height,
        bats,
        throws,
        debut,
        finalGame AS final_game,
        retroID AS retro_id,
        bbrefID AS bbref_id,
        'lahman' AS data_source
    FROM read_csv_auto('{LAHMAN_DIR}/People.csv')
""")

count = con.execute("SELECT COUNT(*) FROM validation.lahman_people").fetchone()[0]
print(f"   Imported: {count:,} player records")

# ============================================================
# 2. Import Batting (1871-2025)
# ============================================================
print("\n2. Creating validation.lahman_batting...")

con.execute("DROP TABLE IF EXISTS validation.lahman_batting")

con.execute(f"""
    CREATE TABLE validation.lahman_batting AS
    SELECT
        b.playerID AS player_id,
        p.retroID AS retro_id,
        b.yearID AS season,
        b.stint,
        b.teamID AS team_id,
        b.lgID AS league_id,
        b.G AS games,
        b.AB AS at_bats,
        b.R AS runs,
        b.H AS hits,
        b.H - b."2B" - b."3B" - b.HR AS singles,
        b."2B" AS doubles,
        b."3B" AS triples,
        b.HR AS home_runs,
        b.RBI AS runs_batted_in,
        b.SB AS stolen_bases,
        b.CS AS caught_stealing,
        b.BB AS walks,
        b.SO AS strikeouts,
        b.IBB AS intentional_walks,
        b.HBP AS hit_by_pitch,
        b.SH AS sacrifice_bunts,
        b.SF AS sacrifice_flies,
        b.GIDP AS grounded_into_double_play,
        'lahman' AS data_source
    FROM read_csv_auto('{LAHMAN_DIR}/Batting.csv') b
    LEFT JOIN read_csv_auto('{LAHMAN_DIR}/People.csv') p ON b.playerID = p.playerID
""")

count = con.execute("SELECT COUNT(*) FROM validation.lahman_batting").fetchone()[0]
year_range = con.execute("SELECT MIN(season), MAX(season) FROM validation.lahman_batting").fetchone()
print(f"   Imported: {count:,} batting records ({year_range[0]}-{year_range[1]})")

# ============================================================
# 3. Import Pitching (1871-2025)
# ============================================================
print("\n3. Creating validation.lahman_pitching...")

con.execute("DROP TABLE IF EXISTS validation.lahman_pitching")

con.execute(f"""
    CREATE TABLE validation.lahman_pitching AS
    SELECT
        p.playerID AS player_id,
        ppl.retroID AS retro_id,
        p.yearID AS season,
        p.stint,
        p.teamID AS team_id,
        p.lgID AS league_id,
        p.W AS wins,
        p.L AS losses,
        p.G AS games,
        p.GS AS games_started,
        p.CG AS complete_games,
        p.SHO AS shutouts,
        p.SV AS saves,
        p.IPOuts AS outs_recorded,
        p.IPOuts / 3.0 AS innings_pitched,
        p.H AS hits,
        p.ER AS earned_runs,
        p.HR AS home_runs_allowed,
        p.R AS runs,
        p.BB AS walks,
        p.SO AS strikeouts,
        p.BAOpp AS opposing_batting_average,
        p.ERA AS era,
        p.BFP AS batters_faced,
        p.GF AS games_finished,
        p.R AS runs_allowed,
        p.SH AS shutouts_hit,
        p.SF AS sacrifice_flies_hit,
        p.GIDP AS ground_into_double_play,
        p.WP AS wild_pitches,
        p.BK AS balks,
        'lahman' AS data_source
    FROM read_csv_auto('{LAHMAN_DIR}/Pitching.csv') p
    LEFT JOIN read_csv_auto('{LAHMAN_DIR}/People.csv') ppl ON p.playerID = ppl.playerID
""")

count = con.execute("SELECT COUNT(*) FROM validation.lahman_pitching").fetchone()[0]
year_range = con.execute("SELECT MIN(season), MAX(season) FROM validation.lahman_pitching").fetchone()
print(f"   Imported: {count:,} pitching records ({year_range[0]}-{year_range[1]})")

# ============================================================
# 4. Import Fielding (1871-2025)
# ============================================================
print("\n4. Creating validation.lahman_fielding...")

con.execute("DROP TABLE IF EXISTS validation.lahman_fielding")

con.execute(f"""
    CREATE TABLE validation.lahman_fielding AS
    SELECT
        f.playerID AS player_id,
        p.retroID AS retro_id,
        f.yearID AS season,
        f.stint,
        f.teamID AS team_id,
        f.lgID AS league_id,
        f.POS AS position,
        f.G AS games,
        f.GS AS games_started,
        f.InnOuts AS innings_played_outs,
        f.PO AS putouts,
        f.A AS assists,
        f.E AS errors,
        f.DP AS double_plays,
        f.PB AS passed_balls,
        f.WP AS wild_pitches,
        f.SB AS stolen_bases_allowed,
        f.CS AS caught_stealing,
        f.ZR AS zone_rating,
        'lahman' AS data_source
    FROM read_csv_auto('{LAHMAN_DIR}/Fielding.csv') f
    LEFT JOIN read_csv_auto('{LAHMAN_DIR}/People.csv') p ON f.playerID = p.playerID
""")

count = con.execute("SELECT COUNT(*) FROM validation.lahman_fielding").fetchone()[0]
year_range = con.execute("SELECT MIN(season), MAX(season) FROM validation.lahman_fielding").fetchone()
print(f"   Imported: {count:,} fielding records ({year_range[0]}-{year_range[1]})")

# ============================================================
# 5. Import Teams
# ============================================================
print("\n5. Creating validation.lahman_teams...")

con.execute("DROP TABLE IF EXISTS validation.lahman_teams")

con.execute(f"""
    CREATE TABLE validation.lahman_teams AS
    SELECT
        yearID AS season,
        lgID AS league_id,
        teamID AS team_id,
        franchID AS franchise_id,
        divID AS division_id,
        divWin AS division_win,
        WCWin AS wild_card_win,
        LgWin AS league_win,
        WSWin AS world_series_win,
        name AS team_name,
        park AS park_name,
        attendance,
        G AS games,
        Ghome AS home_games,
        W AS wins,
        L AS losses,
        DivWin AS division_win_flag,
        WCWin AS wild_card_win_flag,
        LgWin AS league_win_flag,
        WSWin AS world_series_win_flag,
        R AS runs,
        AB AS at_bats,
        H AS hits,
        "2B" AS doubles,
        "3B" AS triples,
        HR AS home_runs,
        BB AS walks,
        SO AS strikeouts,
        SB AS stolen_bases,
        CS AS caught_stealing,
        HBP AS hit_by_pitch,
        SF AS sacrifice_flies,
        RA AS runs_allowed,
        ER AS earned_runs,
        ERA AS era,
        CG AS complete_games,
        SHO AS shutouts,
        SV AS saves,
        IPOuts AS outs_recorded,
        HA AS hits_allowed,
        HRA AS home_runs_allowed,
        BBA AS walks_allowed,
        SOA AS strikeouts_allowed,
        E AS errors,
        DP AS double_plays,
        FP AS fielding_percentage,
        'lahman' AS data_source
    FROM read_csv_auto('{LAHMAN_DIR}/Teams.csv')
""")

count = con.execute("SELECT COUNT(*) FROM validation.lahman_teams").fetchone()[0]
year_range = con.execute("SELECT MIN(season), MAX(season) FROM validation.lahman_teams").fetchone()
print(f"   Imported: {count:,} team records ({year_range[0]}-{year_range[1]})")

# ============================================================
# 6. Create aggregate views for easier validation
# ============================================================
print("\n6. Creating validation summary views...")

con.execute("DROP VIEW IF EXISTS validation.lahman_batting_season_agg")

con.execute("""
    CREATE VIEW validation.lahman_batting_season_agg AS
    SELECT
        player_id,
        retro_id,
        season,
        SUM(at_bats) AS at_bats,
        SUM(hits) AS hits,
        SUM(doubles) AS doubles,
        SUM(triples) AS triples,
        SUM(home_runs) AS home_runs,
        SUM(runs) AS runs,
        SUM(runs_batted_in) AS runs_batted_in,
        SUM(walks) AS walks,
        SUM(intentional_walks) AS intentional_walks,
        SUM(hit_by_pitch) AS hit_by_pitch,
        SUM(sacrifice_bunts) AS sacrifice_bunts,
        SUM(sacrifice_flies) AS sacrifice_flies,
        SUM(strikeouts) AS strikeouts,
        SUM(hits) - SUM(doubles) - SUM(triples) - SUM(home_runs) AS singles,
        SUM(doubles) + 2*SUM(triples) + 3*SUM(home_runs) AS total_bases,
        ROUND(CAST(SUM(hits) AS DOUBLE) / NULLIF(SUM(at_bats), 0), 3) AS batting_average,
        ROUND(CAST(SUM(hits) + SUM(walks) + SUM(hit_by_pitch) AS DOUBLE) /
              NULLIF(SUM(at_bats) + SUM(walks) + SUM(hit_by_pitch) + SUM(sacrifice_flies), 0), 3) AS on_base_percentage,
        ROUND(CAST(SUM(doubles) + 2*SUM(triples) + 3*SUM(home_runs) AS DOUBLE) /
              NULLIF(SUM(at_bats), 0), 3) AS slugging_percentage,
        ROUND((CAST(SUM(hits) + SUM(walks) + SUM(hit_by_pitch) AS DOUBLE) /
              NULLIF(SUM(at_bats) + SUM(walks) + SUM(hit_by_pitch) + SUM(sacrifice_flies), 0)) +
              (CAST(SUM(doubles) + 2*SUM(triples) + 3*SUM(home_runs) AS DOUBLE) /
              NULLIF(SUM(at_bats), 0)), 3) AS ops
    FROM validation.lahman_batting
    GROUP BY player_id, retro_id, season
""")

con.execute("DROP VIEW IF EXISTS validation.lahman_pitching_season_agg")

con.execute("""
    CREATE VIEW validation.lahman_pitching_season_agg AS
    SELECT
        player_id,
        retro_id,
        season,
        SUM(outs_recorded) AS outs_recorded,
        SUM(outs_recorded) / 3.0 AS innings_pitched,
        SUM(hits) AS hits,
        SUM(walks) AS walks,
        SUM(strikeouts) AS strikeouts,
        SUM(home_runs_allowed) AS home_runs_allowed,
        SUM(runs) AS runs,
        SUM(earned_runs) AS earned_runs,
        ROUND(CAST(SUM(hits) + SUM(walks) AS DOUBLE) /
              NULLIF(SUM(batters_faced), 0), 3) AS whip,
        ROUND(CAST(SUM(earned_runs) * 9 AS DOUBLE) /
              NULLIF(SUM(outs_recorded) / 3, 0), 2) AS era
    FROM validation.lahman_pitching
    GROUP BY player_id, retro_id, season
""")

print("   Created: lahman_batting_season_agg view")
print("   Created: lahman_pitching_season_agg view")

# ============================================================
# 7. Run validation checks
# ============================================================
print("\n" + "="*70)
print("Running Validation Checks (Comparing Our Metrics vs Lahman)")
print("="*70)

validation_years = [1950, 1980, 2000, 2020, 2022, 2023, 2024]

for year in validation_years:
    print(f"\n{year} Season:")

    # Batting comparison
    our_batting = con.execute(f"""
        SELECT
            ROUND(SUM(hits)::DOUBLE / SUM(at_bats)::DOUBLE, 3) as ba
        FROM metrics_player_season_league_offense
        WHERE season = {year} AND at_bats > 0
    """).fetchone()

    lahman_batting = con.execute(f"""
        SELECT
            ROUND(SUM(hits)::DOUBLE / SUM(at_bats)::DOUBLE, 3) as ba
        FROM validation.lahman_batting_season_agg
        WHERE season = {year} AND at_bats > 0
    """).fetchone()

    if lahman_batting[0]:
        diff = (our_batting[0] - lahman_batting[0]) if our_batting[0] and lahman_batting[0] else 0
        match = "✓" if abs(diff) < 0.001 else "✗"
        print(f"  BA - Ours: {our_batting[0]}, Lahman: {lahman_batting[0]}, Diff: {diff:+.4f} {match}")

    # Pitching comparison - use league average from Lahman teams table
    our_pitching = con.execute(f"""
        SELECT
            ROUND(SUM(earned_runs) * 9 / SUM(outs_recorded / 3), 2) as era
        FROM metrics_player_season_league_pitching
        WHERE season = {year} AND outs_recorded > 0
    """).fetchone()

    lahman_pitching = con.execute(f"""
        SELECT ROUND(AVG(era), 2)
        FROM validation.lahman_teams
        WHERE season = {year} AND era IS NOT NULL
    """).fetchone()

    if lahman_pitching and lahman_pitching[0]:
        diff = (our_pitching[0] - lahman_pitching[0]) if our_pitching[0] and lahman_pitching[0] else 0
        match = "✓" if abs(diff) < 0.10 else "✗"
        print(f"  ERA - Ours: {our_pitching[0]}, Lahman: {lahman_pitching[0]}, Diff: {diff:+.2f} {match}")

print("\n" + "="*70)
print("Data Coverage")
print("="*70)
print("  - Lahman Baseball Database: 1871-2025")
print("  - Our Retrosheet event data: 1910-2024")
print("  - Validation can be done for overlapping years: 1910-2024")

print("\n" + "="*70)
print("Done! Validation tables created in 'validation' schema.")
print("="*70)

con.close()
