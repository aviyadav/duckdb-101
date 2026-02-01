from datetime import datetime
from pathlib import Path

import duckdb


def main():
    DB = "micro_warehouse.duckdb"

    con = duckdb.connect(DB)
    # One-time installs (safe to run repeatedly in most setups)
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Example: read partitioned parquet as a logical table
    con.execute("""
        CREATE OR REPLACE VIEW bronze_events AS
        SELECT *
        FROM read_parquet('data/events/date=*/events-*.parquet');
    """)

    # Silver layer: typed + cleaned
    con.execute("""
        CREATE OR REPLACE VIEW silver_events AS
        SELECT
            CAST(event_time AS TIMESTAMP) AS event_time,
            user_id,
            team_id,
            event_name,
            NULLIF(properties, '') AS properties
        FROM bronze_events
        WHERE user_id IS NOT NULL;
    """)

    # Gold layer: metrics (stable interface)
    con.execute("""
        CREATE OR REPLACE VIEW gold_weekly_active_teams AS
        SELECT
            date_trunc('week', event_time) AS week,
            COUNT(DISTINCT team_id) AS weekly_active_teams
        FROM silver_events
        GROUP BY 1
        ORDER BY 1;
    """)

    # Create exports directory if it doesn't exist
    Path("exports").mkdir(parents=True, exist_ok=True)

    # Export a curated table for
    con.execute("""
        COPY (SELECT *  FROM gold_weekly_active_teams)
        TO 'exports/gold_weekly_active_teams.parquet' (FORMAT PARQUET);
    """)

    con.execute("CREATE OR REPLACE TABLE ops_refresh_log AS SELECT 1 AS dummy;")
    con.execute("""
        INSERT INTO ops_refresh_log
        SELECT 1;
    """)

    print(
        f"Refresh job completed successfully. {datetime.utcnow().isoformat()}, DB = {DB}"
    )


if __name__ == "__main__":
    main()
