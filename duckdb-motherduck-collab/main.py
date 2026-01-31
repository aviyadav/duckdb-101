import os

import duckdb

os.environ["MOTHERDUCK_TOKEN"] = "your-motherduck-api-key"


def main():
    con = duckdb.connect("md:team_analytics")

    # Create a shared table
    con.sql("""
        CREATE TABLE IF NOT EXISTS events (
            user_id VARCHAR,
            event_name VARCHAR,
            ts TIMESTAMP
        )
    """)

    # Create or replace a shared table
    con.sql("""
        CREATE OR REPLACE TABLE events AS
        SELECT * FROM read_parquet('data/events.parquet')
    """)

    summary = con.sql("""
        SELECT event_name, COUNT(*) AS events
        FROM events
        GROUP BY event_name
        ORDER BY events DESC
    """).df()

    print(summary.head())


if __name__ == "__main__":
    main()
