"""Example script using persistent scoped secrets from Scoped-Persistent-Secrets.docx."""

import duckdb


def query_scoped_secrets() -> None:
    # Connect to DuckDB (automatically loads persistent secrets from ~/.duckdb/stored_secrets/)
    con = duckdb.connect()

    # Query directly across both buckets—DuckDB matches the S3 paths to your saved scopes
    query = """
        SELECT 
            a.user_id,
            a.user_name,
            l.event_type,
            l.event_time
        FROM read_parquet('s3://company-analytics-bucket/users.parquet') AS a
        JOIN read_parquet('s3://company-logs-bucket/2026/events.parquet') AS l
          ON a.user_id = l.user_id
        LIMIT 10;
    """
    df = con.execute(query).df()
    print(df)


if __name__ == "__main__":
    query_scoped_secrets()
