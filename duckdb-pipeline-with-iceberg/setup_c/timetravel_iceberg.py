import duckdb

con = duckdb.connect()

con.execute("INSTALL iceberg; LOAD iceberg")
con.execute("INSTALL httpfs;  LOAD httpfs")

# Configure storage credentials (MinIO for local lab)
con.execute("""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE s3,
        KEY_ID    'admin',
        SECRET    'password',
        ENDPOINT  '127.0.0.1:9000',
        URL_STYLE 'path',
        USE_SSL   false
    )
""")

# Attach the Iceberg REST catalog
con.execute("""
    ATTACH '' AS lakehouse (
        TYPE          iceberg,
        CLIENT_ID     'admin',
        CLIENT_SECRET 'password',
        ENDPOINT      'http://127.0.0.1:8181'
    )
""")

con.execute("SET memory_limit = '12GB'")
con.execute("SET temp_directory = '/tmp/duckdb_spill'")
con.execute("SET max_temp_directory_size = '50GB'")
con.execute("SET threads = 8")  # or: SET threads = system_threads
con.execute("SET preserve_insertion_order = false")


# Inspect the full snapshot history
snapshots = con.execute("""
    SELECT * FROM iceberg_snapshots('lakehouse.analytics.sales_summary')
""").fetchdf()

# sequence_number: version counter (1, 2, 3...)
# snapshot_id:     unique commit identifier used for AT (VERSION => ...)
# timestamp_ms:    wall-clock time of the commit
print(snapshots[["sequence_number", "snapshot_id", "timestamp_ms"]])

# Query the table as it existed before the upsert
# Sort ascending so iloc[0] is the oldest snapshot (initial load), not the newest
first_snapshot = snapshots.sort_values("sequence_number")["snapshot_id"].iloc[0]

historical = con.execute(f"""
    SELECT category, SUM(net_revenue) AS revenue
    FROM lakehouse.analytics.sales_summary
        AT (VERSION => {first_snapshot})
    GROUP BY category
    ORDER BY revenue DESC
""").fetchdf()

print("State before upsert:")
print(historical)

con.close()
