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


# Stage incremental data from a new Parquet file
con.execute("""
    CREATE OR REPLACE TEMP TABLE incremental AS
    SELECT
        category,
        country,
        DATE_TRUNC('month', order_date)        AS month,
        COUNT(*)                                AS total_orders,
        SUM(amount * (1 - discount_rate))       AS net_revenue
    FROM read_parquet('data/raw/sales_incremental.parquet')
    GROUP BY 1, 2, 3
""")

# MERGE INTO: update existing rows, insert new ones.
# Verified: MERGE INTO against Iceberg REST catalog tables was announced
# in the official DuckDB v1.5.3 release post (duckdb.org, May 29 2026).
con.execute("""
    MERGE INTO lakehouse.analytics.sales_summary AS target
    USING incremental AS source
        ON  target.category = source.category
        AND target.country  = source.country
        AND target.month    = source.month
    WHEN MATCHED THEN
        UPDATE SET
            total_orders = target.total_orders + source.total_orders,
            net_revenue  = target.net_revenue  + source.net_revenue
    WHEN NOT MATCHED THEN
        INSERT (category, country, month, total_orders, net_revenue)
        VALUES (source.category, source.country, source.month,
                source.total_orders, source.net_revenue)
""")

print("Upsert complete")
