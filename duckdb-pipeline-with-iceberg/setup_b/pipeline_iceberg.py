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

# Attach the Iceberg REST catalog.
# The empty string '' is the warehouse name for this local catalog.
# For named catalogs or cloud providers, replace '' with the warehouse identifier.
# Verified against DuckDB v1.5.3 Iceberg extension docs (May 2026).
con.execute("""
    ATTACH '' AS lakehouse (
        TYPE          iceberg,
        CLIENT_ID     'admin',
        CLIENT_SECRET 'password',
        ENDPOINT      'http://127.0.0.1:8181'
    )
""")

con.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.analytics")

con.execute("""
    CREATE TABLE IF NOT EXISTS lakehouse.analytics.sales_summary (
        category     VARCHAR,
        country      VARCHAR,
        month        TIMESTAMP,
        total_orders BIGINT,
        net_revenue  DECIMAL(18,2)
    )
""")

# Initial load from local Parquet
con.execute("""
    INSERT INTO lakehouse.analytics.sales_summary
    SELECT
        category,
        country,
        DATE_TRUNC('month', order_date)        AS month,
        COUNT(*)                                AS total_orders,
        SUM(amount * (1 - discount_rate))       AS net_revenue
    FROM read_parquet('data/raw/sales.parquet')
    GROUP BY 1, 2, 3
""")

print("Initial load complete")
