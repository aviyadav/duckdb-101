import duckdb

con = duckdb.connect()

con.execute("INSTALL iceberg; LOAD iceberg")
con.execute("INSTALL httpfs;  LOAD httpfs")

# Configure storage credentials for RustFS
con.execute("""
    CREATE OR REPLACE SECRET rustfs_secret (
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

print("Initial load to RustFS-backed Iceberg complete")
