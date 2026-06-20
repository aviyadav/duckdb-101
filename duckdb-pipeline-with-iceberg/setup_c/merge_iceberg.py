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

print("Upsert to RustFS complete")
