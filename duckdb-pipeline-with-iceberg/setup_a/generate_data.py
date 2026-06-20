import duckdb

con = duckdb.connect()

con.execute("""
    COPY (
        SELECT
            (random() * 1000000)::INTEGER AS order_id,
            ['electronics', 'clothing', 'food', 'books'][floor(random() * 4 + 1)::INTEGER] AS category,
            ['US', 'DE', 'FR', 'BR', 'JP'][floor(random() * 5 + 1)::INTEGER] AS country,
            (random() * 500 + 5)::DECIMAL(10, 2) AS amount,
            (random() * 0.3)::DECIMAL(5, 4) AS discount_rate,
            NOW() - INTERVAL (random() * 365) DAY AS order_date
        FROM range(2000000)
    )
    TO 'data/raw/sales.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD)
""")

con.close()
