import duckdb

# Incremental batch sizes.
# RECENT_ROWS : orders within the last 7 days.
#               Their month already exists in the base data, so merge_iceberg.py
#               will hit the WHEN MATCHED → UPDATE path.
# FUTURE_ROWS : orders dated 32-62 days from now.
#               Those months are not in the base data yet, so the merge will
#               hit the WHEN NOT MATCHED → INSERT path.
RECENT_ROWS = 90_000
FUTURE_ROWS = 10_000

con = duckdb.connect()

con.execute(f"""
    COPY (
        -- Recent orders: overlap with existing monthly aggregates (MERGE UPDATE)
        SELECT
            (random() * 1000000)::INTEGER          AS order_id,
            ['electronics', 'clothing', 'food', 'books'][floor(random() * 4 + 1)::INTEGER] AS category,
            ['US', 'DE', 'FR', 'BR', 'JP'][floor(random() * 5 + 1)::INTEGER]               AS country,
            (random() * 500 + 5)::DECIMAL(10, 2)   AS amount,
            (random() * 0.3)::DECIMAL(5, 4)         AS discount_rate,
            NOW() - INTERVAL (random() * 7) DAY     AS order_date
        FROM range({RECENT_ROWS})

        UNION ALL

        -- Future orders: fall in months absent from the base data (MERGE INSERT)
        SELECT
            (random() * 1000000)::INTEGER          AS order_id,
            ['electronics', 'clothing', 'food', 'books'][floor(random() * 4 + 1)::INTEGER] AS category,
            ['US', 'DE', 'FR', 'BR', 'JP'][floor(random() * 5 + 1)::INTEGER]               AS country,
            (random() * 500 + 5)::DECIMAL(10, 2)   AS amount,
            (random() * 0.3)::DECIMAL(5, 4)         AS discount_rate,
            NOW() + INTERVAL (32 + random() * 30) DAY AS order_date
        FROM range({FUTURE_ROWS})
    )
    TO 'data/raw/sales_incremental.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD)
""")

summary = con.execute("""
    SELECT
        COUNT(*)             AS total_rows,
        MIN(order_date)::DATE AS earliest_date,
        MAX(order_date)::DATE AS latest_date,
        COUNT(DISTINCT category) AS categories,
        COUNT(DISTINCT country)  AS countries
    FROM read_parquet('data/raw/sales_incremental.parquet')
""").fetchone()

print(f"Generated {summary[0]:,} incremental rows → data/raw/sales_incremental.parquet")
print(f"Date range : {summary[1]}  →  {summary[2]}")
print(f"Categories : {summary[3]}  |  Countries : {summary[4]}")
print(
    f"  {RECENT_ROWS:,} recent rows  (last 7 days)    → MERGE will UPDATE existing months"
)
print(f"  {FUTURE_ROWS:,} future rows  (32-62 days out) → MERGE will INSERT new months")

con.close()
