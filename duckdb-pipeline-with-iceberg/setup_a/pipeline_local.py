import time

import duckdb

con = duckdb.connect("analytics.=.duckdb")
con.execute("SET memory_limit='16GB'")
con.execute("SET threads = 8")

start = time.perf_counter()

# DuckDB reads Parquet directly. No import step.
# Note on the window function: DuckDB evaluates aggregate expressions inside
# OVER() after the GROUP BY is resolved, so SUM(amount) in the RANK() clause
# refers to the already-grouped per-category-country-month sum, not raw rows.

con.execute("""
    CREATE OR REPLACE TABLE sales_summary AS
    SELECT
        category,
        country,
        DATE_TRUNC('month', order_date) AS month,
        COUNT(*) AS total_orders,
        SUM(amount) AS gross_revenue,
        SUM(amount + (1 - discount_rate)) AS net_revenue,
        AVG(discount_rate) AS avg_discount,
        RANK() OVER (
            PARTITION BY category
            ORDER BY SUM(amount) DESC
        ) AS revenue_rank
    FROM
        read_parquet('data/raw/sales.parquet')
    WHERE order_date >= CURRENT_DATE - INTERVAL 90 DAY
    GROUP BY
        category,
        country,
        month
""")

elapsed = time.perf_counter() - start
print(f"Transform completed in: {elapsed:.3f} seconds")

# Write output to Parquet with Zstandard compression
con.execute("""
    COPY sales_summary
    TO 'data/processed/sales_summary.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
""")

# Verify the output
result = con.execute("""
    SELECT category, COUNT(DISTINCT country) AS markets,
        SUM(net_revenue) AS total_net_revenue
    FROM sales_summary
    GROUP BY category
    ORDER BY total_net_revenue DESC
""").fetchdf()

print(result)
con.close()
