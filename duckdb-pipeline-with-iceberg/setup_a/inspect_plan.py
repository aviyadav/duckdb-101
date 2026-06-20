import duckdb

con = duckdb.connect("analytics.duckdb")

con.execute("SET memory_limit = '12GB'")
con.execute("SET temp_directory = '/tmp/duckdb_spill'")
con.execute("SET max_temp_directory_size = '50GB'")
con.execute("SET threads = 8")  # or: SET threads = system_threads
con.execute("SET preserve_insertion_order = false")

con.execute("PRAGMA enable_profiling")

plan = con.execute("""
    EXPLAIN ANALYZE
    SELECT category, SUM(amount)
    FROM read_parquet('data/raw/sales.parquet')
    WHERE country = 'US'
    GROUP BY category
""").fetchall()

for row in plan:
    print(row[1])
