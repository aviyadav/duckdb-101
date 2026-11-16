import time
import duckdb

DB_PATH = "analytics.db"

def run_v2_benchmark():
    print(f"================ [DuckDB 2.x on v68 Format] ================")
    print(f"Target file: {DB_PATH}")
    print(f"DuckDB Version: {duckdb.__version__}")

    # 1. Cold Startup / Metadata Load Time
    print("-> Initializing connection and loading metadata...")
    start = time.perf_counter()
    con = duckdb.connect(DB_PATH, read_only=True)
    startup_time = time.perf_counter() - start
    print(f"   Cold Connection / Metadata Load: {startup_time:.4f} seconds")

    # Extract Catalog Info
    try:
        version_info = con.sql("PRAGMA database_list;").fetchall()
        print(f"   Connected Database Catalog: {version_info}")
    except Exception:
        print("   Storage Format Version: [v68 backward compatibility mode]")

    # 2. Schema Inspection
    print("-> Inspecting database schema (SHOW TABLES)...")
    start = time.perf_counter()
    tables = con.sql("SHOW TABLES;").fetchall()
    schema_time = time.perf_counter() - start
    print(f"   SHOW TABLES ({len(tables)} tables found): {schema_time:.4f} seconds")

    # 3. Heavy Analytical Scan / Full Row Count
    table_name = "flow_logs"
    print(f"-> Executing full table scan / row count on '{table_name}'...")
    start = time.perf_counter()
    # Fixed for DuckDB 2.0 relation API (.fetchall()[0][0] or con.execute())
    row_count = con.sql(f"SELECT count(*) FROM {table_name};").fetchall()[0][0]
    scan_time = time.perf_counter() - start
    print(f"   Full Table Scan ({row_count:,} rows): {scan_time:.4f} seconds")

    # 4. Monthly Aggregation Query
    print(f"-> Executing monthly GB aggregation query on '{table_name}'...")
    start = time.perf_counter()
    query_df = con.sql(f"""
        SELECT
            year,
            month,
            round(SUM(byt) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb
        FROM {table_name}
        GROUP BY year, month
        ORDER BY year DESC, month DESC;
    """).pl()
    agg_time = time.perf_counter() - start
    print(f"   Monthly Aggregation Query: {agg_time:.4f} seconds ({len(query_df)} rows returned)\n")

    print("--- Query Results Preview ---")
    print(query_df)
    print("-----------------------------\n")

    con.close()
    print("[+] DuckDB 2.x benchmark complete.")

if __name__ == "__main__":
    run_v2_benchmark()
