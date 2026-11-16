import os
import time
import duckdb

OLD_DB_PATH = "analytics.db"
NEW_DB_PATH = "analytics_v69.db"
TEMP_DIR = "data/duckdb_tmp"
TARGET_DIR = "my_benchmark_v2"

def convert_database():
    print(f"================ [DuckDB Storage Version 69 Migration] ================")
    print(f"Source (v68): {OLD_DB_PATH}")
    print(f"Target (v69): {NEW_DB_PATH}")
    print(f"DuckDB Version: {duckdb.__version__}\n")

    if not os.path.exists(OLD_DB_PATH):
        raise FileNotFoundError(f"Source database not found at {OLD_DB_PATH}")

    # Ensure target and temporary directories exist
    os.makedirs(TARGET_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Remove existing target file if a previous migration run left it behind
    if os.path.exists(NEW_DB_PATH):
        print(f"-> Removing existing target file at {NEW_DB_PATH}...")
        os.remove(NEW_DB_PATH)

    print("-> Initializing migration connection...")
    con = duckdb.connect()

    # Enforce resource limits and temporary directory configuration
    print(f"-> Applying resource limits (Memory: 8GB, Threads: 4, Temp Dir: {TEMP_DIR})...")
    con.execute("SET memory_limit = '8GB';")
    con.execute("SET threads = 4;")
    con.execute(f"SET temp_directory = '{TEMP_DIR}';")

    # Attach old database in read-only mode for safety
    print("-> Attaching source v68 database...")
    con.execute(f"ATTACH '{OLD_DB_PATH}' AS old_db (READ_ONLY);")

    # Attach new database explicitly requesting the latest storage format version (format 69)
    print("-> Creating target database with STORAGE_VERSION 'latest' (Format 69)...")
    con.execute(f"ATTACH '{NEW_DB_PATH}' AS new_db (STORAGE_VERSION 'latest');")

    print("-> Copying all tables, schema, and 1.44B rows over (this may take a minute)...")
    start = time.perf_counter()

    # Copy entire database contents from old to new
    con.execute("COPY FROM DATABASE old_db TO new_db;")

    migration_time = time.perf_counter() - start
    print(f"-> Migration completed successfully in {migration_time:.4f} seconds!")

    # Verify the table row count on the new v69 database
    print("-> Verifying row count on converted v69 file...")
    row_count = con.sql("SELECT count(*) FROM new_db.flow_logs;").fetchall()[0][0]
    print(f"   Verified Rows in v69 Database: {row_count:,}")

    con.close()

    # Report file sizes
    old_size_gb = os.path.getsize(OLD_DB_PATH) / (1024**3)
    new_size_gb = os.path.getsize(NEW_DB_PATH) / (1024**3)
    print(f"\n[+] Storage Footprint Comparison:")
    print(f"    - Original v68 size: {old_size_gb:.2f} GB")
    print(f"    - Converted v69 size: {new_size_gb:.2f} GB")
    print(f"[+] Migration complete! Ready for format 69 benchmarks in {TARGET_DIR}.")

if __name__ == "__main__":
    convert_database()
