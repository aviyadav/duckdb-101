import duckdb
import polars as pl
import time
import os

def run_etl():
    data_dir = "data"
    if not os.path.exists(data_dir):
        print("Data directory not found. Please run create-fake-data.py first.")
        return

    print("--- Starting ETL Process ---")

    # 1. Use DuckDB to efficiently read all Parquet files
    start_duck = time.time()
    con = duckdb.connect()

    query = f"SELECT * FROM read_parquet('{data_dir}/*.parquet')"

    # 2. Convert DuckDB result directly to a Polars DataFrame
    # This is zero-copy via the Arrow IPC protocol
    print("Loading data into Polars via DuckDB...")
    df = con.execute(query).pl()
    end_duck = time.time()

    print(f"Loaded {len(df)} rows in {end_duck - start_duck:.4f} seconds.")
    print(f"Memory Usage: {df.estimated_size('mb'):.2f} MB")
    print(f"Schema:\n{df.schema}\n")

    # 3. Fast Data Processing with Polars lazy API
    print("Running fast aggregation...")
    start_proc = time.time()
    agg = (
        df.lazy()
        .group_by('category')
        .agg([
            pl.col('amount').mean().alias('mean'),
            pl.col('amount').sum().alias('sum'),
            pl.col('amount').count().alias('count'),
        ])
        .sort('category')
        .collect()
    )
    end_proc = time.time()

    print(f"Aggregation took {end_proc - start_proc:.4f} seconds.")
    print(agg)

    # 4. (Conceptual) Load to PostgreSQL
    # Use DuckDB's postgres extension for direct transfer, e.g.:
    # con.execute("ATTACH 'dbname=... user=...' AS pg (TYPE postgres);")
    # con.execute("INSERT INTO pg.public.transactions SELECT * FROM df")
    print("\nETL COMPLETE: Data is ready for downstream use.")

if __name__ == "__main__":
    run_etl()
