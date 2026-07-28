import duckdb
import time
import tracemalloc

def run_duckdb_query_csv():
    query = """
        SELECT
            country,
            SUM(revenue) AS revenue
        FROM read_csv('data/events.csv', header = true)
        WHERE CAST(event_date AS DATE) >= DATE '2025-01-01'
        AND country IN ('US', 'UK', 'DE')
        AND revenue > 0
        GROUP BY country
        ORDER BY revenue DESC;
    """

    # 1. Start the memory and time trackers
    tracemalloc.start()
    start_time = time.perf_counter()

    # 2. Run the query
    result = duckdb.sql(query).df()

    # 3. Stop the trackers and calculate metrics
    end_time = time.perf_counter()

    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # 4. Print results and metrics
    print(" - - Query Result - -")
    print(result)
    print("\n" + "="*30)
    print(" - - Performance Metrics - -")
    print(f"Execution Time: {end_time - start_time:.4f} seconds")
    print(f"Peak Memory Usage: {peak_mem / (1024 * 1024):.2f} MB")
    print("="*30)


def run_duckdb_query_parquet():
    query = """
        SELECT
            country,
            SUM(revenue) AS revenue
        FROM read_parquet('data/events_compressed.parquet')
        WHERE CAST(event_date AS DATE) >= DATE '2025-01-01'
        AND country IN ('US', 'UK', 'DE')
        AND revenue > 0
        GROUP BY country
        ORDER BY revenue DESC;
    """

    # 1. Start the memory and time trackers
    tracemalloc.start()
    start_time = time.perf_counter()

    # 2. Run the query
    result = duckdb.sql(query).df()

    # 3. Stop the trackers and calculate metrics
    end_time = time.perf_counter()

    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # 4. Print results and metrics
    print(" - - Query Result - -")
    print(result)
    print("\n" + "="*30)
    print(" - - Performance Metrics - -")
    print(f"Execution Time: {end_time - start_time:.4f} seconds")
    print(f"Peak Memory Usage: {peak_mem / (1024 * 1024):.2f} MB")
    print("="*30)


def run_duckdb_query_parquet_uncompressed():
    query = """
        SELECT
            country,
            SUM(revenue) AS revenue
        FROM read_parquet('data/events_uncompressed.parquet')
        WHERE CAST(event_date AS DATE) >= DATE '2025-01-01'
        AND country IN ('US', 'UK', 'DE')
        AND revenue > 0
        GROUP BY country
        ORDER BY revenue DESC;
    """

    # 1. Start the memory and time trackers
    tracemalloc.start()
    start_time = time.perf_counter()

    # 2. Run the query
    result = duckdb.sql(query).df()

    # 3. Stop the trackers and calculate metrics
    end_time = time.perf_counter()

    current_mem, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # 4. Print results and metrics
    print(" - - Query Result - -")
    print(result)
    print("\n" + "="*30)
    print(" - - Performance Metrics - -")
    print(f"Execution Time: {end_time - start_time:.4f} seconds")
    print(f"Peak Memory Usage: {peak_mem / (1024 * 1024):.2f} MB")
    print("="*30)


if __name__ == "__main__":
    run_duckdb_query_csv()
    run_duckdb_query_parquet()
    run_duckdb_query_parquet_uncompressed()
