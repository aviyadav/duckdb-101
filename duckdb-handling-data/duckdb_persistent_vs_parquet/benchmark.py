import duckdb
import time
import sys

# Use 'resource' on Unix/Linux to measure memory and CPU time
try:
    import resource
except ImportError:
    resource = None


def run_analysis(db_path):
    print(f"Connecting to {db_path}…")
    # 1. Establish connection to the DuckDB file
    # We open in read-only mode to prevent any accidental locks
    conn = duckdb.connect(database=db_path, read_only=True)
    query = """
        SELECT
            year,
            month,
            round(SUM(byt) / 1024.0 / 1024.0 / 1024.0, 2) AS total_gb
        FROM flow_logs
        GROUP BY year, month
        ORDER BY year DESC, month DESC;
    """

    print("Executing query and gathering resource metrics…\n")

    # Record baseline resource usage
    start_wall = time.perf_counter()
    if resource:
        start_usage = resource.getrusage(resource.RUSAGE_SELF)

        # 2. Execute query and fetch results
        # We fetch the results into memory to ensure the query fully executes
        result_relation = conn.sql(query)
        results = result_relation.fetchall()

        # Record ending resource usage
        end_wall = time.perf_counter()

        if resource:
            end_usage = resource.getrusage(resource.RUSAGE_SELF)

            # Print Query Results
            print(" - - QUERY RESULTS - -")
            print(f"{'Year':<6} | {'Month':<6} | {'Total GB':<10}")
            print("-" * 30)

            for row in results:
                print(f"{row[0]:<6} | {row[1]:<6} | {row[2]:<10}")
            print("-" * 30 + "\n")

            # 4. Calculate and Print Metrics
            execution_time_ms = (end_wall - start_wall) * 1000
            print(" - - PERFORMANCE METRICS - -")
            print(f"Total Execution Time (Wall-clock): {execution_time_ms:.2f} ms")

            if resource:
                # User CPU time: Time spent executing your code
                user_cpu = (end_usage.ru_utime - start_usage.ru_utime) * 1000

                # System CPU time: Time spent by the OS kernel on behalf of your process
                sys_cpu = (end_usage.ru_stime - start_usage.ru_stime) * 1000

                # Max RSS (Resident Set Size) represents peak physical memory
                # On Linux, ru_maxrss is in kilobytes. On macOS, it is in bytes.
                if sys.platform == 'darwin':
                    peak_memory_mb = end_usage.ru_maxrss / 1024.0 / 1024.0
                else:
                    peak_memory_mb = end_usage.ru_maxrss / 1024.0
                print(f"User CPU Time: {user_cpu:.2f} ms")
                print(f"System CPU Time: {sys_cpu:.2f} ms")
                print(f"Peak Process Memory (Max RSS): {peak_memory_mb:.2f} MB")
    else:
        print("System resource tracking (resource module) is not supported on this platform.")
    conn.close()

if __name__ == "__main__":
    # db_file = "vpc_analysis.db"  # running with view
    db_file = "analytics.db"   # running with table
    run_analysis(db_file)
