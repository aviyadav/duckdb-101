from pathlib import Path
import threading  # Added for background memory tracking
import time
import duckdb
import psutil  # Make sure to run: uv pip install psutil

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
row_count = 100_000_000
csv_path = DATA_DIR / "events.csv"

# Connect to an in-memory DuckDB database
con = duckdb.connect()

# -- BACKGROUND MEMORY MONITOR LOGIC --
peak_memory = 0
monitor_active = True


def monitor_memory():
    """Continuously tracks the process memory to catch the peak spike."""
    global peak_memory
    process = psutil.Process()
    while monitor_active:
        try:
            current_mem = process.memory_info().rss  # RSS = Physical RAM used
            if current_mem > peak_memory:
                peak_memory = current_mem
        except Exception:
            break
        time.sleep(0.01)  # Sample every 10ms


# Start the memory tracking thread
mem_thread = threading.Thread(target=monitor_memory, daemon=True)
mem_thread.start()

# -- START TIMING --
start_time = time.perf_counter()

# We use a single SQL query to generate, transform, and write the data
con.execute(
    f"""
    COPY (
    SELECT
      -- 1. Generate random dates between 2024-01-01 and 2026-01-31
      '2024-01-01'::DATE + CAST(floor(random() * (DATE '2026-01-31' - DATE '2024-01-01' + 1)) AS INTEGER) AS event_date,
      -- 2. Randomly pick a country from the list
      (['US', 'UK', 'DE', 'FR', 'IN', 'JP'])[CAST(floor(random() * 6) + 1 AS INTEGER)] AS country,
      -- 3. Randomly pick a marketing channel
      (['search', 'social', 'email', 'direct'])[CAST(floor(random() * 4) + 1 AS INTEGER)] AS channel,
      -- 4. Generate random user_id between 1 and 200,000
      CAST(floor(random() * 200000) + 1 AS INTEGER) AS user_id,
      -- 5. Generate random order_id between 1 and 900,000
      CAST(floor(random() * 900000) + 1 AS INTEGER) AS order_id,
      -- 6. Generate Gamma-like skewed revenue, with a 15% chance of being 0
      CASE
        WHEN random() < 0.15 THEN 0.0
        ELSE round(
          -- Simple mathematical trick to mimic a Gamma shape=2 distribution using uniform randoms
          (-log(random()) - log(random())) * 30.0, 2
        )
      END AS revenue
    FROM generate_series(1, {row_count})
    ) TO '{csv_path}' (HEADER, DELIMITER ',');
    """
)

end_time = time.perf_counter()
# -- END TIMING --

# Stop the memory tracking thread safely
monitor_active = False
mem_thread.join()

# -- PRINT RESULTS --
total_duration = end_time - start_time
peak_mb = peak_memory / (1024 * 1024)  # Convert bytes to Megabytes

print("=" * 40)
print(f"DuckDB Performance Metrics ({row_count:,} rows):")
print("=" * 40)
print(f"Total Generation + CSV Write Time: {total_duration:.4f} seconds")
print(f"Peak Memory Usage: {peak_mb:.2f} MB")
print("=" * 40)
