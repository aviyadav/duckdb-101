import threading
import time
from pathlib import Path

import psutil
from datafusion import SessionContext

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

row_count = 1_000_000
csv_path = DATA_DIR / "events_df.csv"

# Create a DataFusion execution context
ctx = SessionContext()

# -- BACKGROUND MEMORY MONITOR LOGIC --
peak_memory = 0
monitor_active = True


def monitor_memory():
    """Continuously track process memory to catch the peak spike."""
    global peak_memory
    process = psutil.Process()
    while monitor_active:
        try:
            current_mem = process.memory_info().rss  # RSS = physical RAM used
            if current_mem > peak_memory:
                peak_memory = current_mem
        except Exception:
            break
        time.sleep(0.01)  # Sample every 10 ms


# Start the memory tracking thread
mem_thread = threading.Thread(target=monitor_memory, daemon=True)
mem_thread.start()

# -- START TIMING --
start_time = time.perf_counter()

# DataFusion builds the query lazily and executes it while writing the CSV file.
df = ctx.sql(
    f"""
    SELECT
      CAST(
        DATE '2024-01-01'
          + CAST(
              floor(
                random()
                  * (DATE '2026-01-31' - DATE '2024-01-01' + 1)
              ) AS INTEGER
            )
        AS DATE
      ) AS event_date,
      CASE CAST(floor(random() * 6) AS INTEGER)
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'DE'
        WHEN 3 THEN 'FR'
        WHEN 4 THEN 'IN'
        ELSE 'JP'
      END AS country,
      CASE CAST(floor(random() * 4) AS INTEGER)
        WHEN 0 THEN 'search'
        WHEN 1 THEN 'social'
        WHEN 2 THEN 'email'
        ELSE 'direct'
      END AS channel,
      CAST(floor(random() * 199999) + 1 AS INTEGER) AS user_id,
      CAST(floor(random() * 899999) + 1 AS INTEGER) AS order_id,
      CASE
        WHEN random() < 0.15 THEN 0.0
        ELSE round((-ln(random()) - ln(random())) * 30.0, 2)
      END AS revenue
    FROM generate_series(1, {row_count})
    """
)
df.write_csv(csv_path, with_header=True)

end_time = time.perf_counter()
# -- END TIMING --

# Stop the memory tracking thread safely
monitor_active = False
mem_thread.join()

# -- PRINT RESULTS --
total_duration = end_time - start_time
peak_mb = peak_memory / (1024 * 1024)  # Convert bytes to megabytes

print("=" * 40)
print(f"DataFusion Performance Metrics ({row_count:,} rows):")
print("=" * 40)
print(f"Total Generation + CSV Write Time: {total_duration:.4f} seconds")
print(f"Peak Memory Usage: {peak_mb:.2f} MB")
print("=" * 40)
