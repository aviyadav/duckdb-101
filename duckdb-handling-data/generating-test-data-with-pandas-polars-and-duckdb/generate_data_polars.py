from pathlib import Path
import threading  # Added for background memory tracking
import time
import numpy as np
import polars as pl
import psutil  # Make sure to run: uv pip install psutil

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

row_count = 1_000_000
rng = np.random.default_rng(42)

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

# -- START GENERATION TIMING --
start_gen = time.perf_counter()
event_dates = rng.choice(
    pl.date_range(
        start=pl.datetime(2024, 1, 1),
        end=pl.datetime(2026, 1, 31),
        interval="1d",
        eager=True,
    ),
    size=row_count,
)
countries = rng.choice(["US", "UK", "DE", "FR", "IN", "JP"], size=row_count)
channels = rng.choice(["search", "social", "email", "direct"], size=row_count)
user_ids = rng.integers(1, 200_000, size=row_count)
order_ids = rng.integers(1, 900_000, size=row_count)
revenue = rng.gamma(shape=2.0, scale=30.0, size=row_count).round(2)
revenue[rng.random(row_count) < 0.15] = 0.0
df = pl.DataFrame(
    {
        "event_date": event_dates,
        "country": countries,
        "channel": channels,
        "user_id": user_ids,
        "order_id": order_ids,
        "revenue": revenue,
    }
)
end_gen = time.perf_counter()
# -- END GENERATION TIMING --

# -- START WRITE TIMING --
start_write = time.perf_counter()
df.write_csv(DATA_DIR / "events_pl.csv")
end_write = time.perf_counter()
# -- END WRITE TIMING --

# Stop the memory tracking thread safely
monitor_active = False
mem_thread.join()

# -- PRINT RESULTS --
gen_duration = end_gen - start_gen
write_duration = end_write - start_write
total_duration = gen_duration + write_duration
peak_mb = peak_memory / (1024 * 1024)  # Convert bytes to Megabytes

print("=" * 40)
print(f"Polars Performance Metrics ({row_count:,} rows):")
print("=" * 40)
print(f"Data Generation Time: {gen_duration:.4f} seconds")
print(f"CSV Writing Time: {write_duration:.4f} seconds")
print(f"Total Elapsed Time: {total_duration:.4f} seconds")
print(f"Peak Memory Usage: {peak_mb:.2f} MB")
print("=" * 40)
