import os
import random
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
import polars as pl
import numpy as np

base_dir = "data_pl/events"
tenants = ["acme", "globex", "initech", "umbrella"]
country_codes = ["US", "CA", "GB", "DE", "FR", "AU", "JP", "IN", "BR", "ZA"]
total_rows = 1_000_000
rows_per_file = 50000
num_files = total_rows // rows_per_file
dates = [datetime(2026, 1, 1) + timedelta(days=i) for i in range(18)]


def generate_data_chunk(rows_per_chunk, date_obj, tenant):
    rng = np.random.default_rng()
    user_ids = [f"user_{rng.integers(1, 10000):08d}" for _ in range(rows_per_chunk)]
    countries = rng.choice(country_codes, size=rows_per_chunk).tolist()

    return pl.DataFrame(
        {
            "user_id": user_ids,
            "dt": [date_obj.date()] * rows_per_chunk,
            "tenant": [tenant] * rows_per_chunk,
            "country": countries,
        }
    )


def write_parquet_file(df, file_path):
    df.write_parquet(file_path)


def generate_tenant_data(args):
    date_obj, tenant = args
    dt_str = date_obj.strftime("%Y-%m-%d")
    dt_dir = os.path.join(base_dir, f"dt={dt_str}")
    tenant_dir = os.path.join(dt_dir, f"tenant={tenant}")
    os.makedirs(tenant_dir, exist_ok=True)

    df = generate_data_chunk(rows_per_file, date_obj, tenant)

    file_paths = []
    for i in range(num_files):
        file_path = os.path.join(tenant_dir, f"part-{i + 1:04d}.parquet")
        file_paths.append((df, file_path))

    with ThreadPoolExecutor(max_workers=num_files) as executor:
        executor.map(write_parquet_file_from_args, file_paths)

    return f"Generated data for {dt_dir}/{tenant}"


def write_parquet_file_from_args(args):
    df, file_path = args
    df.write_parquet(file_path)


if __name__ == "__main__":
    start_time = time.perf_counter()

    os.makedirs(base_dir, exist_ok=True)

    tasks = [(date, tenant) for date in dates for tenant in tenants]

    with ProcessPoolExecutor(max_workers=min(len(tasks), 60)) as executor:
        results = list(executor.map(generate_tenant_data, tasks))

    for result in results:
        print(result)

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"Total time: {total_time:.2f} seconds")
