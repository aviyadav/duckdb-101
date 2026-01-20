import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

total_rows = 1_000_000
num_dates = 20
num_tenants = 3
parts_per_partition = 5
countries = ["US", "GB", "DE", "FR", "CA", "AU", "JP", "IN", "BR", "ES"]
tenants = ["acme", "nikon", "byod"]
base_date = datetime(2026, 1, 1)
rows_per_partition = total_rows // (num_dates * num_tenants)
rows_per_part = rows_per_partition // parts_per_partition


def generate_data_chunk(date_obj, tenant, part_num):
    np.random.seed(42 + hash(f"{date_obj}_{tenant}_{part_num}") % 10000)

    user_ids = np.random.randint(1, 10001, size=rows_per_part)
    dates = [date_obj] * rows_per_part
    tenant_col = [tenant] * rows_per_part
    countries_col = np.random.choice(countries, size=rows_per_part)

    df = pd.DataFrame(
        {
            "user_id": user_ids,
            "dt": dates,
            "tenant": tenant_col,
            "country": countries_col,
        }
    )

    df["dt"] = pd.to_datetime(df["dt"])
    return df


def write_parquet_file(args):
    df, file_path = args
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    return f"Created {file_path} with {len(df)} rows"


def generate_partition(args):
    date_obj, tenant = args
    dt_str = date_obj.strftime("%Y-%m-%d")

    partition_path = f"data/events/dt={dt_str}/tenant={tenant}"

    tasks = []
    for part_num in range(1, parts_per_partition + 1):
        file_path = f"{partition_path}/part-{part_num:04d}.parquet"
        df = generate_data_chunk(date_obj, tenant, part_num)
        tasks.append((df, file_path))

    with ThreadPoolExecutor(max_workers=parts_per_partition) as executor:
        results = list(executor.map(write_parquet_file, tasks))

    return results


def process_partition(args):
    return generate_partition(args)


if __name__ == "__main__":
    start_time = time.perf_counter()

    np.random.seed(42)
    random.seed(42)

    tasks = [
        (base_date + timedelta(days=date_idx), tenant)
        for date_idx in range(num_dates)
        for tenant in tenants[:num_tenants]
    ]

    all_results = []

    with ProcessPoolExecutor(max_workers=min(len(tasks), 60)) as executor:
        all_results = list(executor.map(process_partition, tasks))

    for partition_results in all_results:
        for result in partition_results:
            print(result)

    end_time = time.perf_counter()
    total_time = end_time - start_time
    print(f"Data generation complete!")
    print(f"Total time: {total_time:.2f} seconds")
