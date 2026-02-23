import os
import time

import numpy as np

# import pandas as pd
import polars as pl
from faker import Faker


def generate_fast_transactions(n_rows=100000):
    fake = Faker()

    # 1. Pre-generate static/categorical data using NumPy (Extremely Fast)
    categories = ["Electronics", "Groceries", "Dining", "Travel", "Entertainment"]
    statuses = ["Completed", "Pending", "Failed"]

    data = {
        "transaction_id": [fake.uuid4() for _ in range(n_rows)],
        "timestamp": [fake.date_time_this_year() for _ in range(n_rows)],
        "customer_id": [
            f"CUST-{i}" for i in np.random.randint(1000, 9999, size=n_rows)
        ],
        "merchant": [fake.company() for _ in range(n_rows)],
        "category": np.random.choice(categories, size=n_rows),
        "amount": np.round(np.random.uniform(5.0, 1500.0, size=n_rows), 2),
        "status": np.random.choice(statuses, size=n_rows, p=[0.85, 0.10, 0.05]),
    }

    # df = pd.DataFrame(data)
    # return df.sort_values("timestamp").reset_index(drop=True)

    df = pl.DataFrame(data)
    return df.sort("timestamp")


os.makedirs("data", exist_ok=True)
start = time.time()
for i in range(250):
    # generate_fast_transactions(1000).to_parquet(f"data/txn-pl-{i:03d}.parquet", engine="pyarrow", compression="zstd")
    generate_fast_transactions(1000).write_parquet(f"data/txn-pl-{i:03d}.parquet", compression="zstd")
end = time.time()
print(f"Time taken: {end - start} seconds")
