import polars as pl
from faker import Faker
import os, time
import numpy as np

def generate_fast_transactions(n_rows: int = 1_000) -> pl.DataFrame:
    fake = Faker()

    # Categories
    categories = ['Electronics', 'Clothing', 'Groceries', 'Home & Kitchen', 'Beauty & Personal Care', 'Sports & Fitness', 'Books', 'Toys & Games', 'Furniture', 'Other']
    statuses = ['Completed', 'Failed', 'Pending']
    status_probs = [0.8, 0.15, 0.05]
    payment_methods = ['Credit_Card', 'Debit_Card', 'UPI', 'Net_Banking']

    # Build data using Polars native types
    df = pl.DataFrame({
        'transaction_id': [fake.uuid4() for _ in range(n_rows)],
        'timestamp': [fake.date_time_this_year() for _ in range(n_rows)],
        'customer_id': [f"CUST-{i}" for i in np.random.randint(1000, 9999, size=n_rows)],
        'merchant': [fake.company() for _ in range(n_rows)],
        'category': np.random.choice(categories, size=n_rows).tolist(),
        'amount': np.round(np.random.uniform(250.0, 15000.0, size=n_rows), 2).tolist(),
        'status': np.random.choice(statuses, size=n_rows, p=status_probs).tolist(),
        'payment_method': np.random.choice(payment_methods, size=n_rows).tolist(),
    })

    # Cast categorical columns to Enum for memory efficiency
    df = df.with_columns([
        pl.col('category').cast(pl.Categorical),
        pl.col('status').cast(pl.Categorical),
        pl.col('payment_method').cast(pl.Categorical),
    ])

    return df.sort('timestamp')

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

start = time.time()
for i in range(50):
    df = generate_fast_transactions(1000)
    df.write_parquet(f"{data_dir}/{i:02d}.parquet", compression="zstd")
end = time.time()

print(f"Time taken: {end - start:.4f}s")
print(f"\nSample Schema:\n{df.schema}")