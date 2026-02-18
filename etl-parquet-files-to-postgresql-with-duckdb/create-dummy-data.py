import pandas as pd
import numpy as np
from faker import Faker
import os, time

# Enable PyArrow-backed strings in Pandas 3.0+
pd.options.future.infer_string = True

def generate_fast_transactions(n_rows: int = 1_000) -> pd.DataFrame:
    fake = Faker()
    
    # Categories
    categories = ['Electronics', 'Clothing', 'Groceries', 'Home & Kitchen', 'Beauty & Personal Care', 'Sports & Fitness', 'Books', 'Toys & Games', 'Furniture', 'Other']
    statuses = ['Completed', 'Failed', 'Pending']
    payment_methods = ['Credit_Card', 'Debit_Card', 'UPI', 'Net_Banking']

    # Data - Basic lists for Faker objects, NumPy for native types
    data = {
        'transaction_id': [fake.uuid4() for _ in range(n_rows)],
        'timestamp': [fake.date_time_this_year() for _ in range(n_rows)],
        'customer_id': [f"CUST-{i}" for i in np.random.randint(1000, 9999, size=n_rows)],
        'merchant': [fake.company() for _ in range(n_rows)],
        'category': np.random.choice(categories, size=n_rows),
        'amount': np.round(np.random.uniform(250.0, 15000.0, size=n_rows), 2),
        'status': np.random.choice(statuses, size=n_rows, p=[0.8, 0.15, 0.05]),
        'payment_method': np.random.choice(payment_methods, size=n_rows)
    }

    # Use PyArrow dtypes backend
    df = pd.DataFrame(data).convert_dtypes(dtype_backend="pyarrow")
    
    # Cast categories to categorical for even better compression/speed
    categorical_cols = ['category', 'status', 'payment_method']
    for col in categorical_cols:
        df[col] = df[col].astype('category')
        
    return df.sort_values('timestamp').reset_index(drop=True)

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

start = time.time()
for i in range(50):
    df = generate_fast_transactions(1000)
    df.to_parquet(f"{data_dir}/{i:02d}.parquet", engine="pyarrow", compression="zstd")
end = time.time()
print(f"Time taken: {end - start}")
print(f"Sample Dtypes:\n{df.dtypes}")
    