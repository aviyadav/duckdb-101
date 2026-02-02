#!/usr/bin/env python3
"""
Generate users parquet file with random data using multiprocessing
"""
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import random
from multiprocessing import Pool, cpu_count
import os
from pathlib import Path

# Configuration
NUM_USERS = 1500
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "users.parquet"

# Data generation parameters
ACQUISITION_CHANNELS = ["organic", "paid_search", "social", "referral", "email", "direct"]
COUNTRIES = ["US", "GB", "CA", "AU", "DE", "FR", "IN", "BR", "JP", "SG", "NL", "ES", "IT", "MX", "AR"]
USER_STATUSES = ["active", "inactive", "suspended", "deactivated"]

# Date range for signup
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2025, 1, 31)


def generate_user_batch(batch_info):
    """Generate a batch of users"""
    start_idx, end_idx = batch_info
    users = []
    
    for i in range(start_idx, end_idx):
        user_id = f"u_{i + 1:04d}"
        
        # Generate random signup datetime
        time_delta = END_DATE - START_DATE
        random_days = random.randint(0, time_delta.days)
        random_seconds = random.randint(0, 86400)
        signup_at = START_DATE + timedelta(days=random_days, seconds=random_seconds)
        signup_date = signup_at.date()
        
        # Generate other fields
        acquisition_channel = random.choice(ACQUISITION_CHANNELS)
        country = random.choice(COUNTRIES)
        
        # Weight user statuses (more active users)
        status_weights = [0.70, 0.15, 0.05, 0.10]  # active, inactive, suspended, deactivated
        user_status = random.choices(USER_STATUSES, weights=status_weights, k=1)[0]
        
        users.append({
            "user_id": user_id,
            "signup_at": signup_at,
            "signup_date": signup_date,
            "acquisition_channel": acquisition_channel,
            "country": country,
            "user_status": user_status
        })
    
    return users


def main():
    """Main function to generate users data"""
    print(f"Generating {NUM_USERS} users using multiprocessing...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Determine number of processes
    num_processes = cpu_count()
    print(f"Using {num_processes} processes")
    
    # Split work into batches
    batch_size = NUM_USERS // num_processes
    batches = []
    for i in range(num_processes):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size if i < num_processes - 1 else NUM_USERS
        batches.append((start_idx, end_idx))
    
    # Generate data in parallel
    with Pool(processes=num_processes) as pool:
        results = pool.map(generate_user_batch, batches)
    
    # Flatten results
    all_users = [user for batch in results for user in batch]
    print(f"Generated {len(all_users)} users")
    
    # Create Polars DataFrame
    df = pl.DataFrame(all_users)
    
    # Display sample
    print("\nSample of generated data:")
    print(df.head(10))
    
    print("\nData statistics:")
    print(f"Total users: {len(df)}")
    print(f"\nUser status distribution:")
    print(df.group_by("user_status").agg(pl.count()).sort("user_status"))
    print(f"\nAcquisition channel distribution:")
    print(df.group_by("acquisition_channel").agg(pl.count()).sort("acquisition_channel"))
    print(f"\nCountry distribution:")
    print(df.group_by("country").agg(pl.count()).sort("count", descending=True))
    
    # Write to parquet
    print(f"\nWriting to {OUTPUT_FILE}...")
    df.write_parquet(OUTPUT_FILE, compression="snappy")
    
    # Verify file
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"✓ Successfully created {OUTPUT_FILE} ({file_size_mb:.2f} MB)")
    
    # Verify by reading back
    df_verify = pl.read_parquet(OUTPUT_FILE)
    print(f"✓ Verified: Read {len(df_verify)} users from parquet file")


if __name__ == "__main__":
    main()
