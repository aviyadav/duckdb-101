#!/usr/bin/env python3
"""
Generate events parquet file with random data using multiprocessing
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
NUM_EVENTS = 1_000_000
NUM_USERS = 1500
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "events.parquet"

# Data generation parameters
EVENT_TYPES = ["purchase", "session", "activation", "signup", "view", "click", "add_to_cart", "checkout"]
EVENT_TYPE_WEIGHTS = [0.05, 0.50, 0.03, 0.02, 0.25, 0.10, 0.03, 0.02]  # Weights for each event type

# Date range for events
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2025, 2, 1)

# Revenue parameters (only for purchase and checkout events)
REVENUE_MIN = 5.0
REVENUE_MAX = 500.0


def generate_event_batch(batch_info):
    """Generate a batch of events"""
    start_idx, end_idx = batch_info
    events = []
    
    for i in range(start_idx, end_idx):
        # Generate random user_id (some users will have more events than others)
        # Use a power law distribution to simulate realistic user behavior
        user_idx = int(random.paretovariate(1.5)) % NUM_USERS + 1
        user_id = f"u_{user_idx:04d}"
        
        # Generate random event datetime
        time_delta = END_DATE - START_DATE
        random_days = random.randint(0, time_delta.days)
        random_seconds = random.randint(0, 86400)
        event_at = START_DATE + timedelta(days=random_days, seconds=random_seconds)
        
        # Generate event type
        event_type = random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0]
        
        # Generate revenue (only for purchase and checkout events)
        if event_type in ["purchase", "checkout"]:
            # Most purchases are small, but some are large
            if random.random() < 0.8:
                revenue = round(random.uniform(REVENUE_MIN, REVENUE_MAX * 0.3), 2)
            else:
                revenue = round(random.uniform(REVENUE_MAX * 0.3, REVENUE_MAX), 2)
        else:
            revenue = 0.0
        
        events.append({
            "user_id": user_id,
            "event_at": event_at,
            "event_type": event_type,
            "revenue": revenue
        })
    
    return events


def main():
    """Main function to generate events data"""
    print(f"Generating {NUM_EVENTS:,} events using multiprocessing...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Determine number of processes
    num_processes = cpu_count()
    print(f"Using {num_processes} processes")
    
    # Split work into batches
    batch_size = NUM_EVENTS // num_processes
    batches = []
    for i in range(num_processes):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size if i < num_processes - 1 else NUM_EVENTS
        batches.append((start_idx, end_idx))
    
    print(f"Split into {len(batches)} batches")
    
    # Generate data in parallel
    print("Generating events in parallel...")
    with Pool(processes=num_processes) as pool:
        results = pool.map(generate_event_batch, batches)
    
    # Flatten results
    all_events = [event for batch in results for event in batch]
    print(f"Generated {len(all_events):,} events")
    
    # Create Polars DataFrame
    print("Creating Polars DataFrame...")
    df = pl.DataFrame(all_events)
    
    # Sort by event_at for better compression and query performance
    print("Sorting by event_at...")
    df = df.sort("event_at")
    
    # Display sample
    print("\nSample of generated data:")
    print(df.head(10))
    
    print("\nData statistics:")
    print(f"Total events: {len(df):,}")
    print(f"Date range: {df['event_at'].min()} to {df['event_at'].max()}")
    print(f"Unique users: {df['user_id'].n_unique():,}")
    
    print(f"\nEvent type distribution:")
    print(df.group_by("event_type").agg(pl.count()).sort("count", descending=True))
    
    print(f"\nRevenue statistics:")
    revenue_df = df.filter(pl.col("revenue") > 0)
    if len(revenue_df) > 0:
        print(f"Total revenue: ${revenue_df['revenue'].sum():,.2f}")
        print(f"Average revenue per transaction: ${revenue_df['revenue'].mean():.2f}")
        print(f"Min revenue: ${revenue_df['revenue'].min():.2f}")
        print(f"Max revenue: ${revenue_df['revenue'].max():.2f}")
        print(f"Revenue events: {len(revenue_df):,}")
    
    print(f"\nTop 10 users by event count:")
    top_users = df.group_by("user_id").agg(pl.count().alias("event_count")).sort("event_count", descending=True).head(10)
    print(top_users)
    
    # Write to parquet
    print(f"\nWriting to {OUTPUT_FILE}...")
    df.write_parquet(OUTPUT_FILE, compression="snappy")
    
    # Verify file
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"✓ Successfully created {OUTPUT_FILE} ({file_size_mb:.2f} MB)")
    
    # Verify by reading back
    print("Verifying parquet file...")
    df_verify = pl.read_parquet(OUTPUT_FILE)
    print(f"✓ Verified: Read {len(df_verify):,} events from parquet file")


if __name__ == "__main__":
    main()
