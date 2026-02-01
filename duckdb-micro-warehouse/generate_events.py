import json
import random
import string
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List, Tuple

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
TOTAL_ROWS = 1_000_000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)
OUTPUT_DIR = Path("data/events")

# Event names pool
EVENT_NAMES = [
    "page_view",
    "button_click",
    "form_submit",
    "video_play",
    "video_pause",
    "add_to_cart",
    "remove_from_cart",
    "checkout",
    "purchase",
    "sign_up",
    "sign_in",
    "sign_out",
    "search",
    "filter_apply",
    "share",
]

# Property keys pool
PROPERTY_KEYS = [
    "page_url",
    "button_id",
    "form_id",
    "video_id",
    "product_id",
    "category",
    "price",
    "quantity",
    "search_term",
    "filter_type",
    "platform",
    "browser",
    "device_type",
    "session_id",
]


def generate_random_properties() -> str:
    """Generate random event properties as JSON string."""
    num_props = random.randint(2, 5)
    props = {}

    for _ in range(num_props):
        key = random.choice(PROPERTY_KEYS)
        # Generate different types of values
        value_type = random.choice(["string", "number", "boolean"])

        if value_type == "string":
            props[key] = "".join(
                random.choices(
                    string.ascii_letters + string.digits, k=random.randint(5, 15)
                )
            )
        elif value_type == "number":
            props[key] = round(random.uniform(1, 1000), 2)
        else:
            props[key] = random.choice([True, False])

    return json.dumps(props)


def generate_batch_data(args: Tuple[int, int, datetime, datetime]) -> pl.DataFrame:
    """
    Generate a batch of event data.

    Args:
        args: Tuple of (batch_id, batch_size, start_date, end_date)

    Returns:
        Polars DataFrame with generated events
    """
    batch_id, batch_size, start_date, end_date = args

    # Set random seed for reproducibility with variation per batch
    random.seed(batch_id)
    np.random.seed(batch_id)

    # Calculate date range in days
    date_range_days = (end_date - start_date).days

    # Generate timestamps
    random_days = np.random.randint(0, date_range_days, batch_size)
    random_seconds = np.random.randint(0, 86400, batch_size)  # seconds in a day

    event_times = [
        start_date + timedelta(days=int(days), seconds=int(secs))
        for days, secs in zip(random_days, random_seconds)
    ]

    # Generate other fields
    user_ids = np.random.randint(1, 100000, batch_size).tolist()
    team_ids = np.random.randint(1, 1000, batch_size).tolist()
    event_names = [random.choice(EVENT_NAMES) for _ in range(batch_size)]
    properties = [generate_random_properties() for _ in range(batch_size)]

    # Create DataFrame
    df = pl.DataFrame(
        {
            "event_time": event_times,
            "user_id": user_ids,
            "team_id": team_ids,
            "event_name": event_names,
            "properties": properties,
        }
    )

    print(f"Generated batch {batch_id} with {batch_size} rows")
    return df


def write_partition(args: Tuple[str, pl.DataFrame]) -> str:
    """
    Write a partition of data to Parquet file.

    Args:
        args: Tuple of (date_str, dataframe)

    Returns:
        Path to written file
    """
    date_str, df = args

    # Create partition directory
    partition_dir = OUTPUT_DIR / f"date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # Generate unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = partition_dir / f"events-{timestamp}.parquet"

    # Write to Parquet using Polars
    df.write_parquet(filename, compression="snappy", use_pyarrow=True)

    print(f"Written {len(df)} rows to {filename}")
    return str(filename)


def main():
    """Main function to orchestrate parallel event generation."""
    print(f"Starting event generation...")
    print(f"Total rows: {TOTAL_ROWS:,}")
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"CPU cores available: {cpu_count()}")

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Determine batch configuration
    num_processes = cpu_count()
    batch_size = TOTAL_ROWS // num_processes

    # Prepare batch arguments
    batch_args = [
        (
            i,
            batch_size
            if i < num_processes - 1
            else TOTAL_ROWS - (batch_size * (num_processes - 1)),
            START_DATE,
            END_DATE,
        )
        for i in range(num_processes)
    ]

    print(f"\nGenerating data using {num_processes} processes...")

    # Generate data in parallel
    with Pool(processes=num_processes) as pool:
        batch_dataframes = pool.map(generate_batch_data, batch_args)

    # Combine all batches
    print("\nCombining batches...")
    combined_df = pl.concat(batch_dataframes)

    print(f"Total rows generated: {len(combined_df):,}")

    # Add date column for partitioning
    combined_df = combined_df.with_columns(
        pl.col("event_time").cast(pl.Date).alias("date")
    )

    # Group by date for partitioning
    print("\nPartitioning by date...")
    partitions = combined_df.partition_by("date", as_dict=True)

    print(f"Created {len(partitions)} partitions")

    # Prepare partition write arguments
    partition_args = []
    for date_tuple, df in partitions.items():
        # date_tuple is like (datetime.date(2023, 1, 15),)
        date_obj = date_tuple[0]
        date_str = date_obj.strftime("%Y-%m-%d")
        partition_args.append((date_str, df.drop("date")))

    # Write partitions in parallel
    print("\nWriting partitions to disk...")
    with Pool(processes=num_processes) as pool:
        written_files = pool.map(write_partition, partition_args)

    print(f"\n✓ Successfully wrote {len(written_files)} partition files")
    print(f"\nSummary:")
    print(f"  Total events: {len(combined_df):,}")
    print(f"  Date partitions: {len(partitions)}")
    print(f"  Output location: {OUTPUT_DIR.absolute()}")

    # Display sample statistics
    print("\nSample statistics:")
    print(f"  Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
    print(f"  Unique users: {combined_df['user_id'].n_unique():,}")
    print(f"  Unique teams: {combined_df['team_id'].n_unique():,}")
    print(f"  Event types: {combined_df['event_name'].n_unique()}")

    # Show first few rows
    print("\nSample data (first 5 rows):")
    print(combined_df.head(5))


if __name__ == "__main__":
    main()
