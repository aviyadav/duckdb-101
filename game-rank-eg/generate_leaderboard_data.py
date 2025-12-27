#!/usr/bin/env python3
"""
Generate random leaderboard data for testing game-rank.sql query.
Creates partitioned parquet files matching the schema in game-rank.sql.
"""

import random
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from faker import Faker

# Configuration
NUM_ROWS = 1_000_000
OUTPUT_DIR = Path("leaderboard")
GAME_IDS = [
    "space_racers",
    "battle_arena",
    "dragon_legends",
    "turbo_drift",
    "shadow_strike",
    "dungeon_quest",
    "candy_cascade",
    "galaxy_wars",
    "tower_defense",
    "cyber_warriors",
]
REGIONS = ["NA", "EU", "ASIA", "SA", "OCE"]
NUM_PLAYERS = 5000
SCORE_MIN = 0
SCORE_MAX = 1_000_000

# Date range for partitions (last 7 days)
END_DATE = datetime(2025, 12, 31)
START_DATE = END_DATE - timedelta(days=30)

fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_leaderboard_data(num_rows: int) -> pl.DataFrame:
    """Generate random leaderboard data."""

    data = {
        "game_id": [random.choice(GAME_IDS) for _ in range(num_rows)],
        "region": [random.choice(REGIONS) for _ in range(num_rows)],
        "day": [
            fake.date_between(start_date=START_DATE, end_date=END_DATE)
            for _ in range(num_rows)
        ],
        "player_id": [f"player_{random.randint(1, NUM_PLAYERS):06d}" for _ in range(num_rows)],
        "score": [random.randint(SCORE_MIN, SCORE_MAX) for _ in range(num_rows)],
        "ts": [
            fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
            for _ in range(num_rows)
        ],
    }

    return pl.DataFrame(data)


def write_partitioned_parquet(df: pl.DataFrame, output_dir: Path) -> None:
    """Write dataframe to partitioned parquet files by day."""

    # Group by day and write each partition
    for day_value, day_df in df.group_by("day"):
        day_str = day_value[0].strftime("%Y-%m-%d")
        partition_dir = output_dir / f"day={day_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Write parquet file (remove 'day' column as it's in the partition path)
        output_file = partition_dir / "data.parquet"
        day_df.drop("day").write_parquet(
            output_file,
            compression="snappy",
            use_pyarrow=True,
        )

        print(f"Written {len(day_df):,} rows to {output_file}")


def main():
    """Generate and write leaderboard data."""

    print(f"Generating {NUM_ROWS:,} rows of leaderboard data...")
    df = generate_leaderboard_data(NUM_ROWS)

    print(f"\nDataframe shape: {df.shape}")
    print(f"\nSchema:\n{df.schema}")
    print(f"\nSample data:\n{df.head(10)}")

    print(f"\nWriting partitioned parquet files to {OUTPUT_DIR}/...")
    write_partitioned_parquet(df, OUTPUT_DIR)

    print(f"\nDone! Generated {NUM_ROWS:,} rows across {df['day'].n_unique()} day partitions")
    print(f"\nTo query this data with DuckDB, update game-rank.sql to use:")
    print(f"  FROM read_parquet('{OUTPUT_DIR}/day=*/*.parquet')")


if __name__ == "__main__":
    main()
