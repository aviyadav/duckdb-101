"""
generate-transaction-simple.py
──────────────────────────────
Simple single-process CSV generator for 500,000 transaction records.
Uses only the Python standard library: csv, random, datetime, time, os.
"""

import csv
import os
import random
import time
from datetime import datetime, timedelta

# ── Configuration ──────────────────────────────────────────────────────────────

NUM_RECORDS = 500_000
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "transaction-data-1.csv")

PRODUCTS = [
    "Camera",
    "Charger",
    "Tablet",
    "Printer",
    "Monitor",
    "Laptop",
    "Headphones",
    "Keyboard",
    "Phone",
    "Mouse",
]
STORES = ["Store_A", "Store_B", "Store_C", "Store_D", "Store_E"]
PAYMENT_METHODS = ["Cash", "Credit Card", "Debit Card", "UPI payment", "Apple Pay"]

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


# ── Helpers ────────────────────────────────────────────────────────────────────


def random_date() -> str:
    """Return a random date string (YYYY-MM-DD) between START_DATE and END_DATE."""
    offset = random.randint(0, DATE_RANGE_DAYS)
    return (START_DATE + timedelta(days=offset)).strftime("%Y-%m-%d")


def generate_row(index: int) -> list:
    """Build and return a single transaction record as a list."""
    return [
        f"TXN{index:07d}",
        f"CUST{random.randint(1, 99_999):05d}",
        random.choice(PRODUCTS),
        round(random.uniform(100.00, 2500.00), 2),
        random.randint(1, 10),
        random.choice(STORES),
        random.choice(PAYMENT_METHODS),
        random_date(),
    ]


HEADER = [
    "transaction_id",
    "customer_id",
    "product",
    "amount",
    "quantity",
    "store",
    "payment_method",
    "date",
]


# ── Main ───────────────────────────────────────────────────────────────────────


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n{'═' * 52}")
    print(f"  Generating {NUM_RECORDS:,} transaction records")
    print(f"  Output : {OUTPUT_FILE}")
    print(f"{'═' * 52}\n")

    t_start = time.perf_counter()

    with open(OUTPUT_FILE, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(HEADER)

        for i in range(1, NUM_RECORDS + 1):
            writer.writerow(generate_row(i))

            # Progress indicator every 100,000 rows
            if i % 100_000 == 0:
                elapsed = time.perf_counter() - t_start
                pct = i / NUM_RECORDS * 100
                print(f"  {i:>10,} rows written  ({pct:5.1f}%)  {elapsed:.2f}s")

    total_s = time.perf_counter() - t_start
    file_mb = os.path.getsize(OUTPUT_FILE) / 1_048_576

    print(f"\n{'═' * 52}")
    print(f"  Records    : {NUM_RECORDS:>10,}")
    print(f"  File size  : {file_mb:>9.1f} MB")
    print(f"  Total time : {total_s:>9.2f}s")
    print(f"  Rows/sec   : {NUM_RECORDS / total_s:>9,.0f}")
    print(f"  Saved to   : {OUTPUT_FILE}")
    print(f"{'═' * 52}\n")


if __name__ == "__main__":
    main()
