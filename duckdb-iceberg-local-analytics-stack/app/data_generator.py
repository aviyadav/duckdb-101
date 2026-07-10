import argparse
import random
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from app.config import BRONZE, ensure_directories
from app.utils import configure_logging, logger

REGIONS = ("North America", "Europe", "Asia Pacific", "Latin America")
SEGMENTS = ("Consumer", "Small Business", "Enterprise")
CATEGORIES = ("Software", "Hardware", "Services", "Training")
STATUSES = ("completed", "completed", "completed", "completed", "cancelled", "refunded")


def generate_data(order_count: int = 25_000, customer_count: int = 500, seed: int = 42) -> dict[str, int]:
    """Generate deterministic e-commerce data as reasonably sized Parquet batches."""
    if order_count < 1 or customer_count < 1:
        raise ValueError("order_count and customer_count must be positive")

    ensure_directories()
    rng = random.Random(seed)
    customers_dir = BRONZE / "customers"
    orders_dir = BRONZE / "orders"
    for old_file in orders_dir.glob("generated-*.parquet"):
        old_file.unlink()

    customer_ids = list(range(1, customer_count + 1))
    signup_start = date(2024, 1, 1)
    customers = pa.table(
        {
            "customer_id": pa.array(customer_ids, type=pa.int64()),
            "customer_name": [f"Customer {customer_id:04d}" for customer_id in customer_ids],
            "region": [rng.choice(REGIONS) for _ in customer_ids],
            "segment": [rng.choices(SEGMENTS, weights=(65, 25, 10), k=1)[0] for _ in customer_ids],
            "signup_date": pa.array(
                [signup_start + timedelta(days=rng.randrange(730)) for _ in customer_ids],
                type=pa.date32(),
            ),
        }
    )
    pq.write_table(customers, customers_dir / "customers.parquet", compression="zstd")

    order_start = datetime(2025, 1, 1)
    day_range = (datetime(2026, 6, 30) - order_start).days
    batch_size = 5_000
    for offset in range(0, order_count, batch_size):
        size = min(batch_size, order_count - offset)
        ids = range(offset + 1, offset + size + 1)
        amounts = [round(rng.lognormvariate(4.5, 0.75), 2) for _ in ids]
        table = pa.table(
            {
                "order_id": pa.array(list(ids), type=pa.int64()),
                "customer_id": pa.array(
                    [rng.choice(customer_ids) for _ in range(size)], type=pa.int64()
                ),
                "order_time": pa.array(
                    [
                        order_start
                        + timedelta(
                            days=rng.randrange(day_range + 1),
                            seconds=rng.randrange(86_400),
                        )
                        for _ in range(size)
                    ],
                    type=pa.timestamp("us"),
                ),
                "status": [rng.choice(STATUSES) for _ in range(size)],
                "product_category": [rng.choice(CATEGORIES) for _ in range(size)],
                "total_amount": pa.array(amounts, type=pa.float64()),
            }
        )
        pq.write_table(
            table,
            orders_dir / f"generated-{offset // batch_size + 1:03d}.parquet",
            compression="zstd",
        )

    result = {"customers": customer_count, "orders": order_count}
    logger.info("generated_bronze_data customers=%s orders=%s", customer_count, order_count)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sample analytics data in Parquet")
    parser.add_argument("--orders", type=int, default=25_000)
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    configure_logging()
    generate_data(args.orders, args.customers, args.seed)


if __name__ == "__main__":
    main()
