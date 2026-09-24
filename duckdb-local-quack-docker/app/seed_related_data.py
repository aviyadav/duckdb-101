#!/usr/bin/env python3
import argparse
from pathlib import Path

import duckdb


def build_sql(worker: int, rows: int) -> str:
    end = rows + 1

    if worker == 1:
        return f"""
CREATE OR REPLACE TABLE sales AS
SELECT
    i::INTEGER AS sale_id,
    ((abs(hash(i)) % 10000000) + 1)::INTEGER AS customer_id,
    ((abs(hash(i + 1000000)) % 1000000) + 1)::INTEGER AS product_id,
    ((abs(hash(i + 2000000)) % 5) + 1)::INTEGER AS quantity,

    CASE abs(hash(i + 3000000)) % 5
        WHEN 0 THEN 'store'
        WHEN 1 THEN 'marketplace'
        WHEN 2 THEN 'telephone'
        WHEN 3 THEN 'online'
        ELSE 'partner'
    END AS sales_channel,

    CASE abs(hash(i + 4000000)) % 4
        WHEN 0 THEN 'bank_transfer'
        WHEN 1 THEN 'wallet'
        WHEN 2 THEN 'invoice'
        ELSE 'card'
    END AS payment_method,

    CASE abs(hash(i + 5000000)) % 5
        WHEN 0 THEN 'cancelled'
        WHEN 1 THEN 'completed'
        WHEN 2 THEN 'processing'
        WHEN 3 THEN 'returned'
        ELSE 'shipped'
    END AS sale_status,

    DATE '2025-01-01'
        + (
            CAST((abs(hash(i + 6000000)) % 365) AS INTEGER)
            * INTERVAL '1 day'
        ) AS sale_date,

    ROUND(
        1.0 + ((abs(hash(i + 7000000)) % 100000) / 100.0),
        2
    )::DOUBLE AS sold_unit_price,

    (abs(hash(i + 8000000)) % 25)::DOUBLE AS discount_pct

FROM range(1, {end}) AS t(i)
"""

    if worker == 2:
        return f"""
CREATE OR REPLACE TABLE customers AS
SELECT
    i::INTEGER AS customer_id,
    'CUST-' || lpad(CAST(i AS VARCHAR), 10, '0') AS customer_code,

    CASE abs(hash(i + 10)) % 6
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'DE'
        WHEN 2 THEN 'FR'
        WHEN 3 THEN 'CA'
        WHEN 4 THEN 'AU'
        ELSE 'UK'
    END AS country,

    CASE abs(hash(i + 20)) % 4
        WHEN 0 THEN 'consumer'
        WHEN 1 THEN 'small_business'
        WHEN 2 THEN 'enterprise'
        ELSE 'public_sector'
    END AS segment,

    CASE abs(hash(i + 30)) % 3
        WHEN 0 THEN 'standard'
        WHEN 1 THEN 'silver'
        ELSE 'gold'
    END AS membership_tier,

    (abs(hash(i + 40)) % 100) < 90 AS is_active,

    DATE '2015-01-01'
        + (
            CAST((abs(hash(i + 50)) % 3650) AS INTEGER)
            * INTERVAL '1 day'
        ) AS joined_date,

    TIMESTAMP '2026-09-01 00:00:00'
        - (
            CAST((abs(hash(i + 60)) % 87600) AS INTEGER)
            * INTERVAL '1 hour'
        ) AS last_seen_at,

    ROUND(
        1000.0 + ((abs(hash(i + 70)) % 15000))::DOUBLE,
        2
    ) AS credit_limit

FROM range(1, {end}) AS t(i)
"""

    if worker == 3:
        return f"""
CREATE OR REPLACE TABLE products AS
SELECT
    i::INTEGER AS product_id,
    'SKU-' || lpad(CAST(i AS VARCHAR), 10, '0') AS sku,

    CASE abs(hash(i + 10)) % 6
        WHEN 0 THEN 'home'
        WHEN 1 THEN 'garden'
        WHEN 2 THEN 'sports'
        WHEN 3 THEN 'clothing'
        WHEN 4 THEN 'food'
        ELSE 'electronics'
    END AS category,

    CASE abs(hash(i + 20)) % 5
        WHEN 0 THEN 'Bramble'
        WHEN 1 THEN 'Cobalt'
        WHEN 2 THEN 'Dove'
        WHEN 3 THEN 'Elm'
        ELSE 'Aster'
    END AS brand,

    CASE abs(hash(i + 30)) % 5
        WHEN 0 THEN 'EU'
        WHEN 1 THEN 'US'
        WHEN 2 THEN 'APAC'
        WHEN 3 THEN 'UK'
        ELSE 'LATAM'
    END AS supplier_region,

    ROUND(
        1.0 + ((abs(hash(i + 40)) % 100000) / 100.0),
        2
    )::DOUBLE AS catalogue_price,

    ((abs(hash(i + 50)) % 5000) + 1)::INTEGER AS stock_quantity,

    (abs(hash(i + 60)) % 100) < 5 AS discontinued,

    DATE '2018-01-01'
        + (
            CAST((abs(hash(i + 70)) % 3000) AS INTEGER)
            * INTERVAL '1 day'
        ) AS introduced_date

FROM range(1, {end}) AS t(i)
"""

    raise ValueError("worker must be 1, 2, or 3")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--worker",
        type=int,
        choices=[1, 2, 3],
        required=True,
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100000,
    )
    parser.add_argument(
        "--database",
        default="/data/worker.duckdb",
    )

    args = parser.parse_args()

    database_file = Path(args.database)
    database_file.parent.mkdir(parents=True, exist_ok=True)

    table_name = {
        1: "sales",
        2: "customers",
        3: "products",
    }[args.worker]

    print(
        f"Seeding worker {args.worker} table '{table_name}' "
        f"with {args.rows} rows into {args.database}",
        flush=True,
    )

    connection = duckdb.connect(str(database_file))

    try:
        connection.execute(build_sql(args.worker, args.rows))

        count = connection.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(f"Created {table_name} with {count} rows", flush=True)
    finally:
        connection.close()


if __name__ == "__main__":
    main()