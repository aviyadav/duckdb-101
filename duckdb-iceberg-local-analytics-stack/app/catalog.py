import duckdb

from app.config import (
    BRONZE_CUSTOMERS,
    BRONZE_ORDERS_GLOB,
    DATABASE,
    SILVER_CUSTOMERS,
    SILVER_ORDERS,
    THREADS,
)


def _parquet_path(path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def connection() -> duckdb.DuckDBPyConnection:
    """Create a configured DuckDB connection and register available datasets."""
    conn = duckdb.connect(str(DATABASE))
    conn.execute(f"SET threads = {THREADS}")

    if list(BRONZE_ORDERS_GLOB.parent.glob(BRONZE_ORDERS_GLOB.name)):
        conn.execute(
            "CREATE OR REPLACE VIEW bronze_orders AS "
            f"SELECT * FROM read_parquet('{_parquet_path(BRONZE_ORDERS_GLOB)}', "
            "union_by_name = true)"
        )
    if BRONZE_CUSTOMERS.exists():
        conn.execute(
            "CREATE OR REPLACE VIEW bronze_customers AS "
            f"SELECT * FROM read_parquet('{_parquet_path(BRONZE_CUSTOMERS)}')"
        )
    if SILVER_ORDERS.exists():
        conn.execute(
            "CREATE OR REPLACE VIEW silver_orders AS "
            f"SELECT * FROM read_parquet('{_parquet_path(SILVER_ORDERS)}')"
        )
    if SILVER_CUSTOMERS.exists():
        conn.execute(
            "CREATE OR REPLACE VIEW silver_customers AS "
            f"SELECT * FROM read_parquet('{_parquet_path(SILVER_CUSTOMERS)}')"
        )

    return conn
