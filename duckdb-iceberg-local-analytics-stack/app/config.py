from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE = PROJECT_ROOT / "analytics.duckdb"
SQL_DIR = PROJECT_ROOT / "sql"
WAREHOUSE = PROJECT_ROOT / "warehouse"
BRONZE = WAREHOUSE / "bronze"
SILVER = WAREHOUSE / "silver"
GOLD = WAREHOUSE / "gold"
INCOMING = PROJECT_ROOT / "incoming"
THREADS = 8

BRONZE_ORDERS_GLOB = BRONZE / "orders" / "*.parquet"
BRONZE_CUSTOMERS = BRONZE / "customers" / "customers.parquet"
SILVER_ORDERS = SILVER / "orders.parquet"
SILVER_CUSTOMERS = SILVER / "customers.parquet"


def ensure_directories() -> None:
    for path in (
        BRONZE / "orders",
        BRONZE / "customers",
        SILVER,
        GOLD,
        INCOMING,
        INCOMING / "processed",
    ):
        path.mkdir(parents=True, exist_ok=True)
