from pathlib import Path

from app.catalog import connection
from app.config import (
    BRONZE_CUSTOMERS,
    BRONZE_ORDERS_GLOB,
    GOLD,
    SILVER_CUSTOMERS,
    SILVER_ORDERS,
    SQL_DIR,
    ensure_directories,
)
from app.data_generator import generate_data
from app.utils import logger


def _sql_string(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _write_parquet(db, query: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp.parquet")
    temporary.unlink(missing_ok=True)
    clean_query = query.strip().rstrip(";")
    db.execute(
        f"COPY ({clean_query}) TO '{_sql_string(temporary)}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    temporary.replace(destination)


def run_pipeline() -> dict[str, int]:
    """Promote raw bronze Parquet into validated silver and aggregate gold data."""
    ensure_directories()
    bronze_orders = list(BRONZE_ORDERS_GLOB.parent.glob(BRONZE_ORDERS_GLOB.name))
    if not bronze_orders or not BRONZE_CUSTOMERS.exists():
        raise FileNotFoundError("Bronze data is missing; run the data generator first")

    db = connection()
    try:
        silver_orders_sql = (SQL_DIR / "silver_orders.sql").read_text(encoding="utf-8")
        _write_parquet(db, silver_orders_sql, SILVER_ORDERS)
        _write_parquet(
            db,
            """
            SELECT
                CAST(customer_id AS BIGINT) AS customer_id,
                customer_name,
                region,
                segment,
                CAST(signup_date AS DATE) AS signup_date
            FROM bronze_customers
            WHERE customer_id IS NOT NULL
            """,
            SILVER_CUSTOMERS,
        )
    finally:
        db.close()

    # Reconnect so the catalog registers the newly created silver datasets.
    db = connection()
    try:
        _write_parquet(
            db,
            (SQL_DIR / "revenue.sql").read_text(encoding="utf-8"),
            GOLD / "daily_revenue.parquet",
        )
        _write_parquet(
            db,
            (SQL_DIR / "growth.sql").read_text(encoding="utf-8"),
            GOLD / "monthly_growth.parquet",
        )
        _write_parquet(
            db,
            """
            SELECT
                customer_id,
                COUNT(*) AS lifetime_orders,
                ROUND(SUM(total_amount), 2) AS lifetime_revenue,
                MIN(order_date) AS first_order_date,
                MAX(order_date) AS last_order_date
            FROM silver_orders
            GROUP BY customer_id
            """,
            GOLD / "customer_summary.parquet",
        )
        counts = db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM bronze_orders),
                (SELECT COUNT(*) FROM silver_orders),
                (SELECT COUNT(*) FROM silver_customers)
            """
        ).fetchone()
        if counts is None:
            raise RuntimeError("DuckDB did not return pipeline row counts")
        result = {
            "bronze_orders": counts[0],
            "silver_orders": counts[1],
            "customers": counts[2],
        }
    finally:
        db.close()

    logger.info(
        "analytics_pipeline_complete bronze_orders=%s silver_orders=%s customers=%s",
        result["bronze_orders"],
        result["silver_orders"],
        result["customers"],
    )
    return result


def bootstrap() -> dict[str, int]:
    """Create sample data and derived layers when the application is first run."""
    ensure_directories()
    if not list(BRONZE_ORDERS_GLOB.parent.glob(BRONZE_ORDERS_GLOB.name)) or not BRONZE_CUSTOMERS.exists():
        generate_data()
    if not SILVER_ORDERS.exists() or not SILVER_CUSTOMERS.exists():
        return run_pipeline()
    return {"bronze_orders": -1, "silver_orders": -1, "customers": -1}


def main() -> None:
    from app.utils import configure_logging

    configure_logging()
    if not list(BRONZE_ORDERS_GLOB.parent.glob(BRONZE_ORDERS_GLOB.name)):
        generate_data()
    run_pipeline()


if __name__ == "__main__":
    main()
