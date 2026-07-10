from functools import lru_cache
from typing import Any

from app.config import BRONZE_ORDERS_GLOB, SILVER_CUSTOMERS, SILVER_ORDERS
from app.repository import AnalyticsRepository


class AnalyticsService:
    def __init__(self) -> None:
        self.repo = AnalyticsRepository()

    @lru_cache(maxsize=16)
    def daily_revenue(self) -> list[dict[str, Any]]:
        return self.repo.execute_sql_file("revenue.sql")

    @lru_cache(maxsize=16)
    def top_customers(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.repo.execute_sql_file("customers.sql", {"limit": limit})

    @lru_cache(maxsize=16)
    def monthly_growth(self) -> list[dict[str, Any]]:
        return self.repo.execute_sql_file("growth.sql")

    @lru_cache(maxsize=16)
    def retention(self) -> list[dict[str, Any]]:
        return self.repo.execute_sql_file("retention.sql")

    def health(self) -> dict[str, Any]:
        bronze_files = len(list(BRONZE_ORDERS_GLOB.parent.glob(BRONZE_ORDERS_GLOB.name)))
        return {
            "status": "healthy",
            "database": "duckdb",
            "warehouse": "available",
            "bronze_parquet_files": bronze_files,
            "orders": self.repo.scalar("SELECT COUNT(*) FROM silver_orders"),
            "customers": self.repo.scalar("SELECT COUNT(*) FROM silver_customers"),
            "silver_files_available": SILVER_ORDERS.exists() and SILVER_CUSTOMERS.exists(),
        }

    def clear_cache(self) -> None:
        self.daily_revenue.cache_clear()
        self.top_customers.cache_clear()
        self.monthly_growth.cache_clear()
        self.retention.cache_clear()

    def close(self) -> None:
        self.repo.close()


RevenueService = AnalyticsService
