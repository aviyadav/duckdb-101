from pathlib import Path
from threading import Lock
from typing import Any

from app.catalog import connection
from app.config import SQL_DIR


class AnalyticsRepository:
    def __init__(self) -> None:
        self.db = connection()
        self._lock = Lock()

    def execute_sql_file(
        self, filename: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        sql_path = self._safe_sql_path(filename)
        query = sql_path.read_text(encoding="utf-8")
        with self._lock:
            result = self.db.execute(query, parameters or {})
            return result.fetch_arrow_table().to_pylist()

    def scalar(self, query: str) -> Any:
        with self._lock:
            row = self.db.execute(query).fetchone()
        if row is None:
            raise RuntimeError("DuckDB scalar query returned no rows")
        return row[0]

    def close(self) -> None:
        self.db.close()

    @staticmethod
    def _safe_sql_path(filename: str) -> Path:
        if Path(filename).name != filename or not filename.endswith(".sql"):
            raise ValueError("filename must name a SQL file in the sql directory")
        path = SQL_DIR / filename
        if not path.is_file():
            raise FileNotFoundError(f"SQL report not found: {filename}")
        return path
