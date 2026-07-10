import argparse
import shutil
from pathlib import Path

import duckdb

from app.config import BRONZE, INCOMING, ensure_directories
from app.pipeline import run_pipeline
from app.utils import configure_logging, logger


def _quoted(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def process_incremental_data() -> int:
    """Validate incoming order Parquet files, add them to bronze, and rebuild derived layers."""
    ensure_directories()
    files = sorted(INCOMING.glob("*.parquet"))
    if not files:
        logger.info("no_incremental_files directory=%s", INCOMING)
        return 0

    destination_dir = BRONZE / "orders"
    processed_dir = INCOMING / "processed"
    db = duckdb.connect()
    try:
        for source in files:
            columns = {
                row[0]
                for row in db.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{_quoted(source)}')"
                ).fetchall()
            }
            required = {
                "order_id",
                "customer_id",
                "order_time",
                "status",
                "product_category",
                "total_amount",
            }
            missing = required - columns
            if missing:
                raise ValueError(f"{source.name} is missing columns: {sorted(missing)}")

            destination = destination_dir / f"incremental-{source.stem}.parquet"
            destination.unlink(missing_ok=True)
            db.execute(
                f"COPY (SELECT * FROM read_parquet('{_quoted(source)}')) "
                f"TO '{_quoted(destination)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            archived = processed_dir / source.name
            archived.unlink(missing_ok=True)
            shutil.move(str(source), archived)
            logger.info("ingested_incremental_file file=%s", source.name)
    finally:
        db.close()

    run_pipeline()
    return len(files)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Parquet files from incoming/")
    parser.parse_args()
    configure_logging()
    process_incremental_data()


if __name__ == "__main__":
    main()
