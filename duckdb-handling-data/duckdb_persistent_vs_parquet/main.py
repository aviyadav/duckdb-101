import argparse
import random
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

START_DATE = date(2024, 1, 1)
END_DATE = date(2026, 7, 16)
ACCOUNTS = ("111122223333", "444455556666", "777788889999")
PROTOCOLS = ("6", "17", "1")
DESTINATION_PORTS = ("22", "53", "80", "123", "443", "3306", "5432")
BATCH_SIZE = 10_000
SCHEMA = pa.schema(
    [
        ("v", pa.string()),
        ("acc", pa.string()),
        ("id", pa.string()),
        ("src", pa.string()),
        ("dst", pa.string()),
        ("sp", pa.string()),
        ("dp", pa.string()),
        ("pr", pa.string()),
        ("pkt", pa.string()),
        ("byt", pa.string()),
        ("start", pa.string()),
        ("end", pa.string()),
        ("act", pa.string()),
        ("st", pa.string()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate partitioned synthetic AWS VPC Flow Logs in Parquet."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("base_flow"),
        help="output directory (default: base_flow)",
    )
    parser.add_argument(
        "--rows-per-day",
        type=int,
        default=1_000,
        help="number of synthetic records per day (default: 1000)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="random seed (default: 42)"
    )
    return parser.parse_args()


def private_ip(rng: random.Random) -> str:
    return f"10.{rng.randrange(256)}.{rng.randrange(256)}.{rng.randrange(1, 255)}"


def public_ip(rng: random.Random) -> str:
    # 198.51.100.0/24 is reserved for documentation and cannot target real hosts.
    return f"198.51.100.{rng.randrange(1, 255)}"


def flow_for_day(day: date, rng: random.Random) -> dict[str, str]:
    day_start = int(datetime.combine(day, time.min, UTC).timestamp())
    started = day_start + rng.randrange(86_400)
    ended = started + rng.randrange(1, 601)
    packets = rng.randrange(1, 10_001)
    interface_id = f"eni-{rng.getrandbits(48):012x}"

    # Roughly 2% of records model NODATA/SKIPDATA lines. AWS represents fields
    # unavailable in these records with a dash, which the query's TRY_CAST handles.
    status = rng.choices(("OK", "NODATA", "SKIPDATA"), (98, 1, 1))[0]
    unavailable = status != "OK"
    protocol = rng.choice(PROTOCOLS)

    return {
        "v": "2",
        "acc": rng.choice(ACCOUNTS),
        "id": interface_id,
        "src": private_ip(rng),
        "dst": public_ip(rng),
        "sp": "-" if unavailable else str(rng.randrange(1_024, 65_536)),
        "dp": "-" if unavailable else rng.choice(DESTINATION_PORTS),
        "pr": "-" if unavailable else protocol,
        "pkt": "-" if unavailable else str(packets),
        "byt": "-" if unavailable else str(packets * rng.randrange(40, 1_501)),
        "start": "-" if unavailable else str(started),
        "end": "-" if unavailable else str(ended),
        "act": "-" if unavailable else rng.choices(("ACCEPT", "REJECT"), (95, 5))[0],
        "st": status,
    }


def open_month_writer(output: Path, year: int, month: int) -> pq.ParquetWriter:
    partition = output / f"year={year}" / f"month={month:02d}"
    partition.mkdir(parents=True, exist_ok=True)
    return pq.ParquetWriter(
        partition / "flow_logs.parquet",
        SCHEMA,
        compression="zstd",
    )


def generate(output: Path, rows_per_day: int, seed: int) -> tuple[int, int]:
    if rows_per_day <= 0:
        raise ValueError("--rows-per-day must be greater than zero")

    rng = random.Random(seed)
    current = START_DATE
    current_month: tuple[int, int] | None = None
    writer: pq.ParquetWriter | None = None
    row_count = 0
    file_count = 0

    try:
        while current <= END_DATE:
            month = (current.year, current.month)
            if month != current_month:
                if writer is not None:
                    writer.close()
                writer = open_month_writer(output, *month)
                current_month = month
                file_count += 1

            assert writer is not None
            remaining = rows_per_day
            while remaining:
                batch_length = min(remaining, BATCH_SIZE)
                records = [flow_for_day(current, rng) for _ in range(batch_length)]
                writer.write_table(pa.Table.from_pylist(records, schema=SCHEMA))
                row_count += batch_length
                remaining -= batch_length

            current += timedelta(days=1)
    finally:
        if writer is not None:
            writer.close()

    return row_count, file_count


def main() -> None:
    args = parse_args()
    rows, files = generate(args.output, args.rows_per_day, args.seed)
    print(f"Generated {rows:,} rows in {files} Parquet files under {args.output}")


if __name__ == "__main__":
    main()
