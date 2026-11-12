"""Generate October 2026 VPC Flow Logs locally and upload them to RUSTFS.

Run with: uv run generate_and_upload_parquet.py
"""

import os
from datetime import UTC, datetime
from pathlib import Path

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TOTAL_RECORDS = 250_000
ROWS_PER_FILE = 25_000
RUSTFS_ENDPOINT = "http://localhost:9001"
ACCOUNT_ID = "015443280042"
REGIONS_CONFIG = [
    {
        "region": "eu-central-1",
        "bucket": "ec1-vpc-flowlogs-bucket",
        "prefix": "prod/AWSLogs/015443280042/vpcflowlogs/eu-central-1/",
    },
    {
        "region": "eu-west-1",
        "bucket": "ew1-vpc-flowlogs-bucket",
        "prefix": "prod/AWSLogs/015443280042/vpcflowlogs/eu-west-1/",
    },
]
SCHEMA = pa.schema(
    [
        ("version", pa.int32()),
        ("account_id", pa.string()),
        ("interface_id", pa.string()),
        ("srcaddr", pa.string()),
        ("dstaddr", pa.string()),
        ("srcport", pa.int32()),
        ("dstport", pa.int32()),
        ("protocol", pa.int32()),
        ("packets", pa.int64()),
        ("bytes", pa.int64()),
        ("start", pa.int64()),
        ("end", pa.int64()),
        ("action", pa.string()),
        ("log_status", pa.string()),
    ]
)


def random_ips(rng: np.random.Generator, count: int, private: bool = False) -> list[str]:
    if private:
        octets = np.column_stack(
            (
                np.full(count, 10, dtype=np.uint8),
                rng.integers(0, 256, size=(count, 3), dtype=np.uint8),
            )
        )
    else:
        octets = rng.integers(1, 255, size=(count, 4), dtype=np.uint8)
    return [".".join(map(str, row)) for row in octets]


def make_flow_logs(rng: np.random.Generator, count: int) -> pa.Table:
    october_start = int(datetime(2026, 10, 1, tzinfo=UTC).timestamp())
    november_start = int(datetime(2026, 11, 1, tzinfo=UTC).timestamp())
    starts = rng.integers(october_start, november_start, size=count, dtype=np.int64)
    packets = rng.integers(1, 1_000, size=count, dtype=np.int64)
    source_pool = random_ips(rng, 256, private=True)
    destination_pool = random_ips(rng, 512)

    return pa.Table.from_pydict(
        {
            "version": np.full(count, 2, dtype=np.int32),
            "account_id": [ACCOUNT_ID] * count,
            "interface_id": [f"eni-{value:017x}" for value in rng.integers(0, 2**60, size=count)],
            "srcaddr": rng.choice(source_pool, size=count),
            "dstaddr": rng.choice(destination_pool, size=count),
            "srcport": rng.integers(1_024, 65_536, size=count, dtype=np.int32),
            "dstport": rng.choice([80, 443, 53, 22, 123], size=count, p=[0.15, 0.65, 0.12, 0.03, 0.05]),
            "protocol": rng.choice([6, 17, 1], size=count, p=[0.75, 0.24, 0.01]),
            "packets": packets,
            "bytes": packets * rng.integers(40, 1_501, size=count, dtype=np.int64),
            "start": starts,
            "end": starts + rng.integers(1, 301, size=count, dtype=np.int64),
            "action": rng.choice(["ACCEPT", "REJECT"], size=count, p=[0.95, 0.05]),
            "log_status": ["OK"] * count,
        },
        schema=SCHEMA,
    )


def generate_files(
    data_dir: Path = DATA_DIR, total_records: int = TOTAL_RECORDS
) -> list[tuple[dict[str, str], Path, str]]:
    rng = np.random.default_rng(42)
    records_per_region, remainder = divmod(total_records, len(REGIONS_CONFIG))
    generated = []

    for region_index, cfg in enumerate(REGIONS_CONFIG):
        region_records = records_per_region + int(region_index < remainder)
        for file_index, offset in enumerate(range(0, region_records, ROWS_PER_FILE)):
            row_count = min(ROWS_PER_FILE, region_records - offset)
            key = f"{cfg['prefix']}2026/10/{cfg['region']}_{file_index:05d}.parquet"
            path = data_dir / cfg["bucket"] / key
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(make_flow_logs(rng, row_count), path, compression="snappy")
            generated.append((cfg, path, key))
            print(f"Generated {row_count:,} records: {path.relative_to(data_dir)}")

    actual_records = sum(pq.read_metadata(path).num_rows for _, path, _ in generated)
    if actual_records != total_records:
        raise RuntimeError(f"Expected {total_records:,} records, generated {actual_records:,}")
    print(f"Generated {actual_records:,} records in {len(generated)} Parquet files.")
    return generated


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
            raise
        client.create_bucket(Bucket=bucket)
        print(f"Created bucket: {bucket}")


def upload_files(files: list[tuple[dict[str, str], Path, str]]) -> None:
    load_dotenv(BASE_DIR / ".env")
    admin = os.getenv("RUSTFS_ADMIN") or os.getenv("RUSTFS_ACCESS_KEY")
    password = os.getenv("RUSTFS_PASSWORD") or os.getenv("RUSTFS_SECRET_KEY")
    if not admin or not password:
        raise RuntimeError(
            f"Set RUSTFS_ADMIN and RUSTFS_PASSWORD in {BASE_DIR / '.env'} before uploading."
        )

    session = boto3.Session(aws_access_key_id=admin, aws_secret_access_key=password)
    endpoint = os.getenv("RUSTFS_ENDPOINT_URL", RUSTFS_ENDPOINT)
    clients = {
        cfg["region"]: session.client(
            "s3",
            endpoint_url=endpoint,
            region_name=cfg["region"],
            config=Config(s3={"addressing_style": "path"}),
        )
        for cfg in REGIONS_CONFIG
    }
    for cfg in REGIONS_CONFIG:
        ensure_bucket(clients[cfg["region"]], cfg["bucket"])

    for cfg, path, key in files:
        clients[cfg["region"]].upload_file(str(path), cfg["bucket"], key)
        print(f"Uploaded s3://{cfg['bucket']}/{key}")


def main() -> None:
    upload_files(generate_files())


if __name__ == "__main__":
    main()
