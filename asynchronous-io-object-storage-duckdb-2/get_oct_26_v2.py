import os
import sys
import time

import boto3
import duckdb
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Start tracking total runtime
start_time = time.perf_counter()

# ----------------------------
# 1. CONFIGURATION & RUSTFS SETUP
# ----------------------------
# AWS_PROFILE = "flowlog_bot"
REGIONS_CONFIG = [
    {"region": "eu-central-1", "bucket": "ec1-vpc-flowlogs-bucket", "prefix": "prod/AWSLogs/015443280042/vpcflowlogs/eu-central-1/"},
    {"region": "eu-west-1", "bucket": "ew1-vpc-flowlogs-bucket", "prefix": "prod/AWSLogs/015443280042/vpcflowlogs/eu-west-1/"}
]

# Use boto3 just for robust S3 object enumeration and storage class checking.
# Original AWS authentication:
# session = boto3.Session(profile_name=AWS_PROFILE)

load_dotenv()
RUSTFS_ADMIN = os.getenv("RUSTFS_ADMIN") or os.getenv("RUSTFS_ACCESS_KEY")
RUSTFS_PASSWORD = os.getenv("RUSTFS_PASSWORD") or os.getenv("RUSTFS_SECRET_KEY")
RUSTFS_ENDPOINT = os.getenv("RUSTFS_ENDPOINT_URL", "http://localhost:9001")
if not RUSTFS_ADMIN or not RUSTFS_PASSWORD:
    raise RuntimeError("Set RUSTFS_ADMIN and RUSTFS_PASSWORD in .env before running.")

rustfs_session = boto3.Session(
    aws_access_key_id=RUSTFS_ADMIN,
    aws_secret_access_key=RUSTFS_PASSWORD,
)
rustfs_config = Config(s3={"addressing_style": "path"})

# ----------------------------
# 2. DISCOVER & FILTER ACTIVE S3 FILES FOR OCTOBER 2026
# ----------------------------
target_s3_paths = []
skipped_archived_count = 0

for cfg in REGIONS_CONFIG:
    oct_prefix = f"{cfg['prefix'].strip('/')}/2026/10/"
    print(f"📂 Scanning RUSTFS bucket [{cfg['bucket']}] with prefix [{oct_prefix}]...")

    # s3 = session.client("s3", region_name=cfg['region'])
    s3 = rustfs_session.client(
        "s3",
        region_name=cfg["region"],
        endpoint_url=RUSTFS_ENDPOINT,
        config=rustfs_config,
    )
    paginator = s3.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=cfg['bucket'], Prefix=oct_prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    storage_class = obj.get("StorageClass", "STANDARD")
                    # Skip Glacier/Deep Archive to prevent HTTP 403 crashes
                    if storage_class in ["GLACIER", "DEEP_ARCHIVE"]:
                        skipped_archived_count += 1
                        continue

                    # Original AWS log filter:
                    # if key.endswith(".log.gz"):
                    if key.endswith(".parquet"):
                        target_s3_paths.append(f"s3://{cfg['bucket']}/{key}")
    except ClientError as e:
        print(f"❌ Failed to list objects for {cfg['region']}: {e}")

file_count = len(target_s3_paths)
print(f"🚀 Active RUSTFS files ready for query: {file_count} (Skipped {skipped_archived_count} archived files)")

if file_count == 0:
    print("ℹ️ No active Parquet files found for October 2026. Exiting.")
    sys.exit(0)

# ----------------------------
# 3. DUCKDB 2.0 CONNECTION & CREDENTIAL CHAIN SETUP
# ----------------------------
con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Print out active DuckDB version string to confirm 2.0 execution
db_version = con.execute("SELECT version()").fetchone()[0]
print(f"ℹ️ Executing with DuckDB Version: {db_version}")
if not db_version.startswith("v2.0."):
    raise RuntimeError(f"DuckDB 2.0 is required; found {db_version}")

# Original DuckDB 2.0 AWS credential-chain configuration:
# con.execute("""
#     CREATE SECRET aws_s3_creds (
#         TYPE S3,
#         PROVIDER credential_chain,
#         PROFILE 'flowlog_bot'
#     );
# """)

# Standard VPC Flow Log CSV column mapping used by the original AWS query:
# cols = {
#     'v': 'VARCHAR', 'acc': 'VARCHAR', 'id': 'VARCHAR', 'src': 'VARCHAR',
#     'dst': 'VARCHAR', 'sp': 'VARCHAR', 'dp': 'VARCHAR', 'pr': 'VARCHAR',
#     'pkt': 'VARCHAR', 'byt': 'VARCHAR', 'start': 'VARCHAR', 'end': 'VARCHAR',
#     'act': 'VARCHAR', 'st': 'VARCHAR'
# }

# DuckDB 2.0 native S3 secret for RUSTFS.
duckdb_s3_endpoint = RUSTFS_ENDPOINT.removeprefix("http://").removeprefix("https://").rstrip("/")
duckdb_s3_use_ssl = str(RUSTFS_ENDPOINT.startswith("https://")).lower()
con.execute(f"""
    CREATE SECRET rustfs_s3_creds (
        TYPE S3,
        KEY_ID '{RUSTFS_ADMIN}',
        SECRET '{RUSTFS_PASSWORD}',
        REGION 'eu-central-1',
        ENDPOINT '{duckdb_s3_endpoint}',
        USE_SSL {duckdb_s3_use_ssl},
        URL_STYLE 'path'
    );
""")

print("⚙️ Executing DuckDB 2.0 async stream scan across RUSTFS files...")

paths_sql_array = str(target_s3_paths)

# Original AWS gzip/CSV query:
# query = f"""
#     SELECT
#         src AS source_ip,
#         dst AS destination_ip,
#         SUM(TRY_CAST(NULLIF(byt, '-') AS UBIGINT)) AS total_bytes,
#         COUNT(*) AS total_packets
#     FROM read_csv(
#         {paths_sql_array},
#         delim=' ',
#         header=False,
#         columns={cols},
#         compression='gzip',
#         ignore_errors=true
#     )
#     WHERE start != 'start' AND TRY_CAST(start AS BIGINT) IS NOT NULL
#     GROUP BY src, dst
#     ORDER BY total_bytes DESC
#     LIMIT 20;
# """

query = f"""
    SELECT
        srcaddr AS source_ip,
        dstaddr AS destination_ip,
        SUM(bytes) AS total_bytes,
        COUNT(*) AS total_packets
    FROM read_parquet({paths_sql_array})
    WHERE start IS NOT NULL
    GROUP BY srcaddr, dstaddr
    ORDER BY total_bytes DESC
    LIMIT 20;
"""

try:
    rows = con.execute(query).fetchall()

    # Calculate total runtime
    end_time = time.perf_counter()
    total_runtime_sec = end_time - start_time

    print(f"\n✅ Successfully completed scan using DuckDB {db_version}.")
    print(f"⏱️ Total Runtime: {total_runtime_sec:.2f} seconds")
    print("\nTop Traffic Summary for October 2026:")
    print("(source_ip, destination_ip, total_bytes, total_packets)")
    for row in rows:
        print(row)

except duckdb.Error as e:
    print(f"\n❌ Query execution failed: {e}")

con.close()
