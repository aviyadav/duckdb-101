"""Set up scoped persistent secrets and seed sample data in RustFS.

Implements the example from Scoped-Persistent-Secrets.docx:
1. Creates 'company-analytics-bucket' and 'company-logs-bucket' on RustFS.
2. Generates and uploads:
   - s3://company-analytics-bucket/users.parquet
   - s3://company-logs-bucket/2026/events.parquet
3. Registers persistent scoped secrets in DuckDB:
   - analytics_s3 -> SCOPE 's3://company-analytics-bucket/'
   - logs_s3      -> SCOPE 's3://company-logs-bucket/'
"""

import datetime
import hashlib
import hmac
import os
import tempfile
import urllib.error
import urllib.request

import duckdb

RUSTFS_HOST = "localhost:9000"
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin"


def _s3_request(method: str, path: str, body: bytes = b"") -> int:
    """Send an AWS SigV4 signed request to RustFS."""
    payload_hash = hashlib.sha256(body).hexdigest()
    amz_date = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]

    canonical_headers = (
        f"host:{RUSTFS_HOST}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )

    def _hmac(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    k_date = _hmac(f"AWS4{SECRET_KEY}".encode(), date_stamp)
    k_region = _hmac(k_date, "us-east-1")
    k_service = _hmac(k_region, "s3")
    k_signing = _hmac(k_service, "aws4_request")

    credential_scope = f"{date_stamp}/us-east-1/s3/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n"
        f"{amz_date}\n"
        f"{credential_scope}\n"
        f"{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    )
    signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

    headers = {
        "Host": RUSTFS_HOST,
        "Authorization": (
            f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
    }
    if body:
        headers["Content-Type"] = "application/octet-stream"

    req = urllib.request.Request(
        f"http://{RUSTFS_HOST}{path}",
        data=body if body else None,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        # 409 Conflict typically means bucket already exists
        if e.code == 409:
            return 200
        raise


def ensure_bucket(bucket: str) -> None:
    """Create the S3 bucket if it doesn't already exist."""
    status = _s3_request("PUT", f"/{bucket}")
    print(f"Bucket '{bucket}' ready (status: {status})")


def upload_parquet_bytes(bucket: str, key: str, data: bytes) -> None:
    """Upload binary parquet content to RustFS."""
    status = _s3_request("PUT", f"/{bucket}/{key}", body=data)
    print(f"Uploaded s3://{bucket}/{key} ({len(data)} bytes, status: {status})")


def generate_and_upload_data() -> None:
    """Generate sample users and events parquet files, then upload to RustFS."""
    ensure_bucket("company-analytics-bucket")
    ensure_bucket("company-logs-bucket")

    con = duckdb.connect()
    with tempfile.TemporaryDirectory() as tmpdir:
        users_file = os.path.join(tmpdir, "users.parquet")
        events_file = os.path.join(tmpdir, "events.parquet")

        con.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    (1, 'Alice Smith'),
                    (2, 'Bob Jones'),
                    (3, 'Charlie Brown'),
                    (4, 'Diana Prince'),
                    (5, 'Evan Wright')
                ) AS t(user_id, user_name)
            ) TO '{users_file}' (FORMAT PARQUET);
        """)

        con.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    (1, 'login', TIMESTAMP '2026-01-15 08:30:00'),
                    (1, 'purchase', TIMESTAMP '2026-01-15 08:45:00'),
                    (2, 'login', TIMESTAMP '2026-01-15 09:12:00'),
                    (2, 'logout', TIMESTAMP '2026-01-15 09:30:00'),
                    (3, 'login', TIMESTAMP '2026-01-16 10:05:00'),
                    (3, 'search', TIMESTAMP '2026-01-16 10:10:00'),
                    (4, 'login', TIMESTAMP '2026-01-16 11:00:00'),
                    (4, 'click_ad', TIMESTAMP '2026-01-16 11:05:00'),
                    (5, 'login', TIMESTAMP '2026-01-17 14:20:00'),
                    (5, 'checkout', TIMESTAMP '2026-01-17 14:35:00')
                ) AS t(user_id, event_type, event_time)
            ) TO '{events_file}' (FORMAT PARQUET);
        """)

        with open(users_file, "rb") as f:
            upload_parquet_bytes("company-analytics-bucket", "users.parquet", f.read())

        with open(events_file, "rb") as f:
            upload_parquet_bytes("company-logs-bucket", "2026/events.parquet", f.read())


def create_persistent_secrets() -> None:
    """Register persistent scoped secrets in DuckDB."""
    con = duckdb.connect()

    # Secret A: Dedicated to the analytics bucket
    con.execute("""
        CREATE OR REPLACE PERSISTENT SECRET analytics_s3 (
            TYPE S3,
            KEY_ID 'rustfsadmin',
            SECRET 'rustfsadmin',
            ENDPOINT 'localhost:9000/company-analytics-bucket',
            REGION 'us-east-1',
            URL_STYLE 'path-style',
            USE_SSL false,
            SCOPE 's3://company-analytics-bucket/'
        );
    """)

    # Secret B: Dedicated to raw log storage
    con.execute("""
        CREATE OR REPLACE PERSISTENT SECRET logs_s3 (
            TYPE S3,
            KEY_ID 'rustfsadmin',
            SECRET 'rustfsadmin',
            ENDPOINT 'localhost:9000/company-logs-bucket',
            REGION 'us-east-1',
            URL_STYLE 'path-style',
            USE_SSL false,
            SCOPE 's3://company-logs-bucket/'
        );
    """)

    secrets_dir = os.path.expanduser("~/.duckdb/stored_secrets")
    print(f"\nPersistent secrets created in {secrets_dir}:")
    for f in sorted(os.listdir(secrets_dir)):
        if f.endswith(".duckdb_secret"):
            print(f"  - {f}")


def setup_scoped_secrets() -> None:
    print("=== Step 1: Generating and uploading Parquet datasets to RustFS ===")
    generate_and_upload_data()

    print("\n=== Step 2: Creating persistent scoped secrets in DuckDB ===")
    create_persistent_secrets()
    print("\nSetup complete!")


if __name__ == "__main__":
    setup_scoped_secrets()
