#!/usr/bin/env python3
"""
Benchmark: MySQL vs Parquet + DuckDB for ad analytics dashboard queries.
Generates realistic ad performance data across 100 clients and 10 ad channels,
loads it into both MySQL and Hive-partitioned Parquet files, then runs a suite
of benchmark queries that simulate common dashboard operations: filtering,
sorting, aggregation, and time-series analysis.
Three result files are produced for blog-ready comparison:
  - results_mysql.txt   — raw MySQL query results
  - results_duckdb.txt  — raw DuckDB query results
  - results_comparison.txt — side-by-side comparison with accuracy checks
Run from repo root:
  pip install -r requirements.txt
  python benchmark.py
Set SKIP_CLEANUP=1 to keep MySQL database and Parquet files after run.
"""

import os
import random
import shutil
import statistics
import time
from datetime import datetime, timedelta
from decimal import Decimal

# ---------------------------------------------------------------------------
# Configuration (adjust via environment variables)
# ---------------------------------------------------------------------------
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")

NUM_ROWS = int(os.environ.get("NUM_ROWS", "10_000_000"))
RANDOM_SEED = 42
DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2024, 12, 31)

# --- Clients: 100 named clients across diverse industries ---
# Generated programmatically from prefix/industry/suffix pools to keep the
# source compact while producing unique, realistic-sounding company names.
_INDUSTRIES = [
    "Retail",
    "Technology",
    "Food & Beverage",
    "Fashion",
    "Automotive",
    "Healthcare",
    "Education",
    "Travel",
    "Finance",
    "Energy",
    "Pet Care",
    "Real Estate",
    "Media",
    "Entertainment",
    "Sports",
    "Fitness",
    "Agriculture",
    "Construction",
    "Logistics",
    "Legal",
    "Insurance",
    "Telecom",
    "Pharma",
    "Hospitality",
    "Aerospace",
]
_COMPANY_PREFIXES = [
    "Apex",
    "Bolt",
    "Crest",
    "Dash",
    "Echo",
    "Flux",
    "Glow",
    "Hive",
    "Iris",
    "Jade",
    "Kite",
    "Luma",
    "Mint",
    "Nova",
    "Onyx",
    "Peak",
    "Quill",
    "Rift",
    "Sage",
    "Tide",
]
_COMPANY_SUFFIXES = ["Corp", "Co", "Inc", "Group", "Labs"]

CLIENTS = {
    i + 1: {
        "name": (
            f"{_COMPANY_PREFIXES[i % len(_COMPANY_PREFIXES)]} "
            f"{_INDUSTRIES[i % len(_INDUSTRIES)]} "
            f"{_COMPANY_SUFFIXES[i % len(_COMPANY_SUFFIXES)]}"
        ),
        "industry": _INDUSTRIES[i % len(_INDUSTRIES)],
    }
    for i in range(100)
}

# --- Channels: 10 major ad platforms ---
CHANNELS = {
    1: "Facebook",
    2: "Google",
    3: "TikTok",
    4: "LinkedIn",
    5: "Snapchat",
    6: "Pinterest",
    7: "YouTube",
    8: "X (Twitter)",
    9: "Reddit",
    10: "Amazon Ads",
}

# Channel-specific metric profiles for realistic data diversity.
# Each channel has different cost structures and performance patterns.
CHANNEL_PROFILES = {
    1: {
        "imp_range": (500, 300_000),
        "cpc_range": (0.20, 2.50),
        "conv_rate": 0.025,
    },  # Facebook
    2: {
        "imp_range": (200, 150_000),
        "cpc_range": (0.50, 8.00),
        "conv_rate": 0.045,
    },  # Google
    3: {
        "imp_range": (1000, 500_000),
        "cpc_range": (0.05, 1.50),
        "conv_rate": 0.010,
    },  # TikTok
    4: {
        "imp_range": (50, 50_000),
        "cpc_range": (2.00, 15.00),
        "conv_rate": 0.035,
    },  # LinkedIn
    5: {
        "imp_range": (800, 400_000),
        "cpc_range": (0.03, 1.00),
        "conv_rate": 0.008,
    },  # Snapchat
    6: {
        "imp_range": (300, 200_000),
        "cpc_range": (0.10, 2.00),
        "conv_rate": 0.030,
    },  # Pinterest
    7: {
        "imp_range": (400, 250_000),
        "cpc_range": (0.08, 3.00),
        "conv_rate": 0.015,
    },  # YouTube
    8: {
        "imp_range": (200, 180_000),
        "cpc_range": (0.15, 3.50),
        "conv_rate": 0.012,
    },  # X (Twitter)
    9: {
        "imp_range": (300, 220_000),
        "cpc_range": (0.10, 2.00),
        "conv_rate": 0.020,
    },  # Reddit
    10: {
        "imp_range": (100, 80_000),
        "cpc_range": (0.30, 5.00),
        "conv_rate": 0.055,
    },  # Amazon Ads
}

# Seasonal spend multipliers by month (simulates real-world ad seasonality).
SEASONAL_MULTIPLIERS = {
    1: 0.80,  # January   — post-holiday lull
    2: 0.85,  # February  — slow recovery
    3: 0.95,  # March     — spring ramp-up
    4: 1.00,  # April     — steady
    5: 1.00,  # May       — steady
    6: 0.90,  # June      — summer slowdown
    7: 0.85,  # July      — summer lull
    8: 0.90,  # August    — back-to-school prep
    9: 1.05,  # September — back to school
    10: 1.15,  # October   — pre-holiday ramp
    11: 1.40,  # November  — Black Friday / Cyber Monday
    12: 1.30,  # December  — holiday season peak
}

NUM_CLIENTS = len(CLIENTS)
NUM_CHANNELS = len(CHANNELS)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PARQUET_BASE = os.path.join(DATA_DIR, "insights")
DB_NAME = "benchmark_poc_db"
NUM_RUNS = 3
SKIP_CLEANUP = os.environ.get("SKIP_CLEANUP", "0") == "1"

MYSQL_RESULTS_FILE = os.path.join(SCRIPT_DIR, "results_mysql.txt")
DUCKDB_RESULTS_FILE = os.path.join(SCRIPT_DIR, "results_duckdb.txt")
COMPARISON_FILE = os.path.join(SCRIPT_DIR, "results_comparison.txt")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_k(client_id, channel_id):
    """Generate the zero-padded Hive partition key for a client-channel pair.
    Format: CCCCH where CCC = client_id (3 digits) and CH = channel_id (2 digits).
    Example: client 1, channel 3 -> '00103'; client 100, channel 10 -> '10010'.
    """
    return f"{client_id:03d}{channel_id:02d}"


def make_k_in(client_ids, channel_ids):
    """Build a SQL IN(...) value string for DuckDB partition key filtering.
    Args:
        client_ids: Iterable of client IDs.
        channel_ids: Iterable of channel IDs.
    Returns:
        A comma-separated string of quoted k values for SQL IN clauses.
    """
    keys = [make_k(c, ch) for c in client_ids for ch in channel_ids]
    return ", ".join(f"'{k}'" for k in keys)


def normalize_value(val):
    """Normalize a query result value for cross-engine comparison.
    Converts Decimal, date, and other types to standard Python types
    so MySQL and DuckDB results can be compared accurately.
    Args:
        val: A single value from a query result row.
    Returns:
        The normalized value.
    """
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    if hasattr(val, "isoformat"):
        return str(val)
    return val


def results_match(rows_a, rows_b, float_tolerance=0.02):
    """Check if two result sets match within tolerance for floating-point values.
    Args:
        rows_a: First result set (list of tuples).
        rows_b: Second result set (list of tuples).
        float_tolerance: Maximum allowed difference for float comparisons.
    Returns:
        A tuple of (is_match: bool, detail: str).
    """
    if len(rows_a) != len(rows_b):
        return False, f"Row count differs: {len(rows_a)} vs {len(rows_b)}"

    for i, (ra, rb) in enumerate(zip(rows_a, rows_b)):
        na = tuple(normalize_value(v) for v in ra)
        nb = tuple(normalize_value(v) for v in rb)
        if len(na) != len(nb):
            return False, f"Row {i}: column count differs ({len(na)} vs {len(nb)})"
        for j, (va, vb) in enumerate(zip(na, nb)):
            if isinstance(va, float) and isinstance(vb, float):
                if abs(va - vb) > float_tolerance:
                    return False, f"Row {i}, col {j}: {va} vs {vb}"
            elif isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                if abs(float(va) - float(vb)) > float_tolerance:
                    return False, f"Row {i}, col {j}: {va} vs {vb}"
            else:
                if str(va) != str(vb):
                    return False, f"Row {i}, col {j}: '{va}' vs '{vb}'"

    return True, "All values match"


# ---------------------------------------------------------------------------
# Step 1: Generate realistic ad performance data
# ---------------------------------------------------------------------------
def generate_data():
    """Generate diverse, production-like ad performance rows.
    Uses channel-specific metric profiles and seasonal multipliers to create
    data that mirrors real-world ad platform behavior across 100 clients and
    10 advertising channels.
    Returns:
        A list of dicts, each representing one day of ad performance for a
        single ad creative.
    """
    print("\n=== Step 1: Generating realistic ad performance data ===\n")
    random.seed(RANDOM_SEED)

    rows = []
    row_id = 0
    days = (DATE_END - DATE_START).days + 1

    # Calculate ads per channel-client combo to reach NUM_ROWS
    ads_per_channel = max(1, NUM_ROWS // (NUM_CLIENTS * NUM_CHANNELS * days))
    if ads_per_channel * NUM_CLIENTS * NUM_CHANNELS * days < NUM_ROWS:
        ads_per_channel += 1

    campaign_types = [
        "Brand Awareness",
        "Retargeting",
        "Lead Gen",
        "Conversion",
        "Prospecting",
        "Engagement",
    ]
    ad_formats = [
        "Video 30s",
        "Video 15s",
        "Carousel",
        "Static Image",
        "Story Ad",
        "Collection",
        "Dynamic",
        "Playable",
    ]

    for client_id in range(1, NUM_CLIENTS + 1):
        client = CLIENTS[client_id]
        for channel_id in range(1, NUM_CHANNELS + 1):
            profile = CHANNEL_PROFILES[channel_id]
            channel_name = CHANNELS[channel_id]

            for ad_idx in range(ads_per_channel):
                camp_type = campaign_types[ad_idx % len(campaign_types)]
                ad_format = ad_formats[ad_idx % len(ad_formats)]
                campaign_name = f"{camp_type} - {client['industry']} - {channel_name}"
                ad_name = f"{ad_format} - {camp_type} #{ad_idx + 1}"

                for d in range(days):
                    if len(rows) >= NUM_ROWS:
                        break
                    row_id += 1
                    dt = DATE_START + timedelta(days=d)
                    seasonal = SEASONAL_MULTIPLIERS[dt.month]

                    # Impressions: channel-specific range with seasonal factor
                    impressions = int(random.randint(*profile["imp_range"]) * seasonal)
                    # CTR: varies by creative quality (0.5% – 8%)
                    ctr = random.uniform(0.005, 0.08)
                    clicks = max(0, int(impressions * ctr))

                    # CPC: channel-specific cost, modulated by season
                    cpc = random.uniform(*profile["cpc_range"])
                    spend = round(clicks * cpc * seasonal, 2)

                    # Conversions: based on channel's typical conversion rate
                    conversions = max(
                        0,
                        int(clicks * profile["conv_rate"] * random.uniform(0.5, 1.5)),
                    )

                    rows.append(
                        {
                            "id": row_id,
                            "client_id": client_id,
                            "channel_id": channel_id,
                            "ad_account_id": f"acc_{client_id:03d}_{channel_id:02d}",
                            "campaign_id": (
                                f"camp_{client_id:03d}_{channel_id:02d}_{ad_idx:03d}"
                            ),
                            "campaign_name": campaign_name,
                            "ad_id": (
                                f"ad_{client_id:03d}_{channel_id:02d}_{ad_idx:03d}"
                            ),
                            "ad_name": ad_name,
                            "impressions": impressions,
                            "clicks": clicks,
                            "spend": spend,
                            "conversions": conversions,
                            "date": dt.strftime("%Y-%m-%d"),
                            "k": make_k(client_id, channel_id),
                        }
                    )

                if len(rows) >= NUM_ROWS:
                    break
            if len(rows) >= NUM_ROWS:
                break
        if len(rows) >= NUM_ROWS:
            break

    first_3 = [CLIENTS[i]["name"] for i in range(1, min(4, NUM_CLIENTS + 1))]
    client_preview = (
        ", ".join(first_3) + ", ..." if NUM_CLIENTS > 3 else ", ".join(first_3)
    )

    print(f"Generated {len(rows):,} rows")
    print(f"  Clients:       {NUM_CLIENTS} ({client_preview})")
    print(f"  Channels:      {NUM_CHANNELS} ({', '.join(CHANNELS.values())})")
    print(f"  Date range:    {DATE_START.date()} to {DATE_END.date()} ({days} days)")
    print(f"  Ads/partition: ~{ads_per_channel}")
    print(f"  Sample row:    {rows[0]}\n")
    return rows


# ---------------------------------------------------------------------------
# Step 2: Load into MySQL
# ---------------------------------------------------------------------------
def load_mysql(rows):
    """Create the MySQL table and bulk-insert all rows.
    Args:
        rows: The generated data rows.
    """
    print("\n=== Step 2: Loading into MySQL ===\n")
    import mysql.connector

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=False,
    )
    cursor = conn.cursor()

    cursor.execute(f"DROP DATABASE IF EXISTS `{DB_NAME}`")
    cursor.execute(f"CREATE DATABASE `{DB_NAME}`")
    conn.database = DB_NAME

    cursor.execute("""
        CREATE TABLE ad_insights (
            id BIGINT NOT NULL,
            client_id INT NOT NULL,
            channel_id INT NOT NULL,
            ad_account_id VARCHAR(64) NOT NULL,
            campaign_id VARCHAR(128) NOT NULL,
            campaign_name VARCHAR(256) NOT NULL,
            ad_id VARCHAR(128) NOT NULL,
            ad_name VARCHAR(256) NOT NULL,
            impressions BIGINT NOT NULL,
            clicks BIGINT NOT NULL,
            spend DOUBLE NOT NULL,
            conversions INT NOT NULL,
            date DATE NOT NULL,
            PRIMARY KEY (id),
            INDEX idx_client_channel_date (client_id, channel_id, date),
            INDEX idx_client_date (client_id, date)
        )
    """)

    t0 = time.perf_counter()
    batch_size = 10_000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(
            """
            INSERT INTO ad_insights
            (id, client_id, channel_id, ad_account_id, campaign_id, campaign_name,
             ad_id, ad_name, impressions, clicks, spend, conversions, date)
            VALUES (%(id)s, %(client_id)s, %(channel_id)s, %(ad_account_id)s,
                    %(campaign_id)s, %(campaign_name)s, %(ad_id)s, %(ad_name)s,
                    %(impressions)s, %(clicks)s, %(spend)s, %(conversions)s, %(date)s)
            """,
            batch,
        )
    conn.commit()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"MySQL load: {elapsed_ms:,.0f} ms ({len(rows):,} rows)\n")
    cursor.close()
    conn.close()


# ---------------------------------------------------------------------------
# Step 3: Write to Parquet with Hive partitioning
# ---------------------------------------------------------------------------
def load_parquet(rows):
    """Write rows to Hive-partitioned Parquet files via DuckDB.
    Pipeline: Python dicts → JSON file → read_json_auto() → Parquet.
    Partitions by the composite key ``k`` (client_id + channel_id, zero-padded).
    Date column is cast to DATE for proper type handling in queries.
    Args:
        rows: The generated data rows.
    """
    print("\n=== Step 3: Writing Parquet (Hive partitioning) ===\n")
    import json as _json

    import duckdb

    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(PARQUET_BASE):
        shutil.rmtree(PARQUET_BASE)

    json_path = os.path.join(DATA_DIR, "temp_insights.json")
    if rows:
        with open(json_path, "w", encoding="utf-8") as f:
            _json.dump(rows, f)

    conn = duckdb.connect(":memory:")
    t0 = time.perf_counter()
    conn.execute(f"""
        COPY (
            SELECT CAST(id AS BIGINT) AS id,
                   CAST(client_id AS INTEGER) AS client_id,
                   CAST(channel_id AS INTEGER) AS channel_id,
                   CAST(ad_account_id AS VARCHAR) AS ad_account_id,
                   CAST(campaign_id AS VARCHAR) AS campaign_id,
                   CAST(campaign_name AS VARCHAR) AS campaign_name,
                   CAST(ad_id AS VARCHAR) AS ad_id,
                   CAST(ad_name AS VARCHAR) AS ad_name,
                   CAST(impressions AS BIGINT) AS impressions,
                   CAST(clicks AS BIGINT) AS clicks,
                   CAST(spend AS DOUBLE) AS spend,
                   CAST(conversions AS INTEGER) AS conversions,
                   CAST(date AS DATE) AS date,
                   CAST(k AS VARCHAR) AS k
            FROM read_json_auto('{json_path.replace(chr(92), "/")}')
        ) TO '{PARQUET_BASE.replace(chr(92), "/")}'
        (FORMAT PARQUET, PARTITION_BY (k), COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)
    """)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    conn.close()
    if os.path.exists(json_path):
        os.remove(json_path)
    print(f"Parquet write: {elapsed_ms:,.0f} ms ({len(rows):,} rows)\n")


# ---------------------------------------------------------------------------
# Step 4 & 5: Query timing helpers
# ---------------------------------------------------------------------------
def run_mysql_query(cursor, sql, params=None):
    """Execute a MySQL query and return all rows."""
    cursor.execute(sql, params or ())
    return cursor.fetchall()


def run_duckdb_query(conn, sql):
    """Execute a DuckDB query and return all rows."""
    return conn.execute(sql).fetchall()


def time_query_mysql(cursor, sql, params=None, runs=NUM_RUNS):
    """Time a MySQL query over multiple runs and return median time + results.
    Args:
        cursor: MySQL cursor.
        sql: SQL query string.
        params: Optional query parameters.
        runs: Number of runs for timing.
    Returns:
        A tuple of (median_ms, result_rows).
    """
    times_ms = []
    result = None
    for _ in range(runs):
        t0 = time.perf_counter()
        result = run_mysql_query(cursor, sql, params)
        times_ms.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times_ms), result


def time_query_duckdb(conn, sql, runs=NUM_RUNS):
    """Time a DuckDB query over multiple runs and return median time + results.
    Args:
        conn: DuckDB connection.
        sql: SQL query string.
        runs: Number of runs for timing.
    Returns:
        A tuple of (median_ms, result_rows).
    """
    times_ms = []
    result = None
    for _ in range(runs):
        t0 = time.perf_counter()
        result = run_duckdb_query(conn, sql)
        times_ms.append((time.perf_counter() - t0) * 1000)
    return statistics.median(times_ms), result


def get_parquet_glob():
    """Return the DuckDB read_parquet() glob expression for our Hive layout."""
    base = PARQUET_BASE.replace("\\", "/")
    return f"read_parquet('{base}/**/*.parquet', hive_partitioning=true)"


# ---------------------------------------------------------------------------
# Step 4 & 5: Define benchmark queries
# ---------------------------------------------------------------------------
def define_queries(parquet_ref):
    """Build the full list of benchmark queries for MySQL and DuckDB.
    Queries are organized into four categories that mirror real dashboard needs:
      A) Accuracy Verification       — prove both engines return identical results.
      B) Dashboard Filters           — multi-criteria filtering with date ranges.
      C) Sorting & Ranking           — ORDER BY on raw and computed metrics.
      D) Time Series & Widgets       — aggregations for charts and KPI cards.
      E) Partition & Columnar Proof  — query designed to be slow in MySQL but
         fast in DuckDB thanks to Hive partition pruning + columnar reads.
    Args:
        parquet_ref: The DuckDB read_parquet(...) glob expression.
    Returns:
        A list of query definition dicts, each with keys: id, name, category,
        headers, mysql, duckdb.
    """
    # Pre-compute partition key IN-clauses for DuckDB Hive partition pruning.
    # Each helper maps (client_ids, channel_ids) -> quoted k values.
    all_ch = range(1, NUM_CHANNELS + 1)
    k_c1_all = make_k_in([1], all_ch)
    k_c2_all = make_k_in([2], all_ch)
    k_c3_all = make_k_in([3], all_ch)
    k_c1_ch1 = f"'{make_k(1, 1)}'"
    k_multi_clients_channels = make_k_in([1, 15, 30, 50, 75, 100], [1, 2, 5])
    k_all_clients_ch2 = make_k_in(range(1, NUM_CLIENTS + 1), [2])
    k_c5_all = make_k_in([5], all_ch)

    return [
        # =================================================================
        # A) Accuracy Verification
        # =================================================================
        {
            "id": "A1",
            "name": "Total row count",
            "category": "Accuracy Verification",
            "headers": ["total_rows"],
            "mysql": """
                SELECT COUNT(*) AS total_rows
                FROM ad_insights
            """,
            "duckdb": f"""
                SELECT COUNT(*) AS total_rows
                FROM {parquet_ref}
            """,
        },
        {
            "id": "A2",
            "name": "Global SUM aggregates",
            "category": "Accuracy Verification",
            "headers": [
                "total_impressions",
                "total_clicks",
                "total_spend",
                "total_conversions",
            ],
            "mysql": """
                SELECT SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM ad_insights
            """,
            "duckdb": f"""
                SELECT SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM {parquet_ref}
            """,
        },
        {
            "id": "A3",
            "name": "Per-channel breakdown (client 1)",
            "category": "Accuracy Verification",
            "headers": [
                "channel_id",
                "row_count",
                "total_impressions",
                "total_clicks",
                "total_spend",
                "total_conversions",
            ],
            "mysql": """
                SELECT channel_id,
                       COUNT(*)          AS row_count,
                       SUM(impressions)  AS total_impressions,
                       SUM(clicks)       AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM ad_insights
                WHERE client_id = 1
                GROUP BY channel_id
                ORDER BY channel_id
            """,
            "duckdb": f"""
                SELECT channel_id,
                       COUNT(*)          AS row_count,
                       SUM(impressions)  AS total_impressions,
                       SUM(clicks)       AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM {parquet_ref}
                WHERE k IN ({k_c1_all})
                GROUP BY channel_id
                ORDER BY channel_id
            """,
        },
        # =================================================================
        # B) Dashboard Filters
        # =================================================================
        {
            "id": "B1",
            "name": "Single client + channel + month filter",
            "category": "Dashboard Filters",
            "headers": [
                "id",
                "ad_id",
                "ad_name",
                "impressions",
                "clicks",
                "spend",
                "conversions",
                "date",
            ],
            "mysql": """
                SELECT id, ad_id, ad_name, impressions, clicks, spend,
                       conversions, date
                FROM ad_insights
                WHERE client_id = 1 AND channel_id = 1
                  AND date BETWEEN '2024-06-01' AND '2024-06-30'
                ORDER BY date, id
                LIMIT 500
            """,
            "duckdb": f"""
                SELECT id, ad_id, ad_name, impressions, clicks, spend,
                       conversions, date
                FROM {parquet_ref}
                WHERE k = {k_c1_ch1}
                  AND date BETWEEN '2024-06-01' AND '2024-06-30'
                ORDER BY date, id
                LIMIT 500
            """,
        },
        {
            "id": "B2",
            "name": "Multi-client + channels + Q3 + spend threshold",
            "category": "Dashboard Filters",
            "headers": [
                "client_id",
                "channel_id",
                "campaign_id",
                "campaign_name",
                "total_impressions",
                "total_clicks",
                "total_spend",
                "total_conversions",
            ],
            "mysql": """
                SELECT client_id, channel_id, campaign_id, campaign_name,
                       SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM ad_insights
                WHERE client_id IN (1, 15, 30, 50, 75, 100)
                  AND channel_id IN (1, 2, 5)
                  AND date BETWEEN '2024-07-01' AND '2024-09-30'
                GROUP BY client_id, channel_id, campaign_id, campaign_name
                HAVING SUM(spend) > 1000
                ORDER BY total_spend DESC
                LIMIT 20
            """,
            "duckdb": f"""
                SELECT client_id, channel_id, campaign_id, campaign_name,
                       SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM {parquet_ref}
                WHERE k IN ({k_multi_clients_channels})
                  AND date BETWEEN '2024-07-01' AND '2024-09-30'
                GROUP BY client_id, channel_id, campaign_id, campaign_name
                HAVING SUM(spend) > 1000
                ORDER BY total_spend DESC
                LIMIT 20
            """,
        },
        {
            "id": "B3",
            "name": "Campaign name search (LIKE) + date range",
            "category": "Dashboard Filters",
            "headers": [
                "campaign_id",
                "campaign_name",
                "channel_id",
                "total_impressions",
                "total_clicks",
                "total_spend",
            ],
            "mysql": """
                SELECT campaign_id, campaign_name, channel_id,
                       SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend
                FROM ad_insights
                WHERE client_id = 2
                  AND campaign_name LIKE '%Retargeting%'
                  AND date BETWEEN '2024-01-01' AND '2024-06-30'
                GROUP BY campaign_id, campaign_name, channel_id
                ORDER BY total_spend DESC
            """,
            "duckdb": f"""
                SELECT campaign_id, campaign_name, channel_id,
                       SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend
                FROM {parquet_ref}
                WHERE k IN ({k_c2_all})
                  AND campaign_name LIKE '%Retargeting%'
                  AND date BETWEEN '2024-01-01' AND '2024-06-30'
                GROUP BY campaign_id, campaign_name, channel_id
                ORDER BY total_spend DESC
            """,
        },
        # =================================================================
        # C) Sorting & Ranking
        # =================================================================
        {
            "id": "C1",
            "name": "Top 10 ads by total spend",
            "category": "Sorting & Ranking",
            "headers": [
                "ad_id",
                "ad_name",
                "channel_id",
                "total_impressions",
                "total_clicks",
                "total_spend",
                "total_conversions",
            ],
            "mysql": """
                SELECT ad_id, ad_name, channel_id,
                       SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM ad_insights
                WHERE client_id = 1
                GROUP BY ad_id, ad_name, channel_id
                ORDER BY total_spend DESC
                LIMIT 10
            """,
            "duckdb": f"""
                SELECT ad_id, ad_name, channel_id,
                       SUM(impressions) AS total_impressions,
                       SUM(clicks)      AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM {parquet_ref}
                WHERE k IN ({k_c1_all})
                GROUP BY ad_id, ad_name, channel_id
                ORDER BY total_spend DESC
                LIMIT 10
            """,
        },
        {
            "id": "C2",
            "name": "Top campaigns by CTR (computed metric sort)",
            "category": "Sorting & Ranking",
            "headers": [
                "campaign_id",
                "campaign_name",
                "channel_id",
                "total_clicks",
                "total_impressions",
                "ctr_pct",
            ],
            "mysql": """
                SELECT campaign_id, campaign_name, channel_id,
                       SUM(clicks)      AS total_clicks,
                       SUM(impressions)  AS total_impressions,
                       ROUND(SUM(clicks) * 100.0
                             / NULLIF(SUM(impressions), 0), 4) AS ctr_pct
                FROM ad_insights
                WHERE client_id = 3
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY campaign_id, campaign_name, channel_id
                HAVING SUM(impressions) > 10000
                ORDER BY ctr_pct DESC
                LIMIT 10
            """,
            "duckdb": f"""
                SELECT campaign_id, campaign_name, channel_id,
                       SUM(clicks)      AS total_clicks,
                       SUM(impressions)  AS total_impressions,
                       ROUND(SUM(clicks) * 100.0
                             / NULLIF(SUM(impressions), 0), 4) AS ctr_pct
                FROM {parquet_ref}
                WHERE k IN ({k_c3_all})
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY campaign_id, campaign_name, channel_id
                HAVING SUM(impressions) > 10000
                ORDER BY ctr_pct DESC
                LIMIT 10
            """,
        },
        {
            "id": "C3",
            "name": "Worst cost-per-conversion (bottom performers)",
            "category": "Sorting & Ranking",
            "headers": [
                "ad_id",
                "ad_name",
                "channel_id",
                "total_spend",
                "total_conversions",
                "cost_per_conv",
            ],
            "mysql": """
                SELECT ad_id, ad_name, channel_id,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions,
                       ROUND(SUM(spend)
                             / NULLIF(SUM(conversions), 0), 2) AS cost_per_conv
                FROM ad_insights
                WHERE client_id = 2
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY ad_id, ad_name, channel_id
                HAVING SUM(conversions) > 0
                ORDER BY cost_per_conv DESC
                LIMIT 10
            """,
            "duckdb": f"""
                SELECT ad_id, ad_name, channel_id,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions,
                       ROUND(SUM(spend)
                             / NULLIF(SUM(conversions), 0), 2) AS cost_per_conv
                FROM {parquet_ref}
                WHERE k IN ({k_c2_all})
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY ad_id, ad_name, channel_id
                HAVING SUM(conversions) > 0
                ORDER BY cost_per_conv DESC
                LIMIT 10
            """,
        },
        # =================================================================
        # D) Time Series & Widgets
        # =================================================================
        {
            "id": "D1",
            "name": "Daily spend trend (line chart — June 2024)",
            "category": "Time Series & Widgets",
            "headers": [
                "date",
                "daily_impressions",
                "daily_clicks",
                "daily_spend",
                "daily_conversions",
            ],
            "mysql": """
                SELECT date,
                       SUM(impressions) AS daily_impressions,
                       SUM(clicks)      AS daily_clicks,
                       ROUND(SUM(spend), 2) AS daily_spend,
                       SUM(conversions)  AS daily_conversions
                FROM ad_insights
                WHERE client_id = 1 AND channel_id = 1
                  AND date BETWEEN '2024-06-01' AND '2024-06-30'
                GROUP BY date
                ORDER BY date
            """,
            "duckdb": f"""
                SELECT date,
                       SUM(impressions) AS daily_impressions,
                       SUM(clicks)      AS daily_clicks,
                       ROUND(SUM(spend), 2) AS daily_spend,
                       SUM(conversions)  AS daily_conversions
                FROM {parquet_ref}
                WHERE k = {k_c1_ch1}
                  AND date BETWEEN '2024-06-01' AND '2024-06-30'
                GROUP BY date
                ORDER BY date
            """,
        },
        {
            "id": "D2",
            "name": "Monthly rollup by channel (bar chart — full year)",
            "category": "Time Series & Widgets",
            "headers": [
                "month",
                "channel_id",
                "monthly_impressions",
                "monthly_clicks",
                "monthly_spend",
                "monthly_conversions",
            ],
            "mysql": """
                SELECT MONTH(date)       AS month,
                       channel_id,
                       SUM(impressions)  AS monthly_impressions,
                       SUM(clicks)       AS monthly_clicks,
                       ROUND(SUM(spend), 2) AS monthly_spend,
                       SUM(conversions)  AS monthly_conversions
                FROM ad_insights
                WHERE client_id = 1
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY MONTH(date), channel_id
                ORDER BY month, channel_id
            """,
            "duckdb": f"""
                SELECT MONTH(date)       AS month,
                       channel_id,
                       SUM(impressions)  AS monthly_impressions,
                       SUM(clicks)       AS monthly_clicks,
                       ROUND(SUM(spend), 2) AS monthly_spend,
                       SUM(conversions)  AS monthly_conversions
                FROM {parquet_ref}
                WHERE k IN ({k_c1_all})
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY MONTH(date), channel_id
                ORDER BY month, channel_id
            """,
        },
        {
            "id": "D3",
            "name": "Channel spend distribution % (pie chart)",
            "category": "Time Series & Widgets",
            "headers": ["channel_id", "channel_spend", "pct_of_total"],
            "mysql": """
                SELECT channel_id,
                       ROUND(SUM(spend), 2) AS channel_spend,
                       ROUND(SUM(spend) * 100.0 / (
                           SELECT SUM(spend)
                           FROM ad_insights
                           WHERE client_id = 1
                       ), 2) AS pct_of_total
                FROM ad_insights
                WHERE client_id = 1
                GROUP BY channel_id
                ORDER BY channel_spend DESC
            """,
            "duckdb": f"""
                SELECT channel_id,
                       ROUND(SUM(spend), 2) AS channel_spend,
                       ROUND(SUM(spend) * 100.0 / (
                           SELECT SUM(spend)
                           FROM {parquet_ref}
                           WHERE k IN ({k_c1_all})
                       ), 2) AS pct_of_total
                FROM {parquet_ref}
                WHERE k IN ({k_c1_all})
                GROUP BY channel_id
                ORDER BY channel_spend DESC
            """,
        },
        # =================================================================
        # E) Partition & Columnar Proof
        #
        # This query is *designed* to be slow in MySQL but fast in DuckDB:
        #
        # MySQL weakness:
        #   - Filters on channel_id=2 WITHOUT client_id.  Neither composite
        #     index (client_id, channel_id, date) nor (client_id, date)
        #     starts with channel_id, so MySQL falls back to a FULL TABLE
        #     SCAN of all rows.
        #   - Row-store reads every column of every row even though we only
        #     need date, impressions, clicks, spend, and conversions.
        #
        # DuckDB + Parquet advantage:
        #   - Hive partition pruning: k IN (...) targets exactly the 100
        #     partitions for channel 2 (one per client) out of 1,000 total,
        #     skipping 90% of the data at the filesystem level.
        #   - Columnar reads: Parquet files store each column separately,
        #     so DuckDB reads only the 5 columns needed (date, impressions,
        #     clicks, spend, conversions), ignoring the other 8+ columns.
        #   - Snappy compression on individual columns further reduces I/O.
        # =================================================================
        {
            "id": "E1",
            "name": "Channel-only filter: monthly rollup across all clients",
            "category": "Partition & Columnar Proof",
            "headers": [
                "month",
                "row_count",
                "total_impressions",
                "total_clicks",
                "total_spend",
                "total_conversions",
            ],
            "mysql": """
                SELECT MONTH(date)       AS month,
                       COUNT(*)          AS row_count,
                       SUM(impressions)  AS total_impressions,
                       SUM(clicks)       AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM ad_insights
                WHERE channel_id = 2
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY MONTH(date)
                ORDER BY month
            """,
            "duckdb": f"""
                SELECT MONTH(date)       AS month,
                       COUNT(*)          AS row_count,
                       SUM(impressions)  AS total_impressions,
                       SUM(clicks)       AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions
                FROM {parquet_ref}
                WHERE k IN ({k_all_clients_ch2})
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY MONTH(date)
                ORDER BY month
            """,
        },
        # -----------------------------------------------------------------
        # E2: Ad-level rollup with computed metrics for a single client.
        #
        # This query DOES include client_id — matching real dashboard usage.
        #
        # MySQL weakness (despite the index being usable):
        #   - The composite index (client_id, channel_id, date) narrows to
        #     ~1M rows for client_id=1, but impressions, clicks, spend, and
        #     conversions are NOT in the index.  For every matching row MySQL
        #     must perform a RANDOM I/O "bookmark lookup" back to the
        #     clustered (primary key) index to fetch those column values.
        #     ~1M random reads are extremely expensive.
        #   - High-cardinality GROUP BY (ad_id) creates many hash buckets,
        #     each accumulated one row at a time.
        #   - Row-store reads all 13 columns per row even though we only
        #     need 7 of them.
        #
        # DuckDB + Parquet advantage:
        #   - Partition pruning: k IN (...) reads only 10 out of 1,000
        #     partitions (client 1, all 10 channels) — 99% pruned.
        #   - Columnar reads: within those 10 partitions, Parquet stores
        #     each column in a separate chunk.  DuckDB reads only the 7
        #     columns referenced (ad_id, ad_name, channel_id, impressions,
        #     clicks, spend, conversions) and skips the other 6 entirely.
        #     All reads are SEQUENTIAL — no random I/O.
        #   - Vectorized hash aggregation processes thousands of rows per
        #     CPU cycle vs MySQL's row-at-a-time execution.
        # -----------------------------------------------------------------
        {
            "id": "E2",
            "name": "Ad-level rollup + computed CTR & CPA (single client)",
            "category": "Partition & Columnar Proof",
            "headers": [
                "ad_id",
                "ad_name",
                "channel_id",
                "total_impressions",
                "total_clicks",
                "total_spend",
                "total_conversions",
                "ctr_pct",
                "cost_per_conv",
            ],
            "mysql": """
                SELECT ad_id, ad_name, channel_id,
                       SUM(impressions)  AS total_impressions,
                       SUM(clicks)       AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions,
                       ROUND(SUM(clicks) * 100.0
                             / NULLIF(SUM(impressions), 0), 4) AS ctr_pct,
                       ROUND(SUM(spend)
                             / NULLIF(SUM(conversions), 0), 2) AS cost_per_conv
                FROM ad_insights
                WHERE client_id = 1
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY ad_id, ad_name, channel_id
                ORDER BY total_spend DESC
                LIMIT 50
            """,
            "duckdb": f"""
                SELECT ad_id, ad_name, channel_id,
                       SUM(impressions)  AS total_impressions,
                       SUM(clicks)       AS total_clicks,
                       ROUND(SUM(spend), 2) AS total_spend,
                       SUM(conversions)  AS total_conversions,
                       ROUND(SUM(clicks) * 100.0
                             / NULLIF(SUM(impressions), 0), 4) AS ctr_pct,
                       ROUND(SUM(spend)
                             / NULLIF(SUM(conversions), 0), 2) AS cost_per_conv
                FROM {parquet_ref}
                WHERE k IN ({k_c1_all})
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY ad_id, ad_name, channel_id
                ORDER BY total_spend DESC
                LIMIT 50
            """,
        },
        # =================================================================
        # F) Row-Based Fetching (MySQL Strength)
        # =================================================================
        {
            "id": "F1",
            "name": "SELECT * for specific client/date (Row fetch)",
            "category": "Row-Based Fetching",
            "headers": [
                "id",
                "client_id",
                "channel_id",
                "ad_account_id",
                "campaign_id",
                "campaign_name",
                "ad_id",
                "ad_name",
                "impressions",
                "clicks",
                "spend",
                "conversions",
                "date",
            ],
            "mysql": """
                SELECT id, client_id, channel_id, ad_account_id,
                       campaign_id, campaign_name, ad_id, ad_name,
                       impressions, clicks, spend, conversions, date
                FROM ad_insights
                WHERE client_id = 5
                  AND date BETWEEN '2024-01-01' AND '2024-01-31'
                ORDER BY date, id
            """,
            "duckdb": f"""
                SELECT id, client_id, channel_id, ad_account_id,
                       campaign_id, campaign_name, ad_id, ad_name,
                       impressions, clicks, spend, conversions, date
                FROM {parquet_ref}
                WHERE k IN ({k_c5_all})
                  AND date BETWEEN '2024-01-01' AND '2024-01-31'
                ORDER BY date, id
            """,
        },
    ]


# ---------------------------------------------------------------------------
# Step 4 & 5: Run all benchmark queries
# ---------------------------------------------------------------------------
def run_benchmarks():
    """Execute all benchmark queries against both MySQL and DuckDB.
    Returns:
        A list of result dicts, each containing: id, name, category, headers,
        ms_mysql, ms_duckdb, rows_mysql, rows_duckdb.
    """
    print("\n=== Step 4 & 5: Running benchmark queries ===\n")

    import duckdb
    import mysql.connector

    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=DB_NAME,
    )
    mysql_cursor = mysql_conn.cursor()

    duck_conn = duckdb.connect(":memory:")
    parquet_ref = get_parquet_glob()

    queries = define_queries(parquet_ref)
    results = []

    for q in queries:
        label = f"[{q['id']}] {q['name']}"
        print(f"  Running: {label} ... ", end="", flush=True)

        ms_mysql, rows_mysql = time_query_mysql(mysql_cursor, q["mysql"])
        ms_duck, rows_duck = time_query_duckdb(duck_conn, q["duckdb"])
        speedup = ms_mysql / ms_duck if ms_duck > 0 else float("inf")

        print(f"MySQL {ms_mysql:,.1f}ms | DuckDB {ms_duck:,.1f}ms | {speedup:.1f}x")

        results.append(
            {
                "id": q["id"],
                "name": q["name"],
                "category": q["category"],
                "headers": q["headers"],
                "ms_mysql": ms_mysql,
                "ms_duckdb": ms_duck,
                "rows_mysql": rows_mysql,
                "rows_duckdb": rows_duck,
            }
        )

    mysql_cursor.close()
    mysql_conn.close()
    duck_conn.close()

    return results


# ---------------------------------------------------------------------------
# Step 6: Write result files
# ---------------------------------------------------------------------------
def write_single_engine_results(filepath, engine_name, results):
    """Write query results for a single engine to a text file.
    Args:
        filepath: Path to the output file.
        engine_name: 'MySQL' or 'DuckDB'.
        results: List of result dicts from run_benchmarks().
    """
    from tabulate import tabulate

    rows_key = "rows_mysql" if engine_name == "MySQL" else "rows_duckdb"
    ms_key = "ms_mysql" if engine_name == "MySQL" else "ms_duckdb"

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"{'=' * 72}\n")
        f.write(f"  {engine_name} Query Results\n")
        f.write(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(
            f"  Dataset: {NUM_ROWS:,} rows | "
            f"{NUM_CLIENTS} clients | {NUM_CHANNELS} channels\n"
        )
        f.write(f"{'=' * 72}\n\n")

        for r in results:
            rows = r[rows_key]
            f.write(f"{'─' * 72}\n")
            f.write(f"[{r['id']}] {r['name']}\n")
            f.write(f"Category: {r['category']}\n")
            f.write(f"Time: {r[ms_key]:,.1f} ms | Rows: {len(rows):,}\n")
            f.write(f"{'─' * 72}\n")
            if rows:
                f.write(
                    tabulate(
                        rows,
                        headers=r["headers"],
                        tablefmt="simple",
                        floatfmt=",.2f",
                        intfmt=",",
                    )
                )
            else:
                f.write("(no rows)")
            f.write("\n\n")


def write_comparison_file(results):
    """Write a combined comparison file with side-by-side results.
    This file is designed for blog readers to verify that both engines
    return identical results and to compare performance at a glance.
    Args:
        results: List of result dicts from run_benchmarks().
    """
    from tabulate import tabulate

    with open(COMPARISON_FILE, "w", encoding="utf-8") as f:
        # === Header ===
        f.write("=" * 78 + "\n")
        f.write("  BENCHMARK COMPARISON: MySQL vs DuckDB + Parquet\n")
        f.write(
            f"  Dataset:    {NUM_ROWS:,} rows | "
            f"{NUM_CLIENTS} clients | {NUM_CHANNELS} channels\n"
        )
        f.write(f"  Date range: {DATE_START.date()} to {DATE_END.date()}\n")
        f.write(f"  Generated:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"  Runs/query: {NUM_RUNS} (median time reported)\n")
        f.write("=" * 78 + "\n\n")

        # === Performance Summary Table ===
        f.write("PERFORMANCE SUMMARY\n")
        f.write("-" * 78 + "\n")
        summary_rows = []
        total_mysql = 0
        total_duck = 0
        match_count = 0
        for r in results:
            speedup = (
                r["ms_mysql"] / r["ms_duckdb"] if r["ms_duckdb"] > 0 else float("inf")
            )
            matched, _ = results_match(r["rows_mysql"], r["rows_duckdb"])
            match_str = "YES" if matched else "NO"
            if matched:
                match_count += 1
            total_mysql += r["ms_mysql"]
            total_duck += r["ms_duckdb"]
            summary_rows.append(
                [
                    r["id"],
                    r["name"],
                    f"{r['ms_mysql']:,.1f}",
                    f"{r['ms_duckdb']:,.1f}",
                    f"{speedup:.1f}x",
                    match_str,
                ]
            )

        overall_speedup = total_mysql / total_duck if total_duck > 0 else 0
        summary_rows.append(
            [
                "",
                "─" * 40,
                "─" * 9,
                "─" * 9,
                "─" * 7,
                "─" * 5,
            ]
        )
        summary_rows.append(
            [
                "",
                "TOTAL",
                f"{total_mysql:,.1f}",
                f"{total_duck:,.1f}",
                f"{overall_speedup:.1f}x",
                f"{match_count}/{len(results)}",
            ]
        )

        f.write(
            tabulate(
                summary_rows,
                headers=["#", "Query", "MySQL (ms)", "DuckDB (ms)", "Speedup", "Match"],
                tablefmt="simple",
                colalign=("left", "left", "right", "right", "right", "center"),
            )
        )
        f.write("\n\n")

        # === Detailed Results ===
        f.write("=" * 78 + "\n")
        f.write("DETAILED QUERY RESULTS (side-by-side)\n")
        f.write("=" * 78 + "\n\n")

        for r in results:
            speedup = (
                r["ms_mysql"] / r["ms_duckdb"] if r["ms_duckdb"] > 0 else float("inf")
            )
            matched, match_detail = results_match(
                r["rows_mysql"],
                r["rows_duckdb"],
            )
            match_symbol = "PASS" if matched else "FAIL"

            f.write("─" * 78 + "\n")
            f.write(f"[{r['id']}] {r['name']}\n")
            f.write(f"Category:    {r['category']}\n")
            f.write(
                f"MySQL:       {r['ms_mysql']:,.1f} ms "
                f"({len(r['rows_mysql']):,} rows)\n"
            )
            f.write(
                f"DuckDB:      {r['ms_duckdb']:,.1f} ms "
                f"({len(r['rows_duckdb']):,} rows)\n"
            )
            f.write(f"Speedup:     {speedup:.1f}x\n")
            f.write(f"Accuracy:    {match_symbol} — {match_detail}\n")
            f.write("─" * 78 + "\n\n")

            # MySQL results
            f.write("  >>> MySQL Result:\n")
            if r["rows_mysql"]:
                table_str = tabulate(
                    r["rows_mysql"],
                    headers=r["headers"],
                    tablefmt="simple",
                    floatfmt=",.2f",
                    intfmt=",",
                )
                for line in table_str.splitlines():
                    f.write(f"  {line}\n")
            else:
                f.write("  (no rows)\n")
            f.write("\n")

            # DuckDB results
            f.write("  >>> DuckDB Result:\n")
            if r["rows_duckdb"]:
                table_str = tabulate(
                    r["rows_duckdb"],
                    headers=r["headers"],
                    tablefmt="simple",
                    floatfmt=",.2f",
                    intfmt=",",
                )
                for line in table_str.splitlines():
                    f.write(f"  {line}\n")
            else:
                f.write("  (no rows)\n")
            f.write("\n\n")


# ---------------------------------------------------------------------------
# Step 6: Print performance summary to console
# ---------------------------------------------------------------------------
def print_results(results):
    """Print the benchmark performance summary and storage comparison.
    Args:
        results: List of result dicts from run_benchmarks().
    """
    print("\n=== Step 6: Results ===\n")
    from tabulate import tabulate

    # Group by category
    categories = []
    seen = set()
    for r in results:
        if r["category"] not in seen:
            categories.append(r["category"])
            seen.add(r["category"])

    table_data = []
    for cat in categories:
        table_data.append([f"--- {cat} ---", "", "", "", ""])
        for r in results:
            if r["category"] != cat:
                continue
            speedup = r["ms_mysql"] / r["ms_duckdb"] if r["ms_duckdb"] > 0 else 0
            matched, _ = results_match(r["rows_mysql"], r["rows_duckdb"])
            table_data.append(
                [
                    f"  [{r['id']}] {r['name']}",
                    f"{r['ms_mysql']:,.1f}",
                    f"{r['ms_duckdb']:,.1f}",
                    f"{speedup:.1f}x",
                    "YES" if matched else "NO",
                ]
            )

    print(
        tabulate(
            table_data,
            headers=["Query", "MySQL (ms)", "DuckDB (ms)", "Speedup", "Match"],
            tablefmt="simple",
            colalign=("left", "right", "right", "right", "center"),
        )
    )

    # Totals
    total_mysql = sum(r["ms_mysql"] for r in results)
    total_duck = sum(r["ms_duckdb"] for r in results)
    overall = total_mysql / total_duck if total_duck > 0 else 0
    match_count = sum(
        1 for r in results if results_match(r["rows_mysql"], r["rows_duckdb"])[0]
    )
    print(
        f"\nTotal:  MySQL {total_mysql:,.1f} ms | "
        f"DuckDB {total_duck:,.1f} ms | "
        f"Overall {overall:.1f}x | "
        f"Accuracy {match_count}/{len(results)} matched"
    )

    # File size comparison
    parquet_size = 0
    if os.path.exists(PARQUET_BASE):
        for dirpath, _dirnames, filenames in os.walk(PARQUET_BASE):
            for fname in filenames:
                parquet_size += os.path.getsize(os.path.join(dirpath, fname))

    print(f"\nParquet data size: {parquet_size / (1024 * 1024):.2f} MB")
    print(
        "(MySQL data dir size depends on your MySQL datadir; "
        "compare manually if needed.)\n"
    )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def cleanup():
    """Remove the MySQL database and Parquet data directory."""
    if SKIP_CLEANUP:
        print("Skipping cleanup (SKIP_CLEANUP=1).\n")
        return
    print("\n=== Cleanup ===\n")
    try:
        import mysql.connector

        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            autocommit=True,
        )
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS `{DB_NAME}`")
        cur.close()
        conn.close()
        print(f"Dropped MySQL database `{DB_NAME}`.")
    except Exception as e:
        print(f"MySQL cleanup warning: {e}")
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
        print(f"Removed {DATA_DIR}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    """Orchestrate the full benchmark pipeline."""

    print("\n" + "=" * 60)
    print("  Benchmark: MySQL vs Parquet + DuckDB")
    print(f"  {NUM_ROWS:,} rows | {NUM_CLIENTS} clients | {NUM_CHANNELS} channels")
    print("=" * 60)
    cleanup()
    rows = generate_data()
    load_mysql(rows)
    load_parquet(rows)

    bench_results = run_benchmarks()

    # Write all three result files
    write_single_engine_results(MYSQL_RESULTS_FILE, "MySQL", bench_results)
    write_single_engine_results(DUCKDB_RESULTS_FILE, "DuckDB", bench_results)
    write_comparison_file(bench_results)

    print("\nResult files written:")
    print(f"  MySQL results  -> {MYSQL_RESULTS_FILE}")
    print(f"  DuckDB results -> {DUCKDB_RESULTS_FILE}")
    print(f"  Comparison     -> {COMPARISON_FILE}")

    print_results(bench_results)


if __name__ == "__main__":
    main()
