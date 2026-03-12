"""
DuckDB + Hive-Partitioned Parquet — Simple Demo
=================================================
This script demonstrates a realistic pipeline for analytical storage:
1. Fetch data from a third-party API (simulated with JSON structures).
2. Save the raw API responses as JSON files on disk.
3. Use DuckDB's read_json_auto() to read the JSON files, cast types,
   and write Hive-partitioned Parquet files in a single COPY statement.
4. Query the Parquet files with SQL, showing how partition pruning works.
Hive partitioning organizes files into directories by key columns:
    data/
    └── insights/
        ├── client_id=1/
        │   └── data_0.parquet      ← only client 1's rows
        ├── client_id=2/
        │   └── data_0.parquet      ← only client 2's rows
        └── client_id=3/
            └── data_0.parquet      ← only client 3's rows
When you query WHERE client_id = 2, DuckDB reads ONLY the client_id=2/
directory and skips the rest entirely — this is called partition pruning.
"""

import json
import os
import shutil

import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = "demo_data"
JSON_DIR = os.path.join(DATA_DIR, "raw_json")
PARQUET_DIR = os.path.join(DATA_DIR, "insights")

# ---------------------------------------------------------------------------
# Step 1: Simulate third-party API responses
# ---------------------------------------------------------------------------
# In a real system each API call returns a JSON response. We simulate two
# separate API calls (e.g., paginated or date-based fetches) that each
# return a batch of ad insight rows.

API_RESPONSE_PAGE_1 = {
    "status": "ok",
    "data": [
        # --- Client 1: Acme Retail ---
        {"client_id": 1, "campaign": "Summer Sale", "channel": "Facebook", "date": "2024-01-01", "impressions": 12000, "clicks": 360, "spend": 120.50},
        {"client_id": 1, "campaign": "Summer Sale", "channel": "Facebook", "date": "2024-01-02", "impressions": 14500, "clicks": 420, "spend": 140.75},
        {"client_id": 1, "campaign": "Brand Boost", "channel": "Google",   "date": "2024-01-01", "impressions":  8000, "clicks": 640, "spend": 310.00},
        {"client_id": 1, "campaign": "Brand Boost", "channel": "Google",   "date": "2024-01-02", "impressions":  9200, "clicks": 710, "spend": 355.00},        
        

        # --- Client 2: Nova Tech ---
        {"client_id": 2, "campaign": "Product Launch", "channel": "TikTok",   "date": "2024-01-01", "impressions": 50000, "clicks": 1500, "spend":  75.00},
        {"client_id": 2, "campaign": "Product Launch", "channel": "TikTok",   "date": "2024-01-02", "impressions": 62000, "clicks": 1800, "spend":  90.00},
    ],
}

API_RESPONSE_PAGE_2 = {
    "status": "ok",
    "data": [
        # --- Client 2 (continued) ---
        {"client_id": 2, "campaign": "Retargeting", "channel": "Facebook", "date": "2024-01-01", "impressions": 22000, "clicks":  880, "spend": 200.00},
        {"client_id": 2, "campaign": "Retargeting", "channel": "Facebook", "date": "2024-01-02", "impressions": 25000, "clicks":  950, "spend": 220.00},

        # --- Client 3: Peak Fitness ---
        {"client_id": 3, "campaign": "New Year Promo", "channel": "Google",  "date": "2024-01-01", "impressions": 18000, "clicks": 1080, "spend": 540.00},
        {"client_id": 3, "campaign": "New Year Promo", "channel": "Google",  "date": "2024-01-02", "impressions": 21000, "clicks": 1260, "spend": 630.00},
        {"client_id": 3, "campaign": "Video Ads",      "channel": "YouTube", "date": "2024-01-01", "impressions": 30000, "clicks":  600, "spend": 180.00},
        {"client_id": 3, "campaign": "Video Ads",      "channel": "YouTube", "date": "2024-01-02", "impressions": 35000, "clicks":  700, "spend": 210.00},
    ],
}

# ---------------------------------------------------------------------------
# Step 2: Save raw API responses as JSON files on disk
# ---------------------------------------------------------------------------

def save_api_response() -> list[str]:
    """Persist each API response to a separate JSON file.
    In production, each API fetch (paginated, per-client, per-date, etc.)
    writes its response to disk. Later, DuckDB reads all of them in bulk.
    Returns:
        A list of file paths to the saved JSON files.
    """

    os.makedirs(JSON_DIR, exist_ok=True)

    json_paths = []

    for i, response in enumerate([API_RESPONSE_PAGE_1, API_RESPONSE_PAGE_2], start=1):
        path = os.path.join(JSON_DIR, f"insights_page_{i}.json")

        # Save only the "data" array — each element is one row.
        # DuckDB's read_json_auto() expects an array of objects at the top level.

        with open(path, "w", encoding="utf-8") as f:
            json.dump(response["data"], f, indent=2)
        
        json_paths.append(path)
        print(f"    Saved  {path} ({len(response['data'])} rows)")

    return json_paths


# ---------------------------------------------------------------------------
# Step 3: Convert JSON → Hive-partitioned Parquet using read_json_auto()
# ---------------------------------------------------------------------------

def write_parquet(source_json_paths):
    """Read JSON files with DuckDB and write Hive-partitioned Parquet.
    Pipeline: JSON files → read_json_auto() → type casts → COPY TO Parquet.
    DuckDB reads ALL the JSON files in a single pass, casts columns to the
    correct types, and writes partitioned Parquet — no intermediate storage.
    Args:
        source_json_paths: List of paths to JSON files from the API.
    """

    os.makedirs(PARQUET_DIR, exist_ok=True)

    con = duckdb.connect(":memory:")

    # Build the file list for read_json_auto().
    # In production this could be dozens of JSON files from paginated API calls.

    json_files_list = ", ".join(f"'{path}'" for path in source_json_paths)

    # read_json_auto() auto-detects the schema from the JSON structure.
    # We add explicit CASTs to enforce the exact types we want in Parquet.

    con.execute(f"""
        COPY (
            SELECT CAST(client_id AS INTEGER) AS client_id,
                CAST(campaign AS VARCHAR) AS campaign,
                CAST(channel AS VARCHAR) AS channel,
                CAST(date AS DATE) AS date,
                CAST(impressions AS BIGINT) AS impressions,
                CAST(clicks AS BIGINT) AS clicks,
                CAST(spend AS DOUBLE) AS spend
            FROM read_json_auto([{json_files_list}])
        ) TO '{PARQUET_DIR}' 
        (FORMAT PARQUET, PARTITION_BY (client_id), OVERWRITE_OR_IGNORE 1)
    """)

    con.close()
    print(f"\nConverted JSON -> Parquet at '{PARQUET_DIR}/")


# ---------------------------------------------------------------------------
# Step 4: Query the Parquet files with DuckDB
# ---------------------------------------------------------------------------
def query_parquet():
    """Run analytical queries on the Parquet files.
    DuckDB's read_parquet() with hive_partitioning=true tells DuckDB to
    interpret the directory names (client_id=1, client_id=2, ...) as column
    values. When a query filters on client_id, DuckDB skips directories
    that don't match — this is partition pruning.
    """

    con = duckdb.connect(":memory:")

    # The glob pattern **/*.parquet matches all Parquet files in all partitions.
    parquet = f"read_parquet('{PARQUET_DIR}/**/*.parquet', hive_partitioning=true)"

    # --- Query 1: Total spend per client ---
    print(f"\n--- Query 1: Total spend per client ---\n")
    results = con.execute(f"""
        SELECT 
            client_id,
            SUM(spend) AS total_spend,
            SUM(clicks) AS total_clicks,
        FROM {parquet}
        GROUP BY client_id
        ORDER BY total_spend DESC
    """).fetchall()

    print(f"{'Client':<10} | {'Total Spend':<12} | {'Total Clicks':<12}")
    print("-" * 40)
    for client_id, spend, clicks in results:
        print(f"{client_id:<10} | {spend:<11.2f} | {clicks:<12}")

    # --- Query 2: Filter a single client (partition pruning in action) ---
    # DuckDB only reads files inside demo_data/insights/client_id=2/
    # and completely skips client_id=1/ and client_id=3/.
    print("\n--- Query 2: Client 2 campaign breakdown (partition pruning) ---\n")
    results = con.execute(f"""
        SELECT 
            campaign,
            channel,
            SUM(impressions) AS total_impressrions,
            SUM(clicks) AS total_clicks,
            ROUND(SUM(spend), 2) AS total_spend
        FROM {parquet}
        WHERE client_id = 2
        GROUP BY campaign, channel
        ORDER BY total_spend DESC
    """).fetchall()

    print(f"{'Campaign':<20} | {'Channel':<10} | {'Impressions':<12} | {'Clicks':<8} | {'Spend':<10}")
    print("-" * 70)
    for campaign, channel, impressions, clicks, spend in results:
        print(f"{campaign:<20} | {channel:<10} | {impressions:<12,} | {clicks:<8,} | {spend:<9.2f}")

    # --- Query 3: Daily trend for a specific campaign ---
    print("\n--- Query 3: Daily trend for 'New Year Promo' (Client 3) ---\n")
    results = con.execute(f"""
        SELECT date,
               impressions,
               clicks,
               spend,
               ROUND(clicks * 100.0 / impressions, 2) AS ctr_pct
        FROM {parquet}
        WHERE client_id = 3
          AND campaign = 'New Year Promo'
        ORDER BY date
    """).fetchall()

    print(f"{'Date':<12} | {'Impressions':<12} | {'Clicks':<8} | {'Spend':<10} | {'CTR %':<6}")
    print("-" * 55)
    for date, impressions, clicks, spend, ctr in results:
        print(f"{date!s:<12} | {impressions:<12,} | {clicks:<8,} | ${spend:<9.2f} | {ctr:<6.2f}")

    con.close()


# ---------------------------------------------------------------------------
# Step 5: Show the generated file structure
# ---------------------------------------------------------------------------

def show_file_tree():
    """Print the Hive partition directory tree for illustration."""
    print("\n--- Generated File Structure ---")
    for dirpath, dirnames, filenames in os.walk(DATA_DIR):
        depth = dirpath.replace(DATA_DIR, '').count(os.sep)
        indent = ' ' * depth
        print(f"{indent}{os.path.basename(dirpath)}/")
        for file in filenames:
            size_kb = os.path.getsize(os.path.join(dirpath, file)) / 1024
            print(f"{indent}  {file}  ({size_kb:.1f} KB)")


# ---------------------------------------------------------------------------
# Step 6: Main execution
# ---------------------------------------------------------------------------

def main():
    """Orchestrate the full demo pipeline."""
    # Cleanup previous runs
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    print("=" * 50)
    print("  DuckDB + Hive-Partitioned Parquet — Simple Demo")
    print("=" * 50)

    # Step 1-2: Fetch data from the "API" and save as JSON files
    print("\nStep 1: Fetching data from API and saving as JSON...\n")
    json_paths = save_api_response()

    # Step 3: Convert JSON → Parquet using read_json_auto()
    print("\nStep 2: Converting JSON → Hive-partitioned Parquet...")
    write_parquet(json_paths)

    # Show the full file tree (JSON source + Parquet output)
    show_file_tree()

    # Step 4: Query the Parquet files
    print("\nStep 3: Querying Parquet files with DuckDB...")
    query_parquet()

    # Cleanup
    shutil.rmtree(DATA_DIR)
    print("\n(Cleaned up demo_data/ directory)")


if __name__ == "__main__":
    main()