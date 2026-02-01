# Event Data Generator & Micro Warehouse

This project generates synthetic event data and provides a micro data warehouse using DuckDB for analytics. It demonstrates a modern data pipeline with bronze/silver/gold layers and partitioned storage.

## Project Overview

The project consists of two main components:

1. **Event Data Generator** (`generate_events.py`) - Generates millions of synthetic event records with realistic data
2. **Data Warehouse Refresh Job** (`refresh_job.py`) - Builds a medallion architecture (bronze/silver/gold) using DuckDB

## Features

### Event Generator
- **Generates 1 million+ event rows** with realistic event data
- **Date partitioning**: Creates `data/events/date=YYYY-MM-DD/events-*.parquet` structure
- **Parallel processing**: Uses all available CPU cores for fast generation
- **Random data generation**:
  - `event_time`: Timestamp between 2023-01-01 and 2025-12-31
  - `user_id`: Randomly generated user IDs (1-100,000)
  - `team_id`: Randomly generated team IDs (1-1,000)
  - `event_name`: Random event names from 15 different event types
  - `properties`: JSON string with random event properties (2-5 properties per event)

### Data Warehouse (Medallion Architecture)
- **Bronze Layer**: Raw partitioned parquet files
- **Silver Layer**: Typed, cleaned, and validated data
- **Gold Layer**: Aggregated metrics and analytics-ready views
- **Export Capability**: Exports curated datasets to parquet for downstream consumption
- **Refresh Tracking**: Logs refresh operations for observability

## Installation

### Prerequisites
- Python 3.9+
- pip or uv package manager

### Install Dependencies

```bash
# Using pip
pip install polars pyarrow numpy duckdb

# Or using uv (recommended)
uv sync
```

## Usage

### 1. Generate Event Data

```bash
python generate_events.py
```

This will:
1. Use all available CPU cores to generate data in parallel
2. Create 1 million rows of event data
3. Partition the data by date
4. Write Parquet files to `data/events/date=*/events-*.parquet`

**Output**: Partitioned parquet files in `data/events/` directory

### 2. Run Data Warehouse Refresh

```bash
python refresh_job.py
```

This will:
1. Create a DuckDB database (`micro_warehouse.duckdb`)
2. Build bronze/silver/gold layer views
3. Calculate weekly active teams metric
4. Export gold layer to `exports/gold_weekly_active_teams.parquet`
5. Log the refresh operation

## Project Structure

```
gen-events-data/
├── generate_events.py          # Event data generator
├── refresh_job.py               # DuckDB warehouse refresh job
├── main.py                      # Entry point (optional)
├── pyproject.toml              # Project dependencies
├── README.md                   # This file
├── data/                       # Generated data (created at runtime)
│   └── events/
│       ├── date=2023-01-15/
│       │   └── events-*.parquet
│       ├── date=2023-02-20/
│       │   └── events-*.parquet
│       └── ...
├── exports/                    # Exported analytics (created at runtime)
│   └── gold_weekly_active_teams.parquet
└── micro_warehouse.duckdb      # DuckDB database (created at runtime)
```

## Data Warehouse Architecture

### Bronze Layer (Raw Data)
```sql
CREATE VIEW bronze_events AS
SELECT * FROM read_parquet('data/events/date=*/events-*.parquet');
```
- Direct read from partitioned parquet files
- No transformations applied
- Preserves all raw data

### Silver Layer (Cleaned Data)
```sql
CREATE VIEW silver_events AS
SELECT
    CAST(event_time AS TIMESTAMP) AS event_time,
    user_id,
    team_id,
    event_name,
    NULLIF(properties, '') AS properties
FROM bronze_events
WHERE user_id IS NOT NULL;
```
- Type casting and validation
- Null handling
- Data quality filters

### Gold Layer (Analytics)
```sql
CREATE VIEW gold_weekly_active_teams AS
SELECT
    date_trunc('week', event_time) AS week,
    COUNT(DISTINCT team_id) AS weekly_active_teams
FROM silver_events
GROUP BY 1
ORDER BY 1;
```
- Business metrics
- Pre-aggregated for performance
- Analytics-ready datasets

## Data Schema

### Events Table Schema

| Column       | Type      | Description                           |
|--------------|-----------|---------------------------------------|
| event_time   | timestamp | When the event occurred               |
| user_id      | int64     | User identifier (1-100,000)           |
| team_id      | int64     | Team identifier (1-1,000)             |
| event_name   | string    | Type of event                         |
| properties   | string    | JSON string with event properties     |
| date         | date      | Partition key (derived from event_time)|

### Event Types

The generator includes 15 different event types:
- `page_view`, `button_click`, `form_submit`
- `video_play`, `video_pause`
- `add_to_cart`, `remove_from_cart`, `checkout`, `purchase`
- `sign_up`, `sign_in`, `sign_out`
- `search`, `filter_apply`, `share`

### Property Keys

Events contain 2-5 random properties from this pool:
- `page_url`, `button_id`, `form_id`, `video_id`, `product_id`
- `category`, `price`, `quantity`, `search_term`, `filter_type`
- `platform`, `browser`, `device_type`, `session_id`

## Configuration

### Event Generator (`generate_events.py`)

Modify these constants at the top of the file:

```python
TOTAL_ROWS = 1_000_000              # Number of events to generate
START_DATE = datetime(2023, 1, 1)   # Start date for events
END_DATE = datetime(2025, 12, 31)   # End date for events
OUTPUT_DIR = Path("data/events")    # Output directory
```

### Data Warehouse (`refresh_job.py`)

```python
DB = "micro_warehouse.duckdb"       # DuckDB database file
```

## Performance

### Event Generation
- **Speed**: 30-60 seconds for 1M rows (varies by CPU)
- **Parallelism**: Uses all available CPU cores
- **Compression**: Snappy compression for efficient storage
- **Memory**: Batched processing to handle large datasets

### DuckDB Queries
- **Fast analytics**: DuckDB optimized for OLAP workloads
- **Partitioning**: Date-based partitioning improves query performance
- **In-process**: No separate database server required

## Example Queries

Once you've run both scripts, you can query the data:

```python
import duckdb

con = duckdb.connect("micro_warehouse.duckdb")

# Query bronze layer
con.execute("SELECT * FROM bronze_events LIMIT 10").fetchdf()

# Query silver layer
con.execute("SELECT * FROM silver_events WHERE event_name = 'purchase' LIMIT 10").fetchdf()

# Query gold layer
con.execute("SELECT * FROM gold_weekly_active_teams").fetchdf()

# Custom analytics
con.execute("""
    SELECT 
        event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users
    FROM silver_events
    GROUP BY event_name
    ORDER BY event_count DESC
""").fetchdf()
```

## Example Output

### Event Data Sample
```
event_time                user_id  team_id  event_name    properties
2023-03-15 14:23:45      42567    234      page_view     {"page_url": "abc123", "platform": "web"}
2024-07-22 09:15:30      89234    789      purchase      {"product_id": "xyz789", "price": 49.99}
2023-11-08 18:42:11      15678    456      video_play    {"video_id": "vid123", "device_type": "mobile"}
```

### Weekly Active Teams
```
week                     weekly_active_teams
2023-01-02               847
2023-01-09               892
2023-01-16               915
```

## Troubleshooting

### FileNotFoundError: data/events
**Solution**: The scripts now automatically create required directories (`data/events` and `exports`)

### No data generated
**Solution**: Check that you have write permissions in the project directory

### DuckDB errors
**Solution**: Ensure you've run `generate_events.py` first to create the source data

### Memory issues
**Solution**: Reduce `TOTAL_ROWS` in `generate_events.py` or increase available RAM

## Use Cases

- **Analytics Prototyping**: Quick setup for testing analytics queries
- **Data Engineering Practice**: Learn medallion architecture patterns
- **Performance Testing**: Benchmark query performance with realistic data volumes
- **DuckDB Learning**: Explore DuckDB features with sample data
- **Pipeline Development**: Develop and test data transformation logic

## Technologies Used

- **Polars**: Fast DataFrame library for data generation
- **PyArrow**: Parquet file format support
- **NumPy**: Efficient numerical operations
- **DuckDB**: Embedded analytical database
- **Python Multiprocessing**: Parallel data generation

## License

This is a sample project for demonstration purposes.