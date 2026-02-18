# ETL: Parquet to PostgreSQL with DuckDB & PyArrow

This project demonstrates a high-performance ETL pipeline that leverages **DuckDB** for lightning-fast Parquet scanning and **PyArrow** as the backend for **Pandas** to optimize memory usage and string processing.

## 🚀 Key Features

- **PyArrow-backed Pandas**: Uses the Arrow memory format for strings and native types, reducing memory overhead and speeding up filtering/aggregations.
- **DuckDB Integration**: Uses DuckDB's native Parquet reader and PostgreSQL scanner (via `postgres_scanner`) for high-throughput data transfer.
- **Efficient Dummy Data Generation**: Fast generation of synthetic transaction data in Parquet format.

## 📂 Project Structure

- `create-dummy-data.py`: Generates synthetic transaction data using `Faker` and exports to Parquet using the PyArrow engine.
- `etl-process.py`: Reads Parquet files via DuckDB, converts them to PyArrow-backed Pandas DataFrames, and performs fast aggregations.
- `conn_pg_to_duckdb.py`: Demonstrates direct integration between DuckDB and a PostgreSQL database for schema creation and data loading (including `ON CONFLICT` handling).

## 🛠 Prerequisites

Ensure you have `uv` installed for dependency management.

```bash
# Install dependencies
uv sync
```

## 📖 How to Use

### 1. Generate Data
Generate 50,000 synthetic transaction records spread across 50 Parquet files.
```bash
uv run python create-dummy-data.py
```

### 2. Run ETL Analysis
Load the data into a memory-efficient Pandas DataFrame and view aggregations.
```bash
uv run python etl-process.py
```

### 3. Load to PostgreSQL
Ensure you have a PostgreSQL instance running and update the connection details in `conn_pg_to_duckdb.py`. Then run:
```bash
uv run python conn_pg_to_duckdb.py
```

## ⚡ Performance Benefits

| Feature | Benefit |
| :--- | :--- |
| **PyArrow Backend** | Up to 10x faster string operations compared to standard NumPy-backed strings. |
| **DuckDB SCAN** | Scans Parquet files in parallel with minimal overhead. |
| **Postgres Scanner** | Low-latency binary data transfer between DuckDB and PostgreSQL. |
