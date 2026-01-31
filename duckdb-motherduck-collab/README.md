# DuckDB & MotherDuck Collaboration

This project demonstrates a data engineering workflow using **DuckDB** for local data generation and processing, and **MotherDuck** for cloud-based data analytics and sharing.

## Tech Stack

- **[DuckDB](https://duckdb.org/)**: An in-process SQL OLAP database management system.
- **[MotherDuck](https://motherduck.com/)**: A collaborative serverless data platform built with DuckDB.
- **Python**: Used for data generation and automation.
- **[uv](https://github.com/astral-sh/uv)**: An extremely fast Python package and project manager.

## Project Structure

- `generate_events_duckdb.py`: Uses multiprocessing to generate 10,000 synthetic event records and saves them to `data/events.parquet`.
- `main.py`: Connects to MotherDuck, creates a table, and loads data from the local Parquet file into a MotherDuck-hosted table for cloud analytics.
- `data/`: Directory containing the generated Parquet files.

## Setup and Usage

### Prerequisites

Ensure you have `uv` installed. If not, you can install it via:

```powershell
powershell -c "irm https://astral-sh.com/uv/install.ps1 | iex"
```

### 1. Install Dependencies

```bash
uv sync
```

### 2. Generate Data

Generate the synthetic event data locally:

```bash
uv run generate_events_duckdb.py
```

### 3. Run Analytics

Upload data to MotherDuck and run the summary query:

```bash
uv run main.py
```

> [!NOTE]
> The `main.py` script currently contains a hardcoded `MOTHERDUCK_TOKEN`. For production use, it is recommended to use environment variables or secret management.
