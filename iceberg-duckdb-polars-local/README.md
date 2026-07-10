# pyiceberg-duckdb-polars-local

A sample Python project demonstrating integration between Apache Iceberg, DuckDB, and Polars for local analytics workflows.

## Overview

This project showcases:

- Creating a data warehouse (based on Iceberg tables) locally using DuckDB and Polars.
- Managing Iceberg tables with `pyiceberg`.
- Generating and inserting sample data using both DuckDB SQL and Polars DataFrames.
- Reading data back via DuckDB and Polars, verifying aggregation and row counts.

All operations are performed locally within a directory called `icehouse`.

## Requirements

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended for dependency management)
- See dependencies in [`pyproject.toml`](./pyproject.toml) and the locked versions in [`uv.lock`](./uv.lock)

## Setup (using uv)

This project uses [uv](https://github.com/astral-sh/uv), a superfast Python package installer and resolver. If you don't already have it, install uv globally:

```bash
pip install uv
```

To install all dependencies for this project:

```bash
uv pip install -r requirements.txt   # if requirements.txt is present
```

Or, directly from the `pyproject.toml` (recommended):

```bash
uv venv    # Create a virtual environment using uv
uv pip install -e .
```

This will use the lock file (`uv.lock`) for reproducible environments.

## Running the Example

To run the demo script, first activate your environment (if using `uv venv`):

```bash
source .venv/bin/activate
```

Then execute:

```bash
python main.py
```

This script will:

1. Create a local Iceberg warehouse at `./icehouse` and a SQLite catalog.
2. Generate 5,000 rows of synthetic data via both DuckDB and Polars.
3. Create two Iceberg tables: `dummy_data.duckdb_data` and `dummy_data.polars_data`, with the generated schema.
4. Insert the generated data into the respective tables.
5. Read the data back using both DuckDB and Polars, and print summary statistics (row count and sum aggregation).

## Code Structure

- `main.py` – Contains the core demo logic: table creation, data generation, data insertion, and validation.
- `pyproject.toml` – Project metadata and dependencies.
- `uv.lock` – Frozen dependency versions resolved by uv.
- `icehouse/` – Local data warehouse storage directory (created automatically).
- `README.md` – This file.

## Example Output

You’ll see output summarizing the number of rows and aggregate values for each table, similar to:

```
duckdb table has 5000 rows
polars table has 5000 rows
Row count in duck db ...
...
```

## Notes

- The script uses the `pyiceberg.catalog.sql.SqlCatalog` to maintain metadata/catalog in SQLite, stored in `icehouse/icyhot.db`.
- Adjust the `rows` variable in `main.py` to control sample data size.
- Both Arrow and Polars DataFrames are used for seamless data movement between Python analytics libraries and Iceberg tables.
- All dependency resolution and management is handled through uv and the lock file (`uv.lock`) for reproducibility.

## References

- [Apache Iceberg](https://iceberg.apache.org/)
- [DuckDB](https://duckdb.org/)
- [Polars](https://pola.rs/)
- [PyIceberg](https://py.iceberg.apache.org/)
- [uv - extremely fast Python package installer and resolver](https://github.com/astral-sh/uv)
