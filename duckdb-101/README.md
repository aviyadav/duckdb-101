# DuckDB 101

This project demonstrates and compares data processing operations using **Pandas**, **DuckDB**, and **Polars** on the Olist customers dataset.

Each implementation performs a series of common data manipulation tasks:
- reading CSV data
- describing the dataset
- filtering and selecting columns
- excluding columns
- aggregations (group by)
- reading and writing Parquet files

## Prerequisites

- Python >= 3.14
- uv (for dependency management)

## Installation

1. Clone the repository.
2. Install dependencies using `uv`:

```bash
uv sync
```

## Usage

The main script `main.py` contains three functions:
- `using_pandas()`
- `using_duckdb(conn)`
- `using_polars()`

You can comment/uncomment the function calls in the `__main__` block of `main.py` to run specific implementations.

To run the script:

```bash
uv run python main.py
```
