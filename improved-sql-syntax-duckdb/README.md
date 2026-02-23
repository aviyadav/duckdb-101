# Improved SQL Syntax in DuckDB Demo

This project demonstrates various improved SQL syntax features in DuckDB using generated transaction data. It includes a Python script to generate sample transaction data in Parquet format and several SQL scripts to showcase DuckDB's modern SQL capabilities.

## Description

The core of this project is to generate synthetic transaction data (e.g., transaction_id, timestamp, customer_id, merchant, category, amount, status) using Polars and Faker libraries. This data is saved as multiple Parquet files in the `data/` directory.

The generated data is then used in DuckDB to run queries that highlight improved SQL syntax features, such as EXCLUDE, QUALIFY, REPLACE, UNION BY NAME, and more. This serves as a practical demo for exploring DuckDB's SQL enhancements.

## Dependencies

- Python 3.10+
- Polars (for data generation)
- Faker (for generating fake data)
- NumPy (for random data generation)
- DuckDB (for running SQL queries)

The project uses `uv` for dependency management (via `pyproject.toml` and `uv.lock`).

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd improved-sql-syntax-duckdb
   ```

2. Install dependencies using `uv` (recommended) or `pip`:
   - With `uv`:
     ```
     uv sync
     ```
   - With `pip` (if `uv` is not installed):
     ```
     pip install polars faker numpy duckdb
     ```

3. (Optional) Activate the virtual environment:
   ```
   source .venv/bin/activate  # On Unix-like systems
   # or
   .venv\Scripts\activate  # On Windows
   ```

## Usage

1. Generate the transaction data:
   ```
   python generate-data.py
   ```
   This will create 250 Parquet files in the `data/` directory, each with 1000 rows of transaction data.

2. Read the data into DuckDB (if needed, using `read-data-ddb.py`):
   ```
   python read-data-ddb.py
   ```

3. Run the SQL query demos using DuckDB CLI or your preferred method. For example:
   ```
   duckdb -init run-query-exclude.sql
   ```
   Replace `run-query-exclude.sql` with any of the available SQL scripts.

## Demonstrated SQL Features

This project includes SQL scripts that demonstrate the following DuckDB SQL features on the generated transaction data:

- **Double Colon Casting** (`run-query-dblcolon.sql`): Using `::` for type casting.
- **EXCLUDE Clause** (`run-query-exclude.sql`): Selecting all columns except specified ones.
- **GROUP BY ALL** (`run-query-grp-by-all.sql`): Grouping by all non-aggregated columns.
- **QUALIFY Clause** (`run-query-qualify.sql`): Filtering window function results.
- **REPLACE Clause** (`run-query-replace.sql`): Replacing values in SELECT.
- **SELECT DISTINCT ON** (`run-query-select-distinct-on.sql`): Selecting distinct rows based on columns.
- **UNION BY NAME** (`run-query-union-by-name.sql`): Union tables by column names.
- **UNION DISTINCT** (`run-query-union-distinct.sql`): Union with distinct rows.
- **Generic Query** (`run-query.sql`): A basic query example.

Each script can be run directly in DuckDB to see the feature in action with the transaction data.

For more details on DuckDB's SQL syntax, refer to the [official DuckDB documentation](https://duckdb.org/docs/).

## License

This project is licensed under the MIT License.