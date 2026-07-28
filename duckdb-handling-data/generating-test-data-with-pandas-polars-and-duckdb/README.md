# Synthetic Event Data Generation Benchmark

This project demonstrates and compares five ways to generate synthetic event data and export it to CSV with Python:

- [pandas](https://pandas.pydata.org/)
- [Polars](https://pola.rs/)
- [DuckDB](https://duckdb.org/)
- [PyArrow](https://arrow.apache.org/docs/python/)
- [Apache DataFusion](https://datafusion.apache.org/python/)

Each implementation creates the same logical event schema, writes an engine-specific CSV file under `data/`, and prints elapsed-time and peak-memory measurements. The project is intended for learning, experimentation, and lightweight performance comparisons between in-memory DataFrame, columnar, and SQL execution engines.

## Contents

- [Project goals](#project-goals)
- [Generated dataset](#generated-dataset)
- [Implementations](#implementations)
- [How generation works](#how-generation-works)
- [Requirements](#requirements)
- [Installation](#installation)
- [Running the programs](#running-the-programs)
- [Output files](#output-files)
- [Performance measurements](#performance-measurements)
- [Reproducibility and benchmark fairness](#reproducibility-and-benchmark-fairness)
- [Customizing the workload](#customizing-the-workload)
- [Validating the output](#validating-the-output)
- [Troubleshooting](#troubleshooting)
- [Project structure](#project-structure)

## Project Goals

The programs show how the same data-generation task can be implemented with different processing models:

1. Build NumPy arrays and convert them into a DataFrame or Arrow table.
2. Generate rows directly inside a SQL execution engine.
3. Write one million or more rows to CSV.
4. Measure generation time, CSV serialization time, and process memory.
5. Compare API design and implementation complexity across engines.

This is not a formal benchmark suite. The scripts are intentionally small and self-contained so each implementation is easy to inspect and run independently.

## Generated Dataset

Every script produces these columns in the same order:

| Column | Logical type | Generation rule | Example |
| --- | --- | --- | --- |
| `event_date` | Date | Random date from `2024-01-01` through `2026-01-31`, inclusive | `2025-04-18` |
| `country` | String | Uniform choice from `US`, `UK`, `DE`, `FR`, `IN`, and `JP` | `DE` |
| `channel` | String | Uniform choice from `search`, `social`, `email`, and `direct` | `social` |
| `user_id` | Integer | Random positive user identifier | `18452` |
| `order_id` | Integer | Random positive order identifier | `632104` |
| `revenue` | Floating point | Skewed positive value rounded to two decimals, with about 15% replaced by zero | `73.42` |

Example CSV:

```csv
event_date,country,channel,user_id,order_id,revenue
2024-10-31,FR,social,165033,85892,0.0
2026-01-10,IN,email,4936,276710,46.72
```

### Identifier ranges

The NumPy-based implementations and DataFusion generate:

- `user_id`: 1 through 199,999
- `order_id`: 1 through 899,999

DuckDB currently generates:

- `user_id`: 1 through 200,000
- `order_id`: 1 through 900,000

The difference comes from NumPy's exclusive upper bound versus the SQL expressions used by DuckDB.

## Implementations

| Script | Default rows | Generation engine | In-memory representation | CSV writer | Output |
| --- | ---: | --- | --- | --- | --- |
| `generate_data_pandas.py` | 1,000,000 | NumPy | pandas `DataFrame` | `DataFrame.to_csv` | `data/events_pd.csv` |
| `generate_data_polars.py` | 1,000,000 | NumPy | Polars `DataFrame` | `DataFrame.write_csv` | `data/events_pl.csv` |
| `generate_data_pyarrow.py` | 1,000,000 | NumPy | PyArrow `Table` | `pyarrow.csv.write_csv` | `data/events_pa.csv` |
| `generate_data_datafusion.py` | 1,000,000 | DataFusion SQL | Lazy DataFusion `DataFrame` | `DataFrame.write_csv` | `data/events_df.csv` |
| `generate_data_duckdb.py` | 100,000,000 | DuckDB SQL | Streamed SQL result | DuckDB `COPY` | `data/events.csv` |

> **Disk-space warning:** DuckDB is configured for 100 million rows, not one million. Its CSV can consume several gigabytes. Reduce `row_count` before running it if you only want a quick comparison.

## How Generation Works

### pandas

`generate_data_pandas.py` creates each column with a seeded NumPy random generator, builds a pandas `DataFrame`, applies the 15% zero-revenue mask, and writes the DataFrame with `to_csv`.

### Polars

`generate_data_polars.py` creates the values with NumPy, uses Polars to construct the date range and DataFrame, and writes the result with Polars' native CSV writer.

### PyArrow

`generate_data_pyarrow.py` generates NumPy arrays, converts them into explicitly typed Arrow arrays, constructs a `pyarrow.Table`, and writes it through the Arrow CSV module. Dates are stored as Arrow `date32` values before serialization.

### DuckDB

`generate_data_duckdb.py` performs generation and CSV output in one SQL statement. `generate_series` supplies the requested row count, SQL expressions produce random values, and `COPY` streams the query result to CSV without first building a Python DataFrame.

### DataFusion

`generate_data_datafusion.py` builds a lazy SQL query with `generate_series`. DataFusion executes the query when `write_csv` is called, generating and serializing the rows through its query engine.

### Revenue distribution

The NumPy implementations use:

```python
rng.gamma(shape=2.0, scale=30.0, size=row_count)
```

DuckDB and DataFusion approximate the same gamma shape with the sum of two negative logarithms of uniform random values:

```text
(-log(random()) - log(random())) * 30.0
```

A separate random condition sets approximately 15% of revenue values to zero. Consequently, the engines produce similar distributions but not identical values.

## Requirements

- Python 3.13 or later
- [uv](https://docs.astral.sh/uv/) for environment and dependency management
- Sufficient free memory and disk space for the selected row count

The project declares these main packages in `pyproject.toml`:

| Package | Minimum version | Purpose |
| --- | ---: | --- |
| `pandas` | 3.0.5 | pandas DataFrame implementation |
| `polars` | 1.43.1 | Polars DataFrame implementation |
| `duckdb` | 1.5.5 | Embedded SQL generation and CSV export |
| `pyarrow` | 25.0.0 | Arrow table and CSV implementation |
| `datafusion` | 54.0.0 | DataFusion SQL execution engine |
| `psutil` | 7.2.2 | Process RSS memory sampling |

NumPy is used directly by the pandas, Polars, and PyArrow scripts and is installed as part of the resolved dependency environment.

## Installation

From the project directory, create the virtual environment and install the locked dependencies:

```bash
uv sync
```

Confirm the Python version:

```bash
uv run python --version
```

The repository pins Python `3.13` in `.python-version`, while `pyproject.toml` allows Python `3.13` or later.

## Running the Programs

Run one implementation at a time:

```bash
uv run python generate_data_pandas.py
uv run python generate_data_polars.py
uv run python generate_data_pyarrow.py
uv run python generate_data_datafusion.py
uv run python generate_data_duckdb.py
```

The scripts create `data/` automatically if it does not exist.

Example terminal output:

```text
========================================
PyArrow Performance Metrics (1,000,000 rows):
========================================
Data Generation Time: 0.3611 seconds
CSV Writing Time: 0.1264 seconds
Total Elapsed Time: 0.4875 seconds
Peak Memory Usage: 211.18 MB
========================================
```

The values above are illustrative. Actual results depend on hardware, operating system, filesystem, package versions, and current system load.

## Output Files

Running all implementations produces:

```text
data/
├── events.csv       # DuckDB
├── events_pd.csv    # pandas
├── events_pl.csv    # Polars
├── events_pa.csv    # PyArrow
└── events_df.csv    # DataFusion
```

A script replaces its own output file when rerun. The generated files can be large and are not currently excluded by `.gitignore`; avoid committing them unless that is intentional.

## Performance Measurements

All scripts start a daemon thread before generation. The thread uses `psutil` to sample the current process's resident set size (RSS) every 10 milliseconds and retains the largest observed value.

The reported peak memory therefore represents total process RSS, including:

- The Python interpreter
- Imported libraries and native runtimes
- Generated arrays, DataFrames, and Arrow buffers
- Engine execution memory
- CSV writer buffers

It is not the incremental memory used only by the generated dataset.

### pandas, Polars, and PyArrow timings

These scripts report three durations:

1. **Data generation time** — generating arrays and constructing the DataFrame or table
2. **CSV writing time** — serializing the in-memory object to disk
3. **Total elapsed time** — the sum of generation and writing

### DuckDB and DataFusion timings

These engines generate rows lazily while executing the CSV write. Their scripts therefore report one combined generation-and-write duration.

### Measurement limitations

- A 10 ms sampling interval can miss short-lived memory spikes.
- RSS includes memory allocated before timing begins.
- Filesystem caching can make later writes appear faster.
- SQL and NumPy random-number implementations differ.
- CSV formatting and quoting may differ slightly by writer.
- DuckDB's default row count currently differs from all other scripts.
- The scripts perform one run and do not calculate statistical summaries.

## Reproducibility and Benchmark Fairness

The pandas, Polars, and PyArrow scripts initialize NumPy with seed `42`:

```python
rng = np.random.default_rng(42)
```

They are deterministic for a given NumPy version and execution path. Because they draw values in the same order, their generated values should be comparable, although date and string serialization can vary by library.

DuckDB and DataFusion call their engines' `random()` functions without a fixed seed, so their output changes on every run.

For a more meaningful performance comparison:

1. Set every script to the same `row_count`.
2. Run each script several times.
3. Treat the first run as a warm-up.
4. Run on an otherwise idle machine.
5. Use the locked package versions from `uv.lock`.
6. Delete old output files or ensure sufficient disk space.
7. Record output file size in addition to time and memory.
8. Compare medians rather than a single run.
9. Avoid running scripts concurrently because they compete for CPU, memory, and disk bandwidth.

## Customizing the Workload

The scripts are standalone programs rather than command-line applications. Edit their constants and generation expressions directly.

### Change the row count

Each script defines `row_count` near the top:

```python
row_count = 1_000_000
```

For a quick smoke test, use a smaller value such as:

```python
row_count = 10_000
```

Use the same value in every script when comparing engines.

### Change the random seed

For NumPy-based implementations, change:

```python
rng = np.random.default_rng(42)
```

Using the same seed preserves comparable generated values across pandas, Polars, and PyArrow.

### Change dates or categories

Update the date boundaries and category lists in each script. The SQL implementations define these values inside their query strings, while the NumPy implementations define them in Python expressions.

### Change the output location

All scripts use:

```python
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
```

Change `DATA_DIR` or the engine-specific CSV filename to write elsewhere.

## Validating the Output

### Count rows

On Linux or macOS, verify a one-million-row output has one header plus one million records:

```bash
wc -l data/events_pd.csv
```

Expected output:

```text
1000001 data/events_pd.csv
```

For the current 100-million-row DuckDB configuration, the expected count is `100000001`.

### Inspect records

```bash
sed -n '1,5p' data/events_pd.csv
```

### Validate with DuckDB

DuckDB can inspect any generated CSV without loading it into a Python DataFrame:

```bash
uv run python -c "import duckdb; print(duckdb.sql(\"SELECT count(*) FROM read_csv_auto('data/events_pd.csv')\").fetchall())"
```

### Compare schemas

You can inspect the inferred columns and types with:

```bash
uv run python -c "import duckdb; print(duckdb.sql(\"DESCRIBE SELECT * FROM read_csv_auto('data/events_pd.csv')\").fetchall())"
```

## Troubleshooting

### `DateParseError` from pandas

Date strings must contain normal ASCII hyphens:

```text
2024-01-01
```

A typographic en dash such as `2024–01–01` is not a valid date separator for pandas.

### Process runs out of memory

Reduce `row_count`. The pandas, Polars, and PyArrow approaches materialize generated arrays and a table in memory. Their peak usage grows with the number of rows.

### Disk fills while running DuckDB

The DuckDB script defaults to 100 million rows and can create a multi-gigabyte file. Stop the process, remove the partial file if necessary, and lower `row_count`.

### Output file already exists

Each implementation is intended to replace its own CSV. If a writer reports a conflict or a previous run was interrupted, delete the relevant file and rerun the script.

### Package import errors

Synchronize the environment and run scripts through `uv`:

```bash
uv sync
uv run python generate_data_pandas.py
```

### Results differ between engines

This is expected. NumPy-based scripts use a fixed seed, while DuckDB and DataFusion use independent SQL random generators. The SQL revenue expression also approximates rather than calls NumPy's gamma generator.

## Project Structure

```text
.
├── .gitignore
├── .python-version
├── README.md
├── generate_data_datafusion.py
├── generate_data_duckdb.py
├── generate_data_pandas.py
├── generate_data_polars.py
├── generate_data_pyarrow.py
├── pyproject.toml
├── uv.lock
└── data/                       # Created at runtime
    ├── events.csv              # DuckDB output
    ├── events_df.csv           # DataFusion output
    ├── events_pa.csv           # PyArrow output
    ├── events_pd.csv           # pandas output
    └── events_pl.csv           # Polars output
```

## Notes

- The scripts execute their workload at module level; importing one runs it immediately.
- There is no shared benchmark runner or command-line argument parser.
- The generated `data/` directory is currently not ignored by Git.
- No automated test suite is currently included.
- Dependency versions are resolved and recorded in `uv.lock`.
