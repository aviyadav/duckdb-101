# DuckDB Transaction Data Analysis

A Python project for generating large-scale synthetic transaction datasets and analysing them with DuckDB. Includes three scripts covering different scale and complexity trade-offs, from a simple stdlib-only generator to a memory-safe streaming pipeline capable of producing 1 billion rows without crashing.

---

## Project Structure

```
duckdb-transaction-data-analysis/
├── data/
│   └── transaction-data.csv          # Generated output (created on first run)
├── generate-transaction-simple.py    # stdlib-only generator  (500 K rows)
├── generate-transaction-data-mp.py   # streaming multiprocessing generator (up to 1 B rows)
├── main.py                           # DuckDB analysis queries
├── pyproject.toml
└── README.md
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | ≥ 3.13 |
| uv _(package manager)_ | latest |

---

## Installation

```sh
# Clone / navigate to the project
cd duckdb-transaction-data-analysis

# Install all dependencies (creates .venv automatically)
uv sync
```

### Dependencies

| Package | Purpose |
|---|---|
| `numpy` | Vectorised random data generation inside worker processes |
| `polars` | Fast DataFrame concat and CSV writing |
| `pyarrow` | Zero-copy Polars → Arrow conversion; streaming CSV writer |
| `duckdb` | In-process SQL engine for analytics queries |
| `pandas` | DataFrame interface for DuckDB query results |

---

## Dataset Schema

All generators produce a CSV with identical columns:

| Column | Type | Example | Description |
|---|---|---|---|
| `transaction_id` | string | `TXN0000001` | Zero-padded sequential ID |
| `customer_id` | string | `CUST03870` | Random customer reference |
| `product` | string | `Laptop` | One of 10 product categories |
| `amount` | float | `1725.82` | Transaction value — $100.00 to $2,500.00 |
| `quantity` | int | `4` | Units purchased — 1 to 10 |
| `store` | string | `Store_E` | One of 5 store locations |
| `payment_method` | string | `Credit Card` | One of 5 payment methods |
| `date` | string | `2024-10-17` | Random date between 2023-01-01 and 2024-12-31 |

### Reference Values

```
Products        : Camera, Charger, Tablet, Printer, Monitor,
                  Laptop, Headphones, Keyboard, Phone, Mouse

Stores          : Store_A, Store_B, Store_C, Store_D, Store_E

Payment methods : Cash, Credit Card, Debit Card, UPI payment, Apple Pay
```

---

## Scripts

### 1. `generate-transaction-simple.py` — Simple Generator

Standard-library-only, single-process generator. No external dependencies beyond the Python built-ins. Suitable for moderate datasets where simplicity matters more than raw speed.

```sh
uv run python generate-transaction-simple.py
```

**How it works**

```
for each row:
  generate_row(i)  →  csv.writer.writerow()
```

- Uses `random` and `csv` from the standard library only
- Writes rows one at a time; constant memory regardless of record count
- Prints progress every 100,000 rows with elapsed time and percentage
- Prints a summary with file size and rows/sec on completion

**Configuration** — edit the constants at the top of the file:

| Constant | Default | Description |
|---|---|---|
| `NUM_RECORDS` | `500_000` | Total rows to generate |
| `OUTPUT_FILE` | `data/transaction-data.csv` | Output path |

**Observed performance**

```
Records    :    500,000
File size  :       32.6 MB
Total time :       2.13s
Rows/sec   :    235,102
```

---

### 2. `generate-transaction-data-mp.py` — High-Scale Streaming Generator

Memory-safe, multiprocessing pipeline designed to generate datasets up to 1 billion rows without running out of RAM. Uses a streaming batch approach so that peak memory consumption is bounded by a single batch, not the full dataset.

```sh
uv run python generate-transaction-data-mp.py
```

**Pipeline — per batch**

```
multiprocessing (NumPy)  →  Polars concat  →  PyArrow (zero-copy)  →  pa.csv.CSVWriter
       ↑ workers fill sub-chunks ↑                                         ↑ streamed to disk ↑
                         del + gc.collect() between every batch
```

1. **multiprocessing + NumPy** — `MAX_WORKERS` processes each generate a sub-chunk using `np.random.default_rng` (vectorised; ~10× faster than `random`)
2. **Polars** — sub-chunks are concatenated into a single batch `DataFrame`
3. **PyArrow** — zero-copy `df.to_arrow()`; the Polars frame is deleted immediately
4. **`pa.csv.CSVWriter`** — streams the Arrow table to disk; header written once on the first batch, data-only on subsequent batches
5. **`del` + `gc.collect()`** — batch memory is explicitly freed before the next batch starts

> **Peak RAM ≈ memory for one batch, regardless of total dataset size.**

**Why the naive approach crashed**

The original single-pass design called `pool.map()` for all records at once, then accumulated every row in RAM through the full chain before writing anything:

```
all chunks → Polars concat → Arrow → Pandas → DuckDB COPY TO CSV
              ↑ 60–100 GB held in RAM for 1 B rows ↑
```

**Configuration** — edit the constants at the top of the file:

| Constant | Default | Notes |
|---|---|---|
| `NUM_RECORDS` | `1_000_000_000` | Total rows to generate |
| `BATCH_SIZE` | `500_000` | Rows per batch (~95 MB RAM). Raise to `2_000_000` for faster throughput if you have ≥ 16 GB free |
| `MAX_WORKERS` | `min(cpu_count, 8)` | Worker-process cap. Higher = faster per-batch generation but more IPC overhead |
| `OUTPUT_FILE` | `data/transaction-data.csv` | Output path |

**RAM budget guide**

| `BATCH_SIZE` | Est. RAM/batch | Recommended free RAM |
|---|---|---|
| `500_000` | ~95 MB | ≥ 4 GB |
| `1_000_000` | ~190 MB | ≥ 8 GB |
| `2_000_000` | ~380 MB | ≥ 16 GB |

**Observed performance (24-core machine, `BATCH_SIZE=500_000`, `MAX_WORKERS=8`)**

```
Batch       1/10             500,000 rows  ( 10.0%)    0.77s/batch      568k rows/s  ETA 0:00:07
Batch       2/10           1,000,000 rows  ( 20.0%)    0.54s/batch      705k rows/s  ETA 0:00:05
...
Batch      10/10           5,000,000 rows  (100.0%)    0.52s/batch      890k rows/s  ETA 0:00:00

Rows written   :        5,000,000
File size      :             0.37 GB
Total time     :           0:00:05
Avg rate       :        888k rows/s
```

**Interruption safety**

Pressing `Ctrl-C` is caught gracefully. The `pa.csv.CSVWriter` is closed in the `finally` block, leaving the output file valid and readable up to the last completed batch.

---

### 3. `main.py` — DuckDB Analysis

Runs six analytical queries against the generated CSV using DuckDB's direct file-scanning capability — no import step needed. Each query is timed independently.

```sh
uv run python main.py
```

**Queries**

| # | Analysis | Key columns |
|---|---|---|
| 1 | Quick peek — first 10 rows | all |
| 2 | Sales by product | `product`, revenue, units sold |
| 3 | Top 10 customers by spending | `customer_id`, total spent |
| 4 | Monthly sales trend (2024) | `date` → month, revenue |
| 5 | Store performance comparison | `store`, unique customers, revenue |
| 6 | Payment method share per store | `store`, `payment_method`, percentage (window function) |

DuckDB reads the CSV directly with `FROM 'data/transaction-data.csv'` — no loading or registration required. Results are returned as Pandas DataFrames.

---

## Quick Start (end-to-end)

```sh
# 1. Install dependencies
uv sync

# 2a. Generate a small dataset quickly (stdlib only, ~2s)
uv run python generate-transaction-simple.py

# 2b. Or generate a large dataset with multiprocessing (tune BATCH_SIZE first)
uv run python generate-transaction-data-mp.py

# 3. Run the analysis
uv run python main.py
```

---

## Reproducibility

Both multiprocessing generators use **deterministic, prime-spaced seeds** derived from the batch number and worker index:

```
seed = batch_num × 104,729  +  worker_idx × 7,919
```

Running the same script twice with the same configuration produces identical output.