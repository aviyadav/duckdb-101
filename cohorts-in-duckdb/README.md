# Cohort Analysis in DuckDB

This project demonstrates how to generate realistic e-commerce data and perform advanced cohort analysis using **DuckDB** and **Polars**. It provides a complete workflow from data generation to SQL-based retention analysis.

## 🚀 Key Features

- **High-Performance Generation**: Uses multiprocessing to generate millions of records in seconds.
- **Modern Data Stack**: Leverages Polars for data processing and DuckDB for ultra-fast SQL analytics.
- **Realistic Data Distribution**: Implements Pareto/Power Law distributions for user activity.
- **Advanced SQL Analytics**: Includes complex SQL examples for cohorting, retention, and window functions.

---

## 🛠️ Technology Stack

- **[DuckDB](https://duckdb.org/)**: The analytical database for running high-performance SQL.
- **[Polars](https://pola.rs/)**: Fast DataFrame library used for synthetic data generation.
- **[Parquet](https://parquet.apache.org/)**: Columnar storage format for efficient data access.
- **Python 3.8+**: Language for data generation and script management.

---

## 📥 Installation

```bash
pip install polars pyarrow duckdb
```

---

## 🏗️ Data Generation

To start the analysis, first generate the synthetic users and events data.

```bash
# Generate user profiles
python generate_users.py

# Generate event logs (1M records)
python generate_events.py
```

### Generated Datasets
- **Users (`data/users.parquet`)**: 1,500 users with signup dates, channels, and countries.
- **Events (`data/events.parquet`)**: 1,000,000 events including sessions, purchases, and revenue.

---

## 📊 Cohort Analysis with DuckDB

The repository contains several SQL scripts to perform retention analysis directly on Parquet files using DuckDB.

### SQL Pipeline Overview

1.  **[01_first_purchase_per_user.sql](file:///home/avinash/codebase/python-base/cohorts-in-duckdb/01_first_purchase_per_user.sql)**: Identifies the first purchase timestamp for every user.
2.  **[02_label_every_purchase.sql](file:///home/avinash/codebase/python-base/cohorts-in-duckdb/02_label_every_purchase.sql)**: Joins purchases with first purchase dates and calculates "months since cohort".
3.  **[03-retention_by_active_purchasers.sql](file:///home/avinash/codebase/python-base/cohorts-in-duckdb/03-retention_by_active_purchasers.sql)**: Aggregates active users per month index.
4.  **[04_retention_rate.sql](file:///home/avinash/codebase/python-base/cohorts-in-duckdb/04_retention_rate.sql)**: Calculates the final retention percentage per cohort.
5.  **[05_rolling_7_day_revenue.sql](file:///home/avinash/codebase/python-base/cohorts-in-duckdb/05_rolling_7_day_revenue.sql)**: Example of advanced window functions for business metrics.
6.  **[06_debugging_cohort.sql](file:///home/avinash/codebase/python-base/cohorts-in-duckdb/06_debugging_cohort.sql)**: Granular view of specific user behavior to verify logic.

### How to Run Queries

You can run these scripts directly using the DuckDB CLI:

```bash
duckdb -c ".read 04_retention_rate.sql"
```

Or via Python:

```python
import duckdb

# Connect to a DuckDB instance and query the Parquet files
query = open('04_retention_rate.sql').read()
results = duckdb.query(query).pl()
print(results)
```

---

## 📈 Data Characteristics

- **User Retention**: Structured around signup cohorts from 2022 to 2025.
- **Revenue Distribution**: Realistic purchase values ($5-$500).
- **Sorted Data**: Events are sorted by timestamp for optimal DuckDB predicate pushdown and scan performance.
