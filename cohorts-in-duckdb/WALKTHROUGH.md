# Walkthrough - PROJECT UPDATE: Cohort Analysis in DuckDB

This project has been updated to provide a comprehensive workflow for cohort analysis using DuckDB and Polars.

## Accomplishments

### 1. Updated README.md
The `README.md` was completely rewritten to serve as a professional landing page for the project.
- **Project Goal**: Defined as a demonstration of high-performance analytics on Parquet data.
- **Tech Stack**: Documented usage of DuckDB, Polars, and Parquet.
- **SQL Pipeline**: Added detailed descriptions for all six SQL analysis scripts.
- **Usage Instructions**: Provided clear CLI and Python examples for running the analysis.

### 2. SQL Analysis Documentation
Each script in the analysis pipeline is now linked and explained:
- **01-04**: Core retention logic (First purchase -> Labelling -> Aggregation -> Rate calculation).
- **05**: Showcase of window functions for rolling metrics.
- **06**: Debugging tools for user-level verification.

## Data Characteristics
The synthetic data generation was tuned to:
- Generate 1.5k users and 1M events.
- Follow a realistic power-law distribution for user activity.
- Support time-based cohorting from 2022 to 2025.

## How to Verify
1. Run `python generate_users.py && python generate_events.py`.
2. Execute the queries using `duckdb -c ".read 04_retention_rate.sql"`.
