# Local DuckDB Analytics Stack

A working implementation of the architecture described in **DuckDB + Iceberg: The Local Analytics Stack You Should Learn**. It keeps the article's local-first principles: columnar files, SQL-first transformations, a repository/service boundary, incremental ingestion, caching, structured logs, and a small FastAPI layer.

The sample warehouse uses Parquet directly so it runs without a catalog service. Its bronze/silver/gold layout is ready to be registered in an Iceberg catalog when snapshot history, schema evolution, or concurrent writers are needed; it does not create fake Iceberg metadata around ordinary Parquet files.

## Architecture

```text
Generated or incoming Parquet
          |
          v
warehouse/bronze       raw orders and customers
          |
          | DuckDB validation and transformation
          v
warehouse/silver       clean analytical datasets
          |
          | DuckDB SQL aggregations
          v
warehouse/gold         daily revenue, growth, customer summaries
          |
          v
Repository -> cached service -> FastAPI / CLI
```

## Quick start

The generated sample contains 500 customers and 25,000 orders from January 2025 through June 2026.

```bash
uv sync
uv run python -m app.data_generator
uv run python -m app.pipeline
uv run uvicorn main:app --reload
```

Open `http://127.0.0.1:8000/docs` for the interactive API.

The API also bootstraps missing data automatically, so after `uv sync` it is sufficient to run:

```bash
uv run uvicorn main:app
```

## API

- `GET /health`
- `GET /analytics/revenue`
- `GET /analytics/top-customers?limit=20`
- `GET /analytics/monthly-growth`
- `GET /analytics/retention`

## CLI reports

```bash
uv run python -m app.analytics revenue
uv run python -m app.analytics customers --limit 10
uv run python -m app.analytics growth
uv run python -m app.analytics retention
```

## Regenerate data

Generation is deterministic by default and writes compressed Parquet batches rather than thousands of tiny files.

```bash
uv run python -m app.data_generator --orders 25000 --customers 500 --seed 42
uv run python -m app.pipeline
```

## Incremental ingestion

Place order files with the bronze order schema in `incoming/`, then run:

```bash
uv run python -m app.process_incremental_data
```

Each file is validated, copied into the immutable bronze layer, archived under `incoming/processed/`, and the silver/gold layers are rebuilt atomically.

## Project layout

```text
app/             configuration, generation, pipeline, repository, services
sql/             independently testable analytical SQL
warehouse/
  bronze/        raw Parquet batches
  silver/        validated Parquet datasets
  gold/          business-ready Parquet aggregates
incoming/        incremental order batches
analytics.duckdb local catalog and views
main.py          FastAPI application
```

## Iceberg migration path

For production requirements such as time travel, atomic multi-writer commits, schema evolution, and rollback, register the silver datasets through an Iceberg REST/SQL catalog and replace the views in `app/catalog.py` with `iceberg_scan(...)` or an attached catalog. The repository, services, API, and report SQL can remain unchanged because they query logical views rather than physical file paths.
