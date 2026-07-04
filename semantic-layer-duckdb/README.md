# Semantic Layer DuckDB

A small Python project demonstrating how to query NYC taxi data through a semantic layer backed by DuckDB and Ibis.

The example uses [`boring-semantic-layer`](https://pypi.org/project/boring-semantic-layer/) to define reusable dimensions, measures, and joins in YAML, then runs analytical queries from Python.

## What this project does

`nyc_taxi.py` loads local NYC taxi data into an in-memory DuckDB connection and exposes it through semantic models defined in `nyc_taxi.yml`.

The script demonstrates queries for:

- Trip volume by pickup borough
- Popular pickup zones
- Service zone metrics
- Revenue by service zone
- Accessibility and shared-trip rates
- A PNG chart for trip volume by pickup borough

`example_materialize.py` is a smaller, self-contained example that builds an in-memory sales table with [`xorq`](https://pypi.org/project/xorq/) and [Polars](https://pola.rs/), defines a semantic model with a time dimension, and runs a time-grained, time-ranged aggregate query.

## Project structure

```text
semantic-layer-duckdb/
├── datalake/
│   └── nyc-taxi/
│       ├── fhvhv_tripdata_2025-06.parquet
│       └── taxi_zone_lookup.csv
├── nyc_taxi.py
├── nyc_taxi.yml
├── nyc_taxi.md
├── example_materialize.py
├── pyproject.toml
├── requirements.txt
├── uv.lock
└── README.md
```

## Data files

The script expects the following local files:

- `datalake/nyc-taxi/taxi_zone_lookup.csv`
  - Taxi zone metadata including `LocationID`, `Borough`, `Zone`, and `service_zone`.
- `datalake/nyc-taxi/fhvhv_tripdata_2025-06.parquet`
  - High Volume For-Hire Vehicle trip records including pickup zone, trip distance, fare, tips, driver pay, and accessibility/shared ride flags.

Download the datafiles instead of downloading it programetically to avoid getting 403 errors from the NYC Taxi API.

```
wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-06.parquet
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
```

## Semantic model

The semantic layer is configured in `nyc_taxi.yml`.

It defines two models:

- `taxi_zones`
  - Dimensions: `location_id`, `borough`, `zone`, `service_zone`
  - Measure: `zone_count`
- `fhvhv_trips`
  - Dimensions: trip timestamps, fare fields, trip metrics, and ride flags
  - Measures: `trip_count`, `avg_trip_miles`, `avg_base_fare`, `total_revenue`, `avg_tips`, `avg_driver_pay`, `shared_trip_rate`, and `wheelchair_request_rate`
  - Join: `pickup_zone`, joining `fhvhv_trips.PULocationID` to `taxi_zones.LocationID`

## Sales example (`example_materialize.py`)

This example does not depend on the NYC taxi data files. It builds a small in-memory sales table directly in Python:

- Uses `xorq.api.connect()` to create an in-memory `xorq` backend connection.
- Builds the source table with a Polars `DataFrame` (not pandas) and loads it via `con.create_table("sales", df)`.
- Defines a `SemanticModel` with:
  - Dimensions: `region` and `date` (`date` is marked as the time dimension with `smallest_time_grain="TIME_GRAIN_DAY"` via the `Dimension` class)
  - Measures: `total_sales`, `order_count`
- Runs `sales_model.query(...)` with `time_grain="TIME_GRAIN_DAY"` and an explicit `time_range` (`start`/`end`) to aggregate sales by day and region, filtered up to a cutoff date, then executes and prints the result.

Note: the installed `boring-semantic-layer` version does not expose a `SemanticModel.materialize()` cube API. The `time_grain` + `time_range` combination on `.query()` is used instead to achieve the same day-level, cutoff-filtered aggregation.

Run it with:

```sh
uv run python example_materialize.py
```

Expected output (row order may vary since no `order_by` is specified):

```text
        date region  total_sales  order_count
0 2025-01-02  south          200            1
1 2025-01-03  north          150            1
2 2025-01-04   east          300            1
3 2025-01-01  north          100            1
```

## Requirements

- Python 3.13
- [`uv`](https://docs.astral.sh/uv/)
- Local NYC taxi data files under `datalake/nyc-taxi/`

Python dependencies are listed in `requirements.txt`:

```text
PyYAML
boring-semantic-layer
boring-semantic-layer[visualization]
altair[all]
ibis-framework[duckdb]
pandas
xorq
polars
```

`pandas` is required because `ibis`'s `.execute()` returns pandas `DataFrame` objects (used throughout `nyc_taxi.py`). `xorq` and `polars` are used by `example_materialize.py`, which builds its source table with Polars instead of pandas.

## Setup

The runtime packages are listed in `requirements.txt` (note that `pyproject.toml` currently declares only `pip` as a dependency, so the runtime packages must be installed from `requirements.txt`).

From the project root:

```sh
uv venv
uv pip install -r requirements.txt
```

## Run the NYC taxi example

```sh
uv run python nyc_taxi.py
```

The script prints query results to the terminal and saves a chart image at:

```text
trip-volume-by-pickup-borough-styled.png
```

## Example output

The script prints sections like:

```text
=== Trip Volume by Pickup Borough ===
Top 5 boroughs by trip volume:
```

and:

```text
=== Popular Pickup Zones ===
Top 10 pickup zones by trip count:
```

It also reports the generated chart path:

```text
Saved chart to .../trip-volume-by-pickup-borough-styled.png
```

## Notes

- DuckDB runs in memory using `ibis.duckdb.connect(":memory:")`.
- `nyc_taxi.py` uses project-relative paths, so it can be run from the repository root without editing absolute paths.
- PNG chart export uses the Altair backend because the ECharts backend returns a chart specification rather than rendered PNG bytes.
- The current YAML model includes the pickup zone join only. `nyc_taxi.md` notes that dual pickup/dropoff joins may require explicit Python join logic to avoid ambiguous references.
- `example_materialize.py` builds its source data with Polars and loads it into an `xorq` in-memory backend, so it does not depend on DuckDB or the NYC taxi datalake files.
