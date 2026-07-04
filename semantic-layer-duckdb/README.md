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
```

## Setup

The runtime packages are listed in `requirements.txt` (note that `pyproject.toml` currently declares only `pip` as a dependency, so the runtime packages must be installed from `requirements.txt`).

From the project root:

```sh
uv venv
uv pip install -r requirements.txt
```

## Run the example

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
