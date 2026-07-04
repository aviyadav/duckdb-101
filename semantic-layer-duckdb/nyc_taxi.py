"""
NYC Taxi Semantic Layer Example

This example demonstrates a semantic layer for NYC taxi data using:
- taxi_zone_lookup.csv: Zone information including borough, zone name, and service zone
- fhvhv_tripdata_2025-06.parquet: High Volume For-Hire Vehicle trip data

YAML File: `nyc_taxi.yml`
- Defines `taxi_zones` and `fhvhv_trips` models
- Includes joins between trips and pickup/dropoff zones
- Uses Ibis deferred expressions with `_` placeholder

Query Examples:
- Trip volume by borough
- Average trip metrics by zone
- Revenue analysis by service zone
- Shared ride adoption rates

Expected Output Examples:
- Top pickup zones by trip count
- Average trip distance and fare by borough
- Revenue trends by time of day
"""

from pathlib import Path

import boring_semantic_layer as bsl
import ibis

con = ibis.duckdb.connect(":memory:")
# con = ibis.duckdb.connect("md:")
PROJECT_ROOT = Path(__file__).parent
BASE_PATH = PROJECT_ROOT / "datalake" / "nyc-taxi"
tables = {
    # local:
    "taxi_zones_tbl": con.read_csv(BASE_PATH / "taxi_zone_lookup.csv"),
    "trips_tbl": con.read_parquet(BASE_PATH / "fhvhv_tripdata_2025-06.parquet"),
    # cloud:
    # "taxi_zones_tbl": con.read_csv(
    #     "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    # ),
    # "trips_tbl": con.read_parquet(
    #     "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-06.parquet"
    # ),
}

models = bsl.from_yaml(str(PROJECT_ROOT / "nyc_taxi.yml"), tables=tables)

taxi_zones_sm = models["taxi_zones"]
trips_sm = models["fhvhv_trips"]

if __name__ == "__main__":
    print("Available dimensions (taxi_zones):", list(taxi_zones_sm.get_dimensions()))
    print("Available measures (taxi_zones):", list(taxi_zones_sm.get_measures()))
    print("\nAvailable dimensions (fhvhv_trips):", list(trips_sm.get_dimensions()))
    print("Available measures (fhvhv_trips):", list(trips_sm.get_measures()))

    print("\n=== Trip Volume by Pickup Borough ===")
    expr = trips_sm.query(
        dimensions=["pickup_zone.borough"],
        measures=["trip_count", "avg_trip_miles", "avg_base_fare"],
        order_by=[("trip_count", "desc")],
        limit=5,
    )
    df = expr.execute()
    print("Top 5 boroughs by trip volume:")
    print(df)

    print("\n=== Popular Pickup Zones ===")
    expr_zones = trips_sm.query(
        dimensions=["pickup_zone.zone", "pickup_zone.borough"],
        measures=["trip_count", "avg_trip_miles", "total_revenue"],
        order_by=[("trip_count", "desc")],
        limit=10,
    )
    df_zones = expr_zones.execute()
    print("Top 10 pickup zones by trip count:")
    print(df_zones)

    print("\n=== Service Zone Analysis ===")
    expr_service = trips_sm.query(
        dimensions=["pickup_zone.service_zone"],
        measures=["trip_count", "avg_base_fare", "avg_tips", "shared_trip_rate"],
        order_by=[("trip_count", "desc")],
    )
    df_service = expr_service.execute()
    print("Trip metrics by service zone:")
    print(df_service)

    print("\n=== Revenue Analysis by Trip Distance ===")
    expr_revenue = trips_sm.query(
        dimensions=["pickup_zone.service_zone"],
        measures=["trip_count", "total_revenue", "avg_driver_pay", "avg_trip_miles"],
        order_by=[("total_revenue", "desc")],
        limit=5,
    )
    df_revenue = expr_revenue.execute()
    print("Revenue by service zone:")
    print(df_revenue)

    print("\n=== Accessibility Metrics ===")
    expr_access = trips_sm.query(
        dimensions=["pickup_zone.borough"],
        measures=["trip_count", "wheelchair_request_rate", "shared_trip_rate"],
        order_by=[("wheelchair_request_rate", "desc")],
    )
    df_access = expr_access.execute()
    print("Accessibility metrics by pickup borough:")
    print(df_access)

    # Charting example. PNG export requires the Altair backend (the default
    # ECharts backend only returns a spec, not rendered image bytes). Field names
    # are flattened for Vega-Lite, so "pickup_zone.borough" becomes
    # "pickup_zone_borough".
    png_bytes = expr.chart(
        backend="altair",
        format="png",
        spec={
            "title": {"text": "NYC Taxi Trip Volume by Borough"},
            "mark": {"type": "bar", "color": "#2E86AB"},
            "encoding": {
                "x": {
                    "field": "pickup_zone_borough",
                    "type": "nominal",
                    "sort": "-y",
                    "title": "Borough",
                },
                "y": {
                    "field": "trip_count",
                    "type": "quantitative",
                    "title": "Number of Trips",
                },
            },
            "width": 500,
            "height": 350,
        },
    )

    chart_path = PROJECT_ROOT / "trip-volume-by-pickup-borough-styled.png"
    chart_path.write_bytes(png_bytes)
    print(f"\nSaved chart to {chart_path}")
