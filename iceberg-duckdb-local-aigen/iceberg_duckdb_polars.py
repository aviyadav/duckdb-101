
import os
import shutil
import polars as pl
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField

def main():
    """
    This script demonstrates creating an Iceberg table, writing data to it using Polars,
    and reading data from it using DuckDB.
    """
    # Define paths and table name
    warehouse_path = os.path.join(os.getcwd(), "iceberg_warehouse")
    table_name = "default.my_iceberg_table"
    table_path = os.path.join(warehouse_path, table_name.replace(".", "/"))

    # Clean up previous runs
    if os.path.exists(warehouse_path):
        shutil.rmtree(warehouse_path)

    os.makedirs(warehouse_path, exist_ok=True)

    # Create a catalog
    catalog = load_catalog(
        "local",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": warehouse_path,
        }
    )

    # Create a schema
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )

    # Create a namespace
    catalog.create_namespace("default")

    # Create a table
    table = catalog.create_table(table_name, schema)

    # Create a Polars DataFrame with 100 random values
    import random
    import string
    
    random.seed(42)  # For reproducibility

    # Generate 100000 random IDs
    random_ids = [random.randint(1, 1000000) for _ in range(100000)]

    # Generate 100000 random names (random strings of 5-10 characters)
    random_names = [
        ''.join(random.choices(string.ascii_uppercase, k=random.randint(5, 10)))
        for _ in range(100000)
    ]
    
    data = pl.DataFrame(
        {
            "id": random_ids,
            "name": random_names,
        }
    ).with_columns(pl.col("id").cast(pl.Int32))

    # Write data to the Iceberg table
    # Convert to PyArrow table with proper schema matching
    arrow_data = data.to_arrow()
    table.append(arrow_data)

    # Read data with DuckDB
    con = duckdb.connect()
    arrow_table = table.scan().to_arrow()
    con.register('my_iceberg_table_arrow', arrow_table)
    result = con.execute(f"SELECT * FROM my_iceberg_table_arrow;").fetchdf()
    print("Data read from Iceberg table using DuckDB:")
    print(result)

    # Read data with Polars
    polars_df = pl.from_arrow(table.scan().to_arrow())
    print("\nData read from Iceberg table using Polars:")
    print(polars_df)

if __name__ == "__main__":
    main()
