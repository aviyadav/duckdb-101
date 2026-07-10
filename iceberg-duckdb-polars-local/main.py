import duckdb
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
import uuid
from datetime import date
import numpy as np
import pyarrow
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

warehouse_path = "./icehouse"
catalog = SqlCatalog(
    "default",
    **{
        "uri": f"sqlite:///{warehouse_path}/icyhot.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

try:
    catalog.create_namespace("dummy_data")
except NamespaceAlreadyExistsError:
    pass  # Namespace already exists, continue

rows = 5000

# duckdb

sql = f"""
    select t.row_id, uuid() as txn_key, current_date as rpt_dt, round(random() * 100, 2) as some_val
    from generate_series(1, {rows}) t(row_id)
"""

duck_df = duckdb.execute(sql).arrow()

# polars
polars_df = pl.DataFrame({
    'row_id': pl.arange(0, rows, eager=True),
    'rpt_dt': pl.Series([date.today()] * rows),
    'some_val': pl.Series(np.floor(np.random.rand(rows) * 100).astype(int)),
    'txn_key': pl.Series([str(uuid.uuid4()) for _ in range(rows)])
}).to_arrow()


try:
    table_ducks = catalog.create_table("dummy_data.duckdb_data", schema = duck_df.schema)
except TableAlreadyExistsError:
    table_ducks = catalog.load_table("dummy_data.duckdb_data")
try:
    table_polars = catalog.create_table("dummy_data.polars_data", schema = polars_df.schema)
except TableAlreadyExistsError:
    table_polars = catalog.load_table("dummy_data.polars_data")

table_ducks.append(duck_df)
table_polars.append(polars_df)

print(f'duckdb table has {len(table_ducks.scan().to_arrow())} rows')
print(f'polars table has {len(table_polars.scan().to_arrow())} rows')


# read back
cn = table_ducks.scan().to_duckdb(table_name="duck_back")
row_count_duck = cn.sql("select count(row_id) as tot_rows, sum(some_val) as agg_val from duck_back")
print(f"Row count in duck db {row_count_duck}")

cn = duckdb.connect()
cn.execute("install iceberg; load iceberg; SET unsafe_enable_version_guessing = true;")

# The following section using DuckDB's iceberg_scan is commented out.
# As of now, DuckDB's iceberg_scan does not reliably support Iceberg tables written with PyIceberg due to metadata/manifest format/version differences.
# Reading/writing with Polars or Arrow, or using PyIceberg, works fine. Re-enable this when compatibility improves.
'''
sql = """
select *
from iceberg_scan('./icehouse/dummy_data.db/duckdb_data')
"""
data = cn.sql(sql)
print(f"DuckDB reading from iceberg {data}")
'''


# read back in polars
df = pl.scan_iceberg(table_polars).collect()

pol_data = df.select(
    pl.col('row_id').count().alias("tot_rows")
    , pl.col('some_val').sum().alias('agg_val')
)

print(pol_data)
