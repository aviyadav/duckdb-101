import polars as pl
import xorq.api as xo
from boring_semantic_layer import Dimension, SemanticModel

df = pl.DataFrame(
    {
        "date": pl.date_range(
            pl.date(2025, 1, 1), pl.date(2025, 1, 5), "1d", eager=True
        ),
        "region": ["north", "south", "north", "east", "south"],
        "sales": [100, 200, 150, 300, 250],
    }
)

con = xo.connect()
tbl = con.create_table("sales", df)

sales_model = SemanticModel(
    table=tbl,
    dimensions={
        "region": lambda t: t.region,
        "date": Dimension(
            expr=lambda t: t.date,
            is_time_dimension=True,
            smallest_time_grain="TIME_GRAIN_DAY",
        ),
    },
    measures={
        "total_sales": lambda t: t.sales.sum(),
        "order_count": lambda t: t.sales.count(),
    },
)

# The installed boring-semantic-layer version does not expose a
# `SemanticModel.materialize()` cube API, so aggregate directly with a
# time grain and an explicit time range (acting as the "cutoff").
df_cube = sales_model.query(
    dimensions=["date", "region"],
    measures=["total_sales", "order_count"],
    time_grain="TIME_GRAIN_DAY",
    time_range={"start": "2025-01-01", "end": "2025-01-04"},
).execute()

print(df_cube)
