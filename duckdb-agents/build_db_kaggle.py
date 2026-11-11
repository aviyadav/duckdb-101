import pathlib

import duckdb
import kagglehub

path = pathlib.Path(kagglehub.dataset_download("olistbr/brazilian-ecommerce"))
print(f"dataset from Kaggle: {path}")

con = duckdb.connect("olist.duckdb")

for file in sorted(path.rglob("*.csv")):
    name = file.stem.replace("olist_", "").replace("_dataset", "")
    con.execute(
        f'CREATE OR REPLACE TABLE "{name}" AS SELECT * FROM read_csv_auto(?)',
        [str(file)],
    )

    count = con.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
    print(f"{name}: {count} rows")

con.close()
print("\n✅ Done!")
