# Iceberg-DuckDB-Polars Integration

A demonstration project showcasing the integration of Apache Iceberg, DuckDB, and Polars for efficient data lake operations. This project demonstrates how to:
- Create and manage Iceberg tables
- Write data using Polars DataFrames
- Read and query data using DuckDB
- Perform analytics with Polars

## Features

- 🗄️ **Apache Iceberg**: Table format for large analytic datasets
- 🦆 **DuckDB**: In-process SQL OLAP database for analytics
- ⚡ **Polars**: Lightning-fast DataFrame library for Python
- 🔄 **PyArrow**: Zero-copy data interchange between tools
- 📊 **End-to-End Workflow**: Write with Polars, query with DuckDB

## Prerequisites

- Python 3.13 or higher
- pip or uv package manager

## Installation

### Option 1: Using pip

1. Clone the repository:
```bash
git clone <repository-url>
cd iceberg-duckdb-polars
```

2. Create a virtual environment (recommended):
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install the package:
```bash
pip install -e .
```

### Option 2: Using uv (faster)

```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e .
```

## Project Structure

```
iceberg-duckdb-polars/
├── iceberg_duckdb_polars.py    # Main script
├── test_iceberg_duckdb_polars.py  # Test suite
├── pyproject.toml              # Project configuration
├── README.md                   # This file
├── TESTING.md                  # Testing guide
└── iceberg_warehouse_001/      # Generated Iceberg warehouse (gitignored)
```

## Usage

### Running the Main Script

The main script demonstrates a complete workflow:

```bash
python iceberg_duckdb_polars.py
```

This will:
1. Create a local Iceberg catalog with SQLite backend
2. Define a schema with `id` (Integer) and `name` (String) fields
3. Create a namespace and table
4. Write sample data using Polars DataFrame
5. Read and query data using DuckDB
6. Read and display data using Polars

### Expected Output

```
Data read from Iceberg table using DuckDB:
   id     name
0   1    Alice
1   2      Bob
2   3  Charlie

Data read from Iceberg table using Polars:
shape: (3, 2)
┌─────┬─────────┐
│ id  ┆ name    │
│ --- ┆ ---     │
│ i32 ┆ str     │
╞═════╪═════════╡
│ 1   ┆ Alice   │
│ 2   ┆ Bob     │
│ 3   ┆ Charlie │
└─────┴─────────┘
```

## Code Examples

### Creating an Iceberg Table

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField

# Create catalog
catalog = load_catalog("local", **{
    "type": "sql",
    "uri": "sqlite:///warehouse/catalog.db",
    "warehouse": "warehouse_path"
})

# Define schema
schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)

# Create table
catalog.create_namespace("default")
table = catalog.create_table("default.my_table", schema)
```

### Writing Data with Polars

```python
import polars as pl

# Create DataFrame
data = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"]
}).with_columns(pl.col("id").cast(pl.Int32))

# Write to Iceberg
table.append(data.to_arrow())
```

### Querying with DuckDB

```python
import duckdb

con = duckdb.connect()
arrow_table = table.scan().to_arrow()
con.register('my_table', arrow_table)
result = con.execute("SELECT * FROM my_table WHERE id > 1").fetchdf()
```

### Reading with Polars

```python
polars_df = pl.from_arrow(table.scan().to_arrow())
filtered = polars_df.filter(pl.col("id") > 1)
```

## Testing

### Install Test Dependencies

```bash
pip install -e ".[dev]"
```

### Run All Tests

```bash
pytest test_iceberg_duckdb_polars.py -v
```

### Run with Coverage

```bash
pytest test_iceberg_duckdb_polars.py --cov=iceberg_duckdb_polars --cov-report=html
```

For detailed testing information, see [TESTING.md](TESTING.md).

## Architecture

```
┌─────────────┐
│   Polars    │  Write DataFrames
│  DataFrame  │─────────────┐
└─────────────┘             │
                            ▼
                    ┌──────────────┐
                    │   PyArrow    │  Zero-copy bridge
                    │    Table     │
                    └──────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │   Iceberg    │  Table format
                    │    Table     │  with metadata
                    └──────────────┘
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
        ┌──────────────┐        ┌──────────────┐
        │   DuckDB     │        │   Polars     │
        │  SQL Engine  │        │  DataFrame   │
        └──────────────┘        └──────────────┘
```

## Dependencies

- **pyiceberg** (≥0.10.0): Apache Iceberg Python implementation
- **polars** (≥1.35.1): Fast DataFrame library
- **duckdb** (≥1.4.1): In-process SQL OLAP database
- **pyarrow** (≥22.0.0): Columnar data format
- **sqlalchemy** (≥2.0.44): SQL toolkit for catalog
- **pandas** (≥2.3.3): Data manipulation (for DuckDB output)
- **numpy** (≥2.3.4): Numerical computing

## Troubleshooting

### Field Mismatch Errors

If you encounter "Mismatch in fields" errors:
- Ensure all fields in the schema have `required=False` for nullable fields
- Match the field types between Polars and Iceberg schema
- Cast Polars columns to the correct types (e.g., `.cast(pl.Int32)`)

### Import Errors

If you get module not found errors:
```bash
pip install --upgrade -e .
```

### Virtual Environment Issues

If the virtual environment is incomplete:
```bash
rm -rf .venv
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Performance Considerations

- **Polars**: Optimized for speed with lazy evaluation and parallel processing
- **DuckDB**: Vectorized query execution for fast analytics
- **Iceberg**: Efficient metadata handling for large-scale data lakes
- **PyArrow**: Zero-copy data sharing between components

## Use Cases

1. **Data Lake Management**: Use Iceberg for ACID transactions and time travel
2. **Fast Analytics**: Leverage DuckDB for SQL queries on Iceberg tables
3. **ETL Pipelines**: Use Polars for high-performance data transformations
4. **Multi-Tool Integration**: Combine the strengths of all three tools

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

[Add your license here]

## Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Polars Documentation](https://pola.rs/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)

## Contact

[Add your contact information here]



[CMD]
duckdb
INSTALL iceberg;
LOAD iceberg;

SET unsafe_enable_version_guessing = true;

SELECT * FROM iceberg_scan('iceberg_warehouse/default/my_iceberg_table', allow_moved_paths = true);