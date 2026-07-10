# Testing Guide

## Setup

Install the development dependencies:

```bash
pip install -e ".[dev]"
```

Or install pytest directly:

```bash
pip install pytest pytest-cov
```

## Running Tests

### Run all tests:
```bash
pytest test_iceberg_duckdb_polars.py -v
```

### Run tests with coverage:
```bash
pytest test_iceberg_duckdb_polars.py -v --cov=iceberg_duckdb_polars --cov-report=html
```

### Run specific test class:
```bash
pytest test_iceberg_duckdb_polars.py::TestIcebergCatalog -v
```

### Run specific test:
```bash
pytest test_iceberg_duckdb_polars.py::TestDataOperations::test_write_data_to_iceberg_table -v
```

## Test Structure

The test suite is organized into the following test classes:

1. **TestIcebergCatalog**: Tests for catalog and namespace operations
   - Catalog creation
   - Namespace creation
   - Table creation

2. **TestDataOperations**: Tests for data write and read operations
   - Writing Polars DataFrame to Iceberg
   - Reading with DuckDB
   - Reading with Polars
   - Multiple appends
   - Empty table scans

3. **TestDataTypes**: Tests for different data types and schemas
   - Nullable fields
   - Schema validation

4. **TestDuckDBQueries**: Tests for DuckDB query operations
   - SELECT queries
   - Aggregate queries
   - WHERE clauses

5. **TestPolarsOperations**: Tests for Polars-specific operations
   - Filtering
   - Transformations

6. **TestErrorHandling**: Tests for error scenarios
   - Duplicate table creation
   - Invalid namespaces

## Test Coverage

The tests cover:
- ✅ Iceberg table creation and schema definition
- ✅ Writing data from Polars to Iceberg
- ✅ Reading data with DuckDB
- ✅ Reading data with Polars
- ✅ Multiple data appends
- ✅ Null/nullable field handling
- ✅ SQL queries via DuckDB
- ✅ Polars data transformations
- ✅ Error handling and edge cases

## Fixtures

The test suite uses pytest fixtures for:
- `temp_warehouse`: Creates temporary warehouse directory
- `catalog`: Creates test Iceberg catalog
- `schema`: Provides test schema
- `sample_data`: Provides sample Polars DataFrame

All fixtures include automatic cleanup after tests.
