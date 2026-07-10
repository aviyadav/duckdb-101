import os
import shutil
import tempfile
import pytest
import polars as pl
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField


@pytest.fixture
def temp_warehouse():
    """Create a temporary warehouse directory for testing."""
    temp_dir = tempfile.mkdtemp()
    warehouse_path = os.path.join(temp_dir, "test_iceberg_warehouse")
    os.makedirs(warehouse_path, exist_ok=True)
    yield warehouse_path
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def catalog(temp_warehouse):
    """Create a test catalog."""
    catalog = load_catalog(
        "test_catalog",
        **{
            "type": "sql",
            "uri": f"sqlite:///{temp_warehouse}/pyiceberg_catalog.db",
            "warehouse": temp_warehouse,
        }
    )
    return catalog


@pytest.fixture
def schema():
    """Create a test schema."""
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )


@pytest.fixture
def sample_data():
    """Create sample Polars DataFrame."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    ).with_columns(pl.col("id").cast(pl.Int32))


class TestIcebergCatalog:
    """Tests for Iceberg catalog operations."""

    def test_catalog_creation(self, temp_warehouse):
        """Test that catalog can be created successfully."""
        catalog = load_catalog(
            "test_catalog",
            **{
                "type": "sql",
                "uri": f"sqlite:///{temp_warehouse}/pyiceberg_catalog.db",
                "warehouse": temp_warehouse,
            }
        )
        assert catalog is not None
        assert catalog.name == "test_catalog"

    def test_namespace_creation(self, catalog):
        """Test that namespace can be created."""
        catalog.create_namespace("test_namespace")
        namespaces = catalog.list_namespaces()
        assert ("test_namespace",) in namespaces

    def test_table_creation(self, catalog, schema):
        """Test that table can be created with schema."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.test_table", schema)
        assert table is not None
        assert table.name() == ("default", "test_table")
        assert len(table.schema().fields) == 2


class TestDataOperations:
    """Tests for data write and read operations."""

    def test_write_data_to_iceberg_table(self, catalog, schema, sample_data):
        """Test writing Polars DataFrame to Iceberg table."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.write_test", schema)
        
        # Write data
        arrow_data = sample_data.to_arrow()
        table.append(arrow_data)
        
        # Verify data was written
        scan_result = table.scan().to_arrow()
        assert scan_result.num_rows == 3

    def test_read_data_with_duckdb(self, catalog, schema, sample_data):
        """Test reading Iceberg table data with DuckDB."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.duckdb_test", schema)
        
        # Write data
        arrow_data = sample_data.to_arrow()
        table.append(arrow_data)
        
        # Read with DuckDB
        con = duckdb.connect()
        arrow_table = table.scan().to_arrow()
        con.register('test_table', arrow_table)
        result = con.execute("SELECT * FROM test_table ORDER BY id;").fetchdf()
        
        assert len(result) == 3
        assert result['id'].tolist() == [1, 2, 3]
        assert result['name'].tolist() == ["Alice", "Bob", "Charlie"]

    def test_read_data_with_polars(self, catalog, schema, sample_data):
        """Test reading Iceberg table data with Polars."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.polars_test", schema)
        
        # Write data
        arrow_data = sample_data.to_arrow()
        table.append(arrow_data)
        
        # Read with Polars
        polars_df = pl.from_arrow(table.scan().to_arrow())
        
        assert polars_df.height == 3
        assert polars_df['id'].to_list() == [1, 2, 3]
        assert polars_df['name'].to_list() == ["Alice", "Bob", "Charlie"]

    def test_multiple_appends(self, catalog, schema):
        """Test appending data multiple times."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.append_test", schema)
        
        # First append
        data1 = pl.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"]
        }).with_columns(pl.col("id").cast(pl.Int32))
        table.append(data1.to_arrow())
        
        # Second append
        data2 = pl.DataFrame({
            "id": [3, 4],
            "name": ["Charlie", "David"]
        }).with_columns(pl.col("id").cast(pl.Int32))
        table.append(data2.to_arrow())
        
        # Verify total records
        scan_result = table.scan().to_arrow()
        assert scan_result.num_rows == 4

    def test_empty_table_scan(self, catalog, schema):
        """Test scanning an empty table."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.empty_test", schema)
        
        # Scan without any data
        scan_result = table.scan().to_arrow()
        assert scan_result.num_rows == 0


class TestDataTypes:
    """Tests for different data types and schemas."""

    def test_nullable_fields(self, catalog):
        """Test table with nullable fields."""
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )
        
        catalog.create_namespace("default")
        table = catalog.create_table("default.nullable_test", schema)
        
        # Data with null values
        data = pl.DataFrame({
            "id": [1, None, 3],
            "name": ["Alice", "Bob", None]
        }).with_columns(pl.col("id").cast(pl.Int32))
        
        table.append(data.to_arrow())
        
        # Verify data
        result = pl.from_arrow(table.scan().to_arrow())
        assert result.height == 3
        assert result['id'][1] is None
        assert result['name'][2] is None

    def test_schema_fields_match(self, catalog, schema):
        """Test that schema fields match after table creation."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.schema_test", schema)
        
        table_schema = table.schema()
        assert len(table_schema.fields) == 2
        assert table_schema.fields[0].name == "id"
        assert table_schema.fields[1].name == "name"


class TestDuckDBQueries:
    """Tests for DuckDB query operations."""

    def test_duckdb_select_query(self, catalog, schema, sample_data):
        """Test SELECT query with DuckDB."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.query_test", schema)
        table.append(sample_data.to_arrow())
        
        con = duckdb.connect()
        arrow_table = table.scan().to_arrow()
        con.register('query_table', arrow_table)
        
        result = con.execute("SELECT name FROM query_table WHERE id = 2;").fetchdf()
        assert len(result) == 1
        assert result['name'].iloc[0] == "Bob"

    def test_duckdb_aggregate_query(self, catalog, schema, sample_data):
        """Test aggregate query with DuckDB."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.agg_test", schema)
        table.append(sample_data.to_arrow())
        
        con = duckdb.connect()
        arrow_table = table.scan().to_arrow()
        con.register('agg_table', arrow_table)
        
        result = con.execute("SELECT COUNT(*) as cnt FROM agg_table;").fetchdf()
        assert result['cnt'].iloc[0] == 3


class TestPolarsOperations:
    """Tests for Polars-specific operations."""

    def test_polars_filter(self, catalog, schema, sample_data):
        """Test filtering data with Polars after reading from Iceberg."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.filter_test", schema)
        table.append(sample_data.to_arrow())
        
        polars_df = pl.from_arrow(table.scan().to_arrow())
        filtered = polars_df.filter(pl.col("id") > 1)
        
        assert filtered.height == 2
        assert filtered['id'].to_list() == [2, 3]

    def test_polars_transform(self, catalog, schema, sample_data):
        """Test transforming data with Polars after reading from Iceberg."""
        catalog.create_namespace("default")
        table = catalog.create_table("default.transform_test", schema)
        table.append(sample_data.to_arrow())
        
        polars_df = pl.from_arrow(table.scan().to_arrow())
        transformed = polars_df.with_columns(
            pl.col("name").str.to_uppercase().alias("name_upper")
        )
        
        assert "name_upper" in transformed.columns
        assert transformed['name_upper'].to_list() == ["ALICE", "BOB", "CHARLIE"]


class TestErrorHandling:
    """Tests for error handling scenarios."""

    def test_duplicate_table_creation(self, catalog, schema):
        """Test that creating duplicate table raises error."""
        catalog.create_namespace("default")
        catalog.create_table("default.dup_test", schema)
        
        with pytest.raises(Exception):
            catalog.create_table("default.dup_test", schema)

    def test_invalid_namespace(self, catalog, schema):
        """Test creating table in non-existent namespace."""
        with pytest.raises(Exception):
            catalog.create_table("nonexistent.table", schema)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
