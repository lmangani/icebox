package duckdb

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEngine(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	// Create catalog
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	// Create engine
	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	assert.NotNil(t, engine)
	assert.NotNil(t, engine.db)
	assert.NotNil(t, engine.catalog)
	assert.NotNil(t, engine.allocator)
}

func TestEngineBasicQueries(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	// Create catalog and engine
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Test basic DuckDB functionality
	result, err := engine.ExecuteQuery(ctx, "SELECT 1 as test_column")
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowCount)
	assert.Equal(t, []string{"test_column"}, result.Columns)
	assert.Equal(t, 1, len(result.Rows))
}

func TestEngineListTables(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	// Create catalog and engine
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Initially should have no tables
	tables, err := engine.ListTables(ctx)
	require.NoError(t, err)
	assert.Empty(t, tables)
}

func TestEngineWithIcebergTable(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	// Create catalog and engine
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Create a namespace
	namespace := table.Identifier{"test_namespace"}
	err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{"description": "Test namespace"})
	require.NoError(t, err)

	// Create a test schema
	fields := []iceberg.NestedField{
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "amount", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	}
	schema := iceberg.NewSchema(0, fields...)

	// Create table
	tableIdent := table.Identifier{"test_namespace", "test_table"}
	icebergTable, err := catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	// Register table with engine
	err = engine.RegisterTable(ctx, tableIdent, icebergTable)
	require.NoError(t, err)

	// List tables should now show our table
	tables, err := engine.ListTables(ctx)
	require.NoError(t, err)
	assert.Contains(t, tables, "test_namespace_test_table")

	// Describe the table
	result, err := engine.DescribeTable(ctx, "test_namespace_test_table")
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.RowCount > 0)
}

func TestEngineTypeConversion(t *testing.T) {
	engine := &Engine{}

	tests := []struct {
		icebergType iceberg.Type
		expectError bool
	}{
		{iceberg.PrimitiveTypes.Bool, false},
		{iceberg.PrimitiveTypes.Int32, false},
		{iceberg.PrimitiveTypes.Int64, false},
		{iceberg.PrimitiveTypes.Float32, false},
		{iceberg.PrimitiveTypes.Float64, false},
		{iceberg.PrimitiveTypes.String, false},
		{iceberg.PrimitiveTypes.Date, false},
		{iceberg.PrimitiveTypes.Timestamp, false},
	}

	for _, test := range tests {
		_, err := engine.convertIcebergTypeToArrow(test.icebergType)
		if test.expectError {
			assert.Error(t, err, "Expected error for type %v", test.icebergType)
		} else {
			assert.NoError(t, err, "Expected no error for type %v", test.icebergType)
		}
	}
}

func TestEngineIdentifierToTableName(t *testing.T) {
	engine := &Engine{}

	tests := []struct {
		identifier table.Identifier
		expected   string
	}{
		{table.Identifier{"default", "users"}, "default_users"},
		{table.Identifier{"analytics", "events"}, "analytics_events"},
		{table.Identifier{"warehouse", "inventory", "products"}, "warehouse_inventory_products"},
	}

	for _, test := range tests {
		result := engine.identifierToTableName(test.identifier)
		assert.Equal(t, test.expected, result)
	}
}

func TestEngineQuoteName(t *testing.T) {
	engine := &Engine{}

	tests := []struct {
		name     string
		expected string
	}{
		{"simple", `"simple"`},
		{"with space", `"with space"`},
		{"with\"quote", `"with""quote"`},
		{"table_name", `"table_name"`},
	}

	for _, test := range tests {
		result := engine.quoteName(test.name)
		assert.Equal(t, test.expected, result)
	}
}

func TestEngineClose(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	// Create catalog and engine
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(t, err)

	// Close should work without error
	err = engine.Close()
	assert.NoError(t, err)

	// Multiple closes should be safe
	err = engine.Close()
	assert.NoError(t, err)
}

func TestEngineQueryErrorHandling(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	// Create catalog and engine
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	ctx := context.Background()

	// Test invalid SQL
	_, err = engine.ExecuteQuery(ctx, "INVALID SQL STATEMENT")
	assert.Error(t, err)

	// Test query with non-existent table
	_, err = engine.ExecuteQuery(ctx, "SELECT * FROM non_existent_table")
	assert.Error(t, err)
}

// Test that demonstrates the engine structure is ready for full implementation
func TestEngineStructure(t *testing.T) {
	// Create test configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			SQLite: &config.SQLiteConfig{
				Path: ":memory:",
			},
		},
		Storage: config.StorageConfig{
			FileSystem: &config.FileSystemConfig{
				RootPath: "/tmp/test-warehouse",
			},
		},
	}

	// Create catalog and engine
	catalog, err := sqlite.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	engine, err := NewEngine(catalog)
	require.NoError(t, err)
	defer engine.Close()

	// Verify all expected methods exist (this will compile if the structure is correct)
	assert.NotNil(t, engine.ExecuteQuery)
	assert.NotNil(t, engine.RegisterTable)
	assert.NotNil(t, engine.ListTables)
	assert.NotNil(t, engine.DescribeTable)
	assert.NotNil(t, engine.Close)

	// Verify internal methods exist
	assert.NotNil(t, engine.convertIcebergTypeToArrow)
	assert.NotNil(t, engine.identifierToTableName)
	assert.NotNil(t, engine.quoteName)
}
