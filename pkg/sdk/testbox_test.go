package sdk

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTestBox(t *testing.T) {
	// Test basic creation
	testBox := NewTestBox(t)
	assert.NotNil(t, testBox)
	assert.NotNil(t, testBox.GetConfig())
	assert.NotNil(t, testBox.GetCatalog())
	assert.NotNil(t, testBox.GetEngine())
	assert.NotNil(t, testBox.GetMemoryFS())
}

func TestNewTestBoxWithOptions(t *testing.T) {
	// Test with custom name
	testBox := NewTestBox(t, WithName("custom-test-catalog"))
	config := testBox.GetConfig()
	assert.Equal(t, "custom-test-catalog", config.Name)

	// Test with filesystem storage
	testBox2 := NewTestBox(t, WithFileSystem())
	assert.Nil(t, testBox2.GetMemoryFS()) // Should be nil when using filesystem
}

func TestNewTestBoxWithDefaults(t *testing.T) {
	// Test CI defaults
	testBox := NewTestBox(t, WithDefaults(CIDefaults))
	assert.NotNil(t, testBox)
	assert.NotNil(t, testBox.GetMemoryFS()) // Should use memory FS for CI

	// Test with memory limit
	testBox2 := NewTestBox(t, WithMemoryLimit("2GB"))
	assert.NotNil(t, testBox2)
}

func TestCreateNamespace(t *testing.T) {
	testBox := NewTestBox(t)

	// Create a namespace
	namespace := testBox.CreateNamespace("test_ns")
	assert.Equal(t, "test_ns", namespace[0])

	// Verify namespace exists in catalog
	exists, err := testBox.GetCatalog().CheckNamespaceExists(testBox.t.Context(), namespace)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestCreateTable(t *testing.T) {
	testBox := NewTestBox(t)

	// Create namespace first
	testBox.CreateNamespace("test_ns")

	// Create table with default schema
	table := testBox.CreateTable("test_ns", "test_table")
	assert.NotNil(t, table)
	assert.Equal(t, "test_ns", table.Identifier()[0])
	assert.Equal(t, "test_table", table.Identifier()[1])

	// Verify table exists in catalog
	exists, err := testBox.GetCatalog().CheckTableExists(testBox.t.Context(), table.Identifier())
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestCreateTableWithCustomSchema(t *testing.T) {
	testBox := NewTestBox(t)

	// Create namespace
	testBox.CreateNamespace("test_ns")

	// Create custom schema
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       1,
			Name:     "user_id",
			Type:     iceberg.PrimitiveTypes.Int64,
			Required: true,
		},
		iceberg.NestedField{
			ID:       2,
			Name:     "email",
			Type:     iceberg.PrimitiveTypes.String,
			Required: false,
		},
	)

	// Create table with custom schema
	table := testBox.CreateTable("test_ns", "users", schema)
	assert.NotNil(t, table)

	// Verify schema
	tableSchema := table.Schema()
	assert.Len(t, tableSchema.Fields(), 2)
	assert.Equal(t, "user_id", tableSchema.Fields()[0].Name)
	assert.Equal(t, "email", tableSchema.Fields()[1].Name)
}

func TestExecuteSQL(t *testing.T) {
	testBox := NewTestBox(t)

	// Create namespace and table
	testBox.CreateNamespace("test_ns")
	table := testBox.CreateTable("test_ns", "test_table")

	// Register table with engine
	testBox.RegisterTable(table)

	// Execute basic SQL query
	result := testBox.MustExecuteSQL("SELECT COUNT(*) FROM test_ns_test_table")
	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.RowCount)
}

func TestExecuteSQLWithError(t *testing.T) {
	testBox := NewTestBox(t)

	// Test error handling
	_, err := testBox.ExecuteSQL("SELECT * FROM nonexistent_table")
	assert.Error(t, err)
}

func TestRegisterTable(t *testing.T) {
	testBox := NewTestBox(t)

	// Create namespace and table
	testBox.CreateNamespace("test_ns")
	table := testBox.CreateTable("test_ns", "test_table")

	// Register table
	testBox.RegisterTable(table)

	// Verify table is registered by checking engine
	tables, err := testBox.GetEngine().ListTables(testBox.t.Context())
	require.NoError(t, err)
	assert.Contains(t, tables, "test_ns_test_table")
}

func TestCleanup(t *testing.T) {
	// This test ensures cleanup works without panicking
	testBox := NewTestBox(t)

	// Use the test box
	testBox.CreateNamespace("cleanup_test")
	testBox.CreateTable("cleanup_test", "table1")

	// Cleanup should happen automatically via t.Cleanup()
	// If there are any issues, they would show up as test failures
}

func TestMemoryFileSystemIntegration(t *testing.T) {
	testBox := NewTestBox(t, WithDefaults(CIDefaults))

	// Get memory filesystem
	memFS := testBox.GetMemoryFS()
	require.NotNil(t, memFS)

	// Test basic file operations
	err := memFS.WriteFile("/test.txt", []byte("Hello, TestBox!"))
	assert.NoError(t, err)

	// Read file back
	data, err := memFS.ReadFile("/test.txt")
	assert.NoError(t, err)
	assert.Equal(t, "Hello, TestBox!", string(data))

	// Check file exists
	exists, err := memFS.Exists("/test.txt")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestWithPropertyOption(t *testing.T) {
	testBox := NewTestBox(t,
		WithProperty("test.key1", "value1"),
		WithProperty("test.key2", "value2"),
	)

	// Test that testbox was created successfully
	assert.NotNil(t, testBox)
	// Properties are stored in the configuration but may not be directly accessible
	// The important thing is that the option doesn't cause errors
}

func TestMultipleOptions(t *testing.T) {
	testBox := NewTestBox(t,
		WithName("multi-option-test"),
		WithDefaults(FastDefaults),
		WithMemoryLimit("1GB"),
		WithProperty("custom.setting", "test-value"),
	)

	assert.NotNil(t, testBox)
	assert.Equal(t, "multi-option-test", testBox.GetConfig().Name)
}

// Test that demonstrates the intended usage pattern from the design document
func TestDesignDocumentUsagePattern(t *testing.T) {
	// This mimics: testBox := sdk.NewTestBox(t)
	testBox := NewTestBox(t)

	// Create some test data
	namespace := testBox.CreateNamespace("analytics")
	table := testBox.CreateTable("analytics", "events")

	// Verify namespace was created
	assert.Equal(t, "analytics", namespace[0])

	// Register for querying
	testBox.RegisterTable(table)

	// Execute queries
	result := testBox.MustExecuteSQL("SHOW TABLES")
	assert.NotNil(t, result)
	assert.True(t, result.RowCount >= 1)

	// Test with CI-friendly configuration
	ciTestBox := NewTestBox(t, WithDefaults(CIDefaults))
	assert.NotNil(t, ciTestBox.GetMemoryFS())
}

// Benchmark to ensure TestBox creation is fast enough for CI
func BenchmarkNewTestBox(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testBox := NewTestBox(&testing.T{}, WithDefaults(FastDefaults))
		_ = testBox
	}
}
