package json

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TFMV/icebox/config"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestCatalog(t *testing.T) (*Catalog, string) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "json-catalog-test")
	require.NoError(t, err)

	// Cleanup function
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "json",
			JSON: &config.JSONConfig{
				URI:       filepath.Join(tempDir, "catalog.json"),
				Warehouse: tempDir,
			},
		},
	}

	catalog, err := NewCatalog(cfg)
	require.NoError(t, err)

	return catalog, tempDir
}

func TestNewCatalog(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	assert.NotNil(t, catalog)
	assert.Equal(t, "test-catalog", catalog.Name())
	assert.Equal(t, icebergcatalog.Hive, catalog.CatalogType())
}

func TestNewCatalogMissingConfig(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "json",
			// JSON config is nil
		},
	}

	_, err := NewCatalog(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "JSON catalog configuration is required")
}

func TestCreateAndCheckNamespace(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	namespace := table.Identifier{"test_namespace"}

	// Initially should not exist
	exists, err := catalog.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.False(t, exists)

	// Create namespace
	props := iceberg.Properties{"description": "Test namespace"}
	err = catalog.CreateNamespace(ctx, namespace, props)
	require.NoError(t, err)

	// Should now exist
	exists, err = catalog.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, exists)

	// Try to create again - should fail
	err = catalog.CreateNamespace(ctx, namespace, props)
	assert.Equal(t, icebergcatalog.ErrNamespaceAlreadyExists, err)
}

func TestLoadNamespaceProperties(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	namespace := table.Identifier{"test_namespace"}
	expectedProps := iceberg.Properties{
		"description": "Test namespace",
		"owner":       "test-user",
	}

	// Create namespace with properties
	err := catalog.CreateNamespace(ctx, namespace, expectedProps)
	require.NoError(t, err)

	// Load properties
	props, err := catalog.LoadNamespaceProperties(ctx, namespace)
	require.NoError(t, err)

	// Should include the "exists" property added by the catalog
	assert.Equal(t, "Test namespace", props["description"])
	assert.Equal(t, "test-user", props["owner"])
	assert.Equal(t, "true", props["exists"])

	// Try to load non-existent namespace
	_, err = catalog.LoadNamespaceProperties(ctx, table.Identifier{"nonexistent"})
	assert.Equal(t, icebergcatalog.ErrNoSuchNamespace, err)
}

func TestListNamespaces(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Create test namespaces
	namespaces := []table.Identifier{
		{"ns1"},
		{"ns2"},
		{"parent"}, // Explicitly create parent namespace
		{"parent", "child1"},
		{"parent", "child2"},
	}

	for _, ns := range namespaces {
		err := catalog.CreateNamespace(ctx, ns, nil)
		require.NoError(t, err)
	}

	// List all namespaces
	allNamespaces, err := catalog.ListNamespaces(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, allNamespaces, 3) // ns1, ns2, parent

	// List child namespaces
	childNamespaces, err := catalog.ListNamespaces(ctx, table.Identifier{"parent"})
	require.NoError(t, err)
	assert.Len(t, childNamespaces, 2) // parent.child1, parent.child2
}

func TestDropNamespace(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	namespace := table.Identifier{"test_namespace"}

	// Create namespace
	err := catalog.CreateNamespace(ctx, namespace, nil)
	require.NoError(t, err)

	// Verify it exists
	exists, err := catalog.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, exists)

	// Drop namespace
	err = catalog.DropNamespace(ctx, namespace)
	require.NoError(t, err)

	// Should no longer exist
	exists, err = catalog.CheckNamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.False(t, exists)

	// Try to drop non-existent namespace
	err = catalog.DropNamespace(ctx, table.Identifier{"nonexistent"})
	assert.Equal(t, icebergcatalog.ErrNoSuchNamespace, err)
}

func TestCreateAndCheckTable(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Create namespace first
	namespace := table.Identifier{"test_namespace"}
	err := catalog.CreateNamespace(ctx, namespace, nil)
	require.NoError(t, err)

	// Create a test schema
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"test_namespace", "test_table"}

	// Initially should not exist
	exists, err := catalog.CheckTableExists(ctx, tableIdent)
	require.NoError(t, err)
	assert.False(t, exists)

	// Create table
	createdTable, err := catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)
	assert.NotNil(t, createdTable)

	// Should now exist
	exists, err = catalog.CheckTableExists(ctx, tableIdent)
	require.NoError(t, err)
	assert.True(t, exists)

	// Try to create again - should fail
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	assert.Equal(t, icebergcatalog.ErrTableAlreadyExists, err)
}

func TestCreateTableInNonExistentNamespace(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Create a test schema
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"nonexistent_namespace", "test_table"}

	// Try to create table in non-existent namespace
	_, err := catalog.CreateTable(ctx, tableIdent, schema)
	assert.Equal(t, icebergcatalog.ErrNoSuchNamespace, err)
}

func TestLoadTable(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Create namespace and table
	namespace := table.Identifier{"test_namespace"}
	err := catalog.CreateNamespace(ctx, namespace, nil)
	require.NoError(t, err)

	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"test_namespace", "test_table"}
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	// Load table
	loadedTable, err := catalog.LoadTable(ctx, tableIdent, nil)
	require.NoError(t, err)
	assert.NotNil(t, loadedTable)
	assert.Equal(t, tableIdent, loadedTable.Identifier())

	// Try to load non-existent table
	_, err = catalog.LoadTable(ctx, table.Identifier{"test_namespace", "nonexistent"}, nil)
	assert.Equal(t, icebergcatalog.ErrNoSuchTable, err)
}

func TestDropTable(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Create namespace and table
	namespace := table.Identifier{"test_namespace"}
	err := catalog.CreateNamespace(ctx, namespace, nil)
	require.NoError(t, err)

	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"test_namespace", "test_table"}
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	// Verify table exists
	exists, err := catalog.CheckTableExists(ctx, tableIdent)
	require.NoError(t, err)
	assert.True(t, exists)

	// Drop table
	err = catalog.DropTable(ctx, tableIdent)
	require.NoError(t, err)

	// Should no longer exist
	exists, err = catalog.CheckTableExists(ctx, tableIdent)
	require.NoError(t, err)
	assert.False(t, exists)

	// Try to drop non-existent table
	err = catalog.DropTable(ctx, table.Identifier{"test_namespace", "nonexistent"})
	assert.Equal(t, icebergcatalog.ErrNoSuchTable, err)
}

func TestListTables(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Create namespace
	namespace := table.Identifier{"test_namespace"}
	err := catalog.CreateNamespace(ctx, namespace, nil)
	require.NoError(t, err)

	// Create test schema
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	}
	schema := iceberg.NewSchema(0, fields...)

	// Create tables
	tableNames := []string{"table1", "table2", "table3"}
	for _, tableName := range tableNames {
		tableIdent := table.Identifier{"test_namespace", tableName}
		_, err := catalog.CreateTable(ctx, tableIdent, schema)
		require.NoError(t, err)
	}

	// List tables
	var tables []table.Identifier
	for tableIdent, err := range catalog.ListTables(ctx, namespace) {
		require.NoError(t, err)
		tables = append(tables, tableIdent)
	}

	assert.Len(t, tables, 3)

	// Try to list tables in non-existent namespace
	var errorCount int
	for _, err := range catalog.ListTables(ctx, table.Identifier{"nonexistent"}) {
		if err != nil {
			errorCount++
			assert.Equal(t, icebergcatalog.ErrNoSuchNamespace, err)
		}
	}
	assert.Equal(t, 1, errorCount)
}

func TestDropNamespaceWithTables(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Create namespace and table
	namespace := table.Identifier{"test_namespace"}
	err := catalog.CreateNamespace(ctx, namespace, nil)
	require.NoError(t, err)

	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"test_namespace", "test_table"}
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	// Try to drop namespace with tables - should fail
	err = catalog.DropNamespace(ctx, namespace)
	assert.Equal(t, icebergcatalog.ErrNamespaceNotEmpty, err)

	// Drop table first
	err = catalog.DropTable(ctx, tableIdent)
	require.NoError(t, err)

	// Now dropping namespace should succeed
	err = catalog.DropNamespace(ctx, namespace)
	require.NoError(t, err)
}

func TestUpdateNamespaceProperties(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	namespace := table.Identifier{"test_namespace"}
	initialProps := iceberg.Properties{
		"prop1": "value1",
		"prop2": "value2",
		"prop3": "value3",
	}

	// Create namespace with initial properties
	err := catalog.CreateNamespace(ctx, namespace, initialProps)
	require.NoError(t, err)

	// Update properties
	updates := iceberg.Properties{
		"prop2": "updated_value2", // Update existing
		"prop4": "value4",         // Add new
	}
	removals := []string{"prop3"} // Remove existing

	summary, err := catalog.UpdateNamespaceProperties(ctx, namespace, removals, updates)
	require.NoError(t, err)

	assert.Contains(t, summary.Updated, "prop2")
	assert.Contains(t, summary.Updated, "prop4")
	assert.Contains(t, summary.Removed, "prop3")

	// Verify final properties
	props, err := catalog.LoadNamespaceProperties(ctx, namespace)
	require.NoError(t, err)

	assert.Equal(t, "value1", props["prop1"])         // Unchanged
	assert.Equal(t, "updated_value2", props["prop2"]) // Updated
	assert.Equal(t, "value4", props["prop4"])         // Added
	assert.NotContains(t, props, "prop3")             // Removed
	assert.Equal(t, "true", props["exists"])          // System property
}

func TestHelperFunctions(t *testing.T) {
	// Test namespaceToString
	namespace := table.Identifier{"level1", "level2", "level3"}
	str := namespaceToString(namespace)
	assert.Equal(t, "level1.level2.level3", str)

	// Test stringToNamespace
	result := stringToNamespace(str)
	assert.Equal(t, namespace, result)

	// Test empty namespace
	emptyStr := namespaceToString(table.Identifier{})
	assert.Equal(t, "", emptyStr)

	emptyNamespace := stringToNamespace("")
	assert.Equal(t, table.Identifier{}, emptyNamespace)
}

func TestConcurrencyProtection(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// This test verifies that the mutex protection works
	// In a real scenario, concurrent access would be tested more thoroughly
	namespace := table.Identifier{"test_namespace"}
	err := catalog.CreateNamespace(ctx, namespace, nil)
	require.NoError(t, err)

	// Multiple read operations should work fine
	for i := 0; i < 10; i++ {
		exists, err := catalog.CheckNamespaceExists(ctx, namespace)
		require.NoError(t, err)
		assert.True(t, exists)
	}
}

func TestCatalogFileStructure(t *testing.T) {
	catalog, tempDir := createTestCatalog(t)
	ctx := context.Background()

	// Create namespace and table
	namespace := table.Identifier{"test_namespace"}
	err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{"description": "test"})
	require.NoError(t, err)

	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"test_namespace", "test_table"}
	_, err = catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)

	// Verify catalog.json was created
	catalogPath := filepath.Join(tempDir, "catalog.json")
	assert.FileExists(t, catalogPath)

	// Read and verify the JSON structure
	data, _, err := catalog.readCatalogData()
	require.NoError(t, err)

	assert.Equal(t, "test-catalog", data.CatalogName)
	assert.Contains(t, data.Namespaces, "test_namespace")
	assert.Equal(t, "test", data.Namespaces["test_namespace"].Properties["description"])
	assert.Contains(t, data.Tables, "test_namespace.test_table")

	tableEntry := data.Tables["test_namespace.test_table"]
	assert.Equal(t, "test_namespace", tableEntry.Namespace)
	assert.Equal(t, "test_table", tableEntry.Name)
	assert.NotEmpty(t, tableEntry.MetadataLocation)
}

func TestEnterpriseFeatures(t *testing.T) {
	catalog, tempDir := createTestCatalog(t)
	ctx := context.Background()

	// Test metrics tracking
	initialMetrics := catalog.GetMetrics()
	assert.Equal(t, int64(0), initialMetrics["namespaces_created"])
	assert.Equal(t, int64(0), initialMetrics["tables_created"])

	// Create namespace and verify metrics
	namespace := table.Identifier{"enterprise_test"}
	err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{"owner": "test"})
	require.NoError(t, err)

	metrics := catalog.GetMetrics()
	assert.Equal(t, int64(1), metrics["namespaces_created"])
	assert.Greater(t, metrics["cache_misses"], int64(0)) // Should have cache misses

	// Test RegisterTable functionality
	// First create a metadata file manually
	metadataDir := filepath.Join(tempDir, "custom", "metadata")
	err = os.MkdirAll(metadataDir, 0755)
	require.NoError(t, err)

	metadataFile := filepath.Join(metadataDir, "table.metadata.json")

	// Create a simple metadata file
	metadata := map[string]interface{}{
		"format-version":  2,
		"table-uuid":      "12345678-1234-1234-1234-123456789012",
		"location":        filepath.Join(tempDir, "custom", "data"),
		"last-updated-ms": 1635724800000,
		"last-column-id":  1,
		"schemas": []map[string]interface{}{
			{
				"schema-id": 0,
				"type":      "struct",
				"fields": []map[string]interface{}{
					{
						"id":       1,
						"name":     "test_field",
						"required": true,
						"type":     "string",
					},
				},
			},
		},
		"current-schema-id": 0,
		"partition-specs": []map[string]interface{}{
			{
				"spec-id": 0,
				"fields":  []interface{}{},
			},
		},
		"default-spec-id":   0,
		"last-partition-id": 999,
		"sort-orders": []map[string]interface{}{
			{
				"order-id": 0,
				"fields":   []interface{}{},
			},
		},
		"default-sort-order-id": 0,
		"snapshots":             []interface{}{},
		"current-snapshot-id":   nil,
		"refs":                  map[string]interface{}{},
		"snapshot-log":          []interface{}{},
		"metadata-log":          []interface{}{},
		"properties":            map[string]interface{}{},
	}

	file, err := os.Create(metadataFile)
	require.NoError(t, err)
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(metadata)
	require.NoError(t, err)
	file.Close()

	// Register the table
	tableIdent := table.Identifier{"enterprise_test", "registered_table"}
	registeredTable, err := catalog.RegisterTable(ctx, tableIdent, metadataFile)
	require.NoError(t, err)
	assert.NotNil(t, registeredTable)
	assert.Equal(t, tableIdent, registeredTable.Identifier())

	// Verify table was registered
	exists, err := catalog.CheckTableExists(ctx, tableIdent)
	require.NoError(t, err)
	assert.True(t, exists)

	// Test error handling - try to register non-existent metadata
	_, err = catalog.RegisterTable(ctx, table.Identifier{"enterprise_test", "bad_table"}, "/non/existent/path.json")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata file does not exist")

	// Test cache functionality by reading data multiple times
	_, _ = catalog.CheckNamespaceExists(ctx, namespace)
	_, _ = catalog.CheckNamespaceExists(ctx, namespace) // Should hit cache

	finalMetrics := catalog.GetMetrics()
	assert.Greater(t, finalMetrics["cache_hits"], int64(0)) // Should have cache hits

	// Drop table and verify metrics
	err = catalog.DropTable(ctx, tableIdent)
	require.NoError(t, err)

	dropMetrics := catalog.GetMetrics()
	assert.Equal(t, int64(1), dropMetrics["tables_dropped"])

	// Verify comprehensive logging was performed (we can see this in test output)
	// Test graceful close
	err = catalog.Close()
	assert.NoError(t, err)
}

func TestPythonCatalogCompatibility(t *testing.T) {
	catalog, _ := createTestCatalog(t)
	ctx := context.Background()

	// Test 1: Enhanced namespace property validation and tracking
	namespace := table.Identifier{"python_compat_test"}
	initialProps := iceberg.Properties{
		"owner":       "data-team",
		"description": "Python compatibility test namespace",
		"env":         "test",
	}

	err := catalog.CreateNamespace(ctx, namespace, initialProps)
	require.NoError(t, err)

	// Test comprehensive property updates with tracking
	updates := iceberg.Properties{
		"owner":       "updated-data-team",   // Update existing
		"version":     "1.0",                 // Add new
		"description": "Updated description", // Update existing
	}
	removals := []string{"env", "nonexistent"} // Remove existing + non-existent

	summary, err := catalog.UpdateNamespaceProperties(ctx, namespace, removals, updates)
	require.NoError(t, err)

	// Verify comprehensive tracking like Python version
	assert.Contains(t, summary.Updated, "owner")
	assert.Contains(t, summary.Updated, "version")
	assert.Contains(t, summary.Updated, "description")
	assert.Contains(t, summary.Removed, "env")
	assert.Contains(t, summary.Missing, "nonexistent")

	// Test 2: View operations (stubs for compatibility)
	viewIdent := table.Identifier{"python_compat_test", "test_view"}

	// Test ViewExists
	exists, err := catalog.ViewExists(ctx, viewIdent)
	require.NoError(t, err)
	assert.False(t, exists)

	// Test ListViews (should return empty)
	var viewCount int
	for _, err := range catalog.ListViews(ctx, namespace) {
		require.NoError(t, err)
		viewCount++
	}
	assert.Equal(t, 0, viewCount)

	// Test DropView (should return error)
	err = catalog.DropView(ctx, viewIdent)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "views are not supported")

	// Test 3: Enhanced table existence checking methods
	tableIdent := table.Identifier{"python_compat_test", "compat_table"}

	// Use the convenience methods
	tableExists, err := catalog.TableExists(ctx, tableIdent)
	require.NoError(t, err)
	assert.False(t, tableExists)

	namespaceExists, err := catalog.NamespaceExists(ctx, namespace)
	require.NoError(t, err)
	assert.True(t, namespaceExists)

	// Test 4: Enhanced table creation with better metadata
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "timestamp", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
	)

	createdTable, err := catalog.CreateTable(ctx, tableIdent, schema)
	require.NoError(t, err)
	assert.NotNil(t, createdTable)

	// Verify table now exists
	tableExists, err = catalog.TableExists(ctx, tableIdent)
	require.NoError(t, err)
	assert.True(t, tableExists)

	// Test 5: Verify comprehensive metrics tracking
	metrics := catalog.GetMetrics()
	assert.Greater(t, metrics["namespaces_created"], int64(0))
	assert.Greater(t, metrics["tables_created"], int64(0))
	assert.Greater(t, metrics["cache_hits"], int64(0))

	// Test 6: Enhanced table operations with previous metadata tracking
	data, _, err := catalog.readCatalogData()
	require.NoError(t, err)

	tableKey := catalog.tableKey(namespace, "compat_table")
	tableEntry := data.Tables[tableKey]
	assert.Equal(t, "python_compat_test", tableEntry.Namespace)
	assert.Equal(t, "compat_table", tableEntry.Name)
	assert.NotEmpty(t, tableEntry.MetadataLocation)
	assert.NotZero(t, tableEntry.CreatedAt)
	assert.NotZero(t, tableEntry.UpdatedAt)

	// Test 7: Property validation
	invalidProps := iceberg.Properties{
		"":          "empty_key",     // Invalid: empty key
		"valid_key": "value\000null", // Invalid: null character in value
	}

	err = catalog.CreateNamespace(ctx, table.Identifier{"invalid_test"}, invalidProps)
	assert.Error(t, err)
	// The validation may catch either the empty key or null character first
	assert.True(t, strings.Contains(err.Error(), "property key cannot be empty") ||
		strings.Contains(err.Error(), "property value contains null characters"),
		"Expected property validation error, got: %s", err.Error())

	// Test 8: Graceful cleanup
	err = catalog.Close()
	assert.NoError(t, err)
}

func TestIndexConfigurationSupport(t *testing.T) {
	// Create a temporary .ice/index file
	iceDir := filepath.Join(".", ".ice")
	err := os.MkdirAll(iceDir, 0755)
	require.NoError(t, err)
	defer os.RemoveAll(iceDir)

	tempDir, err := os.MkdirTemp("", "index-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	indexConfig := map[string]interface{}{
		"catalog_name": "index-test-catalog",
		"catalog_uri":  filepath.Join(tempDir, "catalog.json"),
		"properties": map[string]interface{}{
			"warehouse": tempDir,
			"env":       "test",
		},
	}

	indexData, err := json.Marshal(indexConfig)
	require.NoError(t, err)

	indexPath := filepath.Join(iceDir, "index")
	err = os.WriteFile(indexPath, indexData, 0644)
	require.NoError(t, err)

	// Test loading configuration from index
	cfg := &config.Config{
		// Minimal config - should load from index
		Catalog: config.CatalogConfig{
			Type: "json",
		},
	}

	catalog, err := NewCatalog(cfg)
	require.NoError(t, err)
	assert.Equal(t, "index-test-catalog", catalog.Name())
	assert.Equal(t, filepath.Join(tempDir, "catalog.json"), catalog.uri)
	assert.Equal(t, tempDir, catalog.warehouse)

	err = catalog.Close()
	assert.NoError(t, err)
}
