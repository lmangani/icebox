package importer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TFMV/icebox/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

func TestNewParquetImporter(t *testing.T) {
	cfg := createTestConfig(t)

	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	if importer.config != cfg {
		t.Error("Expected config to be set")
	}

	if importer.catalog == nil {
		t.Error("Expected catalog to be initialized")
	}
}

func TestInferSchema(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	// Create a dummy Parquet file for testing
	parquetFile := createDummyParquetFile(t, "sales_data.parquet", "test sales data")

	schema, stats, err := importer.InferSchema(parquetFile)
	if err != nil {
		t.Fatalf("Failed to infer schema: %v", err)
	}

	// Verify schema
	if schema == nil {
		t.Fatal("Expected schema to be returned")
	}

	if len(schema.Fields) == 0 {
		t.Error("Expected schema to have fields")
	}

	// Should infer sales schema based on filename
	expectedFields := []string{"id", "customer_id", "product_id", "amount", "sale_date"}
	if len(schema.Fields) != len(expectedFields) {
		t.Errorf("Expected %d fields, got %d", len(expectedFields), len(schema.Fields))
	}

	for i, field := range schema.Fields {
		if field.Name != expectedFields[i] {
			t.Errorf("Expected field %d to be '%s', got '%s'", i, expectedFields[i], field.Name)
		}
	}

	// Verify stats
	if stats == nil {
		t.Fatal("Expected stats to be returned")
	}

	if stats.RecordCount <= 0 {
		t.Error("Expected positive record count")
	}

	if stats.FileSize <= 0 {
		t.Error("Expected positive file size")
	}

	if stats.ColumnCount != len(schema.Fields) {
		t.Errorf("Expected column count %d, got %d", len(schema.Fields), stats.ColumnCount)
	}
}

func TestInferSchemaUserFile(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	// Create a dummy user Parquet file
	parquetFile := createDummyParquetFile(t, "user_data.parquet", "test user data")

	schema, _, err := importer.InferSchema(parquetFile)
	if err != nil {
		t.Fatalf("Failed to infer schema: %v", err)
	}

	// Should infer user schema based on filename
	expectedFields := []string{"id", "name", "email", "created_at"}
	if len(schema.Fields) != len(expectedFields) {
		t.Errorf("Expected %d fields, got %d", len(expectedFields), len(schema.Fields))
	}

	for i, field := range schema.Fields {
		if field.Name != expectedFields[i] {
			t.Errorf("Expected field %d to be '%s', got '%s'", i, expectedFields[i], field.Name)
		}
	}
}

func TestInferSchemaGenericFile(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	// Create a generic Parquet file
	parquetFile := createDummyParquetFile(t, "data.parquet", "test generic data")

	schema, _, err := importer.InferSchema(parquetFile)
	if err != nil {
		t.Fatalf("Failed to infer schema: %v", err)
	}

	// Should infer generic schema
	expectedFields := []string{"id", "data", "timestamp"}
	if len(schema.Fields) != len(expectedFields) {
		t.Errorf("Expected %d fields, got %d", len(expectedFields), len(schema.Fields))
	}

	for i, field := range schema.Fields {
		if field.Name != expectedFields[i] {
			t.Errorf("Expected field %d to be '%s', got '%s'", i, expectedFields[i], field.Name)
		}
	}
}

func TestGetTableLocation(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	tableIdent := table.Identifier{"test_namespace", "test_table"}
	location := importer.GetTableLocation(tableIdent)

	expectedPrefix := "file://"
	if !filepath.IsAbs(location) && !filepath.HasPrefix(location, expectedPrefix) {
		t.Errorf("Expected location to start with %s or be absolute, got: %s", expectedPrefix, location)
	}

	// Should contain namespace and table name
	if !strings.Contains(location, "test_namespace") {
		t.Errorf("Expected location to contain namespace, got: %s", location)
	}

	if !strings.Contains(location, "test_table") {
		t.Errorf("Expected location to contain table name, got: %s", location)
	}
}

func TestImportTable(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	ctx := context.Background()

	// Create a dummy Parquet file
	parquetFile := createDummyParquetFile(t, "test_data.parquet", "test data content")

	// Infer schema
	schema, _, err := importer.InferSchema(parquetFile)
	if err != nil {
		t.Fatalf("Failed to infer schema: %v", err)
	}

	// Import the table
	req := ImportRequest{
		ParquetFile:    parquetFile,
		TableIdent:     table.Identifier{"test_ns", "test_table"},
		NamespaceIdent: table.Identifier{"test_ns"},
		Schema:         schema,
		Overwrite:      false,
		PartitionBy:    nil,
	}

	result, err := importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import table: %v", err)
	}

	// Verify result
	if result == nil {
		t.Fatal("Expected import result to be returned")
	}

	if len(result.TableIdent) != 2 || result.TableIdent[0] != "test_ns" || result.TableIdent[1] != "test_table" {
		t.Errorf("Expected table identifier [test_ns, test_table], got %v", result.TableIdent)
	}

	if result.RecordCount <= 0 {
		t.Error("Expected positive record count")
	}

	if result.DataSize <= 0 {
		t.Error("Expected positive data size")
	}

	if result.TableLocation == "" {
		t.Error("Expected table location to be set")
	}

	// Verify table was created in catalog
	exists, err := importer.catalog.CheckTableExists(ctx, req.TableIdent)
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}
	if !exists {
		t.Error("Expected table to exist in catalog")
	}

	// Verify namespace was created
	nsExists, err := importer.catalog.CheckNamespaceExists(ctx, req.NamespaceIdent)
	if err != nil {
		t.Fatalf("Failed to check namespace existence: %v", err)
	}
	if !nsExists {
		t.Error("Expected namespace to exist in catalog")
	}

	// Verify data file was copied
	dataDir := filepath.Join(cfg.Storage.FileSystem.RootPath, "test_ns", "test_table", "data")
	dataFile := filepath.Join(dataDir, "part-00000.parquet")
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Error("Expected data file to be copied to table location")
	}
}

func TestImportTableWithExistingNamespace(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	ctx := context.Background()

	// Create namespace first
	namespaceIdent := table.Identifier{"existing_ns"}
	err = importer.catalog.CreateNamespace(ctx, namespaceIdent, iceberg.Properties{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	// Create a dummy Parquet file
	parquetFile := createDummyParquetFile(t, "test_data.parquet", "test data content")

	// Infer schema
	schema, _, err := importer.InferSchema(parquetFile)
	if err != nil {
		t.Fatalf("Failed to infer schema: %v", err)
	}

	// Import the table
	req := ImportRequest{
		ParquetFile:    parquetFile,
		TableIdent:     table.Identifier{"existing_ns", "test_table"},
		NamespaceIdent: namespaceIdent,
		Schema:         schema,
		Overwrite:      false,
		PartitionBy:    nil,
	}

	_, err = importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import table: %v", err)
	}

	// Should not create duplicate namespace
	namespaces, err := importer.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to list namespaces: %v", err)
	}

	// Should have exactly one namespace
	count := 0
	for _, ns := range namespaces {
		if len(ns) == 1 && ns[0] == "existing_ns" {
			count++
		}
	}

	if count != 1 {
		t.Errorf("Expected exactly one 'existing_ns' namespace, found %d", count)
	}
}

func TestImportTableOverwrite(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	ctx := context.Background()

	// Create a dummy Parquet file
	parquetFile := createDummyParquetFile(t, "test_data.parquet", "test data content")

	// Infer schema
	schema, _, err := importer.InferSchema(parquetFile)
	if err != nil {
		t.Fatalf("Failed to infer schema: %v", err)
	}

	// Import the table first time
	req := ImportRequest{
		ParquetFile:    parquetFile,
		TableIdent:     table.Identifier{"test_ns", "test_table"},
		NamespaceIdent: table.Identifier{"test_ns"},
		Schema:         schema,
		Overwrite:      false,
		PartitionBy:    nil,
	}

	_, err = importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import table first time: %v", err)
	}

	// Try to import again without overwrite - should fail
	_, err = importer.ImportTable(ctx, req)
	if err == nil {
		t.Error("Expected error when importing existing table without overwrite")
	}

	// Import with overwrite - should succeed
	req.Overwrite = true
	_, err = importer.ImportTable(ctx, req)
	if err != nil {
		t.Fatalf("Failed to import table with overwrite: %v", err)
	}
}

func TestInferSchemaNonExistentFile(t *testing.T) {
	cfg := createTestConfig(t)
	importer, err := NewParquetImporter(cfg)
	if err != nil {
		t.Fatalf("Failed to create importer: %v", err)
	}
	defer importer.Close()

	_, _, err = importer.InferSchema("/non/existent/file.parquet")
	if err == nil {
		t.Error("Expected error when inferring schema from non-existent file")
	}
}

// Helper functions

func createTestConfig(t *testing.T) *config.Config {
	tempDir, err := os.MkdirTemp("", "icebox-importer-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(tempDir, "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(tempDir, "data"),
			},
		},
	}

	// Set up cleanup
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return cfg
}

func createDummyParquetFile(t *testing.T, filename, content string) string {
	tempDir, err := os.MkdirTemp("", "icebox-parquet-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	parquetFile := filepath.Join(tempDir, filename)
	err = os.WriteFile(parquetFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create dummy Parquet file: %v", err)
	}

	// Set up cleanup
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return parquetFile
}
