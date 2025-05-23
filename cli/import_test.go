package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestParseTableIdentifier(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		namespace string
		wantTable table.Identifier
		wantNS    table.Identifier
		wantErr   bool
	}{
		{
			name:      "simple table with default namespace",
			table:     "my_table",
			namespace: "",
			wantTable: table.Identifier{"default", "my_table"},
			wantNS:    table.Identifier{"default"},
			wantErr:   false,
		},
		{
			name:      "simple table with explicit namespace",
			table:     "my_table",
			namespace: "analytics",
			wantTable: table.Identifier{"analytics", "my_table"},
			wantNS:    table.Identifier{"analytics"},
			wantErr:   false,
		},
		{
			name:      "qualified table name",
			table:     "analytics.my_table",
			namespace: "",
			wantTable: table.Identifier{"analytics", "my_table"},
			wantNS:    table.Identifier{"analytics"},
			wantErr:   false,
		},
		{
			name:      "qualified table name with conflicting namespace",
			table:     "analytics.my_table",
			namespace: "other",
			wantTable: nil,
			wantNS:    nil,
			wantErr:   true,
		},
		{
			name:      "empty table name",
			table:     "",
			namespace: "",
			wantTable: nil,
			wantNS:    nil,
			wantErr:   true,
		},
		{
			name:      "malformed qualified table name",
			table:     "ns.sub.table",
			namespace: "",
			wantTable: nil,
			wantNS:    nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTable, gotNS, err := parseTableIdentifier(tt.table, tt.namespace)

			if tt.wantErr {
				if err == nil {
					t.Errorf("parseTableIdentifier() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("parseTableIdentifier() unexpected error: %v", err)
				return
			}

			if !identifierEqual(gotTable, tt.wantTable) {
				t.Errorf("parseTableIdentifier() table = %v, want %v", gotTable, tt.wantTable)
			}

			if !identifierEqual(gotNS, tt.wantNS) {
				t.Errorf("parseTableIdentifier() namespace = %v, want %v", gotNS, tt.wantNS)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{1099511627776, "1.0 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatBytes(tt.bytes)
			if got != tt.want {
				t.Errorf("formatBytes(%d) = %s, want %s", tt.bytes, got, tt.want)
			}
		})
	}
}

func TestImportCommandIntegration(t *testing.T) {
	// Create temporary directories for testing
	tempDir, err := os.MkdirTemp("", "icebox-import-integration-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test project
	projectDir := filepath.Join(tempDir, "test-project")
	err = os.MkdirAll(projectDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create project dir: %v", err)
	}

	// Create configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(projectDir, ".icebox", "catalog", "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(projectDir, ".icebox", "data"),
			},
		},
	}

	// Create .icebox directory structure
	err = os.MkdirAll(filepath.Dir(cfg.Catalog.SQLite.Path), 0755)
	if err != nil {
		t.Fatalf("Failed to create catalog dir: %v", err)
	}

	err = os.MkdirAll(cfg.Storage.FileSystem.RootPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create storage dir: %v", err)
	}

	// Write configuration file
	configPath := filepath.Join(projectDir, ".icebox.yml")
	err = config.WriteConfig(configPath, cfg)
	if err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	// Create a dummy Parquet file
	parquetFile := filepath.Join(tempDir, "sales_data.parquet")
	err = os.WriteFile(parquetFile, []byte("dummy parquet content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create Parquet file: %v", err)
	}

	// Change to the project directory so icebox can find config
	originalDir, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(projectDir)
	require.NoError(t, err)
	defer func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Logf("Failed to restore original directory: %v", err)
		}
	}()

	// Test import command with --infer-schema
	importOpts = &importOptions{
		tableName:   "sales_table",
		namespace:   "",
		inferSchema: true,
		dryRun:      false,
		overwrite:   false,
		partitionBy: nil,
	}

	err = runImport(nil, []string{parquetFile})
	if err != nil {
		t.Fatalf("Failed to run import with --infer-schema: %v", err)
	}

	// Test import command with --dry-run
	importOpts.inferSchema = false
	importOpts.dryRun = true

	err = runImport(nil, []string{parquetFile})
	if err != nil {
		t.Fatalf("Failed to run import with --dry-run: %v", err)
	}

	// Test actual import
	importOpts.dryRun = false

	err = runImport(nil, []string{parquetFile})
	if err != nil {
		t.Fatalf("Failed to run actual import: %v", err)
	}

	// Verify that the table was created
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog for verification: %v", err)
	}
	defer catalog.Close()

	tableExists, err := catalog.CheckTableExists(context.Background(), table.Identifier{"default", "sales_table"})
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}

	if !tableExists {
		t.Error("Expected table to exist after import")
	}

	// Verify that the data file was copied
	dataFile := filepath.Join(cfg.Storage.FileSystem.RootPath, "default", "sales_table", "data", "part-00000.parquet")
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Error("Expected data file to be copied to table location")
	}
}

func TestImportCommandWithNonExistentFile(t *testing.T) {
	// Set up import options
	importOpts = &importOptions{
		tableName:   "test_table",
		namespace:   "",
		inferSchema: false,
		dryRun:      false,
		overwrite:   false,
		partitionBy: nil,
	}

	// Try to import non-existent file
	err := runImport(nil, []string{"/non/existent/file.parquet"})
	if err == nil {
		t.Error("Expected error when importing non-existent file")
	}
}

func TestImportCommandWithoutConfiguration(t *testing.T) {
	// Create temporary directory without .icebox.yml
	tempDir, err := os.MkdirTemp("", "icebox-no-config-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a dummy Parquet file
	parquetFile := filepath.Join(tempDir, "test.parquet")
	err = os.WriteFile(parquetFile, []byte("dummy content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create Parquet file: %v", err)
	}

	// Change to directory without configuration
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current dir: %v", err)
	}
	defer func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Logf("Failed to restore original directory: %v", err)
		}
	}()

	err = os.Chdir(tempDir)
	if err != nil {
		t.Fatalf("Failed to change to temp dir: %v", err)
	}

	// Set up import options
	importOpts = &importOptions{
		tableName:   "test_table",
		namespace:   "",
		inferSchema: false,
		dryRun:      false,
		overwrite:   false,
		partitionBy: nil,
	}

	// Try to import without configuration
	err = runImport(nil, []string{parquetFile})
	if err == nil {
		t.Error("Expected error when importing without Icebox configuration")
	}
}

func TestImportCommandQualifiedTableName(t *testing.T) {
	// Create temporary directories for testing
	tempDir, err := os.MkdirTemp("", "icebox-qualified-table-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test project
	projectDir := filepath.Join(tempDir, "test-project")
	err = os.MkdirAll(projectDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create project dir: %v", err)
	}

	// Create configuration
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: filepath.Join(projectDir, ".icebox", "catalog", "catalog.db"),
			},
		},
		Storage: config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: filepath.Join(projectDir, ".icebox", "data"),
			},
		},
	}

	// Create .icebox directory structure
	err = os.MkdirAll(filepath.Dir(cfg.Catalog.SQLite.Path), 0755)
	if err != nil {
		t.Fatalf("Failed to create catalog dir: %v", err)
	}

	err = os.MkdirAll(cfg.Storage.FileSystem.RootPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create storage dir: %v", err)
	}

	// Write configuration file
	configPath := filepath.Join(projectDir, ".icebox.yml")
	err = config.WriteConfig(configPath, cfg)
	if err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	// Create a dummy Parquet file
	parquetFile := filepath.Join(tempDir, "test.parquet")
	err = os.WriteFile(parquetFile, []byte("dummy content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create Parquet file: %v", err)
	}

	// Change to the project directory so icebox can find config
	originalDir, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(projectDir)
	require.NoError(t, err)
	defer func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Logf("Failed to restore original directory: %v", err)
		}
	}()

	// Test import with qualified table name
	importOpts = &importOptions{
		tableName:   "analytics.customer_data",
		namespace:   "",
		inferSchema: false,
		dryRun:      false,
		overwrite:   false,
		partitionBy: nil,
	}

	err = runImport(nil, []string{parquetFile})
	if err != nil {
		t.Fatalf("Failed to run import with qualified table name: %v", err)
	}

	// Verify that the table was created in the correct namespace
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create catalog for verification: %v", err)
	}
	defer catalog.Close()

	tableExists, err := catalog.CheckTableExists(context.Background(), table.Identifier{"analytics", "customer_data"})
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}

	if !tableExists {
		t.Error("Expected table to exist in analytics namespace")
	}

	// Verify that the namespace was created
	nsExists, err := catalog.CheckNamespaceExists(context.Background(), table.Identifier{"analytics"})
	if err != nil {
		t.Fatalf("Failed to check namespace existence: %v", err)
	}

	if !nsExists {
		t.Error("Expected analytics namespace to exist")
	}
}

// Helper function to compare identifiers
func identifierEqual(a, b table.Identifier) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
