package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/config"
)

func TestNewCatalogSQLite(t *testing.T) {
	cfg := createTestSQLiteConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create SQLite catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.Name() != cfg.Name {
		t.Errorf("Expected catalog name %s, got %s", cfg.Name, catalog.Name())
	}

	if catalog.CatalogType() != "sql" {
		t.Errorf("Expected catalog type 'sql', got %s", catalog.CatalogType())
	}
}

func TestNewCatalogREST(t *testing.T) {
	cfg := createTestRESTConfig(t)

	catalog, err := NewCatalog(cfg)
	if err != nil {
		t.Fatalf("Failed to create REST catalog: %v", err)
	}
	defer catalog.Close()

	if catalog.Name() != cfg.Name {
		t.Errorf("Expected catalog name %s, got %s", cfg.Name, catalog.Name())
	}

	if catalog.CatalogType() != "rest" {
		t.Errorf("Expected catalog type 'rest', got %s", catalog.CatalogType())
	}
}

func TestNewCatalogUnsupportedType(t *testing.T) {
	cfg := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "unsupported",
		},
	}

	_, err := NewCatalog(cfg)
	if err == nil {
		t.Error("Expected error for unsupported catalog type")
	}

	expectedError := "unsupported catalog type: unsupported"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestNewCatalogWithMissingConfig(t *testing.T) {
	// Test SQLite catalog with missing config
	sqliteConfig := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			// No SQLite config provided
		},
	}

	_, err := NewCatalog(sqliteConfig)
	if err == nil {
		t.Error("Expected error for missing SQLite config")
	}

	// Test REST catalog with missing config
	restConfig := &config.Config{
		Name: "test-catalog",
		Catalog: config.CatalogConfig{
			Type: "rest",
			// No REST config provided
		},
	}

	_, err = NewCatalog(restConfig)
	if err == nil {
		t.Error("Expected error for missing REST config")
	}
}

// Helper functions

func createTestSQLiteConfig(t *testing.T) *config.Config {
	tempDir, err := os.MkdirTemp("", "icebox-catalog-factory-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Name: "test-sqlite-catalog",
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

func createTestRESTConfig(t *testing.T) *config.Config {
	tempDir, err := os.MkdirTemp("", "icebox-catalog-factory-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Name: "test-rest-catalog",
		Catalog: config.CatalogConfig{
			Type: "rest",
			REST: &config.RESTConfig{
				URI: "http://localhost:8181",
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
