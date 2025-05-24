package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/config"
	"github.com/stretchr/testify/require"
)

func TestInitCommand(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icebox-init-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test basic initialization
	projectDir := filepath.Join(tempDir, "test-project")

	// Mock command arguments
	args := []string{projectDir}

	err = runInit(nil, args)
	if err != nil {
		t.Fatalf("runInit failed: %v", err)
	}

	// Verify that the project directory was created
	if _, err := os.Stat(projectDir); os.IsNotExist(err) {
		t.Error("Project directory was not created")
	}

	// Verify that .icebox.yml was created
	configPath := filepath.Join(projectDir, ".icebox.yml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error(".icebox.yml was not created")
	}

	// Verify config content
	cfg, err := config.ReadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to read config: %v", err)
	}

	if cfg.Name != "test-project" {
		t.Errorf("Expected project name 'test-project', got '%s'", cfg.Name)
	}

	if cfg.Catalog.Type != "sqlite" {
		t.Errorf("Expected catalog type 'sqlite', got '%s'", cfg.Catalog.Type)
	}

	if cfg.Storage.Type != "fs" {
		t.Errorf("Expected storage type 'fs', got '%s'", cfg.Storage.Type)
	}

	// Verify that catalog directory was created
	catalogDir := filepath.Join(projectDir, ".icebox", "catalog")
	if _, err := os.Stat(catalogDir); os.IsNotExist(err) {
		t.Error("Catalog directory was not created")
	}

	// Verify that data directory was created
	dataDir := filepath.Join(projectDir, ".icebox", "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Error("Data directory was not created")
	}
}

func TestInitCommandExistingProject(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icebox-init-existing-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create initial project
	projectDir := filepath.Join(tempDir, "existing-project")
	args := []string{projectDir}

	err = runInit(nil, args)
	if err != nil {
		t.Fatalf("First runInit failed: %v", err)
	}

	// Try to initialize again - should fail
	err = runInit(nil, args)
	if err == nil {
		t.Error("Expected error when initializing existing project, got nil")
	}
}

func TestInitCommandCurrentDirectory(t *testing.T) {
	// This test is no longer relevant as the init command
	// now creates "icebox-lakehouse" directory by default
	// instead of initializing in the current directory.
	// See TestInitCommandDefaultDirectory for the new behavior.
	t.Skip("Test obsolete - init command behavior changed")
}

func TestInitCommandDefaultDirectory(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icebox-init-default-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Change to temp directory
	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		if err := os.Chdir(originalDir); err != nil {
			t.Logf("Failed to restore original directory: %v", err)
		}
	}()

	err = os.Chdir(tempDir)
	require.NoError(t, err)

	// Initialize with no args (should create icebox-lakehouse directory)
	err = runInit(nil, []string{})
	if err != nil {
		t.Fatalf("runInit failed: %v", err)
	}

	// Verify that icebox-lakehouse directory was created
	iceboxDir := filepath.Join(tempDir, "icebox-lakehouse")
	if _, err := os.Stat(iceboxDir); os.IsNotExist(err) {
		t.Error("icebox-lakehouse directory was not created")
	}

	// Verify that .icebox.yml was created in the icebox-lakehouse directory
	configPath := filepath.Join(iceboxDir, ".icebox.yml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error(".icebox.yml was not created in icebox-lakehouse directory")
	}

	// Verify config content
	cfg, err := config.ReadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to read config: %v", err)
	}

	if cfg.Name != "icebox-lakehouse" {
		t.Errorf("Expected project name 'icebox-lakehouse', got '%s'", cfg.Name)
	}
}

func TestInitSQLiteCatalog(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icebox-init-sqlite-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		Name: "test-sqlite",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
		},
	}

	err = initSQLiteCatalog(tempDir, cfg)
	if err != nil {
		t.Fatalf("initSQLiteCatalog failed: %v", err)
	}

	// Verify catalog directory was created
	catalogDir := filepath.Join(tempDir, ".icebox", "catalog")
	if _, err := os.Stat(catalogDir); os.IsNotExist(err) {
		t.Error("Catalog directory was not created")
	}

	// Verify config was updated with database path
	if cfg.Catalog.SQLite == nil {
		t.Error("SQLite config was not set")
	}

	expectedPath := filepath.Join(catalogDir, "catalog.db")
	if cfg.Catalog.SQLite.Path != expectedPath {
		t.Errorf("Expected SQLite path '%s', got '%s'", expectedPath, cfg.Catalog.SQLite.Path)
	}
}

func TestInitStorage(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icebox-init-storage-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test filesystem storage
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Type: "fs",
		},
	}

	err = initStorage(tempDir, cfg)
	if err != nil {
		t.Fatalf("initStorage failed: %v", err)
	}

	// Verify data directory was created
	dataDir := filepath.Join(tempDir, ".icebox", "data")
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Error("Data directory was not created")
	}

	// Verify config was updated
	if cfg.Storage.FileSystem == nil {
		t.Error("FileSystem config was not set")
	}

	if cfg.Storage.FileSystem.RootPath != dataDir {
		t.Errorf("Expected root path '%s', got '%s'", dataDir, cfg.Storage.FileSystem.RootPath)
	}

	// Test memory storage
	memCfg := &config.Config{
		Storage: config.StorageConfig{
			Type: "mem",
		},
	}

	err = initStorage(tempDir, memCfg)
	if err != nil {
		t.Fatalf("initStorage with memory failed: %v", err)
	}

	// Verify memory config was set
	if memCfg.Storage.Memory == nil {
		t.Error("Memory config was not set")
	}
}
