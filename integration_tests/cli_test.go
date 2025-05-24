package integration_tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// isCI checks if we're running in a CI environment
func isCI() bool {
	return os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" || os.Getenv("JENKINS_URL") != ""
}

func TestProjectInitialization(t *testing.T) {
	if isCI() {
		t.Skip("Skipping integration tests in CI - requires icebox binary build")
	}

	projectDir, cleanup := setupTestProject(t)
	defer cleanup()

	// Verify .icebox directory and config were created
	iceboxDir := filepath.Join(projectDir, ".icebox")
	if _, err := os.Stat(iceboxDir); os.IsNotExist(err) {
		t.Errorf(".icebox directory was not created")
	}

	configFile := filepath.Join(projectDir, ".icebox.yml")
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		t.Errorf(".icebox.yml config file was not created")
	}

	// Verify that the catalog database was created (assuming SQLite default)
	if _, err := os.Stat(filepath.Join(projectDir, ".icebox", "catalog", "catalog.db")); os.IsNotExist(err) {
		t.Errorf("Expected .icebox/catalog/catalog.db to be created in %s, but it was not", projectDir)
	}
}

func TestImportAndQueryTitanicData(t *testing.T) {
	if isCI() {
		t.Skip("Skipping integration tests in CI - requires icebox binary build")
	}

	projectDir, cleanup := setupTestProject(t)
	defer cleanup()

	// Copy titanic.parquet to the test project directory
	copiedDataFile := copyTestData(t, projectDir, titanicDataFile)
	baseDataFile := filepath.Base(copiedDataFile)

	// Test icebox import
	tableName := "titanic_test_table"
	stdout, _ := runIceboxCommand(t, projectDir, "import", baseDataFile, "--table", tableName)
	if !strings.Contains(stdout, tableName) || !(strings.Contains(stdout, "imported") || strings.Contains(stdout, "created")) {
		t.Errorf("Expected import success message containing table name '%s' and keyword, got: %s", tableName, stdout)
	}

	// Test icebox table list to verify table creation
	stdout, _ = runIceboxCommand(t, projectDir, "table", "list", "--namespace", "default")
	if !strings.Contains(stdout, tableName) {
		t.Errorf("Expected table list for namespace 'default' to contain '%s', got: %s", tableName, stdout)
	}

	// Test icebox sql: COUNT(*) - Titanic dataset has 891 rows
	stdout, _ = runIceboxCommand(t, projectDir, "sql", "SELECT COUNT(*) FROM "+tableName)
	if !strings.Contains(stdout, "891") {
		t.Errorf("Expected row count 891 for 'SELECT COUNT(*) FROM %s', got: %s", tableName, stdout)
	}

	// Test icebox sql: A more complex query using Titanic columns
	query := `SELECT "Pclass", COUNT(*) AS passenger_count FROM ` + tableName + ` GROUP BY "Pclass" ORDER BY passenger_count DESC LIMIT 3`
	stdout, _ = runIceboxCommand(t, projectDir, "sql", query)
	// Check for presence of expected columns in output
	if !strings.Contains(stdout, "Pclass") || !strings.Contains(stdout, "passenger_count") {
		t.Errorf("Expected 'Pclass' and 'passenger_count' in query output, got: %s", stdout)
	}

	// Test icebox table describe
	stdout, _ = runIceboxCommand(t, projectDir, "table", "describe", tableName)
	if !strings.Contains(stdout, "Schema") || !strings.Contains(stdout, "Location") {
		t.Errorf("Expected 'Schema' and 'Location' in table describe output, got: %s", stdout)
	}

	// Test icebox table history
	stdout, _ = runIceboxCommand(t, projectDir, "table", "history", tableName)
	if !strings.Contains(stdout, "append") && !strings.Contains(stdout, "create") && !strings.Contains(stdout, "No snapshots found") {
		t.Errorf("Expected 'append', 'create' operation, or 'No snapshots found' in table history, got: %s", stdout)
	}
}

func TestCatalogOperations(t *testing.T) {
	if isCI() {
		t.Skip("Skipping integration tests in CI - requires icebox binary build")
	}

	projectDir, cleanup := setupTestProject(t)
	defer cleanup()

	namespaceName := "test_namespace"
	// Test catalog create
	stdout, _ := runIceboxCommand(t, projectDir, "catalog", "create", namespaceName)
	if !strings.Contains(stdout, "Successfully created namespace") && !strings.Contains(stdout, "Namespace created successfully") {
		t.Errorf("Expected namespace creation message, got: %s", stdout)
	}

	// Test catalog list
	stdout, _ = runIceboxCommand(t, projectDir, "catalog", "list")
	if !strings.Contains(stdout, namespaceName) {
		t.Errorf("Expected catalog list to contain '%s', got: %s", namespaceName, stdout)
	}

	// Test catalog drop
	stdout, _ = runIceboxCommand(t, projectDir, "catalog", "drop", namespaceName)
	if !strings.Contains(stdout, "Successfully dropped namespace") && !strings.Contains(stdout, "Namespace dropped successfully") {
		t.Errorf("Expected namespace drop message, got: %s", stdout)
	}

	// Verify namespace is dropped
	stdout, _ = runIceboxCommand(t, projectDir, "catalog", "list")
	if strings.Contains(stdout, namespaceName) {
		t.Errorf("Expected catalog list to not contain '%s' after dropping, got: %s", namespaceName, stdout)
	}
}

// TODO: Add TestTimeTravelQueries
// TODO: Add TestPackAndUnpack
// TODO: Add TestMinIOIntegration (if embedded MinIO is part of the testable features)
