package integration_tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectInitialization(t *testing.T) {
	projectDir, cleanup := setupTestProject(t)
	defer cleanup()

	// Verify that .icebox.yml was created
	if _, err := os.Stat(filepath.Join(projectDir, ".icebox.yml")); os.IsNotExist(err) {
		t.Errorf("Expected .icebox.yml to be created in %s, but it was not", projectDir)
	}

	// Verify that the catalog database was created (assuming SQLite default)
	if _, err := os.Stat(filepath.Join(projectDir, ".icebox", "catalog", "catalog.db")); os.IsNotExist(err) {
		t.Errorf("Expected .icebox/catalog/catalog.db to be created in %s, but it was not", projectDir)
	}
}

func TestImportAndQueryFlightsData(t *testing.T) {
	projectDir, cleanup := setupTestProject(t)
	defer cleanup()

	// Copy flights.parquet to the test project directory
	copiedDataFile := copyTestData(t, projectDir, flightsDataFile)
	baseDataFile := filepath.Base(copiedDataFile)

	// Test icebox import
	tableName := "flights_test_table"
	stdout, _ := runIceboxCommand(t, projectDir, "import", baseDataFile, "--table", tableName)
	if !strings.Contains(stdout, tableName) || !(strings.Contains(stdout, "imported") || strings.Contains(stdout, "created")) {
		t.Errorf("Expected import success message containing table name '%s' and keyword, got: %s", tableName, stdout)
	}

	// Test icebox table list to verify table creation
	stdout, _ = runIceboxCommand(t, projectDir, "table", "list", "--namespace", "default")
	if !strings.Contains(stdout, tableName) {
		t.Errorf("Expected table list for namespace 'default' to contain '%s', got: %s", tableName, stdout)
	}

	// Test icebox sql: COUNT(*)
	stdout, _ = runIceboxCommand(t, projectDir, "sql", "SELECT COUNT(*) FROM "+tableName)
	if !strings.Contains(stdout, "271832") {
		t.Errorf("Expected row count 271832 for 'SELECT COUNT(*) FROM %s', got: %s", tableName, stdout)
	}

	// Test icebox sql: A more complex query
	query := `SELECT "ORIGIN_AIRPORT_ID", COUNT(*) AS flight_count FROM ` + tableName + ` GROUP BY "ORIGIN_AIRPORT_ID" ORDER BY flight_count DESC LIMIT 3`
	stdout, _ = runIceboxCommand(t, projectDir, "sql", query)
	// Check for presence of expected columns in output. Specific values would be better.
	if !strings.Contains(stdout, "ORIGIN_AIRPORT_ID") || !strings.Contains(stdout, "flight_count") {
		t.Errorf("Expected 'ORIGIN_AIRPORT_ID' and 'flight_count' in query output, got: %s", stdout)
	}

	// Test icebox table describe
	stdout, _ = runIceboxCommand(t, projectDir, "table", "describe", tableName)
	if !strings.Contains(stdout, "Schema:") || !strings.Contains(stdout, "Location:") {
		t.Errorf("Expected 'Schema:' and 'Location:' in table describe output, got: %s", stdout)
	}

	// Test icebox table history
	stdout, _ = runIceboxCommand(t, projectDir, "table", "history", tableName)
	if !strings.Contains(stdout, "append") && !strings.Contains(stdout, "create") {
		t.Errorf("Expected 'append' or 'create' operation in table history, got: %s", stdout)
	}
}

func TestCatalogOperations(t *testing.T) {
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
