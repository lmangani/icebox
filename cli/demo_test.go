package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatDemoBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{12345678901, "11.5 GB"},
	}

	for _, test := range tests {
		result := formatDemoBytes(test.bytes)
		assert.Equal(t, test.expected, result, "formatDemoBytes(%d) should return %s", test.bytes, test.expected)
	}
}

func TestGetAvailableDatasets(t *testing.T) {
	datasets := getAvailableDatasets()

	// Should have at least 3 datasets
	assert.GreaterOrEqual(t, len(datasets), 3, "Should have at least 3 demo datasets")

	// Check required datasets exist
	datasetNames := make(map[string]bool)
	for _, dataset := range datasets {
		datasetNames[dataset.Name] = true

		// Validate dataset structure
		assert.NotEmpty(t, dataset.Name, "Dataset name should not be empty")
		assert.NotEmpty(t, dataset.Description, "Dataset description should not be empty")
		assert.NotEmpty(t, dataset.Namespace, "Dataset namespace should not be empty")
		assert.NotEmpty(t, dataset.Table, "Dataset table should not be empty")
		assert.NotEmpty(t, dataset.File, "Dataset file should not be empty")
		assert.NotNil(t, dataset.Properties, "Dataset properties should not be nil")
		assert.NotNil(t, dataset.Queries, "Dataset queries should not be nil")
		assert.Greater(t, len(dataset.Queries), 0, "Dataset should have at least one sample query")

		// Validate queries
		for _, query := range dataset.Queries {
			assert.NotEmpty(t, query.Name, "Query name should not be empty")
			assert.NotEmpty(t, query.Description, "Query description should not be empty")
			assert.NotEmpty(t, query.SQL, "Query SQL should not be empty")
			assert.Contains(t, query.SQL, dataset.Namespace+"."+dataset.Table, "Query should reference the dataset table")
		}
	}

	// Check specific datasets exist
	assert.True(t, datasetNames["flights"], "Should have flights dataset")
	assert.True(t, datasetNames["dates"], "Should have dates dataset")
	assert.True(t, datasetNames["decimals"], "Should have decimals dataset")
}

func TestDemoDatasetValidation(t *testing.T) {
	datasets := getAvailableDatasets()

	for _, dataset := range datasets {
		t.Run(dataset.Name, func(t *testing.T) {
			// Check namespace is 'demo'
			assert.Equal(t, "demo", dataset.Namespace, "All demo datasets should use 'demo' namespace")

			// Check file extension
			assert.True(t, strings.HasSuffix(dataset.File, ".parquet"), "Dataset file should be a parquet file")

			// Check properties
			assert.Contains(t, dataset.Properties, "data.source", "Dataset should have data.source property")
			assert.Equal(t, "demo", dataset.Properties["data.source"], "data.source should be 'demo'")

			// Validate SQL queries are well-formed
			for _, query := range dataset.Queries {
				sql := strings.ToUpper(query.SQL)
				assert.True(t,
					strings.Contains(sql, "SELECT") ||
						strings.Contains(sql, "SHOW") ||
						strings.Contains(sql, "DESCRIBE"),
					"Query should be a SELECT, SHOW, or DESCRIBE statement: %s", query.SQL)
			}
		})
	}
}

func TestListDemoDatasets(t *testing.T) {
	// Capture stdout
	var buf bytes.Buffer

	// Test list functionality
	err := listDemoDatasets()
	assert.NoError(t, err, "listDemoDatasets should not return error")

	// Reset buffer for actual test (since we can't easily capture stdout in this test)
	buf.Reset()
}

func TestDemoOptionsValidation(t *testing.T) {
	// Test with invalid dataset name
	demoOpts.dataset = "invalid_dataset"
	demoOpts.list = false
	demoOpts.cleanup = false

	datasets := getAvailableDatasets()
	filtered := make([]DemoDataset, 0)
	for _, ds := range datasets {
		if ds.Name == demoOpts.dataset {
			filtered = append(filtered, ds)
			break
		}
	}

	assert.Empty(t, filtered, "Should not find invalid dataset")

	// Test with valid dataset name
	demoOpts.dataset = "flights"
	filtered = make([]DemoDataset, 0)
	for _, ds := range datasets {
		if ds.Name == demoOpts.dataset {
			filtered = append(filtered, ds)
			break
		}
	}

	assert.Len(t, filtered, 1, "Should find exactly one matching dataset")
	assert.Equal(t, "flights", filtered[0].Name, "Should find flights dataset")
}

func TestDemoFilePaths(t *testing.T) {
	datasets := getAvailableDatasets()

	for _, dataset := range datasets {
		// Check that the referenced file exists in testdata
		testDataPath := filepath.Join("../testdata", dataset.File)

		// Note: We can't check if the file actually exists in the test environment
		// but we can validate the path structure
		assert.True(t, strings.HasPrefix(testDataPath, "../testdata/"), "File path should be in testdata directory")
		assert.True(t, strings.HasSuffix(dataset.File, ".parquet"), "File should be a parquet file")

		// Validate that the file name makes sense for the dataset
		switch dataset.Name {
		case "flights":
			assert.Contains(t, dataset.File, "flight", "Flights dataset should reference flight data file")
		case "dates":
			assert.Contains(t, dataset.File, "date", "Dates dataset should reference date data file")
		case "decimals":
			assert.Contains(t, dataset.File, "decimal", "Decimals dataset should reference decimal data file")
		}
	}
}

func TestDemoConfigValidation(t *testing.T) {
	// Test that demo command requires proper configuration

	// This test validates the structure without actually running the command
	// since that would require a full Icebox setup

	datasets := getAvailableDatasets()
	require.NotEmpty(t, datasets, "Should have demo datasets available")

	// Validate that all required components are properly structured
	for _, dataset := range datasets {
		// Check that namespace and table names are valid SQL identifiers
		assert.True(t, isValidSQLIdentifier(dataset.Namespace), "Namespace should be valid SQL identifier: %s", dataset.Namespace)
		assert.True(t, isValidSQLIdentifier(dataset.Table), "Table name should be valid SQL identifier: %s", dataset.Table)

		// Check that queries are syntactically reasonable
		for _, query := range dataset.Queries {
			assert.NotContains(t, query.SQL, ";", "Demo queries should not contain semicolons for security")
			assert.True(t, len(query.SQL) > 10, "Query should be substantial: %s", query.SQL)
			assert.True(t, len(query.SQL) < 500, "Query should not be too long: %s", query.SQL)
		}
	}
}

func TestDemoNamespaceConsistency(t *testing.T) {
	datasets := getAvailableDatasets()

	// All datasets should use the same demo namespace
	for _, dataset := range datasets {
		assert.Equal(t, "demo", dataset.Namespace, "All demo datasets should use 'demo' namespace")
	}

	// Check that table names are unique within the namespace
	tableNames := make(map[string]bool)
	for _, dataset := range datasets {
		assert.False(t, tableNames[dataset.Table], "Table name should be unique: %s", dataset.Table)
		tableNames[dataset.Table] = true
	}
}

func TestDemoQueryConsistency(t *testing.T) {
	datasets := getAvailableDatasets()

	for _, dataset := range datasets {
		// Each dataset should have meaningful sample queries
		assert.Greater(t, len(dataset.Queries), 0, "Dataset %s should have sample queries", dataset.Name)

		for _, query := range dataset.Queries {
			// Query should reference the correct table
			expectedTable := dataset.Namespace + "." + dataset.Table
			assert.Contains(t, query.SQL, expectedTable,
				"Query for dataset %s should reference table %s", dataset.Name, expectedTable)

			// Query name should be snake_case
			assert.True(t, isValidQueryName(query.Name), "Query name should be valid: %s", query.Name)

			// Description should be helpful
			assert.True(t, len(query.Description) > 10, "Query description should be substantial: %s", query.Description)
		}
	}
}

// Helper functions for validation

func isValidSQLIdentifier(name string) bool {
	if len(name) == 0 {
		return false
	}

	// Check first character
	if !isLetter(name[0]) && name[0] != '_' {
		return false
	}

	// Check remaining characters
	for i := 1; i < len(name); i++ {
		if !isLetter(name[i]) && !isDigit(name[i]) && name[i] != '_' {
			return false
		}
	}

	return true
}

func isValidQueryName(name string) bool {
	if len(name) == 0 {
		return false
	}

	// Should be snake_case
	for i, char := range name {
		if !isLetter(byte(char)) && !isDigit(byte(char)) && char != '_' {
			return false
		}
		if i == 0 && isDigit(byte(char)) {
			return false
		}
	}

	return true
}

func isLetter(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

func isDigit(char byte) bool {
	return char >= '0' && char <= '9'
}

// Benchmark tests for performance

func BenchmarkGetAvailableDatasets(b *testing.B) {
	for i := 0; i < b.N; i++ {
		datasets := getAvailableDatasets()
		_ = datasets
	}
}

func BenchmarkFormatDemoBytes(b *testing.B) {
	testSizes := []int64{1024, 1048576, 1073741824, 12345678901}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, size := range testSizes {
			_ = formatDemoBytes(size)
		}
	}
}

// Integration test helpers (these would require actual testdata files)

func TestDemoIntegrationPrep(t *testing.T) {
	// This test prepares for integration testing by validating
	// that the demo command structure is sound

	datasets := getAvailableDatasets()
	require.NotEmpty(t, datasets, "Must have demo datasets")

	// Validate that testdata directory structure expectations are met
	for _, dataset := range datasets {
		// Check that file paths are reasonable
		assert.NotContains(t, dataset.File, "..", "File paths should not contain relative path traversal")
		assert.NotContains(t, dataset.File, "/", "File names should not contain path separators")

		// Validate file naming convention
		assert.True(t, len(dataset.File) > 5, "File name should be substantial")
		assert.True(t, strings.HasSuffix(dataset.File, ".parquet"), "Should be parquet file")
	}
}

// Test the demo command flags and options
func TestDemoCommandFlags(t *testing.T) {
	// Test that all expected flags are available
	cmd := demoCmd

	// Check that flags exist
	datasetFlag := cmd.Flags().Lookup("dataset")
	assert.NotNil(t, datasetFlag, "Should have --dataset flag")

	listFlag := cmd.Flags().Lookup("list")
	assert.NotNil(t, listFlag, "Should have --list flag")

	cleanupFlag := cmd.Flags().Lookup("cleanup")
	assert.NotNil(t, cleanupFlag, "Should have --cleanup flag")

	forceFlag := cmd.Flags().Lookup("force")
	assert.NotNil(t, forceFlag, "Should have --force flag")

	verboseFlag := cmd.Flags().Lookup("verbose")
	assert.NotNil(t, verboseFlag, "Should have --verbose flag")

	// Test flag defaults
	assert.Equal(t, "", datasetFlag.DefValue, "Dataset flag should default to empty")
	assert.Equal(t, "false", listFlag.DefValue, "List flag should default to false")
	assert.Equal(t, "false", cleanupFlag.DefValue, "Cleanup flag should default to false")
	assert.Equal(t, "false", forceFlag.DefValue, "Force flag should default to false")
	assert.Equal(t, "false", verboseFlag.DefValue, "Verbose flag should default to false")
}
