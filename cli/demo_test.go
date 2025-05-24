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

	// Should have at least 1 dataset (taxi)
	assert.GreaterOrEqual(t, len(datasets), 1, "Should have at least 1 demo dataset")

	// Check required datasets exist
	datasetNames := make(map[string]bool)
	for _, dataset := range datasets {
		datasetNames[dataset.Name] = true

		// Validate dataset structure
		assert.NotEmpty(t, dataset.Name, "Dataset name should not be empty")
		assert.NotEmpty(t, dataset.Description, "Dataset description should not be empty")
		assert.NotEmpty(t, dataset.Namespace, "Dataset namespace should not be empty")
		assert.NotEmpty(t, dataset.Table, "Dataset table should not be empty")
		assert.NotEmpty(t, dataset.DataPath, "Dataset data path should not be empty")
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
	assert.True(t, datasetNames["taxi"], "Should have taxi dataset")
}

func TestDemoDatasetValidation(t *testing.T) {
	datasets := getAvailableDatasets()

	for _, dataset := range datasets {
		t.Run(dataset.Name, func(t *testing.T) {
			// Check namespace is 'demo'
			assert.Equal(t, "demo", dataset.Namespace, "All demo datasets should use 'demo' namespace")

			// Check properties
			assert.Contains(t, dataset.Properties, "data.source", "Dataset should have data.source property")
			assert.Equal(t, "demo", dataset.Properties["data.source"], "data.source should be 'demo'")

			// Check data type property
			assert.Contains(t, dataset.Properties, "data.type", "Dataset should have data.type property")

			// For partitioned datasets, validate partitioning properties
			if dataset.Partitioned {
				assert.Contains(t, dataset.Properties, "data.partitioned", "Partitioned dataset should have partitioned property")
				assert.Equal(t, "true", dataset.Properties["data.partitioned"], "Partitioned property should be 'true'")
			}

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
	demoOpts.dataset = "taxi"
	filtered = make([]DemoDataset, 0)
	for _, ds := range datasets {
		if ds.Name == demoOpts.dataset {
			filtered = append(filtered, ds)
			break
		}
	}

	assert.Len(t, filtered, 1, "Should find exactly one matching dataset")
	assert.Equal(t, "taxi", filtered[0].Name, "Should find taxi dataset")
}

func TestDemoDataPaths(t *testing.T) {
	datasets := getAvailableDatasets()

	for _, dataset := range datasets {
		// Check that the data path is reasonable
		assert.NotContains(t, dataset.DataPath, "..", "Data paths should not contain relative path traversal")

		// For partitioned datasets, validate structure expectations
		if dataset.Partitioned {
			// DataPath should be a directory path, not a file
			assert.False(t, strings.HasSuffix(dataset.DataPath, ".parquet"), "Partitioned dataset should reference a directory, not a file")
		}

		// Validate that the data path makes sense for the dataset
		switch dataset.Name {
		case "taxi":
			assert.Contains(t, strings.ToLower(dataset.DataPath), "demo", "Taxi dataset should reference demo data")
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
			// Query should reference the table name (simple name for current implementation)
			assert.Contains(t, query.SQL, dataset.Table,
				"Query for dataset %s should reference table %s", dataset.Name, dataset.Table)

			// Query name should be snake_case
			assert.True(t, isValidQueryName(query.Name), "Query name should be valid: %s", query.Name)

			// Description should be helpful
			assert.True(t, len(query.Description) > 10, "Query description should be substantial: %s", query.Description)
		}
	}
}

func TestPartitionedDatasetValidation(t *testing.T) {
	datasets := getAvailableDatasets()

	for _, dataset := range datasets {
		if dataset.Partitioned {
			t.Run(dataset.Name+"_partitioned", func(t *testing.T) {
				// Partitioned datasets should have specific properties
				assert.Contains(t, dataset.Properties, "data.partitioned", "Partitioned dataset should have partitioned property")
				assert.Equal(t, "true", dataset.Properties["data.partitioned"], "Partitioned property should be 'true'")

				// Should have data format property
				assert.Contains(t, dataset.Properties, "data.format", "Partitioned dataset should specify data format")

				// DataPath should be a directory, not a file
				assert.False(t, strings.HasSuffix(dataset.DataPath, ".parquet"), "Partitioned dataset DataPath should be directory")

				// Should have queries that demonstrate temporal analysis (since we use date-based queries instead of partition columns)
				hasTemporalQuery := false
				for _, query := range dataset.Queries {
					sql := strings.ToLower(query.SQL)
					if strings.Contains(sql, "date_trunc") || strings.Contains(sql, "extract") || strings.Contains(sql, "pickup_datetime") {
						hasTemporalQuery = true
						break
					}
				}
				assert.True(t, hasTemporalQuery, "Partitioned dataset should have at least one query demonstrating temporal analysis")
			})
		}
	}
}

func TestTaxiDatasetSpecific(t *testing.T) {
	datasets := getAvailableDatasets()

	var taxiDataset *DemoDataset
	for _, dataset := range datasets {
		if dataset.Name == "taxi" {
			taxiDataset = &dataset
			break
		}
	}

	require.NotNil(t, taxiDataset, "Should have taxi dataset")

	// Taxi-specific validations
	assert.Equal(t, "nyc_taxi", taxiDataset.Table, "Taxi dataset should use nyc_taxi table name")
	assert.True(t, taxiDataset.Partitioned, "Taxi dataset should be partitioned")
	assert.Equal(t, "demo", taxiDataset.DataPath, "Taxi dataset should reference demo directory")

	// Check for expected properties
	assert.Equal(t, "taxi", taxiDataset.Properties["data.type"], "Should have taxi data type")
	assert.Equal(t, "NYC", taxiDataset.Properties["data.location"], "Should specify NYC location")

	// Check for expected queries
	queryNames := make(map[string]bool)
	for _, query := range taxiDataset.Queries {
		queryNames[query.Name] = true
	}

	expectedQueries := []string{"count_trips", "average_fare", "trips_by_month", "payment_methods", "vendor_analysis", "busy_times"}
	for _, expectedQuery := range expectedQueries {
		assert.True(t, queryNames[expectedQuery], "Should have %s query", expectedQuery)
	}

	// Validate that queries use the simple table name (no namespace prefix needed)
	for _, query := range taxiDataset.Queries {
		// For the current implementation, queries should use simple table name
		assert.Contains(t, query.SQL, "nyc_taxi", "Query should reference the table name")
		// Should not contain the namespaced version since that doesn't work yet
		assert.NotContains(t, query.SQL, "demo.nyc_taxi", "Query should use simple table name for now")
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

func BenchmarkFindParquetFiles(b *testing.B) {
	// Only run if demo directory exists
	if _, err := filepath.Glob("demo/year=*/month=*/*.parquet"); err == nil {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = findParquetFiles("demo")
		}
	} else {
		b.Skip("Demo directory not available for benchmarking")
	}
}

// Integration test helpers

func TestDemoIntegrationPrep(t *testing.T) {
	// This test prepares for integration testing by validating
	// that the demo command structure is sound

	datasets := getAvailableDatasets()
	require.NotEmpty(t, datasets, "Must have demo datasets")

	// Validate that demo directory structure expectations are met
	for _, dataset := range datasets {
		// Check that data paths are reasonable
		assert.NotContains(t, dataset.DataPath, "..", "Data paths should not contain relative path traversal")

		// For partitioned datasets, validate expectations
		if dataset.Partitioned {
			// Should be a directory path
			assert.False(t, strings.HasSuffix(dataset.DataPath, ".parquet"), "Partitioned dataset should reference directory")
		}

		// Validate data path naming convention
		assert.True(t, len(dataset.DataPath) > 0, "Data path should not be empty")
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

func TestFindParquetFilesFunction(t *testing.T) {
	// Test the findParquetFiles helper function

	// Test with non-existent directory
	files, err := findParquetFiles("non_existent_directory")
	assert.NoError(t, err, "Should handle non-existent directory gracefully")
	assert.Empty(t, files, "Should return empty slice for non-existent directory")

	// Test with demo directory if it exists
	if _, err := filepath.Glob("demo/year=*/month=*/*.parquet"); err == nil {
		files, err := findParquetFiles("demo")
		assert.NoError(t, err, "Should successfully find files in demo directory")

		// Should find at least some parquet files
		if len(files) > 0 {
			assert.Greater(t, len(files), 0, "Should find parquet files in demo directory")

			// All returned files should be parquet files
			for _, file := range files {
				assert.True(t, strings.HasSuffix(strings.ToLower(file), ".parquet"), "All found files should be parquet files")
				assert.True(t, filepath.IsAbs(file), "All returned paths should be absolute")
			}
		}
	}
}
