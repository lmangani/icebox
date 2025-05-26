package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/importer"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
)

var importCmd = &cobra.Command{
	Use:   "import [file]",
	Short: "Import a data file (Parquet or Avro) into an Iceberg table",
	Long: `Import a data file into an Iceberg table with automatic schema inference.

Supported formats:
- Parquet (.parquet)
- Avro (.avro)

This command will:
- Detect the file format automatically
- Read the file and infer the schema
- Create a namespace if it doesn't exist
- Create an Iceberg table with the inferred schema
- Copy the data to the table location

Examples:
  icebox import data.parquet --table my_table
  icebox import data.avro --table namespace.table_name
  icebox import data.parquet --table sales --namespace analytics
  icebox import data.avro --dry-run --infer-schema`,
	Args: cobra.ExactArgs(1),
	RunE: runImport,
}

type importOptions struct {
	tableName   string
	namespace   string
	inferSchema bool
	dryRun      bool
	overwrite   bool
	partitionBy []string
}

var importOpts = &importOptions{}

func init() {
	rootCmd.AddCommand(importCmd)

	importCmd.Flags().StringVar(&importOpts.tableName, "table", "", "target table name (namespace.table or just table)")
	if err := importCmd.MarkFlagRequired("table"); err != nil {
		panic(fmt.Sprintf("Failed to mark table flag as required: %v", err))
	}
	importCmd.Flags().StringVar(&importOpts.namespace, "namespace", "", "target namespace (optional, can be included in table name)")
	importCmd.Flags().BoolVar(&importOpts.inferSchema, "infer-schema", true, "automatically infer schema from data")
	importCmd.Flags().BoolVar(&importOpts.dryRun, "dry-run", false, "show what would be done without executing")
	importCmd.Flags().BoolVar(&importOpts.overwrite, "overwrite", false, "overwrite existing table")
	importCmd.Flags().StringSliceVar(&importOpts.partitionBy, "partition-by", nil, "partition columns (comma-separated)")
}

func runImport(cmd *cobra.Command, args []string) error {
	dataFile := args[0]

	// Validate that the data file exists
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		return fmt.Errorf("data file does not exist: %s", dataFile)
	}

	// Get absolute path to the data file
	absDataFile, err := filepath.Abs(dataFile)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("failed to find Icebox configuration: %w", err)
	}

	if cmd != nil && cmd.Flag("verbose").Value.String() == "true" {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

	// Parse table identifier
	tableIdent, namespaceIdent, err := parseTableIdentifier(importOpts.tableName, importOpts.namespace)
	if err != nil {
		return fmt.Errorf("failed to parse table identifier: %w", err)
	}

	// Create importer factory and detect file type
	factory := importer.NewImporterFactory(cfg)
	imp, importerType, err := factory.CreateImporter(absDataFile)
	if err != nil {
		return fmt.Errorf("failed to create importer: %w", err)
	}
	defer imp.Close()

	fmt.Printf("📁 Detected file format: %s\n", importerType)

	// Infer schema from data file
	schema, stats, err := imp.InferSchema(absDataFile)
	if err != nil {
		return fmt.Errorf("failed to infer schema from %s file: %w", importerType, err)
	}

	// If just showing inferred schema, print and continue with import
	if importOpts.inferSchema {
		fmt.Printf("📋 Schema inferred from %s:\n\n", dataFile)
		printSchema(schema)
		fmt.Printf("\n📊 File Statistics:\n")
		printStats(stats)
		fmt.Printf("\n") // Add spacing before the import operation
	}

	// If dry run, show what would be done and exit
	if importOpts.dryRun {
		fmt.Printf("🔍 Dry run - would perform the following operations:\n\n")
		fmt.Printf("1. Create namespace: %v\n", namespaceIdent)
		fmt.Printf("2. Create table: %v\n", tableIdent)
		fmt.Printf("3. Import from: %s (%s format)\n", absDataFile, importerType)
		fmt.Printf("4. Table location: %s\n", imp.GetTableLocation(tableIdent))
		fmt.Printf("\n📋 Inferred Schema:\n")
		printSchema(schema)
		fmt.Printf("\n📊 File Statistics:\n")
		printStats(stats)
		return nil
	}

	// Perform the actual import
	fmt.Printf("📥 Importing %s (%s) into table %v...\n", dataFile, importerType, tableIdent)

	result, err := imp.ImportTable(context.Background(), importer.ImportRequest{
		ParquetFile:    absDataFile, // Note: field name is ParquetFile but used for any file type
		TableIdent:     tableIdent,
		NamespaceIdent: namespaceIdent,
		Schema:         schema,
		Overwrite:      importOpts.overwrite,
		PartitionBy:    importOpts.partitionBy,
	})
	if err != nil {
		return fmt.Errorf("failed to import table: %w", err)
	}

	// Print success message
	fmt.Printf("✅ Successfully imported table!\n\n")
	fmt.Printf("📊 Import Results:\n")
	fmt.Printf("   Table: %v\n", result.TableIdent)
	fmt.Printf("   Records: %d\n", result.RecordCount)
	fmt.Printf("   Size: %s\n", formatBytes(result.DataSize))
	fmt.Printf("   Location: %s\n", result.TableLocation)
	fmt.Printf("\n🚀 Next steps:\n")
	fmt.Printf("   icebox sql 'SELECT * FROM %s LIMIT 10'\n", strings.Join(tableIdent, "."))

	return nil
}

// parseTableIdentifier parses table and namespace flags into identifiers
func parseTableIdentifier(tableName, namespace string) (tableIdent table.Identifier, namespaceIdent table.Identifier, err error) {
	if tableName == "" {
		return nil, nil, fmt.Errorf("table name is required")
	}

	// Check if table contains namespace (e.g., "namespace.table")
	if strings.Contains(tableName, ".") {
		if namespace != "" {
			return nil, nil, fmt.Errorf("cannot specify both --namespace flag and namespace in table name")
		}

		parts := strings.Split(tableName, ".")
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("table name must be in format 'namespace.table' or just 'table'")
		}

		namespaceIdent = table.Identifier{parts[0]}
		tableIdent = table.Identifier{parts[0], parts[1]}
	} else {
		// Use provided namespace or default to "default"
		if namespace == "" {
			namespace = "default"
		}

		namespaceIdent = table.Identifier{namespace}
		tableIdent = table.Identifier{namespace, tableName}
	}

	return tableIdent, namespaceIdent, nil
}

// printSchema prints the inferred schema in a readable format
func printSchema(schema *importer.Schema) {
	if schema == nil {
		fmt.Println("  No schema information available")
		return
	}

	fmt.Printf("  Columns (%d):\n", len(schema.Fields))
	for i, field := range schema.Fields {
		nullable := ""
		if field.Nullable {
			nullable = " (nullable)"
		}
		fmt.Printf("    %d. %s: %s%s\n", i+1, field.Name, field.Type, nullable)
	}
}

// printStats prints file statistics in a readable format
func printStats(stats *importer.FileStats) {
	if stats == nil {
		fmt.Println("  No statistics available")
		return
	}

	fmt.Printf("  Records: %d\n", stats.RecordCount)
	fmt.Printf("  File size: %s\n", formatBytes(stats.FileSize))
	fmt.Printf("  Columns: %d\n", stats.ColumnCount)
}

// formatBytes formats a byte count as a human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
