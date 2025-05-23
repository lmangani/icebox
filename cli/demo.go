package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/importer"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Set up demo datasets for exploring Iceberg features",
	Long: `Set up demo datasets to quickly explore Apache Iceberg features.

This command creates sample namespaces and imports demo datasets including:
- Flight data for analytics queries
- Date examples for time-based operations  
- Decimal data for financial calculations

Perfect for newcomers to get started with Iceberg in under 5 minutes!

Examples:
  icebox demo                        # Set up all demo datasets
  icebox demo --dataset flights      # Set up only flights data
  icebox demo --list                 # List available demo datasets`,
	RunE: runDemo,
}

type demoOptions struct {
	dataset string
	list    bool
	cleanup bool
	force   bool
	verbose bool
}

var demoOpts = &demoOptions{}

// DemoDataset represents a demo dataset configuration
type DemoDataset struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Namespace   string            `json:"namespace"`
	Table       string            `json:"table"`
	File        string            `json:"file"`
	Properties  map[string]string `json:"properties"`
	Queries     []DemoQuery       `json:"sample_queries"`
}

// DemoQuery represents a sample query for demo datasets
type DemoQuery struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

func init() {
	rootCmd.AddCommand(demoCmd)

	demoCmd.Flags().StringVar(&demoOpts.dataset, "dataset", "", "specific dataset to set up (flights, dates, decimals)")
	demoCmd.Flags().BoolVar(&demoOpts.list, "list", false, "list available demo datasets")
	demoCmd.Flags().BoolVar(&demoOpts.cleanup, "cleanup", false, "remove all demo datasets")
	demoCmd.Flags().BoolVar(&demoOpts.force, "force", false, "overwrite existing demo datasets")
	demoCmd.Flags().BoolVar(&demoOpts.verbose, "verbose", false, "show detailed progress")
}

func runDemo(cmd *cobra.Command, args []string) error {
	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", err)
	}

	if demoOpts.verbose {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

	// Handle list option
	if demoOpts.list {
		return listDemoDatasets()
	}

	// Handle cleanup option
	if demoOpts.cleanup {
		return cleanupDemoDatasets(cfg)
	}

	// Set up demo datasets
	return setupDemoDatasets(cfg)
}

func listDemoDatasets() error {
	datasets := getAvailableDatasets()

	fmt.Printf("🎬 Available Demo Datasets:\n\n")

	for _, dataset := range datasets {
		fmt.Printf("📊 **%s**\n", dataset.Name)
		fmt.Printf("   Description: %s\n", dataset.Description)
		fmt.Printf("   Namespace: %s\n", dataset.Namespace)
		fmt.Printf("   Table: %s\n", dataset.Table)
		fmt.Printf("   Sample Queries: %d\n", len(dataset.Queries))
		fmt.Printf("\n")
	}

	fmt.Printf("💡 Usage:\n")
	fmt.Printf("   icebox demo                           # Set up all datasets\n")
	fmt.Printf("   icebox demo --dataset flights         # Set up specific dataset\n")
	fmt.Printf("   icebox demo --cleanup                 # Remove all demo data\n")

	return nil
}

func setupDemoDatasets(cfg *config.Config) error {
	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Create importer
	imp, err := importer.NewParquetImporter(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create importer: %w", err)
	}
	defer imp.Close()

	datasets := getAvailableDatasets()

	// Filter datasets if specific dataset requested
	if demoOpts.dataset != "" {
		filtered := make([]DemoDataset, 0)
		for _, ds := range datasets {
			if ds.Name == demoOpts.dataset {
				filtered = append(filtered, ds)
				break
			}
		}
		if len(filtered) == 0 {
			return fmt.Errorf("❌ Dataset '%s' not found. Use --list to see available datasets", demoOpts.dataset)
		}
		datasets = filtered
	}

	fmt.Printf("🎬 Setting up demo datasets...\n\n")

	ctx := context.Background()

	for _, dataset := range datasets {
		if err := setupSingleDataset(ctx, cat, imp, dataset); err != nil {
			if demoOpts.force {
				fmt.Printf("⚠️  Warning: Failed to setup %s: %v\n", dataset.Name, err)
				continue
			}
			return fmt.Errorf("❌ Failed to setup dataset %s: %w", dataset.Name, err)
		}
	}

	fmt.Printf("\n✅ Demo setup complete!\n")
	fmt.Printf("\n🚀 Try these commands:\n")

	for _, dataset := range datasets {
		fmt.Printf("\n📊 %s:\n", dataset.Name)
		for _, query := range dataset.Queries {
			fmt.Printf("   # %s\n", query.Description)
			fmt.Printf("   icebox sql \"%s\"\n", query.SQL)
		}
	}

	fmt.Printf("\n💡 Additional commands:\n")
	fmt.Printf("   icebox table list --namespace demo    # List demo tables\n")
	fmt.Printf("   icebox shell                          # Interactive SQL shell\n")
	fmt.Printf("   icebox demo --cleanup                 # Remove demo data\n")

	return nil
}

func setupSingleDataset(ctx context.Context, cat catalog.CatalogInterface, imp *importer.ParquetImporter, dataset DemoDataset) error {
	// Create namespace
	namespace := table.Identifier{dataset.Namespace}
	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check namespace: %w", err)
	}

	if !exists {
		props := iceberg.Properties{
			"description": "Demo namespace for sample datasets",
			"created_by":  "icebox-demo",
		}
		if err := cat.CreateNamespace(ctx, namespace, props); err != nil {
			return fmt.Errorf("failed to create namespace: %w", err)
		}
		fmt.Printf("✅ Created namespace: %s\n", dataset.Namespace)
	}

	// Check if table exists
	tableIdent := table.Identifier{dataset.Namespace, dataset.Table}
	tableExists, err := cat.CheckTableExists(ctx, tableIdent)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if tableExists {
		if !demoOpts.force {
			fmt.Printf("⚠️  Table %s.%s already exists (use --force to overwrite)\n", dataset.Namespace, dataset.Table)
			return nil
		}

		// Drop existing table
		if err := cat.DropTable(ctx, tableIdent); err != nil {
			return fmt.Errorf("failed to drop existing table: %w", err)
		}
		fmt.Printf("🗑️  Dropped existing table: %s.%s\n", dataset.Namespace, dataset.Table)
	}

	// Get absolute path to dataset file
	dataFile := filepath.Join("testdata", dataset.File)
	absDataFile, err := filepath.Abs(dataFile)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Import the dataset
	fmt.Printf("📥 Importing %s dataset...\n", dataset.Name)

	// Infer schema first
	schema, stats, err := imp.InferSchema(absDataFile)
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	if demoOpts.verbose {
		fmt.Printf("   Schema: %d columns, %d rows, %s\n",
			len(schema.Fields), stats.RecordCount, formatBytes(stats.FileSize))
	}

	// Import table
	result, err := imp.ImportTable(ctx, importer.ImportRequest{
		ParquetFile:    absDataFile,
		TableIdent:     tableIdent,
		NamespaceIdent: namespace,
		Schema:         schema,
		Overwrite:      demoOpts.force,
		PartitionBy:    nil, // No partitioning for demo data
	})
	if err != nil {
		return fmt.Errorf("failed to import dataset: %w", err)
	}

	fmt.Printf("✅ Imported %s: %d records, %s\n",
		dataset.Name, result.RecordCount, formatBytes(result.DataSize))

	return nil
}

func cleanupDemoDatasets(cfg *config.Config) error {
	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	ctx := context.Background()
	namespace := table.Identifier{"demo"}

	// Check if demo namespace exists
	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return fmt.Errorf("❌ Failed to check namespace: %w", err)
	}

	if !exists {
		fmt.Printf("📭 No demo datasets found to clean up\n")
		return nil
	}

	fmt.Printf("🧹 Cleaning up demo datasets...\n")

	// List and drop all tables in demo namespace
	var tables []table.Identifier
	for identifier, err := range cat.ListTables(ctx, namespace) {
		if err != nil {
			return fmt.Errorf("❌ Failed to list tables: %w", err)
		}
		tables = append(tables, identifier)
	}

	for _, tableIdent := range tables {
		if err := cat.DropTable(ctx, tableIdent); err != nil {
			fmt.Printf("⚠️  Failed to drop table %v: %v\n", tableIdent, err)
		} else {
			fmt.Printf("🗑️  Dropped table: %s\n", strings.Join(tableIdent, "."))
		}
	}

	// Drop namespace
	if err := cat.DropNamespace(ctx, namespace); err != nil {
		return fmt.Errorf("❌ Failed to drop demo namespace: %w", err)
	}

	fmt.Printf("✅ Demo cleanup complete!\n")
	return nil
}

func getAvailableDatasets() []DemoDataset {
	return []DemoDataset{
		{
			Name:        "flights",
			Description: "Flight data for analytics and time-series queries",
			Namespace:   "demo",
			Table:       "flights",
			File:        "flights.parquet",
			Properties: map[string]string{
				"data.source": "demo",
				"data.type":   "flights",
			},
			Queries: []DemoQuery{
				{
					Name:        "count_flights",
					Description: "Count total number of flights",
					SQL:         "SELECT COUNT(*) as total_flights FROM demo.flights",
				},
				{
					Name:        "top_carriers",
					Description: "Find top carriers by flight count",
					SQL:         "SELECT carrier, COUNT(*) as flights FROM demo.flights GROUP BY carrier ORDER BY flights DESC LIMIT 5",
				},
				{
					Name:        "average_delay",
					Description: "Calculate average departure delay",
					SQL:         "SELECT AVG(dep_delay) as avg_delay FROM demo.flights WHERE dep_delay IS NOT NULL",
				},
			},
		},
		{
			Name:        "dates",
			Description: "Date examples for temporal operations and time-travel queries",
			Namespace:   "demo",
			Table:       "dates",
			File:        "date.parquet",
			Properties: map[string]string{
				"data.source": "demo",
				"data.type":   "temporal",
			},
			Queries: []DemoQuery{
				{
					Name:        "list_dates",
					Description: "Show all date records",
					SQL:         "SELECT * FROM demo.dates",
				},
				{
					Name:        "date_functions",
					Description: "Demonstrate date functions",
					SQL:         "SELECT date_col, EXTRACT(year FROM date_col) as year, EXTRACT(month FROM date_col) as month FROM demo.dates",
				},
			},
		},
		{
			Name:        "decimals",
			Description: "Decimal precision data for financial calculations",
			Namespace:   "demo",
			Table:       "decimals",
			File:        "decimals.parquet",
			Properties: map[string]string{
				"data.source": "demo",
				"data.type":   "financial",
			},
			Queries: []DemoQuery{
				{
					Name:        "precision_demo",
					Description: "Show decimal precision handling",
					SQL:         "SELECT * FROM demo.decimals LIMIT 5",
				},
				{
					Name:        "decimal_math",
					Description: "Perform calculations with decimal data",
					SQL:         "SELECT SUM(value) as total, AVG(value) as average FROM demo.decimals",
				},
			},
		},
	}
}
