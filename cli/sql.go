package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/engine/duckdb"
	"github.com/spf13/cobra"
)

var sqlCmd = &cobra.Command{
	Use:   "sql [query]",
	Short: "Execute SQL queries against Iceberg tables",
	Long: `Execute SQL queries against your Iceberg tables using DuckDB.

The SQL engine automatically discovers and registers all tables from your
Icebox catalog, making them available for querying.

Examples:
  icebox sql "SELECT COUNT(*) FROM sales"
  icebox sql "SELECT region, SUM(amount) FROM sales GROUP BY region"
  icebox sql "SHOW TABLES"
  icebox sql "DESCRIBE sales"`,
	Args: cobra.ExactArgs(1),
	RunE: runSQL,
}

type sqlOptions struct {
	format       string
	maxRows      int
	showSchema   bool
	timing       bool
	autoRegister bool
}

var sqlOpts = &sqlOptions{}

func init() {
	rootCmd.AddCommand(sqlCmd)

	sqlCmd.Flags().StringVar(&sqlOpts.format, "format", "table", "output format: table, csv, json")
	sqlCmd.Flags().IntVar(&sqlOpts.maxRows, "max-rows", 1000, "maximum number of rows to display")
	sqlCmd.Flags().BoolVar(&sqlOpts.showSchema, "show-schema", false, "show column schema information")
	sqlCmd.Flags().BoolVar(&sqlOpts.timing, "timing", true, "show query execution time")
	sqlCmd.Flags().BoolVar(&sqlOpts.autoRegister, "auto-register", true, "automatically register catalog tables")
	sqlCmd.Flags().Bool("metrics", false, "show engine performance metrics after query")
}

func runSQL(cmd *cobra.Command, args []string) error {
	query := args[0]

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", err)
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

	// Create catalog
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w\n"+
			"💡 Your catalog may be corrupted. Try backing up and running 'icebox init' again", err)
	}
	defer catalog.Close()

	// Create SQL engine with enhanced configuration
	engineConfig := duckdb.DefaultEngineConfig()
	if cmd.Flag("verbose").Value.String() == "true" {
		engineConfig.EnableQueryLog = true
	}

	engine, err := duckdb.NewEngineWithConfig(catalog, engineConfig)
	if err != nil {
		return fmt.Errorf("❌ Failed to create SQL engine: %w\n"+
			"💡 This might be a DuckDB installation issue", err)
	}
	defer engine.Close()

	// Auto-register tables if enabled
	if sqlOpts.autoRegister {
		if err := autoRegisterTables(cmd.Context(), engine, catalog); err != nil {
			// Don't fail the query if auto-registration fails, just warn
			fmt.Fprintf(os.Stderr, "⚠️  Warning: Failed to auto-register some tables: %v\n", err)
		}
	}

	// Execute the query
	start := time.Now()
	result, err := engine.ExecuteQuery(cmd.Context(), query)
	if err != nil {
		// Enhanced error handling with helpful suggestions
		if strings.Contains(err.Error(), "timeout") {
			return fmt.Errorf("❌ Query timed out: %w\n"+
				"💡 Try simplifying your query or increasing --timeout", err)
		}
		if strings.Contains(err.Error(), "table") && strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("❌ Table not found: %w\n"+
				"💡 Run 'icebox sql \"SHOW TABLES\"' to see available tables", err)
		}
		return fmt.Errorf("❌ Query failed: %w", err)
	}
	duration := time.Since(start)

	// Display results
	if err := displayResults(result, duration); err != nil {
		return fmt.Errorf("❌ Failed to display results: %w", err)
	}

	// Show metrics if requested
	showMetrics, _ := cmd.Flags().GetBool("metrics")
	if showMetrics {
		metrics := engine.GetMetrics()
		fmt.Printf("\n📈 Engine Metrics:\n")
		fmt.Printf("  Queries Executed: %d\n", metrics.QueriesExecuted)
		fmt.Printf("  Tables Registered: %d\n", metrics.TablesRegistered)
		fmt.Printf("  Cache Hits: %d\n", metrics.CacheHits)
		fmt.Printf("  Cache Misses: %d\n", metrics.CacheMisses)
		fmt.Printf("  Total Query Time: %v\n", metrics.TotalQueryTime)
		fmt.Printf("  Error Count: %d\n", metrics.ErrorCount)
		if metrics.QueriesExecuted > 0 {
			avgTime := metrics.TotalQueryTime / time.Duration(metrics.QueriesExecuted)
			fmt.Printf("  Average Query Time: %v\n", avgTime)
		}
	}

	return nil
}

// autoRegisterTables automatically registers all catalog tables with the SQL engine
func autoRegisterTables(ctx context.Context, engine *duckdb.Engine, catalog catalog.CatalogInterface) error {
	// Get all namespaces
	namespaces, err := catalog.ListNamespaces(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	if len(namespaces) == 0 {
		fmt.Printf("📭 No namespaces found in catalog\n")
		fmt.Printf("💡 Try running 'icebox import <file.parquet> --table <table_name>' to create a table\n")
		return nil
	}

	fmt.Printf("🔍 Found %d namespaces: %v\n", len(namespaces), namespaces)

	registeredCount := 0
	var errors []string

	for _, namespace := range namespaces {
		fmt.Printf("🔍 Checking namespace '%s' for tables...\n", strings.Join(namespace, "."))

		// List tables in this namespace
		var tableCount int
		for identifier, err := range catalog.ListTables(ctx, namespace) {
			if err != nil {
				errors = append(errors, fmt.Sprintf("failed to list tables in namespace %v: %v", namespace, err))
				continue
			}

			tableCount++
			fmt.Printf("🔍 Found table: %s\n", strings.Join(identifier, "."))

			// Load the table
			icebergTable, err := catalog.LoadTable(ctx, identifier, nil)
			if err != nil {
				errors = append(errors, fmt.Sprintf("failed to load table %v: %v", identifier, err))
				continue
			}

			// Register with the SQL engine
			if err := engine.RegisterTable(ctx, identifier, icebergTable); err != nil {
				errors = append(errors, fmt.Sprintf("failed to register table %v: %v", identifier, err))
				continue
			}

			fmt.Printf("✅ Successfully registered table: %s\n", strings.Join(identifier, "."))
			registeredCount++
		}

		if tableCount == 0 {
			fmt.Printf("📭 No tables found in namespace '%s'\n", strings.Join(namespace, "."))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("registration errors: %s", strings.Join(errors, "; "))
	}

	if registeredCount > 0 {
		fmt.Printf("📋 Registered %d tables for querying\n", registeredCount)
	} else {
		fmt.Printf("📭 No tables found to register\n")
		fmt.Printf("💡 Try running 'icebox table list' to see what tables exist in your catalog\n")
	}

	return nil
}

// displayResults displays query results in the specified format with enterprise features
func displayResults(result *duckdb.QueryResult, duration time.Duration) error {
	// Show timing if enabled
	if sqlOpts.timing {
		fmt.Printf("⏱️  Query [%s] executed in %v\n", result.QueryID, result.Duration)
	}

	// Show row count
	if result.RowCount == 0 {
		fmt.Println("📭 No rows returned")
		return nil
	}

	// Handle large result sets with user-friendly messaging
	if result.RowCount >= 100000 {
		fmt.Printf("⚠️  Large result set detected (%d rows) - performance may vary\n", result.RowCount)
	}

	fmt.Printf("📊 %d rows returned\n", result.RowCount)

	// Show schema if requested
	if sqlOpts.showSchema {
		fmt.Println("📋 Schema:")
		for i, col := range result.Columns {
			fmt.Printf("  %d. %s\n", i+1, col)
		}
		fmt.Println()
	}

	// Limit rows if necessary
	rows := result.Rows
	if int64(len(rows)) > int64(sqlOpts.maxRows) {
		rows = rows[:sqlOpts.maxRows]
		fmt.Printf("⚠️  Showing first %d rows (use --max-rows to adjust)\n", sqlOpts.maxRows)
	}

	// Display results based on format
	switch sqlOpts.format {
	case "table":
		return displayTableFormat(result.Columns, rows)
	case "csv":
		return displayCSVFormat(result.Columns, rows)
	case "json":
		return displayJSONFormat(result.Columns, rows)
	default:
		return fmt.Errorf("unsupported format: %s", sqlOpts.format)
	}
}

// displayTableFormat displays results in a formatted table
func displayTableFormat(columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Calculate column widths
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}

	// Check data widths
	for _, row := range rows {
		for i, value := range row {
			if i < len(widths) {
				str := formatValue(value)
				if len(str) > widths[i] {
					widths[i] = len(str)
				}
			}
		}
	}

	// Cap column widths at 50 characters
	for i := range widths {
		if widths[i] > 50 {
			widths[i] = 50
		}
	}

	// Print header
	fmt.Print("┌")
	for i, width := range widths {
		fmt.Print(strings.Repeat("─", width+2))
		if i < len(widths)-1 {
			fmt.Print("┬")
		}
	}
	fmt.Println("┐")

	// Print column names
	fmt.Print("│")
	for i, col := range columns {
		fmt.Printf(" %-*s │", widths[i], truncateString(col, widths[i]))
	}
	fmt.Println()

	// Print separator
	fmt.Print("├")
	for i, width := range widths {
		fmt.Print(strings.Repeat("─", width+2))
		if i < len(widths)-1 {
			fmt.Print("┼")
		}
	}
	fmt.Println("┤")

	// Print rows
	for _, row := range rows {
		fmt.Print("│")
		for i, value := range row {
			if i < len(widths) {
				str := formatValue(value)
				fmt.Printf(" %-*s │", widths[i], truncateString(str, widths[i]))
			}
		}
		fmt.Println()
	}

	// Print footer
	fmt.Print("└")
	for i, width := range widths {
		fmt.Print(strings.Repeat("─", width+2))
		if i < len(widths)-1 {
			fmt.Print("┴")
		}
	}
	fmt.Println("┘")

	return nil
}

// displayCSVFormat displays results in CSV format
func displayCSVFormat(columns []string, rows [][]interface{}) error {
	// Print header
	fmt.Println(strings.Join(columns, ","))

	// Print rows
	for _, row := range rows {
		values := make([]string, len(row))
		for i, value := range row {
			values[i] = formatValueCSV(value)
		}
		fmt.Println(strings.Join(values, ","))
	}

	return nil
}

// displayJSONFormat displays results in JSON format
func displayJSONFormat(columns []string, rows [][]interface{}) error {
	fmt.Println("[")
	for i, row := range rows {
		fmt.Print("  {")
		for j, col := range columns {
			if j < len(row) {
				fmt.Printf(`"%s": "%s"`, col, formatValue(row[j]))
				if j < len(columns)-1 {
					fmt.Print(", ")
				}
			}
		}
		fmt.Print("}")
		if i < len(rows)-1 {
			fmt.Print(",")
		}
		fmt.Println()
	}
	fmt.Println("]")

	return nil
}

// formatValue formats a value for display
func formatValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}

// formatValueCSV formats a value for CSV output
func formatValueCSV(value interface{}) string {
	if value == nil {
		return ""
	}
	str := fmt.Sprintf("%v", value)
	// Escape newlines and other special characters for cleaner CSV output
	str = strings.ReplaceAll(str, "\n", "\\n")
	str = strings.ReplaceAll(str, "\r", "\\r")
	str = strings.ReplaceAll(str, "\t", "\\t")
	// Escape quotes and wrap in quotes if contains comma or quotes
	if strings.Contains(str, ",") || strings.Contains(str, "\"") || strings.Contains(str, "\\") {
		str = strings.ReplaceAll(str, "\"", "\"\"")
		str = "\"" + str + "\""
	}
	return str
}

// truncateString truncates a string to the specified length
func truncateString(str string, maxLen int) string {
	if len(str) <= maxLen {
		return str
	}
	if maxLen <= 3 {
		return str[:maxLen]
	}
	return str[:maxLen-3] + "..."
}
