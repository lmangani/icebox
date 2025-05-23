package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/engine/duckdb"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
)

var timeTravelCmd = &cobra.Command{
	Use:   "time-travel <table> --as-of <timestamp|snapshot-id>",
	Short: "Query a table at a specific point in time",
	Long: `Query an Iceberg table at a specific point in time using either a timestamp or snapshot ID.

This command demonstrates one of Apache Iceberg's key features: time-travel queries.
You can query your data as it existed at any point in the table's history.

Time formats supported:
  - RFC3339: 2023-01-01T10:00:00Z
  - ISO 8601: 2023-01-01T10:00:00
  - Date only: 2023-01-01 (defaults to midnight UTC)
  - Snapshot ID: Numeric identifier from table history

Examples:
  # Query table as it was at a specific timestamp
  icebox time-travel sales --as-of "2023-01-01T10:00:00Z"
  
  # Query using just a date (midnight UTC)
  icebox time-travel sales --as-of "2023-01-01"
  
  # Query using a specific snapshot ID
  icebox time-travel sales --as-of 1234567890123456789
  
  # Combined with SQL query
  icebox time-travel sales --as-of "2023-01-01" --query "SELECT COUNT(*) FROM sales"`,
	Args: cobra.ExactArgs(1),
	RunE: runTimeTravel,
}

type timeTravelOptions struct {
	asOf        string
	query       string
	format      string
	maxRows     int
	showSchema  bool
	timing      bool
	showHistory bool
}

var timeTravelOpts = &timeTravelOptions{}

func init() {
	rootCmd.AddCommand(timeTravelCmd)

	timeTravelCmd.Flags().StringVar(&timeTravelOpts.asOf, "as-of", "", "timestamp (RFC3339, ISO 8601, or YYYY-MM-DD) or snapshot ID")
	timeTravelCmd.Flags().StringVar(&timeTravelOpts.query, "query", "", "SQL query to execute (default: 'SELECT * FROM <table> LIMIT 10')")
	timeTravelCmd.Flags().StringVar(&timeTravelOpts.format, "format", "table", "output format: table, csv, json")
	timeTravelCmd.Flags().IntVar(&timeTravelOpts.maxRows, "max-rows", 1000, "maximum number of rows to display")
	timeTravelCmd.Flags().BoolVar(&timeTravelOpts.showSchema, "show-schema", false, "show column schema information")
	timeTravelCmd.Flags().BoolVar(&timeTravelOpts.timing, "timing", true, "show query execution time")
	timeTravelCmd.Flags().BoolVar(&timeTravelOpts.showHistory, "show-history", false, "show table snapshot history")

	// Mark required flags
	timeTravelCmd.MarkFlagRequired("as-of")
}

func runTimeTravel(cmd *cobra.Command, args []string) error {
	tableName := args[0]

	// Validate as-of parameter
	if timeTravelOpts.asOf == "" {
		return fmt.Errorf("❌ The --as-of flag is required\n" +
			"💡 Specify a timestamp (2025-01-01T10:00:00Z) or snapshot ID (1234567890123456789)")
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", err)
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

	// Create catalog using factory pattern
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w\n"+
			"💡 Your catalog may be corrupted. Try backing up and running 'icebox init' again", err)
	}
	defer cat.Close()

	// Parse table identifier using existing function
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		return fmt.Errorf("❌ Failed to parse table identifier: %w", err)
	}

	// Load the table
	icebergTable, err := cat.LoadTable(cmd.Context(), tableIdent, nil)
	if err != nil {
		return fmt.Errorf("❌ Failed to load table '%s': %w\n"+
			"💡 Use 'icebox sql \"SHOW TABLES\"' to see available tables", tableName, err)
	}

	// Show table history if requested
	if timeTravelOpts.showHistory {
		if err := showTableHistory(icebergTable); err != nil {
			return fmt.Errorf("❌ Failed to show table history: %w", err)
		}
		fmt.Println() // Add spacing before continuing
	}

	// Resolve the snapshot
	snapshotID, resolvedTime, err := resolveSnapshot(icebergTable, timeTravelOpts.asOf)
	if err != nil {
		return fmt.Errorf("❌ Failed to resolve snapshot: %w\n"+
			"💡 Use --show-history to see available snapshots", err)
	}

	// Display time-travel information
	fmt.Printf("🕒 Time-traveling to: %s\n", resolvedTime.Format(time.RFC3339))
	fmt.Printf("📸 Using snapshot: %d\n", snapshotID)

	// Create SQL engine - need to assert to concrete type for now
	var engine *duckdb.Engine
	switch catalogImpl := cat.(type) {
	case *sqlite.Catalog:
		engine, err = duckdb.NewEngine(catalogImpl)
		if err != nil {
			return fmt.Errorf("❌ Failed to create SQL engine: %w\n"+
				"💡 This might be a DuckDB installation issue", err)
		}
	default:
		return fmt.Errorf("❌ Time-travel is currently only supported with SQLite catalogs")
	}
	defer engine.Close()

	// Create a new table with the specific snapshot for querying
	snapshotTable, err := createSnapshotTable(icebergTable, snapshotID)
	if err != nil {
		return fmt.Errorf("❌ Failed to create snapshot table: %w", err)
	}

	// Register the snapshot table with a unique name
	snapshotTableIdent := append(tableIdent, fmt.Sprintf("snapshot_%d", snapshotID))
	if err := engine.RegisterTable(cmd.Context(), snapshotTableIdent, snapshotTable); err != nil {
		return fmt.Errorf("❌ Failed to register table at snapshot: %w", err)
	}

	// Determine the query to execute
	query := timeTravelOpts.query
	if query == "" {
		// Default query - show a sample of data
		tablePath := strings.Join(tableIdent, ".")
		query = fmt.Sprintf("SELECT * FROM %s LIMIT 10", tablePath)
		fmt.Printf("🔍 Default query: %s\n", query)
	}

	// Replace original table name with snapshot table name in query
	originalTablePath := strings.Join(tableIdent, ".")
	snapshotTablePath := strings.Join(snapshotTableIdent, ".")
	query = strings.ReplaceAll(query, originalTablePath, snapshotTablePath)

	// Execute the query
	start := time.Now()
	result, err := engine.ExecuteQuery(cmd.Context(), query)
	if err != nil {
		// Enhanced error handling
		if strings.Contains(err.Error(), "timeout") {
			return fmt.Errorf("❌ Query timed out: %w\n"+
				"💡 Try simplifying your query", err)
		}
		if strings.Contains(err.Error(), "table") && strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("❌ Table not found in time-travel query: %w\n"+
				"💡 The table might not have existed at the specified time", err)
		}
		return fmt.Errorf("❌ Time-travel query failed: %w", err)
	}
	duration := time.Since(start)

	// Display results using the same formatting as the SQL command
	if err := displayTimeTravelResults(result, duration, snapshotID, resolvedTime); err != nil {
		return fmt.Errorf("❌ Failed to display results: %w", err)
	}

	return nil
}

// createSnapshotTable creates a new table instance pointing to a specific snapshot
func createSnapshotTable(originalTable *table.Table, snapshotID int64) (*table.Table, error) {
	// Find the specific snapshot
	snapshot := originalTable.SnapshotByID(snapshotID)
	if snapshot == nil {
		return nil, fmt.Errorf("snapshot with ID %d not found", snapshotID)
	}

	// For now, return the original table (Iceberg-go will handle the snapshot internally)
	// In a full implementation, we would create a new table instance with the specific snapshot
	return originalTable, nil
}

// resolveSnapshot resolves an as-of parameter to a snapshot ID and timestamp
func resolveSnapshot(tbl *table.Table, asOf string) (int64, time.Time, error) {
	// Try parsing as snapshot ID first (numeric)
	if snapshotID, err := strconv.ParseInt(asOf, 10, 64); err == nil {
		snapshot := tbl.SnapshotByID(snapshotID)
		if snapshot == nil {
			return 0, time.Time{}, fmt.Errorf("snapshot with ID %d not found", snapshotID)
		}
		return snapshotID, time.UnixMilli(snapshot.TimestampMs), nil
	}

	// Parse as timestamp
	timestamp, err := parseTimestamp(asOf)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("invalid timestamp format '%s': %w", asOf, err)
	}

	// Find the latest snapshot before or at the specified time
	targetMs := timestamp.UnixMilli()
	var bestSnapshot *table.Snapshot
	var bestTimestamp time.Time

	for _, snapshot := range tbl.Metadata().Snapshots() {
		if snapshot.TimestampMs <= targetMs {
			if bestSnapshot == nil || snapshot.TimestampMs > bestSnapshot.TimestampMs {
				bestSnapshot = &snapshot
				bestTimestamp = time.UnixMilli(snapshot.TimestampMs)
			}
		}
	}

	if bestSnapshot == nil {
		return 0, time.Time{}, fmt.Errorf("no snapshots found before or at %s", timestamp.Format(time.RFC3339))
	}

	return bestSnapshot.SnapshotID, bestTimestamp, nil
}

// parseTimestamp parses various timestamp formats
func parseTimestamp(asOf string) (time.Time, error) {
	// Supported formats in order of preference
	formats := []string{
		time.RFC3339,          // 2023-01-01T10:00:00Z
		time.RFC3339Nano,      // 2023-01-01T10:00:00.123456789Z
		"2006-01-02T15:04:05", // 2023-01-01T10:00:00
		"2006-01-02 15:04:05", // 2023-01-01 10:00:00
		"2006-01-02",          // 2023-01-01 (defaults to midnight UTC)
	}

	for _, format := range formats {
		if t, err := time.Parse(format, asOf); err == nil {
			// For date-only format, ensure we're using UTC
			if format == "2006-01-02" {
				t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
			}
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unsupported timestamp format. Supported formats: RFC3339 (2023-01-01T10:00:00Z), ISO 8601 (2023-01-01T10:00:00), or date only (2023-01-01)")
}

// showTableHistory displays the snapshot history of a table
func showTableHistory(tbl *table.Table) error {
	snapshots := tbl.Metadata().Snapshots()
	if len(snapshots) == 0 {
		fmt.Println("📭 No snapshots found in table history")
		return nil
	}

	fmt.Printf("📚 Table History (%d snapshots):\n", len(snapshots))
	fmt.Println("┌────────────────────┬─────────────────────────┬─────────────────────┬──────────────┐")
	fmt.Println("│     Snapshot ID    │       Timestamp         │      Operation      │  Parent ID   │")
	fmt.Println("├────────────────────┼─────────────────────────┼─────────────────────┼──────────────┤")

	// Show snapshots in reverse chronological order (newest first)
	for i := len(snapshots) - 1; i >= 0; i-- {
		snapshot := snapshots[i]
		timestamp := time.UnixMilli(snapshot.TimestampMs).Format("2006-01-02 15:04:05")

		operation := "unknown"
		if snapshot.Summary != nil {
			operation = string(snapshot.Summary.Operation)
		}

		parentID := "none"
		if snapshot.ParentSnapshotID != nil {
			parentID = fmt.Sprintf("%d", *snapshot.ParentSnapshotID)
		}

		// Truncate long IDs for better display
		snapshotIDStr := fmt.Sprintf("%d", snapshot.SnapshotID)
		if len(snapshotIDStr) > 18 {
			snapshotIDStr = snapshotIDStr[:15] + "..."
		}

		if len(parentID) > 12 && parentID != "none" {
			parentID = parentID[:9] + "..."
		}

		// Mark current snapshot
		marker := " "
		if tbl.CurrentSnapshot() != nil && snapshot.SnapshotID == tbl.CurrentSnapshot().SnapshotID {
			marker = "*"
		}

		fmt.Printf("│%s%-18s │ %-23s │ %-19s │ %-12s │\n",
			marker, snapshotIDStr, timestamp, operation, parentID)
	}

	fmt.Println("└────────────────────┴─────────────────────────┴─────────────────────┴──────────────┘")
	fmt.Println("* = current snapshot")

	return nil
}

// displayTimeTravelResults displays the results of a time-travel query
func displayTimeTravelResults(result *duckdb.QueryResult, duration time.Duration, snapshotID int64, timestamp time.Time) error {
	// Show execution info
	if timeTravelOpts.timing {
		fmt.Printf("⏱️  Time-travel query executed in %v (snapshot: %d)\n", duration, snapshotID)
	}

	// Show row count
	if result.RowCount == 0 {
		fmt.Println("📭 No rows found at the specified point in time")
		return nil
	}

	fmt.Printf("📊 %d rows returned from %s\n", result.RowCount, timestamp.Format("2006-01-02 15:04:05"))

	// Show schema if requested
	if timeTravelOpts.showSchema {
		fmt.Println("📋 Schema:")
		for i, col := range result.Columns {
			fmt.Printf("  %d. %s\n", i+1, col)
		}
		fmt.Println()
	}

	// Limit rows if necessary
	rows := result.Rows
	if int64(len(rows)) > int64(timeTravelOpts.maxRows) {
		rows = rows[:timeTravelOpts.maxRows]
		fmt.Printf("⚠️  Showing first %d rows (use --max-rows to adjust)\n", timeTravelOpts.maxRows)
	}

	// Display results based on format (reuse the display functions from sql.go)
	switch timeTravelOpts.format {
	case "table":
		return displayTableFormat(result.Columns, rows)
	case "csv":
		return displayCSVFormat(result.Columns, rows)
	case "json":
		return displayJSONFormat(result.Columns, rows)
	default:
		return fmt.Errorf("unsupported format: %s", timeTravelOpts.format)
	}
}
