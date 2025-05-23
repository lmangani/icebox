package cli

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "icebox",
	Short: "A single-binary playground for Apache Iceberg",
	Long: `Icebox is a single-binary playground for Apache Iceberg that provides
a "five-minutes-to-first-query" experience for table format experimentation.

It includes an embedded SQLite catalog, local filesystem storage,
and DuckDB integration for SQL queries.`,
	Version: "0.1.0",
}

// Execute runs the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags can be added here
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "verbose output")
}
