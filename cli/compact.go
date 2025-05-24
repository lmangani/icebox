package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"database/sql"
	"time"

	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/tableops"
	"github.com/spf13/cobra"
)

var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "Compact Parquet files for a table by timerange",
	RunE:  runCompact,
}

func init() {
	rootCmd.AddCommand(compactCmd)
	compactCmd.Flags().String("table", "", "target table (namespace.table or just table)")
	compactCmd.Flags().String("by", "day", "timerange for compaction (e.g., day, hour)")
	compactCmd.Flags().String("commit-parquet", "", "commit a Parquet file as a new snapshot for the table")
	compactCmd.Flags().String("glob", "", "glob pattern to select Parquet files for compaction (e.g., 'data/*.parquet')")
	compactCmd.MarkFlagRequired("table")
}

func runCompact(cmd *cobra.Command, args []string) error {
	// 1. Load config and catalog
	_, cfg, err := config.FindConfig()
	if err != nil {
		return err
	}
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		return err
	}
	defer catalog.Close()

	// 2. Parse table identifier
	tableName, _ := cmd.Flags().GetString("table")
	tableIdent, namespaceIdent, err := parseTableIdentifier(tableName, "")
	if err != nil {
		return err
	}

	commitParquet, _ := cmd.Flags().GetString("commit-parquet")
	if commitParquet != "" {
		fmt.Printf("Committing Parquet file %s as new snapshot for table %v...\n", commitParquet, tableIdent)
		ctx := context.Background()
		tbl, err := catalog.LoadTable(ctx, tableIdent, nil)
		if err != nil {
			return fmt.Errorf("failed to load table: %w", err)
		}
		writer := tableops.NewWriter(catalog)
		opts := tableops.DefaultWriteOptions()
		err = writer.WriteParquetFile(ctx, tbl, commitParquet, opts)
		if err != nil {
			return fmt.Errorf("failed to write compacted file: %w", err)
		}
		fmt.Println("✅ Compacted file committed as new snapshot.")
		return nil
	}

	globPattern, _ := cmd.Flags().GetString("glob")
	if globPattern != "" {
		fmt.Printf("Compacting files matching glob: %s\n", globPattern)
		files, err := filepath.Glob(globPattern)
		if err != nil {
			return fmt.Errorf("failed to glob files: %w", err)
		}
		if len(files) == 0 {
			return fmt.Errorf("no files matched glob: %s", globPattern)
		}
		fmt.Printf("Found %d files to compact:\n", len(files))
		for _, f := range files {
			fmt.Printf("  - %s\n", f)
		}

		// Load the table
		ctx := context.Background()
		_, err = catalog.LoadTable(ctx, tableIdent, nil)
		if err != nil {
			return fmt.Errorf("failed to load table: %w", err)
		}

		// Use DuckDB to merge files into one Arrow table
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			return fmt.Errorf("failed to open DuckDB: %w", err)
		}
		defer db.Close()

		var unionParts []string
		for _, file := range files {
			abs, _ := filepath.Abs(file)
			unionParts = append(unionParts, fmt.Sprintf("SELECT * FROM read_parquet('%s')", abs))
		}
		unionQuery := strings.Join(unionParts, " UNION ALL ")
		tableName := "compacted"
		createSQL := fmt.Sprintf("CREATE TABLE %s AS %s", tableName, unionQuery)
		_, err = db.Exec(createSQL)
		if err != nil {
			return fmt.Errorf("failed to create compacted table in DuckDB: %w", err)
		}

		rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
		if err != nil {
			return fmt.Errorf("failed to query compacted table: %w", err)
		}
		defer rows.Close()

		// Export merged DuckDB table to a new Parquet file
		compactedName := fmt.Sprintf("compacted-%d.parquet", time.Now().UnixNano())
		compactedPath := filepath.Join(filepath.Dir(files[0]), compactedName)
		exportSQL := fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' (FORMAT 'parquet')", tableName, compactedPath)
		_, err = db.Exec(exportSQL)
		if err != nil {
			return fmt.Errorf("failed to export compacted table to Parquet: %w", err)
		}
		fmt.Printf("Exported merged table to: %s\n", compactedPath)

		// Commit the new Parquet file as a new snapshot
		writer := tableops.NewWriter(catalog)
		tbl, err := catalog.LoadTable(ctx, tableIdent, nil)
		if err != nil {
			return fmt.Errorf("failed to reload table: %w", err)
		}
		err = writer.WriteParquetFile(ctx, tbl, compactedPath, tableops.DefaultWriteOptions())
		if err != nil {
			return fmt.Errorf("failed to commit compacted file: %w", err)
		}
		fmt.Printf("✅ Compacted %d files into %s and committed as new snapshot.\n", len(files), compactedPath)

		// Remove old files
		for _, f := range files {
			if err := os.Remove(f); err != nil {
				fmt.Printf("Warning: failed to remove old file %s: %v\n", f, err)
			}
		}
		fmt.Println("Old files removed.")
		return nil
	}

	fmt.Printf("🔍 Compaction for table: %v (namespace: %v)\n", tableIdent, namespaceIdent)

	// 3. Load the Iceberg table and print data files referenced by the latest snapshot
	ctx := context.Background()
	tbl, err := catalog.LoadTable(ctx, tableIdent, nil)
	if err != nil {
		return fmt.Errorf("failed to load table: %w", err)
	}
	meta := tbl.Metadata()
	snapshots := meta.Snapshots()
	if len(snapshots) == 0 {
		fmt.Printf("No snapshots found for table %v.\n", tableIdent)
	}
	if len(snapshots) > 0 {
		latest := snapshots[len(snapshots)-1]
		fmt.Printf("Latest snapshot ID: %v\n", latest.SnapshotID)
		fmt.Printf("Timestamp: %v\n", latest.TimestampMs)
		fmt.Printf("Manifest count: %d\n", len(latest.ManifestList))
		if len(latest.ManifestList) > 0 {
			fmt.Printf("Manifest files for latest snapshot (these reference the data files):\n")
			for _, manifest := range latest.ManifestList {
				fmt.Printf("  - %s\n", manifest)
			}
		}
	}

	// 4. Use the table location to find all Parquet files in the data directory (like DuckDB)
	location := tbl.Location()
	if location == "" {
		fmt.Printf("Table %v has no location set.\n", tableIdent)
		return nil
	}
	localPath := location
	if len(localPath) > 7 && localPath[:7] == "file://" {
		localPath = localPath[7:]
	}
	dataPath := localPath + "/data"
	files, err := filepath.Glob(dataPath + "/*.parquet")
	if err != nil {
		return fmt.Errorf("failed to list Parquet files in %s: %w", dataPath, err)
	}
	if len(files) == 0 {
		fmt.Printf("No Parquet files found in %s\n", dataPath)
		return nil
	}
	fmt.Printf("Found %d Parquet files in %s:\n", len(files), dataPath)
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			fmt.Printf("  - %s (error reading file info)\n", f)
			continue
		}
		fmt.Printf("  - %s (size: %d bytes)\n", f, info.Size())
	}

	// 5. Group by timerange (to be implemented)
	// 6. Merge with DuckDB (to be implemented)
	// 7. Update catalog and remove old files (to be implemented)

	return nil
} 