package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
	"github.com/parquet-go/parquet-go"
)

// ParquetRow defines the schema for the demo Parquet file
type ParquetRow struct {
	Timestamp int64
	Value     int32
}

func main() {
	// Allow ICEBOX_BIN env to override binary path
	iceboxBin := os.Getenv("ICEBOX_BIN")
	if iceboxBin == "" {
		iceboxBin = "../icebox"
	}
	// Resolve to absolute path
	if !filepath.IsAbs(iceboxBin) {
		abs, err := filepath.Abs(iceboxBin)
		if err != nil {
			panic(fmt.Errorf("failed to resolve ICEBOX_BIN absolute path: %w", err))
		}
		iceboxBin = abs
	}

	// 1. Init Icebox project in a subdirectory of the current working directory
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	demoDir := filepath.Join(cwd, "icebox-demo-tmp")

	iceboxConfig := filepath.Join(demoDir, ".icebox.yml")
	if _, err := os.Stat(iceboxConfig); os.IsNotExist(err) {
		if err := os.MkdirAll(demoDir, 0755); err != nil {
			panic(err)
		}
		fmt.Println("✅ Created demo directory:", demoDir)

		// Initialize Icebox only if not already present
		initCmd := exec.Command(iceboxBin, "init", "icebox-demo-tmp")
		initCmd.Dir = cwd
		if out, err := initCmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "%s", out)
			panic(fmt.Errorf("failed to init icebox: %w", err))
		}
	} else {
		fmt.Println("✅ Using existing demo directory:", demoDir)
	}

	// 3. Create initial demo data (Parquet)
	parquetPath := filepath.Join(demoDir, "demo.parquet")
	writeParquetRows(parquetPath, []ParquetRow{{Timestamp: time.Now().Unix(), Value: 42}})

	// 4. Import initial data
	importCmd := exec.Command(iceboxBin, "import", parquetPath, "--table", "default.demo_table", "--infer-schema", "--overwrite")
	importCmd.Dir = demoDir
	if out, err := importCmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", out)
		panic(fmt.Errorf("failed to import initial data: %w", err))
	}
	os.Remove(parquetPath)

	// 5. Start appending and querying in a loop
	interval := 10 * time.Second
	if v := os.Getenv("ICEBOX_DEMO_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Get number of rows per round from env, default to 1
	rowsPerRound := 1
	if v := os.Getenv("ICEBOX_DEMO_ROWS_PER_ROUND"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			rowsPerRound = n
		}
	}

	for i := 0; i < 5; i++ { // Run 5 times for demo
		<-ticker.C
		// Write a new Parquet file with the desired number of new rows
		appendPath := filepath.Join(demoDir, fmt.Sprintf("demo_append_%d.parquet", i))
		var newRows []ParquetRow
		for j := 0; j < rowsPerRound; j++ {
			newRows = append(newRows, ParquetRow{Timestamp: time.Now().Unix(), Value: int32(i*rowsPerRound + j)})
		}
		writeParquetRows(appendPath, newRows)

		// Import just these new rows with --overwrite for diagnostic purposes
		appendCmd := exec.Command(iceboxBin, "import", appendPath, "--table", "default.demo_table", "--overwrite")
		appendCmd.Dir = demoDir
		if out, err := appendCmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "%s", out)
			os.Remove(appendPath)
			panic(fmt.Errorf("failed to append data: %w", err))
		}
		os.Remove(appendPath)

		// Query row count
		queryCmd := exec.Command(iceboxBin, "sql", "SELECT count(*) FROM demo_table")
		queryCmd.Dir = demoDir
		out, err := queryCmd.CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", out)
			panic(fmt.Errorf("failed to query count: %w", err))
		}
		fmt.Printf("Row count after append %d: %s\n", i+1, out)
	}

	fmt.Println("✅ Demo complete!")
}

// printTables runs SHOW TABLES and prints the result
func printTables(iceboxBin, demoDir string) {
	cmd := exec.Command(iceboxBin, "sql", "SHOW TABLES")
	cmd.Dir = demoDir
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("Error running SHOW TABLES: %v\n", err)
		return
	}
	fmt.Printf("Current tables:\n%s\n", out.String())
}

// writeParquetRows writes a slice of ParquetRow to a Parquet file
func writeParquetRows(path string, rows []ParquetRow) {
	if err := parquet.WriteFile(path, rows); err != nil {
		panic(fmt.Errorf("failed to write parquet file: %w", err))
	}
}

// readAllRowsFromIcebox queries all rows from the demo_table using icebox sql and returns them as []ParquetRow
func readAllRowsFromIcebox(demoDir, iceboxBin string) []ParquetRow {
	cmd := exec.Command(iceboxBin, "sql", "SELECT timestamp, value FROM demo_table")
	cmd.Dir = demoDir
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		// If table doesn't exist yet, return empty
		return []ParquetRow{}
	}
	// Parse output (assume CSV-like output)
	lines := bytes.Split(out.Bytes(), []byte{'\n'})
	var rows []ParquetRow
	for _, line := range lines[1:] { // skip header
		if len(line) == 0 {
			continue
		}
		var ts int64
		var val int32
		fmt.Sscanf(string(line), "%d,%d", &ts, &val)
		rows = append(rows, ParquetRow{Timestamp: ts, Value: val})
	}
	return rows
} 