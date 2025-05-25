package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/icebox/catalog"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table"
	_ "github.com/marcboeker/go-duckdb/v2"
)

// Engine provides SQL query capabilities using DuckDB with native Iceberg support
type Engine struct {
	db               *sql.DB
	catalog          catalog.CatalogInterface
	allocator        memory.Allocator
	initialized      bool
	config           *EngineConfig
	logger           *log.Logger
	metrics          *EngineMetrics
	mutex            sync.RWMutex
	icebergAvailable bool // Track if iceberg extension is available
}

// EngineConfig holds configuration options for the engine
type EngineConfig struct {
	MaxMemoryMB        int
	QueryTimeoutSec    int
	EnableQueryLog     bool
	EnableOptimization bool
	CacheSize          int
	IcebergCatalogName string // Name for the attached Iceberg catalog in DuckDB
}

// EngineMetrics tracks engine performance metrics
type EngineMetrics struct {
	QueriesExecuted  int64
	TablesRegistered int64
	CacheHits        int64
	CacheMisses      int64
	TotalQueryTime   time.Duration
	ErrorCount       int64
	mu               sync.RWMutex
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	Schema   *arrow.Schema
	Table    arrow.Table
	RowCount int64
	Duration time.Duration
	QueryID  string
}

// DefaultEngineConfig returns a default configuration for the engine
func DefaultEngineConfig() *EngineConfig {
	return &EngineConfig{
		MaxMemoryMB:        512,
		QueryTimeoutSec:    300,
		EnableQueryLog:     false,
		EnableOptimization: true,
		CacheSize:          100,
		IcebergCatalogName: "iceberg_catalog",
	}
}

// NewEngine creates a new DuckDB engine instance with catalog-agnostic support
func NewEngine(cat catalog.CatalogInterface) (*Engine, error) {
	return NewEngineWithConfig(cat, DefaultEngineConfig())
}

// NewEngineWithConfig creates a new DuckDB engine with custom configuration
func NewEngineWithConfig(cat catalog.CatalogInterface, config *EngineConfig) (*Engine, error) {
	if cat == nil {
		return nil, fmt.Errorf("catalog cannot be nil")
	}

	if config == nil {
		config = DefaultEngineConfig()
	}

	// Create DuckDB connection with optimized settings
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping DuckDB: %w", err)
	}

	// Set connection pool settings for better performance
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	engine := &Engine{
		db:        db,
		catalog:   cat,
		allocator: memory.NewGoAllocator(),
		config:    config,
		metrics:   &EngineMetrics{},
		logger:    log.Default(),
	}

	// Initialize the engine with optimizations
	if err := engine.initialize(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize engine: %w", err)
	}

	return engine, nil
}

// initialize sets up the DuckDB engine with performance optimizations and Iceberg support
func (e *Engine) initialize() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Configure DuckDB for optimal performance
	coreOptimizations := []string{
		"SET enable_progress_bar = false",
		"SET enable_object_cache = true",
		"SET enable_http_metadata_cache = true",
		"SET unsafe_enable_version_guessing = true", // Enable Iceberg version guessing
	}

	// Memory limit optimization
	_, err := e.db.Exec("SET memory_limit = ?", fmt.Sprintf("%dMB", e.config.MaxMemoryMB))
	if err != nil {
		e.logger.Printf("Warning: Failed to set memory limit: %v", err)
	}

	// Apply core optimizations
	for _, opt := range coreOptimizations {
		if _, err := e.db.Exec(opt); err != nil {
			e.logger.Printf("Warning: Failed to apply optimization '%s': %v", opt, err)
		}
	}

	// Install and load required extensions for Iceberg support
	if err := e.initializeExtensions(); err != nil {
		return fmt.Errorf("failed to initialize extensions: %w", err)
	}

	// Attach the Iceberg catalog to DuckDB
	if err := e.attachIcebergCatalog(); err != nil {
		return fmt.Errorf("failed to attach Iceberg catalog: %w", err)
	}

	e.logger.Printf("DuckDB engine initialized successfully with catalog: %s", e.catalog.Name())
	e.initialized = true
	return nil
}

// initializeExtensions installs and loads required DuckDB extensions
func (e *Engine) initializeExtensions() error {
	// httpfs is required for most functionality
	requiredExtensions := []string{"httpfs"}

	// iceberg is optional - not available on all platforms (e.g., windows_amd64_mingw)
	optionalExtensions := []string{"iceberg"}

	// Install and load required extensions
	for _, ext := range requiredExtensions {
		// Try to install extension
		if _, err := e.db.Exec(fmt.Sprintf("INSTALL %s", ext)); err != nil {
			e.logger.Printf("Info: Extension %s already installed or installation failed: %v", ext, err)
		}

		// Try to load extension - this must succeed for required extensions
		if _, err := e.db.Exec(fmt.Sprintf("LOAD %s", ext)); err != nil {
			return fmt.Errorf("failed to load required %s extension: %w", ext, err)
		}

		e.logger.Printf("Info: %s extension loaded successfully", ext)
	}

	// Install and load optional extensions
	for _, ext := range optionalExtensions {
		// Try to install extension
		if _, err := e.db.Exec(fmt.Sprintf("INSTALL %s", ext)); err != nil {
			e.logger.Printf("Info: Extension %s already installed or installation failed: %v", ext, err)
		}

		// Try to load extension - failure is acceptable for optional extensions
		if _, err := e.db.Exec(fmt.Sprintf("LOAD %s", ext)); err != nil {
			e.logger.Printf("Warning: Optional %s extension not available on this platform: %v", ext, err)
			e.logger.Printf("Info: Icebox will continue without native Iceberg support - some features may be limited")
			if ext == "iceberg" {
				e.icebergAvailable = false
			}
			continue
		}

		e.logger.Printf("Info: %s extension loaded successfully", ext)
		if ext == "iceberg" {
			e.icebergAvailable = true
		}
	}

	return nil
}

// attachIcebergCatalog attaches the Iceberg catalog to DuckDB using the appropriate method
func (e *Engine) attachIcebergCatalog() error {
	catalogName := e.config.IcebergCatalogName

	// Determine the catalog type and create appropriate ATTACH statement
	switch e.catalog.(type) {
	case interface{ GetJSONConfig() interface{} }: // JSON catalog
		// For JSON catalogs, we'll use file-based access
		// This is a simplified approach. Might want to expose the JSON catalog as a REST endpoint
		e.logger.Printf("Info: JSON catalog detected - using direct file access")
		return nil // JSON catalogs don't need ATTACH - we'll handle them differently

	case interface{ GetRESTConfig() interface{} }: // REST catalog
		// For REST catalogs, use ATTACH with REST endpoint
		attachSQL := fmt.Sprintf(`
			ATTACH '%s' AS %s (
				TYPE iceberg,
				ENDPOINT_TYPE rest
			)`, "rest_endpoint", catalogName) // You'd need to extract the actual REST endpoint

		if _, err := e.db.Exec(attachSQL); err != nil {
			return fmt.Errorf("failed to attach REST catalog: %w", err)
		}
		e.logger.Printf("Info: Attached REST catalog as '%s'", catalogName)

	case interface{ GetSQLiteConfig() interface{} }: // SQLite catalog
		// For SQLite catalogs, we'll use file-based access to the underlying data
		e.logger.Printf("Info: SQLite catalog detected - using direct file access")
		return nil // SQLite catalogs don't need ATTACH - we'll handle them differently

	default:
		e.logger.Printf("Info: Unknown catalog type - using direct file access")
		return nil
	}

	return nil
}

// Close closes the DuckDB connection and cleans up resources
func (e *Engine) Close() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

// GetMetrics returns current engine performance metrics
func (e *Engine) GetMetrics() *EngineMetrics {
	e.metrics.mu.RLock()
	defer e.metrics.mu.RUnlock()

	// Return a copy to avoid race conditions
	return &EngineMetrics{
		QueriesExecuted:  e.metrics.QueriesExecuted,
		TablesRegistered: e.metrics.TablesRegistered,
		CacheHits:        e.metrics.CacheHits,
		CacheMisses:      e.metrics.CacheMisses,
		TotalQueryTime:   e.metrics.TotalQueryTime,
		ErrorCount:       e.metrics.ErrorCount,
	}
}

// GetConfig returns the current engine configuration
func (e *Engine) GetConfig() *EngineConfig {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// Return a copy to avoid race conditions
	return &EngineConfig{
		MaxMemoryMB:        e.config.MaxMemoryMB,
		QueryTimeoutSec:    e.config.QueryTimeoutSec,
		EnableQueryLog:     e.config.EnableQueryLog,
		EnableOptimization: e.config.EnableOptimization,
		CacheSize:          e.config.CacheSize,
		IcebergCatalogName: e.config.IcebergCatalogName,
	}
}

// ExecuteQuery executes a SQL query and returns the results
func (e *Engine) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	if !e.initialized {
		return nil, fmt.Errorf("engine not initialized")
	}

	// Update metrics and generate unique query ID for tracking
	e.metrics.mu.Lock()
	currentQueryCount := e.metrics.QueriesExecuted
	e.metrics.QueriesExecuted++
	e.metrics.mu.Unlock()

	queryID := fmt.Sprintf("query_%d_%d", time.Now().UnixNano(), currentQueryCount)

	// Add query timeout if configured
	if e.config.QueryTimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(e.config.QueryTimeoutSec)*time.Second)
		defer cancel()
	}

	// Log query if enabled
	if e.config.EnableQueryLog {
		e.logger.Printf("Executing query [%s]: %s", queryID, query)
	}

	start := time.Now()

	// Preprocess the query to handle Iceberg table references
	processedQuery, err := e.preprocessQuery(ctx, query)
	if err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("failed to preprocess query [%s]: %w", queryID, err)
	}

	// Execute the query with timeout context
	rows, err := e.db.QueryContext(ctx, processedQuery)
	if err != nil {
		e.incrementErrorCount()
		// Provide better error messages for common issues
		if strings.Contains(err.Error(), "timeout") {
			return nil, fmt.Errorf("query [%s] timed out after %ds: %w", queryID, e.config.QueryTimeoutSec, err)
		}
		if strings.Contains(err.Error(), "table") && strings.Contains(err.Error(), "not found") {
			return nil, fmt.Errorf("table not found in query [%s]. Use 'SHOW TABLES' to see available tables: %w", queryID, err)
		}
		return nil, fmt.Errorf("failed to execute query [%s]: %w", queryID, err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("failed to get columns for query [%s]: %w", queryID, err)
	}

	// Fetch all rows with memory management
	var resultRows [][]interface{}
	rowCount := int64(0)

	for rows.Next() {
		// Memory management: limit result size for very large queries
		if rowCount > 100000 { // 100k row limit
			e.logger.Printf("Warning: Query [%s] result truncated at 100,000 rows", queryID)
			break
		}

		// Create slice to hold row values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			e.incrementErrorCount()
			return nil, fmt.Errorf("failed to scan row %d in query [%s]: %w", rowCount, queryID, err)
		}

		resultRows = append(resultRows, values)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("error iterating rows in query [%s]: %w", queryID, err)
	}

	duration := time.Since(start)

	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.TotalQueryTime += duration
	e.metrics.mu.Unlock()

	// Log completion if enabled
	if e.config.EnableQueryLog {
		e.logger.Printf("Query [%s] completed in %v, returned %d rows", queryID, duration, rowCount)
	}

	return &QueryResult{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: rowCount,
		Duration: duration,
		QueryID:  queryID,
	}, nil
}

// RegisterTable registers an Iceberg table for querying using DuckDB's native Iceberg support
func (e *Engine) RegisterTable(ctx context.Context, identifier table.Identifier, icebergTable *table.Table) error {
	if !e.initialized {
		return fmt.Errorf("engine not initialized")
	}

	if icebergTable == nil {
		return fmt.Errorf("iceberg table cannot be nil")
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	// Convert table identifier to SQL-safe name
	tableName := e.identifierToTableName(identifier)

	start := time.Now()

	// Check if Iceberg extension is available
	if !e.icebergAvailable {
		e.logger.Printf("Warning: Iceberg extension not available - creating placeholder table for %s", tableName)

		// Create a placeholder table that explains the limitation
		createPlaceholderSQL := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS 
			SELECT 'Iceberg extension not available on this platform' as error_message,
			       'Table %s cannot be queried without native Iceberg support' as details
		`, e.quoteName(tableName), tableName)

		if _, err := e.db.Exec(createPlaceholderSQL); err != nil {
			e.incrementErrorCount()
			return fmt.Errorf("failed to create placeholder for table %s: %w", tableName, err)
		}

		// Create an alias with just the table name for easier querying
		simpleTableName := identifier[len(identifier)-1]
		if simpleTableName != tableName && simpleTableName != "" {
			aliasSQL := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s",
				e.quoteName(simpleTableName), e.quoteName(tableName))

			if _, err := e.db.Exec(aliasSQL); err != nil {
				e.logger.Printf("Warning: Could not create alias %s for placeholder table %s: %v", simpleTableName, tableName, err)
			}
		}

		// Update metrics
		e.metrics.mu.Lock()
		e.metrics.TablesRegistered++
		e.metrics.mu.Unlock()

		duration := time.Since(start)
		e.logger.Printf("Created placeholder for table %s in %v (Iceberg extension unavailable)", tableName, duration)
		return nil
	}

	// Get table location from metadata
	location := icebergTable.Location()
	if location == "" {
		return fmt.Errorf("table %s has no location", tableName)
	}

	// DuckDB v1.3.0 has significantly improved Iceberg support
	// We can now use iceberg_scan directly with both SQLite and JSON catalogs
	// Use the metadata location directly from the table object
	metadataLocation := icebergTable.MetadataLocation()

	createViewSQL := fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS 
		SELECT * FROM iceberg_scan('%s')
	`, e.quoteName(tableName), metadataLocation)

	if _, err := e.db.Exec(createViewSQL); err != nil {
		e.incrementErrorCount()
		return fmt.Errorf("failed to register table %s: %w", tableName, err)
	}

	// Create an alias with just the table name for easier querying
	simpleTableName := identifier[len(identifier)-1]
	if simpleTableName != tableName && simpleTableName != "" {
		aliasSQL := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s",
			e.quoteName(simpleTableName), e.quoteName(tableName))

		if _, err := e.db.Exec(aliasSQL); err != nil {
			e.logger.Printf("Warning: Could not create alias %s for table %s: %v", simpleTableName, tableName, err)
		} else {
			e.logger.Printf("Info: Created alias '%s' -> '%s'", simpleTableName, tableName)
		}
	}

	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.TablesRegistered++
	e.metrics.mu.Unlock()

	duration := time.Since(start)
	e.logger.Printf("Registered table %s in %v using DuckDB v1.3.0 native Iceberg support", tableName, duration)

	return nil
}

// ListTables returns a list of all registered tables
func (e *Engine) ListTables(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	// Ensure we always return a non-nil slice
	if tables == nil {
		tables = []string{}
	}

	return tables, nil
}

// DescribeTable returns schema information for a table
func (e *Engine) DescribeTable(ctx context.Context, tableName string) (*QueryResult, error) {
	query := fmt.Sprintf("DESCRIBE %s", e.quoteName(tableName))
	return e.ExecuteQuery(ctx, query)
}

// ClearTableCache clears any cached table information (no-op for this implementation)
func (e *Engine) ClearTableCache() {
	// Tables are registered as views in DuckDB directly
	e.logger.Printf("Info: ClearTableCache called - no cache to clear in this implementation")
}

// preprocessQuery preprocesses SQL queries to handle Iceberg table references
func (e *Engine) preprocessQuery(ctx context.Context, query string) (string, error) {
	// For now, return the query as-is
	// In the future, this could:
	// 1. Parse table references and auto-register Iceberg tables
	// 2. Rewrite queries to use the attached catalog
	// 3. Handle time travel queries
	return query, nil
}

// incrementErrorCount safely increments the error counter
func (e *Engine) incrementErrorCount() {
	e.metrics.mu.Lock()
	e.metrics.ErrorCount++
	e.metrics.mu.Unlock()
}

// identifierToTableName converts a table identifier to a SQL-safe table name
func (e *Engine) identifierToTableName(identifier table.Identifier) string {
	return strings.Join(identifier, "_")
}

// quoteName quotes a SQL identifier to make it safe for use in queries
func (e *Engine) quoteName(name string) string {
	// Escape any existing quotes by doubling them
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}
