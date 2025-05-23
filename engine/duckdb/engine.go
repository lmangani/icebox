package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	_ "github.com/marcboeker/go-duckdb/v2"
)

// Engine provides SQL query capabilities using DuckDB with Arrow integration
type Engine struct {
	db          *sql.DB
	catalog     *sqlite.Catalog
	allocator   memory.Allocator
	tableCache  sync.Map // Cache for registered tables
	initialized bool
	config      *EngineConfig
	logger      *log.Logger
	metrics     *EngineMetrics
}

// EngineConfig holds configuration options for the engine
type EngineConfig struct {
	MaxMemoryMB        int
	QueryTimeoutSec    int
	EnableQueryLog     bool
	EnableOptimization bool
	CacheSize          int
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
	}
}

// NewEngine creates a new DuckDB engine instance with enterprise-grade features
func NewEngine(catalog *sqlite.Catalog) (*Engine, error) {
	return NewEngineWithConfig(catalog, DefaultEngineConfig())
}

// NewEngineWithConfig creates a new DuckDB engine with custom configuration
func NewEngineWithConfig(catalog *sqlite.Catalog, config *EngineConfig) (*Engine, error) {
	if catalog == nil {
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
		catalog:   catalog,
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

// initialize sets up the DuckDB engine with performance optimizations
func (e *Engine) initialize() error {
	// Configure DuckDB for optimal performance
	optimizations := []string{
		"SET memory_limit = ?",
		"INSTALL arrow",
		"LOAD arrow",
		"SET enable_progress_bar = false",
		"SET enable_object_cache = true",
	}

	for _, opt := range optimizations {
		var err error
		if strings.Contains(opt, "?") {
			_, err = e.db.Exec(opt, fmt.Sprintf("%dMB", e.config.MaxMemoryMB))
		} else {
			_, err = e.db.Exec(opt)
		}

		if err != nil {
			// Log warnings but don't fail for non-critical optimizations
			e.logger.Printf("Warning: Failed to apply optimization '%s': %v", opt, err)
		}
	}

	e.initialized = true
	return nil
}

// Close closes the DuckDB connection and cleans up resources
func (e *Engine) Close() error {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()

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

// ExecuteQuery executes a SQL query and returns the results with enterprise-grade features
func (e *Engine) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	if !e.initialized {
		return nil, fmt.Errorf("engine not initialized")
	}

	// Generate unique query ID for tracking
	queryID := fmt.Sprintf("query_%d_%d", time.Now().UnixNano(), e.metrics.QueriesExecuted)

	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.QueriesExecuted++
	e.metrics.mu.Unlock()

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

// incrementErrorCount safely increments the error counter
func (e *Engine) incrementErrorCount() {
	e.metrics.mu.Lock()
	e.metrics.ErrorCount++
	e.metrics.mu.Unlock()
}

// RegisterTable registers an Iceberg table as a DuckDB view for querying with caching
func (e *Engine) RegisterTable(ctx context.Context, identifier table.Identifier, icebergTable *table.Table) error {
	if !e.initialized {
		return fmt.Errorf("engine not initialized")
	}

	if icebergTable == nil {
		return fmt.Errorf("iceberg table cannot be nil")
	}

	// Convert table identifier to SQL-safe name
	tableName := e.identifierToTableName(identifier)
	cacheKey := strings.Join(identifier, ".")

	// Check cache first
	if cached, exists := e.tableCache.Load(cacheKey); exists {
		e.metrics.mu.Lock()
		e.metrics.CacheHits++
		e.metrics.mu.Unlock()

		e.logger.Printf("Table %s found in cache", tableName)
		_ = cached // Use cached entry if needed
		return nil
	}

	e.metrics.mu.Lock()
	e.metrics.CacheMisses++
	e.metrics.mu.Unlock()

	start := time.Now()

	// Read the Iceberg table into Arrow format
	arrowTable, err := e.readIcebergTable(ctx, icebergTable)
	if err != nil {
		e.incrementErrorCount()
		return fmt.Errorf("failed to read Iceberg table %s: %w", tableName, err)
	}
	defer arrowTable.Release()

	// Register the Arrow table with DuckDB
	if err := e.registerArrowTable(tableName, arrowTable); err != nil {
		e.incrementErrorCount()
		return fmt.Errorf("failed to register Arrow table %s: %w", tableName, err)
	}

	// Cache the registration
	tableInfo := map[string]interface{}{
		"identifier":   identifier,
		"tableName":    tableName,
		"registeredAt": time.Now(),
		"schema":       arrowTable.Schema(),
		"rowCount":     arrowTable.NumRows(),
	}
	e.tableCache.Store(cacheKey, tableInfo)

	// Update metrics
	e.metrics.mu.Lock()
	e.metrics.TablesRegistered++
	e.metrics.mu.Unlock()

	duration := time.Since(start)
	e.logger.Printf("Registered table %s in %v (%d rows)", tableName, duration, arrowTable.NumRows())

	return nil
}

// UnregisterTable removes a table from the cache and DuckDB
func (e *Engine) UnregisterTable(identifier table.Identifier) error {
	cacheKey := strings.Join(identifier, ".")
	tableName := e.identifierToTableName(identifier)

	// Remove from cache
	e.tableCache.Delete(cacheKey)

	// Drop from DuckDB
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", e.quoteName(tableName))
	if _, err := e.db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	e.logger.Printf("Unregistered table %s", tableName)
	return nil
}

// ClearTableCache clears all cached table registrations
func (e *Engine) ClearTableCache() {
	e.tableCache.Range(func(key, value interface{}) bool {
		e.tableCache.Delete(key)
		return true
	})
	e.logger.Printf("Cleared table cache")
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

	return tables, nil
}

// DescribeTable returns schema information for a table
func (e *Engine) DescribeTable(ctx context.Context, tableName string) (*QueryResult, error) {
	query := fmt.Sprintf("DESCRIBE %s", e.quoteName(tableName))
	return e.ExecuteQuery(ctx, query)
}

// preprocessQuery preprocesses SQL queries to handle Iceberg table references
func (e *Engine) preprocessQuery(ctx context.Context, query string) (string, error) {
	// For now, return the query as-is
	// In the future, this could:
	// 1. Parse table references and auto-register Iceberg tables
	// 2. Rewrite queries to use registered table names
	// 3. Handle time travel queries
	return query, nil
}

// readIcebergTable reads an Iceberg table and converts it to Arrow format
func (e *Engine) readIcebergTable(ctx context.Context, icebergTable *table.Table) (arrow.Table, error) {
	// Get the table schema
	schema := icebergTable.Schema()
	if schema == nil {
		return nil, fmt.Errorf("table has no schema")
	}

	// For now, create an empty Arrow table with the correct schema
	// In a full implementation, this would scan the actual data files
	arrowSchema, err := e.convertIcebergSchemaToArrow(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	// Create empty columns for each field
	columns := make([]arrow.Column, len(arrowSchema.Fields()))
	for i, field := range arrowSchema.Fields() {
		// Create empty array for this field type
		emptyArray := e.createEmptyArray(field.Type)
		chunked := arrow.NewChunked(field.Type, []arrow.Array{emptyArray})
		columns[i] = *arrow.NewColumn(field, chunked)
	}

	return array.NewTable(arrowSchema, columns, 0), nil
}

// convertIcebergSchemaToArrow converts an Iceberg schema to Arrow schema
func (e *Engine) convertIcebergSchemaToArrow(icebergSchema *iceberg.Schema) (*arrow.Schema, error) {
	// This is a simplified conversion - a full implementation would handle all Iceberg types
	fields := make([]arrow.Field, 0, len(icebergSchema.Fields()))

	for _, field := range icebergSchema.Fields() {
		arrowType, err := e.convertIcebergTypeToArrow(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}

		arrowField := arrow.Field{
			Name:     field.Name,
			Type:     arrowType,
			Nullable: !field.Required,
		}
		fields = append(fields, arrowField)
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertIcebergTypeToArrow converts an Iceberg data type to Arrow data type
func (e *Engine) convertIcebergTypeToArrow(icebergType iceberg.Type) (arrow.DataType, error) {
	switch icebergType.Type() {
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	case "int":
		return arrow.PrimitiveTypes.Int32, nil
	case "long":
		return arrow.PrimitiveTypes.Int64, nil
	case "float":
		return arrow.PrimitiveTypes.Float32, nil
	case "double":
		return arrow.PrimitiveTypes.Float64, nil
	case "string":
		return arrow.BinaryTypes.String, nil
	case "date":
		return arrow.PrimitiveTypes.Date32, nil
	case "timestamp":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "timestamptz":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "uuid":
		return arrow.BinaryTypes.String, nil // UUID as string
	default:
		// Handle complex types like decimal, fixed
		switch t := icebergType.(type) {
		case iceberg.DecimalType:
			return &arrow.Decimal128Type{Precision: int32(t.Precision()), Scale: int32(t.Scale())}, nil
		case iceberg.FixedType:
			return &arrow.FixedSizeBinaryType{ByteWidth: t.Len()}, nil
		default:
			return nil, fmt.Errorf("unsupported Iceberg type: %v", icebergType)
		}
	}
}

// createEmptyArray creates an empty Arrow array of the specified type
func (e *Engine) createEmptyArray(dataType arrow.DataType) arrow.Array {
	builder := array.NewBuilder(e.allocator, dataType)
	defer builder.Release()
	return builder.NewArray()
}

// registerArrowTable registers an Arrow table with DuckDB
func (e *Engine) registerArrowTable(tableName string, arrowTable arrow.Table) error {
	// This would use DuckDB's Arrow integration to register the table
	// For now, we'll create a placeholder table
	// In a full implementation, this would use DuckDB's arrow_scan function

	// Get schema information
	schema := arrowTable.Schema()
	if schema == nil {
		return fmt.Errorf("arrow table has no schema")
	}

	// Build CREATE TABLE statement
	var columns []string
	for _, field := range schema.Fields() {
		sqlType := e.arrowTypeToSQL(field.Type)
		nullability := ""
		if !field.Nullable {
			nullability = " NOT NULL"
		}
		columns = append(columns, fmt.Sprintf("%s %s%s", e.quoteName(field.Name), sqlType, nullability))
	}

	createSQL := fmt.Sprintf("CREATE OR REPLACE TABLE %s (%s)",
		e.quoteName(tableName),
		strings.Join(columns, ", "))

	_, err := e.db.Exec(createSQL)
	return err
}

// arrowTypeToSQL converts Arrow data types to SQL types
func (e *Engine) arrowTypeToSQL(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "REAL"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "VARCHAR"
	case arrow.DATE32:
		return "DATE"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	default:
		return "VARCHAR" // Default fallback
	}
}

// identifierToTableName converts a table identifier to a SQL-safe table name
func (e *Engine) identifierToTableName(identifier table.Identifier) string {
	return strings.Join(identifier, "_")
}

// quoteName quotes SQL identifiers to handle special characters
func (e *Engine) quoteName(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}
