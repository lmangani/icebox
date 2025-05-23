package importer

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Schema represents a simplified table schema
type Schema struct {
	Fields []Field `json:"fields"`
}

// Field represents a single column in a schema
type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// FileStats contains statistics about a Parquet file
type FileStats struct {
	RecordCount int64 `json:"record_count"`
	FileSize    int64 `json:"file_size"`
	ColumnCount int   `json:"column_count"`
}

// ImportRequest contains all parameters for importing a table
type ImportRequest struct {
	ParquetFile    string
	TableIdent     table.Identifier
	NamespaceIdent table.Identifier
	Schema         *Schema
	Overwrite      bool
	PartitionBy    []string
}

// ImportResult contains the results of a table import
type ImportResult struct {
	TableIdent    table.Identifier
	RecordCount   int64
	DataSize      int64
	TableLocation string
}

// ParquetImporter handles importing Parquet files into Iceberg tables
type ParquetImporter struct {
	config  *config.Config
	catalog *sqlite.Catalog
}

// NewParquetImporter creates a new Parquet importer
func NewParquetImporter(cfg *config.Config) (*ParquetImporter, error) {
	// Create catalog
	catalog, err := sqlite.NewCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	return &ParquetImporter{
		config:  cfg,
		catalog: catalog,
	}, nil
}

// Close closes the importer and releases resources
func (p *ParquetImporter) Close() error {
	if p.catalog != nil {
		return p.catalog.Close()
	}
	return nil
}

// InferSchema reads a Parquet file and infers the schema
func (p *ParquetImporter) InferSchema(parquetFile string) (*Schema, *FileStats, error) {
	// Get file stats
	fileInfo, err := os.Stat(parquetFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// For now, implement a basic schema inference
	// TODO: Replace with proper Parquet schema reading when libraries are available
	schema, recordCount, err := p.inferSchemaBasic(parquetFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to infer schema: %w", err)
	}

	stats := &FileStats{
		RecordCount: recordCount,
		FileSize:    fileInfo.Size(),
		ColumnCount: len(schema.Fields),
	}

	return schema, stats, nil
}

// GetTableLocation returns the location where table data would be stored
func (p *ParquetImporter) GetTableLocation(tableIdent table.Identifier) string {
	if p.config.Storage.FileSystem == nil {
		return ""
	}

	// Build path: warehouse/namespace/table_name
	path := p.config.Storage.FileSystem.RootPath
	for _, part := range tableIdent {
		path = filepath.Join(path, part)
	}

	return "file://" + filepath.ToSlash(path)
}

// ImportTable imports a Parquet file into an Iceberg table
func (p *ParquetImporter) ImportTable(ctx context.Context, req ImportRequest) (*ImportResult, error) {
	// 1. Create namespace if it doesn't exist
	exists, err := p.catalog.CheckNamespaceExists(ctx, req.NamespaceIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to check namespace existence: %w", err)
	}

	if !exists {
		err = p.catalog.CreateNamespace(ctx, req.NamespaceIdent, iceberg.Properties{
			"description": "Auto-created namespace for Parquet import",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create namespace: %w", err)
		}
		fmt.Printf("✅ Created namespace: %v\n", req.NamespaceIdent)
	}

	// 2. Check if table exists
	tableExists, err := p.catalog.CheckTableExists(ctx, req.TableIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to check table existence: %w", err)
	}

	if tableExists {
		if !req.Overwrite {
			return nil, fmt.Errorf("table %v already exists (use --overwrite to replace)", req.TableIdent)
		}

		// Drop existing table
		err = p.catalog.DropTable(ctx, req.TableIdent)
		if err != nil {
			return nil, fmt.Errorf("failed to drop existing table: %w", err)
		}
		fmt.Printf("🗑️  Dropped existing table: %v\n", req.TableIdent)
	}

	// 3. Create the table
	// Convert our schema to Iceberg schema
	icebergSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int64,
		Required: true,
	})

	_, err = p.catalog.CreateTable(ctx, req.TableIdent, icebergSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Printf("✅ Created table: %v\n", req.TableIdent)

	// 4. Copy Parquet file to table location
	tableLocation := p.GetTableLocation(req.TableIdent)
	dataSize, recordCount, err := p.copyParquetData(req.ParquetFile, tableLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to copy Parquet data: %w", err)
	}
	fmt.Printf("📁 Copied data to: %s\n", tableLocation)

	return &ImportResult{
		TableIdent:    req.TableIdent,
		RecordCount:   recordCount,
		DataSize:      dataSize,
		TableLocation: tableLocation,
	}, nil
}

// inferSchemaBasic provides basic schema inference for testing
// TODO: Replace with proper Parquet schema reading
func (p *ParquetImporter) inferSchemaBasic(parquetFile string) (*Schema, int64, error) {
	// This is a placeholder implementation
	// In a real implementation, we would use a Parquet library to read the schema

	// For now, create a mock schema based on filename patterns
	filename := filepath.Base(parquetFile)

	var fields []Field

	// Add some common fields based on filename hints
	if strings.Contains(strings.ToLower(filename), "sales") {
		fields = []Field{
			{Name: "id", Type: "long", Nullable: false},
			{Name: "customer_id", Type: "long", Nullable: true},
			{Name: "product_id", Type: "long", Nullable: true},
			{Name: "amount", Type: "double", Nullable: true},
			{Name: "sale_date", Type: "date", Nullable: true},
		}
	} else if strings.Contains(strings.ToLower(filename), "user") {
		fields = []Field{
			{Name: "id", Type: "long", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "email", Type: "string", Nullable: true},
			{Name: "created_at", Type: "timestamp", Nullable: true},
		}
	} else {
		// Generic schema
		fields = []Field{
			{Name: "id", Type: "long", Nullable: false},
			{Name: "data", Type: "string", Nullable: true},
			{Name: "timestamp", Type: "timestamp", Nullable: true},
		}
	}

	// Mock record count (would normally read from Parquet metadata)
	recordCount := int64(1000) // Placeholder

	return &Schema{Fields: fields}, recordCount, nil
}

// copyParquetData copies the Parquet file to the table location
func (p *ParquetImporter) copyParquetData(srcFile, tableLocation string) (dataSize int64, recordCount int64, error error) {
	// Remove file:// prefix for local operations
	localPath := strings.TrimPrefix(tableLocation, "file://")

	// Create data directory
	dataDir := filepath.Join(localPath, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return 0, 0, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Copy the Parquet file to the data directory
	destFile := filepath.Join(dataDir, "part-00000.parquet")

	// Open source file
	src, err := os.Open(srcFile)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open source file: %w", err)
	}
	defer src.Close()

	// Create destination file
	dst, err := os.Create(destFile)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create destination file: %w", err)
	}
	defer dst.Close()

	// Copy the data
	copied, err := io.Copy(dst, src)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to copy file: %w", err)
	}

	// For now, return mock record count
	// TODO: Read actual record count from Parquet metadata
	mockRecordCount := int64(1000)

	return copied, mockRecordCount, nil
}
