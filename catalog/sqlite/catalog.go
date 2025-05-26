package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/fs/local"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	_ "github.com/mattn/go-sqlite3"
)

// FileSystemInterface abstracts file operations for different storage backends
type FileSystemInterface interface {
	Create(path string) (io.WriteCloser, error)
}

// Catalog implements the iceberg-go catalog.Catalog interface using SQLite
type Catalog struct {
	name       string
	dbPath     string
	db         *sql.DB
	fileSystem FileSystemInterface
	fileIO     icebergio.IO
	warehouse  string
}

// NewCatalog creates a new SQLite-based catalog
func NewCatalog(cfg *config.Config) (*Catalog, error) {
	if cfg.Catalog.SQLite == nil {
		return nil, fmt.Errorf("SQLite catalog configuration is required")
	}

	dbPath := cfg.Catalog.SQLite.Path

	// Ensure the directory exists
	if err := local.EnsureDir(filepath.Dir(dbPath)); err != nil {
		return nil, fmt.Errorf("failed to create catalog directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Determine warehouse location and create FileIO
	warehouse := ""
	var fileSystem FileSystemInterface
	var fileIO icebergio.IO

	if cfg.Storage.FileSystem != nil {
		warehouse = cfg.Storage.FileSystem.RootPath
		fileSystem = local.NewFileSystem(warehouse)
		// Create a local FileIO implementation
		fileIO = icebergio.LocalFS{}
	}

	return NewCatalogWithIO(cfg.Name, dbPath, db, fileSystem, fileIO, warehouse)
}

// NewCatalogWithIO creates a new SQLite-based catalog with custom file IO
func NewCatalogWithIO(name, dbPath string, db *sql.DB, fileSystem FileSystemInterface, fileIO icebergio.IO, warehouse string) (*Catalog, error) {
	cat := &Catalog{
		name:       name,
		dbPath:     dbPath,
		db:         db,
		fileSystem: fileSystem,
		fileIO:     fileIO,
		warehouse:  warehouse,
	}

	if err := cat.initializeDatabase(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return cat, nil
}

// CatalogType returns the catalog type
func (c *Catalog) CatalogType() catalog.Type {
	return catalog.SQL
}

// Name returns the catalog name
func (c *Catalog) Name() string {
	return c.name
}

// Close closes the database connection
func (c *Catalog) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// initializeDatabase creates the necessary tables if they don't exist
func (c *Catalog) initializeDatabase() error {
	// Create iceberg_tables table
	createTablesSQL := `
	CREATE TABLE IF NOT EXISTS iceberg_tables (
		catalog_name TEXT NOT NULL,
		table_namespace TEXT NOT NULL,
		table_name TEXT NOT NULL,
		metadata_location TEXT,
		previous_metadata_location TEXT,
		PRIMARY KEY (catalog_name, table_namespace, table_name)
	)`

	if _, err := c.db.Exec(createTablesSQL); err != nil {
		return fmt.Errorf("failed to create iceberg_tables table: %w", err)
	}

	// Create iceberg_namespace_properties table
	createNamespacePropsSQL := `
	CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
		catalog_name TEXT NOT NULL,
		namespace TEXT NOT NULL,
		property_key TEXT NOT NULL,
		property_value TEXT,
		PRIMARY KEY (catalog_name, namespace, property_key)
	)`

	if _, err := c.db.Exec(createNamespacePropsSQL); err != nil {
		return fmt.Errorf("failed to create iceberg_namespace_properties table: %w", err)
	}

	return nil
}

// CreateTable creates a new table in the catalog
func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	if len(identifier) == 0 {
		return nil, fmt.Errorf("table identifier cannot be empty")
	}

	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)

	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to check namespace existence: %w", err)
	}
	if !exists {
		return nil, catalog.ErrNoSuchNamespace
	}

	// Check if table already exists
	tableExists, err := c.CheckTableExists(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("failed to check table existence: %w", err)
	}
	if tableExists {
		return nil, catalog.ErrTableAlreadyExists
	}

	// Parse options using a simple configuration structure
	location := c.defaultTableLocation(identifier)
	properties := make(iceberg.Properties)

	for _, opt := range opts {
		// Apply options - this is a simplified approach
		_ = opt // For now, acknowledge the option
	}

	// Create basic table metadata (simplified for demonstration)
	// TODO: use the actual iceberg-go APIs
	metadataLocation := c.newMetadataLocation(identifier, 1)

	// Write metadata to storage (simplified)
	if err := c.writeMetadata(schema, location, metadataLocation); err != nil {
		return nil, fmt.Errorf("failed to write table metadata: %w", err)
	}

	// Insert into database
	insertSQL := `
	INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)
	VALUES (?, ?, ?, ?, ?)`

	namespaceStr := namespaceToString(namespace)
	_, err = c.db.ExecContext(ctx, insertSQL, c.name, namespaceStr, tableName, metadataLocation, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to insert table record: %w", err)
	}

	// Load and return the table
	return c.LoadTable(ctx, identifier, properties)
}

// CommitTable commits table changes to the catalog
func (c *Catalog) CommitTable(ctx context.Context, tbl *table.Table, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	// Get current metadata location
	identifier := tbl.Identifier()
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	var currentMetadataLocation sql.NullString
	query := `SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr, tableName).Scan(&currentMetadataLocation)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, "", catalog.ErrNoSuchTable
		}
		return nil, "", fmt.Errorf("failed to query current metadata: %w", err)
	}

	// Validate requirements against current metadata
	currentMetadata := tbl.Metadata()
	for _, req := range reqs {
		if err := req.Validate(currentMetadata); err != nil {
			return nil, "", fmt.Errorf("requirement validation failed: %w", err)
		}
	}

	// Apply updates to create new metadata
	metadataBuilder, err := table.MetadataBuilderFromBase(currentMetadata)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create metadata builder: %w", err)
	}

	// Apply each update to the metadata builder
	for _, update := range updates {
		if err := update.Apply(metadataBuilder); err != nil {
			return nil, "", fmt.Errorf("failed to apply update %s: %w", update.Action(), err)
		}
	}

	// Build the new metadata
	newMetadata, err := metadataBuilder.Build()
	if err != nil {
		return nil, "", fmt.Errorf("failed to build new metadata: %w", err)
	}

	// Determine the new metadata version and location
	newVersion := c.getNextMetadataVersion(currentMetadataLocation.String)
	newMetadataLocation := c.newMetadataLocation(identifier, newVersion)

	// Write the new metadata file
	if err := c.writeMetadataFile(newMetadata, newMetadataLocation); err != nil {
		return nil, "", fmt.Errorf("failed to write metadata file: %w", err)
	}

	// Update database with the new metadata location
	updateSQL := `UPDATE iceberg_tables SET metadata_location = ?, previous_metadata_location = ? WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	_, err = c.db.ExecContext(ctx, updateSQL, newMetadataLocation, currentMetadataLocation.String, c.name, namespaceStr, tableName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to update table metadata location: %w", err)
	}

	return newMetadata, newMetadataLocation, nil
}

// LoadTable loads a table from the catalog
func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	var metadataLocation sql.NullString
	query := `SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`

	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr, tableName).Scan(&metadataLocation)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, catalog.ErrNoSuchTable
		}
		return nil, fmt.Errorf("failed to query table: %w", err)
	}

	if !metadataLocation.Valid {
		return nil, fmt.Errorf("table metadata location is null")
	}

	// Load table using iceberg-go APIs
	tbl, err := table.NewFromLocation(identifier, metadataLocation.String, c.fileIO, c)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	return tbl, nil
}

// DropTable drops a table from the catalog
func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	// Check if table exists
	exists, err := c.CheckTableExists(ctx, identifier)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}
	if !exists {
		return catalog.ErrNoSuchTable
	}

	// Delete from database
	deleteSQL := `DELETE FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	result, err := c.db.ExecContext(ctx, deleteSQL, c.name, namespaceStr, tableName)
	if err != nil {
		return fmt.Errorf("failed to delete table record: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return catalog.ErrNoSuchTable
	}

	return nil
}

// RenameTable renames a table in the catalog
func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	// Check if source table exists
	sourceTable, err := c.LoadTable(ctx, from, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load source table: %w", err)
	}

	// Check if destination namespace exists
	destNamespace := catalog.NamespaceFromIdent(to)
	nsExists, err := c.CheckNamespaceExists(ctx, destNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to check destination namespace: %w", err)
	}
	if !nsExists {
		return nil, catalog.ErrNoSuchNamespace
	}

	// Check if destination table already exists
	destExists, err := c.CheckTableExists(ctx, to)
	if err != nil {
		return nil, fmt.Errorf("failed to check destination table: %w", err)
	}
	if destExists {
		return nil, catalog.ErrTableAlreadyExists
	}

	// Update the database record
	fromNamespace := catalog.NamespaceFromIdent(from)
	fromTableName := catalog.TableNameFromIdent(from)
	fromNamespaceStr := namespaceToString(fromNamespace)

	toNamespace := catalog.NamespaceFromIdent(to)
	toTableName := catalog.TableNameFromIdent(to)
	toNamespaceStr := namespaceToString(toNamespace)

	updateSQL := `UPDATE iceberg_tables SET table_namespace = ?, table_name = ? WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	_, err = c.db.ExecContext(ctx, updateSQL, toNamespaceStr, toTableName, c.name, fromNamespaceStr, fromTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to rename table in database: %w", err)
	}

	// Return the renamed table
	newTable := table.New(to, sourceTable.Metadata(), sourceTable.MetadataLocation(), c.fileIO, c)
	return newTable, nil
}

// CheckTableExists checks if a table exists in the catalog
func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	var count int
	query := `SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return count > 0, nil
}

// ListTables lists all tables in a namespace
func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		namespaceStr := namespaceToString(namespace)
		query := `SELECT table_namespace, table_name FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ?`

		rows, err := c.db.QueryContext(ctx, query, c.name, namespaceStr)
		if err != nil {
			yield(nil, fmt.Errorf("failed to list tables: %w", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var tableNamespace, tableName string
			if err := rows.Scan(&tableNamespace, &tableName); err != nil {
				yield(nil, fmt.Errorf("failed to scan table row: %w", err))
				return
			}

			ns := stringToNamespace(tableNamespace)
			identifier := append(ns, tableName)
			if !yield(identifier, nil) {
				return
			}
		}

		if err := rows.Err(); err != nil {
			yield(nil, fmt.Errorf("error iterating table rows: %w", err))
		}
	}
}

// CreateNamespace creates a new namespace
func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	// Check if namespace already exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check namespace existence: %w", err)
	}
	if exists {
		return catalog.ErrNamespaceAlreadyExists
	}

	namespaceStr := namespaceToString(namespace)

	// Insert default property to mark namespace as existing
	insertSQL := `INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, 'exists', 'true')`
	_, err = c.db.ExecContext(ctx, insertSQL, c.name, namespaceStr)
	if err != nil {
		return fmt.Errorf("failed to create namespace: %w", err)
	}

	// Insert additional properties
	for key, value := range props {
		insertPropSQL := `INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, ?, ?)`
		_, err = c.db.ExecContext(ctx, insertPropSQL, c.name, namespaceStr, key, value)
		if err != nil {
			return fmt.Errorf("failed to insert namespace property: %w", err)
		}
	}

	return nil
}

// DropNamespace drops a namespace from the catalog
func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check namespace existence: %w", err)
	}
	if !exists {
		return catalog.ErrNoSuchNamespace
	}

	namespaceStr := namespaceToString(namespace)

	// Check if namespace has tables
	var tableCount int
	countQuery := `SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ?`
	err = c.db.QueryRowContext(ctx, countQuery, c.name, namespaceStr).Scan(&tableCount)
	if err != nil {
		return fmt.Errorf("failed to count tables in namespace: %w", err)
	}
	if tableCount > 0 {
		return catalog.ErrNamespaceNotEmpty
	}

	// Delete namespace properties
	deleteSQL := `DELETE FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ?`
	_, err = c.db.ExecContext(ctx, deleteSQL, c.name, namespaceStr)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %w", err)
	}

	return nil
}

// CheckNamespaceExists checks if a namespace exists
func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	namespaceStr := namespaceToString(namespace)

	var count int
	query := `SELECT COUNT(*) FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = 'exists'`
	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check namespace existence: %w", err)
	}

	return count > 0, nil
}

// LoadNamespaceProperties loads properties for a namespace
func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to check namespace existence: %w", err)
	}
	if !exists {
		return nil, catalog.ErrNoSuchNamespace
	}

	namespaceStr := namespaceToString(namespace)
	props := make(iceberg.Properties)

	query := `SELECT property_key, property_value FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ?`
	rows, err := c.db.QueryContext(ctx, query, c.name, namespaceStr)
	if err != nil {
		return nil, fmt.Errorf("failed to load namespace properties: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var value sql.NullString
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("failed to scan property row: %w", err)
		}

		if value.Valid {
			props[key] = value.String
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating property rows: %w", err)
	}

	return props, nil
}

// UpdateNamespaceProperties updates properties for a namespace
func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to check namespace existence: %w", err)
	}
	if !exists {
		return catalog.PropertiesUpdateSummary{}, catalog.ErrNoSuchNamespace
	}

	namespaceStr := namespaceToString(namespace)
	var removed, updated, missing []string

	// Begin transaction for atomic updates
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			// Log error but don't override original error
			log.Printf("Failed to rollback transaction: %v", err)
		}
	}()

	// Handle removals
	for _, key := range removals {
		if key == "exists" {
			// Don't allow removing the 'exists' property
			missing = append(missing, key)
			continue
		}

		deleteSQL := `DELETE FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`
		result, err := tx.ExecContext(ctx, deleteSQL, c.name, namespaceStr, key)
		if err != nil {
			return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to remove property %s: %w", key, err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			removed = append(removed, key)
		} else {
			missing = append(missing, key)
		}
	}

	// Handle updates/additions
	for key, value := range updates {
		// Check if property exists
		var count int
		checkSQL := `SELECT COUNT(*) FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`
		err := tx.QueryRowContext(ctx, checkSQL, c.name, namespaceStr, key).Scan(&count)
		if err != nil {
			return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to check property existence: %w", err)
		}

		if count > 0 {
			// Update existing property
			updateSQL := `UPDATE iceberg_namespace_properties SET property_value = ? WHERE catalog_name = ? AND namespace = ? AND property_key = ?`
			_, err = tx.ExecContext(ctx, updateSQL, value, c.name, namespaceStr, key)
		} else {
			// Insert new property
			insertSQL := `INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, ?, ?)`
			_, err = tx.ExecContext(ctx, insertSQL, c.name, namespaceStr, key, value)
		}

		if err != nil {
			return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to update property %s: %w", key, err)
		}

		updated = append(updated, key)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return catalog.PropertiesUpdateSummary{
		Removed: removed,
		Updated: updated,
		Missing: missing,
	}, nil
}

// ListNamespaces lists all namespaces, optionally filtered by parent
func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	query := `SELECT DISTINCT namespace FROM iceberg_namespace_properties WHERE catalog_name = ?`
	rows, err := c.db.QueryContext(ctx, query, c.name)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	defer rows.Close()

	var namespaces []table.Identifier
	for rows.Next() {
		var namespaceStr string
		if err := rows.Scan(&namespaceStr); err != nil {
			return nil, fmt.Errorf("failed to scan namespace row: %w", err)
		}

		namespace := stringToNamespace(namespaceStr)

		// Filter by parent if specified
		if len(parent) > 0 {
			if len(namespace) <= len(parent) {
				continue
			}
			match := true
			for i, part := range parent {
				if i >= len(namespace) || namespace[i] != part {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		namespaces = append(namespaces, namespace)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating namespace rows: %w", err)
	}

	return namespaces, nil
}

// Helper functions

func namespaceToString(namespace table.Identifier) string {
	if len(namespace) == 0 {
		return ""
	}

	result := ""
	for i, part := range namespace {
		if i > 0 {
			result += "."
		}
		result += part
	}
	return result
}

func stringToNamespace(namespaceStr string) table.Identifier {
	if namespaceStr == "" {
		return table.Identifier{}
	}

	return catalog.ToIdentifier(namespaceStr)
}

func (c *Catalog) defaultTableLocation(identifier table.Identifier) string {
	if c.warehouse == "" {
		return ""
	}

	// Build path: warehouse/namespace/table_name
	path := c.warehouse
	for _, part := range identifier {
		path = filepath.Join(path, part)
	}

	return "file://" + filepath.ToSlash(path)
}

func (c *Catalog) newMetadataLocation(identifier table.Identifier, version int) string {
	tableLocation := c.defaultTableLocation(identifier)
	if tableLocation == "" {
		return ""
	}

	// Handle file:// prefix by removing it
	tableLocation = strings.TrimPrefix(tableLocation, "file://")

	// Remove file:// prefix for path operations
	path := tableLocation
	metadataPath := filepath.Join(path, "metadata", fmt.Sprintf("v%d.metadata.json", version))

	// Only add file:// prefix for local filesystem, not for custom file IO
	if _, isLocalFS := c.fileIO.(icebergio.LocalFS); isLocalFS {
		return "file://" + filepath.ToSlash(metadataPath)
	}

	// For custom file IO (like memory filesystem), return path without file:// prefix
	return filepath.ToSlash(metadataPath)
}

// getNextMetadataVersion determines the next version number for metadata files
func (c *Catalog) getNextMetadataVersion(currentMetadataLocation string) int {
	if currentMetadataLocation == "" {
		return 1
	}

	// Extract version from current metadata location
	// Expected format: .../metadata/v{version}.metadata.json
	filename := filepath.Base(currentMetadataLocation)
	if strings.HasPrefix(filename, "v") && strings.HasSuffix(filename, ".metadata.json") {
		versionStr := filename[1:strings.Index(filename, ".")]
		if version, err := strconv.Atoi(versionStr); err == nil {
			return version + 1
		}
	}

	// Default to version 2 if we can't parse the current version
	return 2
}

// writeMetadataFile writes the metadata to the specified location
func (c *Catalog) writeMetadataFile(metadata table.Metadata, metadataLocation string) error {
	// Serialize metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	// Handle file:// prefix by removing it for file operations
	filePath := strings.TrimPrefix(metadataLocation, "file://")

	// Use the catalog's filesystem if available
	if c.fileSystem != nil {
		file, err := c.fileSystem.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", filePath, err)
		}
		defer file.Close()

		_, err = file.Write(metadataJSON)
		if err != nil {
			return fmt.Errorf("failed to write metadata to file %s: %w", filePath, err)
		}
		return nil
	}

	// Fallback to local file operations
	return writeFile(filePath, metadataJSON)
}

// Helper methods for metadata operations

// writeMetadata writes metadata to the specified location
func (c *Catalog) writeMetadata(schema *iceberg.Schema, location, metadataLocation string) error {
	// Handle file:// prefix by removing it
	metadataLocation = strings.TrimPrefix(metadataLocation, "file://")

	// Create proper Iceberg table metadata using iceberg-go APIs
	metadata, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, iceberg.Properties{
		"format-version": "2",
	})
	if err != nil {
		return fmt.Errorf("failed to create metadata: %w", err)
	}

	// Serialize metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	// Use the catalog's filesystem if available
	if c.fileSystem != nil {
		file, err := c.fileSystem.Create(metadataLocation)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", metadataLocation, err)
		}
		defer file.Close()

		_, err = file.Write(metadataJSON)
		if err != nil {
			return fmt.Errorf("failed to write metadata to file %s: %w", metadataLocation, err)
		}
		return nil
	}

	// Fallback to local file operations
	return writeFile(metadataLocation, metadataJSON)
}

// writeFile writes data to a file (helper function)
func writeFile(path string, data []byte) error {
	if err := local.EnsureDir(filepath.Dir(path)); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", path, err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %w", path, err)
	}

	return nil
}

// FileIO returns the catalog's file IO implementation
func (c *Catalog) FileIO() icebergio.IO {
	return c.fileIO
}
