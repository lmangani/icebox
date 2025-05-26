package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/TFMV/icebox/catalog/json"
	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/display"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init [directory]",
	Short: "Initialize a new Icebox project",
	Long: `Initialize a new Icebox project with a catalog and configuration.

This command creates a new directory (default: icebox-lakehouse) and sets up:
- .icebox.yml configuration file
- Catalog (SQLite database or JSON file)
- Local filesystem storage directory
- .icebox/display.yaml display configuration

If no directory is specified, it creates "icebox-lakehouse" in the current location.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runInit,
}

type initOptions struct {
	catalog string
	storage string
}

var initOpts = &initOptions{}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.Flags().StringVar(&initOpts.catalog, "catalog", "sqlite", "catalog type (sqlite|rest|json)")
	initCmd.Flags().StringVar(&initOpts.storage, "storage", "fs", "storage type (fs|s3|mem)")
}

func runInit(cmd *cobra.Command, args []string) error {
	// Determine target directory
	var targetDir string
	if len(args) > 0 {
		targetDir = args[0]
	} else {
		targetDir = "icebox-lakehouse"
	}

	// Get absolute path
	absPath, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Check if directory exists, create if it doesn't
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		if err := os.MkdirAll(absPath, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", absPath, err)
		}
		fmt.Printf("Created directory: %s\n", absPath)
	}

	// Check if already initialized
	configPath := filepath.Join(absPath, ".icebox.yml")
	if _, err := os.Stat(configPath); err == nil {
		return fmt.Errorf("directory already contains an Icebox project (found .icebox.yml)")
	}

	// Create configuration
	cfg := &config.Config{
		Name: filepath.Base(absPath),
		Catalog: config.CatalogConfig{
			Type: initOpts.catalog,
		},
		Storage: config.StorageConfig{
			Type: initOpts.storage,
		},
	}

	// Initialize based on catalog type
	switch initOpts.catalog {
	case "sqlite":
		if err := initSQLiteCatalog(absPath, cfg); err != nil {
			return fmt.Errorf("failed to initialize SQLite catalog: %w", err)
		}
	case "json":
		if err := initJSONCatalog(absPath, cfg); err != nil {
			return fmt.Errorf("failed to initialize JSON catalog: %w", err)
		}
	case "rest":
		return fmt.Errorf("REST catalog initialization not yet implemented")
	default:
		return fmt.Errorf("unsupported catalog type: %s", initOpts.catalog)
	}

	// Initialize storage
	if err := initStorage(absPath, cfg); err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Write configuration file
	if err := config.WriteConfig(configPath, cfg); err != nil {
		return fmt.Errorf("failed to write configuration: %w", err)
	}

	// Actually initialize the catalog
	if err := initializeCatalog(cfg); err != nil {
		return fmt.Errorf("failed to initialize catalog: %w", err)
	}

	// Initialize display configuration
	if err := initDisplayConfig(absPath); err != nil {
		// Non-fatal error - just warn the user
		fmt.Printf("⚠️  Warning: Could not create display configuration: %v\n", err)
	}

	fmt.Printf("✅ Initialized Icebox project in %s\n", absPath)
	fmt.Printf("   Catalog: %s\n", cfg.Catalog.Type)
	fmt.Printf("   Storage: %s\n", cfg.Storage.Type)
	fmt.Printf("\nNext steps:\n")
	fmt.Printf("   icebox import your-data.parquet --table your_table\n")
	fmt.Printf("   icebox sql 'SELECT * FROM your_table LIMIT 10'\n")

	return nil
}

func initSQLiteCatalog(projectDir string, cfg *config.Config) error {
	// Create catalog directory
	catalogDir := filepath.Join(projectDir, ".icebox", "catalog")
	if err := os.MkdirAll(catalogDir, 0755); err != nil {
		return fmt.Errorf("failed to create catalog directory: %w", err)
	}

	// Set catalog database path
	dbPath := filepath.Join(catalogDir, "catalog.db")
	cfg.Catalog.SQLite = &config.SQLiteConfig{
		Path: dbPath,
	}

	return nil
}

func initJSONCatalog(projectDir string, cfg *config.Config) error {
	// Create catalog directory
	catalogDir := filepath.Join(projectDir, ".icebox", "catalog")
	if err := os.MkdirAll(catalogDir, 0755); err != nil {
		return fmt.Errorf("failed to create catalog directory: %w", err)
	}

	// Create warehouse directory
	warehouseDir := filepath.Join(projectDir, ".icebox", "data")
	if err := os.MkdirAll(warehouseDir, 0755); err != nil {
		return fmt.Errorf("failed to create warehouse directory: %w", err)
	}

	// Set catalog JSON file path and warehouse
	catalogPath := filepath.Join(catalogDir, "catalog.json")
	cfg.Catalog.JSON = &config.JSONConfig{
		URI:       catalogPath,
		Warehouse: warehouseDir,
	}

	return nil
}

func initStorage(projectDir string, cfg *config.Config) error {
	switch cfg.Storage.Type {
	case "fs":
		// Create data directory
		dataDir := filepath.Join(projectDir, ".icebox", "data")
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
		cfg.Storage.FileSystem = &config.FileSystemConfig{
			RootPath: dataDir,
		}
	case "mem":
		// Memory storage needs no initialization
		cfg.Storage.Memory = &config.MemoryConfig{}
	case "s3":
		return fmt.Errorf("S3 storage initialization not yet implemented")
	default:
		return fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}

	return nil
}

// initializeCatalog creates and initializes the actual catalog
func initializeCatalog(cfg *config.Config) error {
	switch cfg.Catalog.Type {
	case "sqlite":
		// Create SQLite catalog to initialize the database
		catalog, err := sqlite.NewCatalog(cfg)
		if err != nil {
			return fmt.Errorf("failed to create SQLite catalog: %w", err)
		}
		defer catalog.Close()

		// The catalog is automatically initialized when created
		return nil
	case "json":
		// Create JSON catalog to initialize the catalog file
		catalog, err := json.NewCatalog(cfg)
		if err != nil {
			return fmt.Errorf("failed to create JSON catalog: %w", err)
		}
		defer catalog.Close()

		// The catalog is automatically initialized when created
		return nil
	default:
		return fmt.Errorf("unsupported catalog type: %s", cfg.Catalog.Type)
	}
}

// initDisplayConfig creates the default display configuration
func initDisplayConfig(projectDir string) error {
	// Create .icebox directory if it doesn't exist
	iceboxDir := filepath.Join(projectDir, ".icebox")
	if err := os.MkdirAll(iceboxDir, 0755); err != nil {
		return fmt.Errorf("failed to create .icebox directory: %w", err)
	}

	// Create default display configuration
	displayConfig := display.DefaultConfig()

	// Customize defaults for new projects
	displayConfig.Table.Pagination = 100      // Show more rows by default
	displayConfig.Table.UnicodeBorders = true // Enable Unicode borders
	displayConfig.Colors.Enabled = "auto"     // Auto-detect color support
	displayConfig.Timing = true               // Show query timing by default

	// Save display configuration
	configPath := filepath.Join(iceboxDir, "display.yaml")
	if err := display.SaveConfigToFile(displayConfig, configPath); err != nil {
		return fmt.Errorf("failed to save display configuration: %w", err)
	}

	return nil
}
