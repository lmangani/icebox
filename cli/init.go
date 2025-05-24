package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/TFMV/icebox/catalog/sqlite"
	"github.com/TFMV/icebox/config"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init [directory]",
	Short: "Initialize a new Icebox project",
	Long: `Initialize a new Icebox project with a catalog and configuration.

This command creates a new directory (default: icebox-lakehouse) and sets up:
- .icebox.yml configuration file
- SQLite catalog database
- Local filesystem storage directory

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

	initCmd.Flags().StringVar(&initOpts.catalog, "catalog", "sqlite", "catalog type (sqlite|rest)")
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
	default:
		return fmt.Errorf("unsupported catalog type: %s", cfg.Catalog.Type)
	}
}
