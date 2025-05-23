package cli

import (
	"fmt"
	"strings"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
)

var catalogMgmtCmd = &cobra.Command{
	Use:   "catalog",
	Short: "Manage catalog namespaces",
	Long: `Manage namespaces within your Iceberg catalog.

This command provides subcommands for catalog operations:
- list: List all namespaces in the catalog
- create: Create a new namespace
- drop: Drop an existing namespace (if empty)

Examples:
  icebox catalog list                        # List all namespaces
  icebox catalog create analytics            # Create 'analytics' namespace  
  icebox catalog create warehouse.inventory  # Create nested namespace
  icebox catalog drop test_namespace         # Drop empty namespace`,
}

var catalogListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all namespaces in the catalog",
	Long: `List all namespaces available in the catalog.

Shows both top-level and nested namespaces with their properties.

Examples:
  icebox catalog list                    # List all namespaces
  icebox catalog list --format json     # JSON output
  icebox catalog list --show-properties # Include namespace properties`,
	RunE: runCatalogList,
}

var catalogCreateCmd = &cobra.Command{
	Use:   "create <namespace>",
	Short: "Create a new namespace",
	Long: `Create a new namespace in the catalog.

Namespaces can be nested using dot notation. Parent namespaces
will be created automatically if they don't exist.

Examples:
  icebox catalog create analytics        # Create top-level namespace
  icebox catalog create warehouse.raw    # Create nested namespace
  icebox catalog create finance.reports.monthly  # Create deeply nested namespace`,
	Args: cobra.ExactArgs(1),
	RunE: runCatalogCreate,
}

var catalogDropCmd = &cobra.Command{
	Use:   "drop <namespace>",
	Short: "Drop an existing namespace",
	Long: `Drop an existing namespace from the catalog.

The namespace must be empty (contain no tables) to be dropped.
Use --force to drop non-empty namespaces (this will also drop all tables).

Examples:
  icebox catalog drop test_namespace     # Drop empty namespace
  icebox catalog drop old_data --force  # Force drop with all tables`,
	Args: cobra.ExactArgs(1),
	RunE: runCatalogDrop,
}

type catalogListOptions struct {
	format         string
	showProperties bool
	parent         string
}

type catalogCreateOptions struct {
	properties map[string]string
	location   string
}

type catalogDropOptions struct {
	force bool
}

var (
	catalogListOpts   = &catalogListOptions{}
	catalogCreateOpts = &catalogCreateOptions{}
	catalogDropOpts   = &catalogDropOptions{}
)

func init() {
	rootCmd.AddCommand(catalogMgmtCmd)

	// Add subcommands
	catalogMgmtCmd.AddCommand(catalogListCmd)
	catalogMgmtCmd.AddCommand(catalogCreateCmd)
	catalogMgmtCmd.AddCommand(catalogDropCmd)

	// Catalog list flags
	catalogListCmd.Flags().StringVar(&catalogListOpts.format, "format", "table", "output format: table, csv, json")
	catalogListCmd.Flags().BoolVar(&catalogListOpts.showProperties, "show-properties", false, "show namespace properties")
	catalogListCmd.Flags().StringVar(&catalogListOpts.parent, "parent", "", "list namespaces under specific parent")

	// Catalog create flags
	catalogCreateCmd.Flags().StringToStringVar(&catalogCreateOpts.properties, "property", nil, "namespace properties (key=value)")
	catalogCreateCmd.Flags().StringVar(&catalogCreateOpts.location, "location", "", "namespace location (optional)")

	// Catalog drop flags
	catalogDropCmd.Flags().BoolVar(&catalogDropOpts.force, "force", false, "force drop non-empty namespace")
}

func runCatalogList(cmd *cobra.Command, args []string) error {
	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", err)
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	ctx := cmd.Context()

	// Determine parent namespace
	var parent table.Identifier
	if catalogListOpts.parent != "" {
		parent = strings.Split(catalogListOpts.parent, ".")
	}

	// List namespaces
	namespaces, err := cat.ListNamespaces(ctx, parent)
	if err != nil {
		return fmt.Errorf("❌ Failed to list namespaces: %w", err)
	}

	// Display results
	if err := displayNamespaceList(namespaces, parent); err != nil {
		return fmt.Errorf("❌ Failed to display namespace list: %w", err)
	}

	return nil
}

func runCatalogCreate(cmd *cobra.Command, args []string) error {
	namespaceName := args[0]

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration: %w", err)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Parse namespace identifier
	namespace := strings.Split(namespaceName, ".")

	// Check if namespace already exists
	exists, err := cat.CheckNamespaceExists(cmd.Context(), namespace)
	if err != nil {
		return fmt.Errorf("❌ Failed to check namespace existence: %w", err)
	}
	if exists {
		return fmt.Errorf("❌ Namespace '%s' already exists", namespaceName)
	}

	// Prepare properties
	properties := iceberg.Properties{}
	for key, value := range catalogCreateOpts.properties {
		properties[key] = value
	}

	if catalogCreateOpts.location != "" {
		properties["location"] = catalogCreateOpts.location
	}

	// Create the namespace
	if err := cat.CreateNamespace(cmd.Context(), namespace, properties); err != nil {
		return fmt.Errorf("❌ Failed to create namespace: %w", err)
	}

	// Display success message
	fmt.Printf("✅ Successfully created namespace!\n\n")
	fmt.Printf("📁 Namespace Details:\n")
	fmt.Printf("   Name: %s\n", namespaceName)
	if len(properties) > 0 {
		fmt.Printf("   Properties:\n")
		for key, value := range properties {
			fmt.Printf("     %s: %s\n", key, value)
		}
	}

	return nil
}

func runCatalogDrop(cmd *cobra.Command, args []string) error {
	namespaceName := args[0]

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration: %w", err)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Parse namespace identifier
	namespace := strings.Split(namespaceName, ".")

	// Check if namespace exists
	exists, err := cat.CheckNamespaceExists(cmd.Context(), namespace)
	if err != nil {
		return fmt.Errorf("❌ Failed to check namespace existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("❌ Namespace '%s' does not exist", namespaceName)
	}

	// Check if namespace is empty (unless force is used)
	if !catalogDropOpts.force {
		var tables []table.Identifier
		for identifier, err := range cat.ListTables(cmd.Context(), namespace) {
			if err != nil {
				return fmt.Errorf("❌ Failed to check if namespace is empty: %w", err)
			}
			tables = append(tables, identifier)
		}

		if len(tables) > 0 {
			return fmt.Errorf("❌ Namespace '%s' is not empty (contains %d tables)\n"+
				"💡 Use --force to drop non-empty namespace or remove tables first", namespaceName, len(tables))
		}
	}

	// Drop the namespace
	if err := cat.DropNamespace(cmd.Context(), namespace); err != nil {
		return fmt.Errorf("❌ Failed to drop namespace: %w", err)
	}

	// Display success message
	fmt.Printf("✅ Successfully dropped namespace '%s'\n", namespaceName)

	return nil
}

// Helper functions for catalog management

func displayNamespaceList(namespaces []table.Identifier, parent table.Identifier) error {
	if len(namespaces) == 0 {
		if parent != nil {
			fmt.Printf("📭 No namespaces found under '%s'\n", strings.Join(parent, "."))
		} else {
			fmt.Println("📭 No namespaces found")
		}
		return nil
	}

	switch catalogListOpts.format {
	case "table":
		return displayNamespaceListTable(namespaces, parent)
	case "csv":
		return displayNamespaceListCSV(namespaces)
	case "json":
		return displayNamespaceListJSON(namespaces)
	default:
		return fmt.Errorf("unsupported format: %s", catalogListOpts.format)
	}
}

func displayNamespaceListTable(namespaces []table.Identifier, parent table.Identifier) error {
	if parent != nil {
		fmt.Printf("📁 Namespaces under '%s' (%d namespaces):\n", strings.Join(parent, "."), len(namespaces))
	} else {
		fmt.Printf("📁 All Namespaces (%d namespaces):\n", len(namespaces))
	}

	fmt.Println("┌────┬─────────────────────────────────────────┬─────────────┐")
	fmt.Println("│ #  │                Namespace                │    Level    │")
	fmt.Println("├────┼─────────────────────────────────────────┼─────────────┤")

	for i, namespace := range namespaces {
		namespaceName := strings.Join(namespace, ".")
		level := fmt.Sprintf("%d", len(namespace))

		fmt.Printf("│%-3d │ %-39s │ %-11s │\n",
			i+1,
			truncateString(namespaceName, 39),
			level)
	}

	fmt.Println("└────┴─────────────────────────────────────────┴─────────────┘")
	return nil
}

func displayNamespaceListCSV(namespaces []table.Identifier) error {
	fmt.Println("namespace,level")
	for _, namespace := range namespaces {
		namespaceName := strings.Join(namespace, ".")
		level := len(namespace)
		fmt.Printf("%s,%d\n", namespaceName, level)
	}
	return nil
}

func displayNamespaceListJSON(namespaces []table.Identifier) error {
	fmt.Println("[")
	for i, namespace := range namespaces {
		namespaceName := strings.Join(namespace, ".")
		level := len(namespace)

		fmt.Printf(`  {"namespace": "%s", "level": %d}`, namespaceName, level)
		if i < len(namespaces)-1 {
			fmt.Print(",")
		}
		fmt.Println()
	}
	fmt.Println("]")
	return nil
}
