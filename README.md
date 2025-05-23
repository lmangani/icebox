# 🧊 Icebox

<div align="center">

**A single-binary playground for Apache Iceberg**  
*Five minutes to first query*

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)](https://golang.org)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v0.3.0--rc0-326ce5?style=flat&logo=apache&logoColor=white)](https://iceberg.apache.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

[Quick Start](#-quick-start) • [Features](#-features) • [Examples](#-examples) • [Architecture](#-architecture) • [Contributing](#-contributing)

</div>

---

## 🎯 What is Icebox?

Icebox is a **zero-configuration data lakehouse** that gets you from zero to querying Iceberg tables in under five minutes. Perfect for:

- 🔬 **Experimenting** with Apache Iceberg table format
- 📚 **Learning** lakehouse concepts and workflows  
- 🧪 **Prototyping** data pipelines locally
- 🚀 **Testing** Iceberg integrations before production

**No servers, no complex setup, no dependencies** - just a single binary and your data.

## ✨ Features

### 🚀 **Zero-Setup Experience**

- **Single binary** - No installation complexity
- **Embedded catalog** - SQLite-based, no external database needed
- **REST catalog support** - Connect to existing Iceberg REST catalogs
- **Local storage** - File system integration out of the box
- **Auto-configuration** - Sensible defaults, minimal configuration required

### 📁 **Data Operations**

- **Parquet import** with automatic schema inference
- **Iceberg table** creation and management
- **Namespace** organization and operations
- **Arrow integration** for efficient data processing
- **Transaction support** with proper ACID guarantees

### 🔍 **SQL Querying**

- **DuckDB integration** for high-performance analytics
- **Interactive SQL shell** with command history and multi-line support
- **Multiple output formats** - table, CSV, JSON
- **Auto-registration** of catalog tables for immediate querying
- **Query performance metrics** and optimization features
- **Rich CLI experience** with timing, caching, and helpful error messages

### 🛠️ **Developer-Friendly**

- **Rich CLI** with intuitive commands and helpful output
- **Dry-run modes** to preview operations
- **Comprehensive error messages** with actionable guidance
- **YAML configuration** for reproducible setups

## 🚀 Quick Start

### 1. Install Icebox

```bash
# Build from source (Go 1.21+ required)
git clone https://github.com/TFMV/icebox.git
cd icebox/icebox
go build -o icebox cmd/icebox/main.go
```

### 2. Initialize Your Lakehouse

```bash
# Create a new lakehouse project
./icebox init my-lakehouse
cd my-lakehouse

# Your project structure is ready
tree .icebox/
# .icebox/
# ├── catalog/
# │   └── catalog.db     # SQLite catalog
# └── data/              # Table storage
```

### 3. Import Your First Table

```bash
# Import a Parquet file (creates namespace and table automatically)
./icebox import sales_data.parquet --table sales

✅ Successfully imported table!

📊 Import Results:
   Table: [default sales]
   Records: 1,000,000
   Size: 45.2 MB
   Location: file:///.icebox/data/default/sales
```

### 4. Start Querying

```bash
# Query your data with SQL
./icebox sql "SELECT COUNT(*) FROM sales"
📋 Registered 1 tables for querying
⏱️  Query [query_1234] executed in 145ms
📊 1 rows returned
┌─────────┐
│ count   │
├─────────┤
│1000000  │
└─────────┘

# Use the interactive shell for complex analysis
./icebox shell

 ██▓ ▄████▄  ▓█████  ▄▄▄▄    ▒█████  ▒██   ██▒
▓██▒▒██▀ ▀█  ▓█   ▀ ▓█████▄ ▒██▒  ██▒▒▒ █ █ ▒░
▒██▒▒▓█    ▄ ▒███   ▒██▒ ▄██▒██░  ██▒░░  █   ░
░██░▒▓▓▄ ▄██▒▒▓█  ▄ ▒██░█▀  ▒██   ██░ ░ █ █ ▒
░██░▒ ▓███▀ ░░▒████▒░▓█  ▀█▓░ ████▓▒░▒██▒ ▒██▒
░▓  ░ ░▒ ▒  ░░░ ▒░ ░░▒▓███▀▒░ ▒░▒░▒░ ▒▒ ░ ░▓ ░
 ▒ ░  ░  ▒    ░ ░  ░▒░▒   ░   ░ ▒ ▒░ ░░   ░▒ ░
 ▒ ░░           ░    ░    ░ ░ ░ ░ ▒   ░    ░
 ░  ░ ░         ░  ░ ░          ░ ░   ░    ░
    ░                     ░
🧊 Icebox SQL Shell v0.1.0
Interactive SQL querying for Apache Iceberg
Type \help for help, \quit to exit

icebox> SELECT region, SUM(amount) FROM sales GROUP BY region LIMIT 3;
⏱️  Query executed in 89ms
📊 3 rows returned
┌────────┬──────────┐
│ region │   sum    │
├────────┼──────────┤
│ North  │ 2456789  │
│ South  │ 1987432  │
│ East   │ 2123456  │
└────────┴──────────┘

icebox> \quit
```

**🎉 That's it! You now have a working Iceberg lakehouse with SQL querying.**

## 📋 Examples

### Schema Inference and Preview

```bash
# Preview schema without importing
./icebox import customer_data.parquet --table customers --infer-schema

📋 Schema inferred from customer_data.parquet:

  Columns (7):
    1. customer_id: long
    2. name: string (nullable)
    3. email: string (nullable)
    4. signup_date: date (nullable)
    5. lifetime_value: double (nullable)
    6. region: string (nullable)
    7. active: boolean (nullable)

📊 File Statistics:
  Records: 50,000
  File size: 12.3 MB
  Columns: 7
```

### SQL Querying and Analysis

```bash
# Quick one-off queries
./icebox sql "SELECT COUNT(*) FROM customers WHERE region = 'North'"
./icebox sql "SHOW TABLES"
./icebox sql "DESCRIBE customers"

# Multiple output formats
./icebox sql "SELECT region, COUNT(*) FROM customers GROUP BY region" --format csv
./icebox sql "SELECT * FROM customers LIMIT 5" --format json

# Performance monitoring
./icebox sql "SELECT AVG(lifetime_value) FROM customers" --metrics

📈 Engine Metrics:
  Queries Executed: 1
  Tables Registered: 3
  Cache Hits: 2
  Cache Misses: 1
  Total Query Time: 45ms
  Average Query Time: 45ms
```

### Interactive Shell Experience

```bash
# Start the interactive shell
./icebox shell

icebox> -- Multi-line queries supported
icebox> SELECT region, 
     ->        AVG(lifetime_value) as avg_ltv,
     ->        COUNT(*) as customers
     -> FROM customers 
     -> GROUP BY region 
     -> ORDER BY avg_ltv DESC;

⏱️  Query executed in 67ms
📊 4 rows returned
┌────────┬──────────┬───────────┐
│ region │ avg_ltv  │ customers │
├────────┼──────────┼───────────┤
│ West   │ 1543.67  │ 12,450    │
│ East   │ 1432.11  │ 11,890    │
│ North  │ 1389.45  │ 13,230    │
│ South  │ 1298.33  │ 12,430    │
└────────┴──────────┴───────────┘

# Shell commands for productivity
icebox> \tables                    -- List all tables
icebox> \schema customers           -- Show table schema
icebox> \history                    -- View command history
icebox> \metrics                    -- Show performance metrics
icebox> \help                       -- Get help
icebox> \quit                       -- Exit shell
```

### Namespace Organization

```bash
# Import to specific namespaces
./icebox import user_events.parquet --table analytics.events
./icebox import product_catalog.parquet --table inventory.products
./icebox import financial_data.parquet --table finance.transactions

# Query across namespaces
./icebox sql "SELECT COUNT(*) FROM analytics.events WHERE event_type = 'purchase'"
./icebox sql "SELECT p.name, SUM(t.amount) FROM inventory.products p JOIN finance.transactions t ON p.id = t.product_id GROUP BY p.name"

# Organize your lakehouse logically
tree .icebox/data/
# .icebox/data/
# ├── analytics/
# │   └── events/
# ├── inventory/
# │   └── products/
# └── finance/
#     └── transactions/
```

### Advanced Import Options

```bash
# Dry run - see what would happen without executing
./icebox import large_dataset.parquet --table warehouse.inventory --dry-run

🔍 Dry run - would perform the following operations:

1. Create namespace: [warehouse]
2. Create table: [warehouse inventory]
3. Import from: /data/large_dataset.parquet
4. Table location: file:///.icebox/data/warehouse/inventory

# Replace existing table
./icebox import updated_sales.parquet --table sales --overwrite

# Use qualified table names
./icebox import metrics.parquet --table analytics.user_metrics
```

### Configuration Management

```yaml
# .icebox.yml - Generated automatically, customize as needed
name: my-lakehouse
catalog:
  sqlite:
    path: .icebox/catalog/catalog.db
storage:
  filesystem:
    root_path: .icebox/data
```

## 🌐 REST Catalog Support

Icebox supports connecting to external **Apache Iceberg REST catalogs**, enabling integration with existing lakehouse infrastructure while maintaining the same simple CLI experience.

### Quick Start with REST Catalog

```bash
# Initialize with REST catalog
./icebox init my-remote-lakehouse --catalog rest --uri http://localhost:8181

# Or configure an existing project
cat > .icebox.yml << EOF
name: my-remote-lakehouse
catalog:
  type: rest
  rest:
    uri: http://localhost:8181
storage:
  type: fs
  filesystem:
    root_path: .icebox/data
EOF

# Use exactly the same commands - Icebox handles the REST catalog transparently
./icebox import sales_data.parquet --table sales
./icebox sql "SELECT COUNT(*) FROM sales"
```

### REST Catalog Configuration

Icebox supports comprehensive REST catalog configuration compatible with the Apache Iceberg REST specification:

```yaml
# .icebox.yml - Full REST catalog configuration
name: production-lakehouse
catalog:
  type: rest
  rest:
    uri: https://catalog.example.com
    
    # OAuth 2.0 Authentication
    oauth:
      token: "your-oauth-token"
      credential: "client_credentials"
      auth_url: "https://auth.example.com/oauth/token"
      scope: "catalog:read catalog:write"
    
    # AWS Signature Version 4 (for AWS services)
    sigv4:
      enabled: true
      region: "us-west-2"
      service: "execute-api"
    
    # TLS Configuration
    tls:
      skip_verify: false  # Set to true for self-signed certificates
    
    # Advanced Options
    warehouse_location: "s3://my-bucket/warehouse"
    metadata_location: "s3://my-bucket/metadata"
    prefix: "v1/catalogs/prod"
    
    # Custom Properties
    additional_properties:
      "custom.property": "value"
      "timeout.seconds": "30"
    
    # Basic Authentication
    credentials:
      username: "catalog-user"
      password: "secure-password"

storage:
  type: fs
  filesystem:
    root_path: /local/cache/data
```

### Authentication Methods

#### OAuth 2.0 Authentication

```yaml
catalog:
  type: rest
  rest:
    uri: https://catalog.example.com
    oauth:
      credential: "client_credentials"
      auth_url: "https://auth.example.com/oauth/token"
      scope: "catalog:read catalog:write"
```

For client credentials flow, you can also provide the token directly:

```yaml
oauth:
  token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

#### AWS Signature Version 4

For AWS-based REST catalogs (like AWS Glue or custom implementations):

```yaml
catalog:
  type: rest
  rest:
    uri: https://your-api-gateway.execute-api.us-west-2.amazonaws.com
    sigv4:
      enabled: true
      region: "us-west-2"
      service: "execute-api"
```

#### Basic Authentication

```yaml
catalog:
  type: rest
  rest:
    uri: https://catalog.example.com
    credentials:
      username: "your-username"
      password: "your-password"
```

#### Custom Headers and Properties

```yaml
catalog:
  type: rest
  rest:
    uri: https://catalog.example.com
    additional_properties:
      "X-Custom-Header": "custom-value"
      "timeout.connect": "30000"
      "timeout.read": "60000"
```

### REST Catalog Examples

#### Connect to Tabular Cloud

```yaml
name: tabular-lakehouse
catalog:
  type: rest
  rest:
    uri: https://api.tabular.io/ws/v1
    oauth:
      credential: "client_credentials"
      token: "your-tabular-token"
    warehouse_location: "s3://your-tabular-bucket/warehouse"
```

#### Connect to AWS Glue via REST API

```yaml
name: aws-glue-lakehouse
catalog:
  type: rest
  rest:
    uri: https://your-glue-api.execute-api.us-east-1.amazonaws.com
    sigv4:
      enabled: true
      region: "us-east-1"
      service: "execute-api"
    warehouse_location: "s3://your-glue-bucket/warehouse"
```

#### Self-Hosted REST Catalog

```yaml
name: self-hosted-lakehouse
catalog:
  type: rest
  rest:
    uri: http://catalog.internal.company.com:8080
    tls:
      skip_verify: true  # For internal certificates
    credentials:
      username: "icebox-user"
      password: "secure-password"
    prefix: "v1/catalogs/analytics"
```

### Testing with Docker

You can easily test REST catalog functionality using the reference implementation:

```bash
# Run the Iceberg REST catalog with Docker
docker run -p 8181:8181 \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password \
  -e AWS_REGION=us-east-1 \
  tabulario/iceberg-rest:latest

# Configure Icebox to use the Docker catalog
cat > .icebox.yml << EOF
name: docker-test
catalog:
  type: rest
  rest:
    uri: http://localhost:8181
storage:
  type: fs
  filesystem:
    root_path: .icebox/data
EOF

# Now use Icebox normally - all operations work transparently
./icebox import test_data.parquet --table test.sample
./icebox sql "SHOW TABLES"
./icebox sql "SELECT * FROM test.sample LIMIT 10"
```

### REST vs SQLite Catalog Comparison

| Feature | SQLite Catalog | REST Catalog |
|---------|---------------|--------------|
| **Setup** | Zero configuration | Requires REST server |
| **Performance** | Local, very fast | Network latency dependent |
| **Sharing** | Single machine | Multi-user, distributed |
| **Production Ready** | Development/testing | Production environments |
| **Authentication** | None needed | OAuth, SigV4, Basic Auth |
| **Scalability** | Single user | Multi-user, enterprise scale |
| **Storage Integration** | Local filesystem | Cloud storage (S3, GCS, etc.) |

### Migrating Between Catalog Types

You can easily migrate projects between SQLite and REST catalogs:

```bash
# Export from SQLite catalog
./icebox sql "SHOW TABLES" --format json > tables.json

# Switch to REST catalog configuration
# Update .icebox.yml to use REST catalog

# Import tables to REST catalog (manual process for table definitions)
# Table data remains accessible if using shared storage
```

## 🏗️ Architecture

Icebox is built on a **modular, extensible architecture** designed for simplicity and reliability:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Layer     │    │  Configuration  │    │   File System   │
│                 │    │                 │    │                 │
│ • init          │◄──►│ • .icebox.yml   │◄──►│ • Local storage │
│ • import        │    │ • Auto-discovery│    │ • File:// URIs  │
│ • sql           │    │ • YAML config   │    │ • Directory mgmt│
│ • shell         │    │ • Catalog types │    │ • Cloud storage │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Table Operations│    │  Catalog Layer  │    │   Data Import   │
│                 │    │                 │    │                 │
│ • Arrow tables  │◄──►│ • SQLite catalog│◄──►│ • Parquet files │
│ • Transactions  │    │ • REST catalog  │    │ • Schema infer  │
│ • ACID guarantee│    │ • Factory pattern│   │ • Auto-discovery│
│ • DuckDB engine │    │ • Unified API   │    │ • Data validation│
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────┐
                    │   Apache Iceberg    │
                    │                     │
                    │ • Table format      │
                    │ • Metadata handling │
                    │ • Transaction log   │
                    │ • REST API spec     │
                    └─────────────────────┘
```

### Core Components

| Component | Purpose | Implementation |
|-----------|---------|----------------|
| **CLI** | User interface and command orchestration | Cobra-based with rich output |
| **Catalog** | Table metadata and namespace management | SQLite & REST with unified interface |
| **Storage** | Data persistence and file operations | Local filesystem with file:// URIs |
| **Import** | Data ingestion and schema inference | Parquet file processing with Arrow |
| **TableOps** | Table manipulation and transactions | Apache Iceberg Go integration |
| **SQL Engine** | Query execution and processing | DuckDB with Arrow integration |

### Catalog Architecture

Icebox supports multiple catalog implementations through a unified interface:

```
┌─────────────────────────────────────────────────────────────┐
│                    Catalog Factory                          │
│   ┌─────────────────────┐    ┌─────────────────────┐        │
│   │   SQLite Catalog    │    │    REST Catalog     │        │
│   │                     │    │                     │        │
│   │ • Local database    │    │ • HTTP client       │        │
│   │ • Zero config       │    │ • OAuth/SigV4 auth │        │
│   │ • Embedded          │    │ • Production ready  │        │
│   │ • Fast operations   │    │ • Multi-user        │        │
│   └─────────────────────┘    └─────────────────────┘        │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Unified Interface  │
                    │                     │
                    │ • Table CRUD        │
                    │ • Namespace mgmt    │
                    │ • Transaction log   │
                    │ • Schema evolution  │
                    └─────────────────────┘
```

### Technology Stack

- **🐹 Go 1.21+** - Performance, reliability, single binary distribution
- **🧊 Apache Iceberg v0.3.0-rc0** - Table format and metadata management  
- **🗃️ SQLite** - Embedded catalog database with zero configuration
- **🏹 Apache Arrow** - Efficient columnar data processing
- **📄 Parquet** - Columnar storage format for analytics workloads

## 📚 Command Reference

### Project Management

```bash
# Initialize new lakehouse project
icebox init [directory]                       # Create new project (SQLite catalog)
icebox init . --catalog sqlite                # Initialize in current directory (explicit SQLite)
icebox init myproject --catalog rest --uri http://localhost:8181  # Create with REST catalog
```

### Data Import

```bash
# Import Parquet files
icebox import <file> --table <name>              # Basic import
icebox import <file> --table <ns>.<table>        # With namespace  
icebox import <file> --table <name> --overwrite  # Replace existing
icebox import <file> --table <name> --dry-run    # Preview only
icebox import <file> --table <name> --infer-schema  # Show schema
```

### SQL Querying

```bash
# Execute SQL queries
icebox sql "<query>"                              # Run single query
icebox sql "<query>" --format table              # Table output (default)
icebox sql "<query>" --format csv                # CSV output
icebox sql "<query>" --format json               # JSON output
icebox sql "<query>" --max-rows 500              # Limit output rows
icebox sql "<query>" --show-schema               # Show column information
icebox sql "<query>" --metrics                   # Show performance metrics
icebox sql "<query>" --no-auto-register          # Skip table auto-registration
```

### Interactive Shell

```bash
# Start interactive SQL shell
icebox shell                                     # Start shell
icebox shell --timing                            # Enable query timing (default)
icebox shell --metrics                           # Show metrics on startup
icebox shell --query-log                         # Enable query logging

# Shell commands (use within shell)
\help, \h                                        # Show help
\tables, \t                                      # List all tables
\schema <table>                                  # Show table schema
\history                                         # Show command history
\metrics, \m                                     # Show performance metrics
\cache [clear|status]                            # Manage table cache
\performance, \perf                              # Show detailed performance statistics
\status                                          # Show engine status and configuration
\timing                                          # Toggle query timing display
\clear, \c                                       # Clear screen
\quit, \q, \exit                                 # Exit shell with session summary
```

### Flags and Options

| Flag | Description | Example |
|------|-------------|---------|
| `--table`, `-t` | Target table name (required) | `--table sales` |
| `--namespace`, `-n` | Target namespace (optional) | `--namespace analytics` |
| `--overwrite` | Replace existing table | `--overwrite` |
| `--dry-run` | Preview without executing | `--dry-run` |
| `--infer-schema` | Show inferred schema only | `--infer-schema` |
| `--format` | Output format for SQL queries | `--format csv` |
| `--max-rows` | Maximum rows to display | `--max-rows 500` |
| `--show-schema` | Show column schema with results | `--show-schema` |
| `--metrics` | Show performance metrics | `--metrics` |
| `--timing` | Enable/disable query timing | `--timing` |

## 🧪 Development

### Prerequisites

- **Go 1.21+** for building and development
- **Git** for version control
- **Make** for build automation (optional)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/TFMV/icebox.git
cd icebox/icebox

# Install dependencies
go mod tidy

# Build the binary
go build -o icebox cmd/icebox/main.go

# Run tests
go test ./...

# Build for multiple platforms
GOOS=linux GOARCH=amd64 go build -o icebox-linux-amd64 cmd/icebox/main.go
GOOS=darwin GOARCH=arm64 go build -o icebox-darwin-arm64 cmd/icebox/main.go
GOOS=windows GOARCH=amd64 go build -o icebox-windows-amd64.exe cmd/icebox/main.go
```

### Project Structure

```
icebox/
├── cmd/icebox/           # Main application entry point
├── cli/                  # Command-line interface
├── catalog/sqlite/       # SQLite catalog implementation  
├── config/               # Configuration management
├── fs/local/             # Local filesystem abstraction
├── importer/             # Data import functionality
├── tableops/             # Table operations and transactions
├── art/                  # Design documents and specifications
└── testdata/             # Test data and fixtures
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific package tests
go test ./catalog/sqlite/...
go test ./cli/...

# Run tests with coverage
go test -cover ./...
```

## 🗺️ Roadmap

### 🎯 Current Version (v0.1.0)

- ✅ Project initialization and configuration
- ✅ SQLite catalog with full namespace/table operations
- ✅ **REST catalog support** - Connect to external Iceberg REST catalogs
- ✅ **OAuth 2.0 & AWS SigV4 authentication** - Enterprise-grade security
- ✅ **Comprehensive catalog configuration** - TLS, custom properties, credentials
- ✅ Parquet import with schema inference
- ✅ Table operations with Arrow integration
- ✅ Rich CLI with comprehensive options
- ✅ **SQL Query Engine** - DuckDB integration for high-performance analytics
- ✅ **Interactive SQL Shell** - REPL with command history and multi-line support
- ✅ **Multiple output formats** - Table, CSV, JSON formatting
- ✅ **Query performance monitoring** - Metrics, timing, and caching

### 🚀 Coming Soon (v0.2.0)

- 🔄 **Advanced Import Options** - Partitioning and incremental updates
- 🔄 **Table Evolution** - Schema changes and column operations
- 🔄 **Performance Optimization** - Parallel processing and enhanced caching
- 🔄 **Query Optimization** - Advanced SQL features and performance tuning

### 🌟 Future Releases

- **Cloud Storage** - S3, GCS, Azure integration
- **Streaming Ingestion** - Real-time data processing
- **Web UI** - Browser-based data exploration
- **SDK Libraries** - Programmatic access and testing utilities
- **Advanced Analytics** - Time-travel queries and table snapshots

## 🤝 Contributing

We welcome contributions! Icebox is designed to be **approachable for developers** at all levels.

### How to Contribute

1. **🍴 Fork the repository** and create a feature branch
2. **🧪 Write tests** for your changes (we maintain high test coverage)
3. **📝 Update documentation** as needed
4. **✅ Ensure tests pass** with `go test ./...`
5. **🔄 Submit a pull request** with a clear description

### Areas for Contribution

- 🐛 **Bug fixes and stability improvements**
- 📚 **Documentation and examples**  
- ✨ **New features and enhancements**
- 🧪 **Test coverage and quality assurance**
- 🎨 **CLI/UX improvements**

### Development Guidelines

- **Test-driven development** - Write tests first
- **Clear commit messages** - Explain what and why
- **Code documentation** - Comment complex logic
- **Error handling** - Provide helpful error messages
- **Backward compatibility** - Don't break existing workflows

## 📄 License

This project is licensed under the **MIT LICENSE** - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Made with ❤️ for the data community**

[⭐ Star this project](https://github.com/TFMV/icebox) if you find it useful!

</div>
