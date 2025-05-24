# 🧊 Icebox

<div align="center">

**A single-binary playground for Apache Iceberg**  
*Five minutes to first query*

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)](https://golang.org)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v0.3.0--rc0-326ce5?style=flat&logo=apache&logoColor=white)](https://iceberg.apache.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

[Quick Start](#-quick-start) • [Features](#-features) • [Examples](#-examples) • [Usage Guide](docs/usage.md) • [Contributing](#-contributing)

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
- **Embedded MinIO server** - S3-compatible storage for testing production workflows
- **Local storage** - File system integration out of the box
- **In-memory filesystem** - Lightning-fast testing and development workflows
- **Auto-configuration** - Sensible defaults, minimal configuration required

### 📁 **Data Operations**

- **Parquet import** with automatic schema inference
- **Demo datasets** - NYC taxi data with realistic analytics examples
- **Iceberg table** creation and management
- **Namespace** organization and operations
- **Pack/Unpack** - Portable project archives for sharing and backup
- **Arrow integration** for efficient data processing
- **Transaction support** with proper ACID guarantees

### 🔍 **SQL Querying**

- **DuckDB integration** for high-performance analytics
- **Interactive SQL shell** with command history and multi-line support
- **Time-travel queries** - Query tables at any point in their history
- **Multiple output formats** - table, CSV, JSON
- **Auto-registration** of catalog tables for immediate querying
- **Query performance metrics** and optimization features

### 🎬 **Demo & Learning**

- **One-command setup** - Instant demo datasets with realistic data
- **NYC taxi analytics** - Real-world data with 5,000+ trip records
- **Sample queries** - Pre-built analytics examples for learning
- **Partitioned datasets** - Explore advanced Iceberg features
- **Temporal analysis** - Date-based queries and time-series operations
- **Business intelligence examples** - Revenue, vendor, and trend analysis

### 🛠️ **Developer-Friendly**

- **Rich CLI** with intuitive commands and helpful output
- **Comprehensive table operations** - create, list, describe, history
- **Namespace management** for organized data governance
- **Dry-run modes** to preview operations
- **YAML configuration** for reproducible setups

## 🚀 Quick Start

### 1. Install Icebox

```bash
# Build from source (Go 1.21+ required)
git clone https://github.com/TFMV/icebox.git
cd iceberg/icebox
go build -o icebox cmd/icebox/main.go
```

### 2. Try the Demo (Fastest Path!)

```bash
# Create demo project with NYC taxi data
./icebox init taxi-demo
cd taxi-demo

# Set up demo dataset (one command!)
./icebox demo

✅ Demo setup complete!

🚀 Try these commands:

📊 NYC Taxi:
   # Count total number of taxi trips
   icebox sql "SELECT COUNT(*) as total_trips FROM nyc_taxi"
   
   # Calculate average fare amount
   icebox sql "SELECT AVG(fare_amount) as avg_fare FROM nyc_taxi WHERE fare_amount > 0"
   
   # Compare taxi vendors by performance
   icebox sql "SELECT vendor_name, COUNT(*) as trips, AVG(fare_amount) as avg_fare FROM nyc_taxi WHERE vendor_name IS NOT NULL GROUP BY vendor_name"
```

### 3. Start Querying Real Data

```bash
# Query your demo data
./icebox sql "SELECT COUNT(*) FROM nyc_taxi"
📋 Registered 1 tables for querying
⏱️  Query executed in 45ms
📊 1 rows returned
┌─────────────┐
│ total_trips │
├─────────────┤
│ 5476        │
└─────────────┘

# Analyze payment methods
./icebox sql "SELECT payment_type, COUNT(*) as count FROM nyc_taxi GROUP BY payment_type"
⏱️  Query executed in 67ms
📊 2 rows returned
┌──────────────┬───────┐
│ payment_type │ count │
├──────────────┼───────┤
│ Credit       │ 3891  │
│ Cash         │ 1585  │
└──────────────┴───────┘

# Use the interactive shell for complex analysis
./icebox shell

🧊 Icebox SQL Shell v0.1.0
Interactive SQL querying for Apache Iceberg
Type \help for help, \quit to exit

icebox> SELECT vendor_name, AVG(trip_distance) as avg_distance FROM nyc_taxi GROUP BY vendor_name;
⏱️  Query executed in 23ms
📊 2 rows returned
┌─────────────┬──────────────┐
│ vendor_name │ avg_distance │
├─────────────┼──────────────┤
│ VTS         │ 2.87         │
│ CMT         │ 3.12         │
└─────────────┴──────────────┘

icebox> \quit
```

### 4. Import Your Own Data

```bash
# Import your own Parquet files
./icebox import sales_data.parquet --table sales

✅ Successfully imported table!

📊 Import Results:
   Table: [default sales]
   Records: 1,000,000
   Size: 45.2 MB
   Location: file:///.icebox/data/default/sales
```

**🎉 You now have a working Iceberg lakehouse with real data and SQL querying!**

## 🌟 New Features

### 🎬 Demo Datasets

Get started immediately with realistic data and pre-built analytics examples:

```bash
# Set up demo environment
./icebox init my-demo
cd my-demo
./icebox demo

# Explore demo datasets
./icebox demo --list

🎬 Available Demo Datasets:

📊 **taxi**
   Description: NYC Taxi trip data with partitioning by year and month - perfect for analytics
   Namespace: demo
   Table: nyc_taxi
   Partitioned: Yes
   Sample Queries: 6

💡 Usage:
   icebox demo                           # Set up all datasets
   icebox demo --dataset taxi            # Set up specific dataset
   icebox demo --cleanup                 # Remove all demo data
```

**Demo Features:**

- 🚖 **Real NYC Taxi Data** - 5,000+ actual trip records with 22 columns
- 📅 **Temporal Patterns** - Data across multiple months for time-series analysis
- 💰 **Financial Analytics** - Fare amounts, tips, and payment methods
- 🏪 **Vendor Comparisons** - Multi-vendor data for comparative analysis
- 📊 **Sample Queries** - 6 pre-built analytics examples
- ⚡ **Instant Setup** - One command to working analytics environment

### 🗄️ Embedded MinIO Server

Test S3-compatible storage workflows locally with zero configuration:

```bash
# Initialize with embedded MinIO
./icebox init my-project --storage minio

# Or enable in existing project
cat >> .icebox.yml << EOF
storage:
  type: minio
  minio:
    embedded: true
    console: true    # Enable web console at http://localhost:9000
EOF

# MinIO starts automatically with Icebox
./icebox sql "SHOW TABLES"
# 🗄️ Starting embedded MinIO server...
# ✅ MinIO server started successfully
```

**Features:**

- 🚀 **S3-Compatible API** - Test cloud storage workflows locally
- 🌐 **Web Console** - Browser-based management interface
- 🛡️ **Secure by Default** - Configurable authentication and TLS
- 📊 **Performance Optimized** - Modern connection pooling and timeouts
- 💾 **In-Memory Mode** - Lightning-fast testing with temporary storage

### 📦 Pack & Unpack

Create portable archives of your lakehouse projects:

```bash
# Create project archive
./icebox pack my-analytics-project.tar.gz

# Share and distribute
scp my-analytics-project.tar.gz colleague@server:/home/colleague/

# Restore anywhere
./icebox unpack my-analytics-project.tar.gz
```

**Perfect for:**

- 📤 **Sharing** projects with colleagues
- 💾 **Backup** and archival
- 🚀 **Distribution** of datasets and schemas
- 🧪 **Testing** with consistent environments

## 📋 Examples

### NYC Taxi Analytics Demo

```bash
# Set up demo with NYC taxi data
./icebox demo

# Revenue analysis
./icebox sql "SELECT AVG(fare_amount) as avg_fare, AVG(total_amount) as avg_total FROM nyc_taxi WHERE fare_amount > 0"

# Temporal patterns
./icebox sql "SELECT DATE_TRUNC('month', pickup_datetime) as month, COUNT(*) as trips FROM nyc_taxi GROUP BY month ORDER BY month"

# Vendor performance comparison
./icebox sql "SELECT vendor_name, COUNT(*) as trips, AVG(fare_amount) as avg_fare, AVG(trip_distance) as avg_distance FROM nyc_taxi WHERE vendor_name IS NOT NULL GROUP BY vendor_name"

# Busy hour analysis
./icebox sql "SELECT EXTRACT(hour FROM pickup_datetime) as hour, COUNT(*) as trips FROM nyc_taxi GROUP BY hour ORDER BY hour"
```

### Quick Data Analysis

```bash
# Import and analyze customer data
./icebox import customers.parquet --table customers
./icebox sql "SELECT region, AVG(lifetime_value) FROM customers GROUP BY region"

# Time-travel to see historical data
./icebox time-travel customers --as-of "2024-01-01" 
  --query "SELECT COUNT(*) FROM customers"
```

### REST Catalog Integration

```bash
# Connect to production Iceberg REST catalog
./icebox init prod-analytics --catalog rest --uri https://catalog.company.com

# Import data and query immediately
./icebox import events.parquet --table analytics.user_events
./icebox sql "SELECT event_type, COUNT(*) FROM analytics.user_events GROUP BY event_type"
```

### Project Organization

```bash
# Create namespaced tables
./icebox import transactions.parquet --table finance.transactions
./icebox import campaigns.parquet --table marketing.campaigns
./icebox import orders.parquet --table sales.orders

# Query across namespaces
./icebox sql "
SELECT f.account_type, SUM(s.amount) 
FROM finance.transactions f 
JOIN sales.orders s ON f.transaction_id = s.id
GROUP BY f.account_type
"
```

For more comprehensive examples and detailed usage, see our **[📚 Usage Guide](docs/usage.md)**.

## 🌐 Storage & Catalog Support

| Storage Type | Description | Use Case |
|-------------|-------------|----------|
| **Local Filesystem** | File-based storage | Development, testing |
| **In-Memory** | Temporary fast storage | Unit testing, experiments |
| **Embedded MinIO** | S3-compatible local server | Cloud workflow testing |
| **External MinIO** | Remote MinIO instance | Shared development |

| Catalog Type | Description | Use Case |
|-------------|-------------|----------|
| **SQLite** | Embedded local catalog | Single-user development |
| **REST** | External Iceberg REST catalog | Multi-user, production |

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Layer     │    │  Storage Layer  │    │  Catalog Layer  │
│                 │    │                 │    │                 │
│ • import        │◄──►│ • Local FS      │◄──►│ • SQLite        │
│ • sql/shell     │    │ • MinIO S3      │    │ • REST API      │
│ • table ops     │    │ • Cloud storage │    │ • Authentication│
│ • pack/unpack   │    │ • File:// URIs  │    │ • Multi-user    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────┐
                    │   Apache Iceberg    │
                    │                     │
                    │ • Table format      │
                    │ • Time travel       │
                    │ • Transaction log   │
                    │ • DuckDB engine     │
                    └─────────────────────┘
```

## 📚 Documentation

- **[📚 Complete Usage Guide](docs/usage.md)** - Comprehensive documentation for all features
- **[⚡ Quick Start](#-quick-start)** - Get up and running in 5 minutes
- **[🔧 Configuration](docs/usage.md#-configuration-reference)** - Complete configuration reference
- **[🔍 Troubleshooting](docs/usage.md#-troubleshooting)** - Common issues and solutions

### Feature Documentation

- **[🗄️ Embedded MinIO](docs/usage.md#-embedded-minio-server)** - S3-compatible local storage
- **[⏰ Time-Travel Queries](docs/usage.md#-time-travel-queries)** - Query historical table states
- **[📊 Table Operations](docs/usage.md#-table-operations)** - Complete table management
- **[📁 Namespace Management](docs/usage.md#-namespace-management)** - Organize your data
- **[📦 Pack & Unpack](docs/usage.md#-pack--unpack)** - Portable project archives
- **[🌐 REST Catalog](docs/usage.md#-catalog-configuration)** - Enterprise catalog integration

## 🗺️ Roadmap

### ✅ Current Version (v0.1.0)

- ✅ SQLite & REST catalog support with authentication
- ✅ **Embedded MinIO server** with S3-compatible API
- ✅ Parquet import with schema inference
- ✅ **SQL engine** with DuckDB integration
- ✅ **Interactive SQL shell** with rich features
- ✅ **Time-travel queries** for historical data analysis
- ✅ **Table & namespace management** operations
- ✅ **Pack/Unpack** for portable project archives

### 🚀 Future Releases

- **Cloud Storage** - Native S3, GCS, Azure integration
- **Streaming Ingestion** - Real-time data processing
- **Web UI** - Browser-based data exploration
- **Advanced Analytics** - Enhanced query capabilities
- **SDK Libraries** - Programmatic access

## 🤝 Contributing

We welcome contributions! Icebox is designed to be **approachable for developers** at all levels.

### Quick Contribution Guide

1. **🍴 Fork** the repository and create a feature branch
2. **🧪 Write tests** for your changes
3. **📝 Update documentation** as needed
4. **✅ Ensure tests pass** with `go test ./...`
5. **🔄 Submit a pull request**

### Development

```bash
# Build from source
git clone https://github.com/TFMV/icebox.git
cd icebox/icebox
go mod tidy
go build -o icebox cmd/icebox/main.go

# Run tests
go test ./...
```

### Areas for Contribution

- 🐛 **Bug fixes** and stability improvements
- 📚 **Documentation** and examples  
- ✨ **New features** and enhancements
- 🧪 **Test coverage** improvements
- 🎨 **CLI/UX** enhancements

## 📄 License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Made with ❤️ for the data community**

[⭐ Star this project](https://github.com/TFMV/icebox) • [📚 Usage Guide](docs/usage.md) • [🐛 Report Issue](https://github.com/TFMV/icebox/issues)

</div>
