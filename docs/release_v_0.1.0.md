# Icebox v0.1.0 Release Notes

**Release Date:** 05/24/2025

We are thrilled to announce the initial release of **Icebox v0.1.0**! 🧊

Icebox is a zero-configuration data lakehouse that gets you from zero to querying Apache Iceberg tables in under five minutes. It's designed to be a single-binary playground, perfect for experimenting with Iceberg, learning lakehouse concepts, prototyping data pipelines locally, and testing Iceberg integrations before production.

This initial release lays the foundation for a powerful and developer-friendly local Iceberg experience.

## ✨ Key Features in v0.1.0

This release focuses on core functionality, enabling a complete local data lakehouse workflow:

### 🚀 Zero-Setup Experience

* **Single Go Binary**: No complex installation or external dependencies required. Download and run!
* **Embedded SQLite Catalog**: Out-of-the-box Iceberg catalog using an embedded SQLite database. No need for an external metastore for local development.
* **REST Catalog Support**: Flexibility to connect to existing Iceberg REST catalogs if needed.
* **Local Filesystem Storage**: Defaults to using the local filesystem for storing table data.
* **Embedded MinIO Server (Experimental)**: Includes an embedded MinIO server for S3-compatible object storage, allowing local testing of cloud-like workflows.
* **Auto-Configuration**: Sensible defaults are applied, minimizing the need for manual configuration to get started.

### 📁 Data Operations

* **Parquet Import**: Easily import data from Parquet files into Iceberg tables (`icebox import`).
* **Automatic Schema Inference**: Schemas are automatically inferred from Parquet files during import.
* **Iceberg Table Management**: Core operations for creating and managing Iceberg tables.
* **Namespace Organization**: Basic support for organizing tables within namespaces.
* **Arrow Integration**: Leverages Apache Arrow for efficient in-memory data processing.
* **ACID Transactions**: Ensures data integrity with ACID guarantees for table operations, powered by Apache Iceberg.

### 🔍 SQL Querying

* **DuckDB Integration**: High-performance analytical SQL querying powered by an embedded DuckDB engine.
* **Interactive SQL Shell**: A user-friendly interactive shell (`icebox shell`) with features like command history, multi-line query support, and special commands for help, table listing, schema inspection, etc.
* **Time-Travel Queries**: Query tables as of a specific snapshot ID or timestamp, a core feature of Apache Iceberg.
* **Multiple Output Formats**: Support for displaying query results in table, CSV, or JSON formats.
* **Automatic Table Registration**: Tables in the catalog are automatically registered with the query engine for immediate querying.

### 🛠️ Developer-Friendly CLI

* **Intuitive Commands**: A rich command-line interface for project initialization (`icebox init`), data import, SQL querying, table operations, and catalog management.
* **Comprehensive Table Operations**: Commands to list, describe, and view the history of tables.
* **Namespace Management**: Commands to create, list, and drop namespaces.
* **YAML Configuration**: Projects are configured via a simple `.icebox.yml` file for clarity and reproducibility.

### 📦 Pack & Unpack (Experimental)

* **Portable Project Archives**: Ability to `pack` an entire Icebox project (catalog, data, configuration) into a distributable archive and `unpack` it on another machine.

## 🚀 Getting Started

1. **Install Icebox**:
    Build from source (Go 1.21+ required):

    ```bash
    git clone https://github.com/TFMV/icebox.git
    cd icebox/icebox 
    go build -o icebox cmd/icebox/main.go # Adjust path as per your project structure
    # Place the 'icebox' binary in your PATH or run it directly
    ```

2. **Initialize Your Lakehouse Project**:

    ```bash
    ./icebox init my-first-lakehouse
    cd my-first-lakehouse
    ```

    This sets up your `.icebox.yml` config, SQLite catalog, and data directory.

3. **Import Data**:
    (Assuming you have a `sales_data.parquet` file)

    ```bash
    cp /path/to/your/sales_data.parquet .
    ./icebox import sales_data.parquet --table sales
    ```

4. **Query Your Data**:

    ```bash
    ./icebox sql "SELECT COUNT(*) FROM sales"
    ```

    Or launch the interactive shell:

    ```bash
    ./icebox shell
    ```

## 🛣️ What's Next?

This v0.1.0 release is just the beginning! We are excited to continue developing Icebox with a focus on:

* Stabilizing experimental features like MinIO integration and Pack/Unpack.
* Enhancing schema evolution capabilities.
* Improving performance and resource management.
* Expanding documentation and examples.
* Adding more advanced table maintenance operations.
* Potential UI development for easier data exploration.

## 🙏 Feedback & Contributions

Icebox is an open-source project, and we welcome your feedback, bug reports, and contributions!

* **GitHub Issues**: [https://github.com/TFMV/icebox/issues](https://github.com/TFMV/icebox/issues)
* **Contributing**: Please see our `README.md` for guidelines on contributing.

Thank you for trying out Icebox! We hope it makes your local data lakehouse explorations more productive and enjoyable.
