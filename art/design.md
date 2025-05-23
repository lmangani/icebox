# Icebox: A Single-Binary Playground for Apache Iceberg

## 1 Vision & guiding principles

| Principle                             | What it means for icebox                                                                                             |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Zero friction**                     | *One download → one command → working catalog*.  No Docker compose, JVM, or extra daemons.                           |
| **Local‑first**                       | Default catalog = embedded SQLite DB placed in `~/.icebox`.  Default object store = local filesystem.                |
| **Stateless by default**              | Each project is a self‑contained directory; delete the folder and nothing lingers elsewhere.                         |
| **Batteries included, not welded‑in** | Everything ships in the binary but can be swapped out (e.g., MinIO instead of FS, REST catalog instead of embedded). |
| **Native Go ergonomics**              | No CGO, pure‑Go deps where possible; simple channel‑based concurrency.                                               |
| **Show, don't tell**                  | Bundled demo datasets + sample notebooks to showcase time‑travel, branching, schema evolution.                       |
| **Composability**                     | Designed to be used standalone or as a library component in larger Go applications.                                  |

---

## 2 Primary developer journeys

1. **"Kick the tires"**

   ```bash
   icebox init mybox          # creates folder .icebox.yml + embedded catalog db
   icebox import nyc-taxi.parquet --table trips
   icebox sql 'SELECT count(*) FROM trips'
   icebox time-travel trips --as-of "2025-05-20T12:00:00"
   ```

2. **Integrate in existing Go test** — use `github.com/icebox/pkg/sdk` for a temp catalog inside unit tests.
3. **Benchmark writer options** — toggle `--write-mode append-data-files|append-stream` (mirrors iceberg‑go flags).
4. **Catalog migration** — point at an external REST catalog (`--uri`) but keep local blobs.
5. **Polished demo** — run `icebox ui` to launch a lightweight web viewer (Wails or HTMX‑templated).
6. **Data pipeline integration** — use `icebox serve` to expose a gRPC or REST API for other tools to interact with.

---

## 3 Feature set v0 → v1

### 3.1 Out‑of‑the‑box v0 (MVP)

| Area             | Capability                                                                   | Notes                                                         |
| ---------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------- |
| **Catalog**      | Embedded SQLite (same schema as REST catalog v2)                             | Zero deps; transactional via WAL                              |
| **Object store** | Local FS & in‑memory FS                                                      | In-mem great for tests/CI                                     |
| **Table ops**    | Create / Drop / List; Append (stream, data‑files)                            | Direct mapping to iceberg-go functionality                    |
| **Query**        | Simple SQL via DuckDB embedded; reads Arrow tables from iceberg‑go scan plan | Avoids re‑implementing query engine; good for interactive use |
| **CLI**          | `init`, `catalog`, `table`, `import`, `sql`, `time-travel`, `ui`, `pack`     | `pack` → zip up catalog + data for sharing                   |
| **REPL**         | `icebox shell` drops you into prompt with command shortcuts + history        | Supports TAB completion and context-aware help                |
| **Language SDK** | Thin Go wrapper to spin up ephemeral catalog in tests (`sdk.NewTestBox(t)`)  | Makes integration testing painless, supports CI config overrides |
| **Formats**      | Support for Parquet and Avro formats                                         | Leveraging iceberg-go's format capabilities                   |

### 3.2 Early v1 stretch goals

* **Branch & Tag** semantics (Iceberg v2)
* **Manifest rewrite / compaction** helpers
* **Schema evolution wizard** (generate `ALTER TABLE` diff plan)
* **S3‑style endpoint**: embed MinIO in process, switch via `--storage s3://…`
* **gRPC service mode** so Python/Rust clients can hit `icebox serve` as local "lakehouse in a box"
* **Web UI**: timeline graph, file size histograms, snapshot diff viewer
* **Metadata metrics**: statistics on table size, file counts, partition distribution
* **Optimization suggestions**: analyze dataset and suggest partitioning or clustering improvements
* **Multi-catalog federation**: query across multiple catalogs, including external ones

---

## 4 Architecture sketch

```text
cmd/
 └ icebox/          # Main CLI entry point
├ catalog/          # embedded sqlite + adapters to iceberg-go REST interface
│  └ sqlite/        # SQLite implementation of iceberg catalog interfaces
│  └ adapter/       # Adapters to connect with iceberg-go catalog interfaces
├ fs/               # local + mem + minio driver (implements iceberg-go FileIO)
│  └ local/         # Local filesystem implementation
│  └ memory/        # In-memory filesystem for testing
│  └ minio/         # Embedded MinIO S3 implementation
├ engine/           # shim over DuckDB (go-duckdb) for SQL query
│  └ duckdb/        # DuckDB integration
│  └ arrow/         # Arrow conversion utilities
├ tableops/         # wraps iceberg-go high‑level APIs
│  └ writer/        # Table write operations
│  └ reader/        # Table read operations
│  └ evolution/     # Schema evolution utilities
├ ui/               # optional web UI (html/template + embedded static)
│  └ api/           # API endpoints for UI
│  └ static/        # Static assets (JS, CSS, embedded with go:embed)
│  └ templates/     # HTML templates (embedded with go:embed)
└ cli/              # Cobra commands and CLI logic
   └ init/          # Init command
   └ catalog/       # Catalog operations
   └ table/         # Table operations
   └ import/        # Data import
   └ sql/           # SQL execution
pkg/
 └ sdk/             # public helpers for tests and embedding
   └ test/          # Test helpers with CI-friendly configuration options
   └ config/        # Configuration helpers with defaults override capability
assets/             # demo Parquet + QuickStart markdown
```

*All packages remain pure‑Go; no CGO unless you opt‑in to DuckDB (compile tags can gate this).*

---

## 5 Key design decisions & trade‑offs

| Decision                          | Rationale                                                                        | Alternatives                                           |
| --------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------ |
| **Embedded SQLite vs BoltDB**     | SQL schema identical to REST catalog tables ⇒ future migration effortless        | Badger/Bolt for simplicity but less portable           |
| **DuckDB for querying**           | Mature, Arrow‑native; lets users prove correctness quickly                      | Write our own Substrait→execution engine—overkill now  |
| **Single process MinIO (opt‑in)** | Keeps the "one binary" promise while letting people test S3 multipart, IAM, etc. | Mock S3 (go‑mock‑s3) isn't production‑like             |
| **Config file `.icebox.yml`**     | Declarative, supports multiple boxes per workspace                              | Env‑vars only (harder for multi‑catalog scenarios)     |
| **Pure Go adapters**              | Leverages iceberg-go while providing clean abstraction                          | Direct dependency coupling would limit flexibility     |
| **Pluggable catalog architecture**| Allows swapping between SQLite, REST, and other catalogs                        | Tight coupling would limit extensibility               |
| **go:embed for UI assets**        | Clean binary builds without external dependencies                               | File-based loading adds complexity and deployment steps |

---

## 6 CLI ergonomics (Cobra syntax)

```text
icebox init <dir> [--catalog sqlite|rest] [--storage fs|s3|mem]

icebox catalog list
icebox catalog create <name> [--uri <catalog-uri>]

icebox table create <name> [--schema <schema-file>] [--partition-by <expr>]
icebox table list [--catalog <name>] [--namespace <namespace>]
icebox table describe <name> [--snapshot <id>]
icebox table history <name>

icebox import <file|dir> --table <name> [--partition-by <expr>] [--format parquet|avro] [--infer-schema] [--dry-run]

icebox sql '<statement>'
icebox shell                      # interactive prompt

icebox time-travel <table> --as-of <timestamp|snapshot-id>

icebox ui [--port 9090]

icebox serve [--port 8080] [--mode grpc|rest] [--profile local|dev|prod]

icebox pack <dir> [--include-data] [--checksum] # produce .tar.gz shareable sandbox with checksum manifest
```

Autocompletion (`icebox completion bash/zsh`) and a `--demo` flag that bootstraps the NYC taxi dataset so newcomers can play immediately.

---

## 7 Implementation details

### 7.1 SQLite Catalog

The SQLite catalog will implement the same schema as the REST catalog v2 protocol. This ensures:

1. Easy migration between SQLite and REST catalogs
2. Compatibility with iceberg-go's existing catalog interfaces
3. Familiar SQL schema for those already using the REST catalog

Key tables:

* `namespaces`: Stores namespace information and properties
* `tables`: Stores table metadata and references to metadata files
* `snapshots`: Stores snapshot information
* `refs`: Stores branch and tag references
* `properties`: Stores configuration properties

### 7.2 Integration with iceberg-go

Icebox will use iceberg-go as a library dependency but provide its own abstractions on top. This allows:

1. Leveraging the mature code in iceberg-go for table operations
2. Adding convenience methods and simplifications for common operations
3. Providing a cleaner, more focused API for Icebox users

The integration will happen primarily through:

* FileIO implementations that satisfy iceberg-go interfaces
* Catalog implementations that connect to iceberg-go's catalog interfaces
* Table operation wrappers that simplify common tasks

### 7.3 Arrow Integration

For efficient data processing, Icebox will use Arrow as the in-memory format:

1. Arrow tables from iceberg-go scan operations
2. DuckDB integration for SQL queries over Arrow data
3. Efficient import/export operations using Arrow as an intermediate format

### 7.4 Embedded MinIO

For S3 compatibility without external dependencies:

1. Embedded MinIO server within the Icebox process
2. Configurable via simple options (port, credentials)
3. Implements same interfaces as production S3, allowing testing of multipart uploads, etc.
4. Can be disabled with compile flags for smaller binary size

### 7.5 Service Profiles

The `icebox serve` command will support named service profiles:

1. **local**: Default configuration optimized for local development
   * Relaxed authentication
   * Debug logging enabled
   * Memory-optimized for developer machines

2. **dev**: Configuration for development/staging environments
   * Basic authentication
   * Structured logging
   * More resource allocation than local

3. **prod**: Production-ready configuration
   * Strong authentication required
   * Performance-optimized logging
   * Resource allocation for production workloads
   * Metrics collection enabled

### 7.6 SDK Test Utilities

The SDK will include test utilities that make it easy to integrate Icebox into CI pipelines:

```go
// Create test catalog with default configuration
testBox := sdk.NewTestBox(t)

// Override defaults for CI environment
testBox := sdk.NewTestBox(t, 
    config.WithDefaults(config.CIDefaults),
    config.WithTempDir("/tmp/ci-artifacts"),
    config.WithMemoryLimit(2048),
)
```

### 7.7 Import Command Features

The import command will include:

1. **--infer-schema**: Automatically detect and create schema from data files
   * Analyze sample of data to infer types
   * Handle nested structures
   * Set appropriate precision for numeric fields

2. **--dry-run**: Validate import without actually writing data
   * Check file formats
   * Validate schema compatibility
   * Report potential partition distribution
   * Estimate final table size

### 7.8 Packing and Reproducibility

The `icebox pack` command will generate a shareable sandbox that includes:

1. Catalog metadata
2. Table data (optional)
3. Configuration files
4. SHA-256 checksums for all included files
5. Manifest file listing all contents with their versions and checksums

This ensures reproducibility across environments and allows sharing complete examples.

---

## 8 Roadmap milestones

1. **0.1.0** – init/list/create, Arrow scan → DuckDB query, local FS, unit‑test SDK
   * SQLite catalog implementation
   * Local filesystem support
   * Basic table operations (create, list, drop)
   * Simple SQL queries via DuckDB
   * Initial CLI commands

2. **0.2.0** – MinIO‑embedded, S3 credentials pass‑through, import parquet directory
   * Add embedded MinIO server
   * Parquet import/export functionality
   * S3-compatible storage backend
   * Improved CLI experience with autocompletion

3. **0.3.0** – Snapshot branching, manifest‑rewrite helper, basic web UI
   * Branch and tag support
   * Manifest optimization utilities
   * Simple web UI for table exploration
   * Schema evolution wizards

4. **0.4.0** – gRPC "lakehouse" server mode + Python quick‑start notebook
   * gRPC service implementation
   * Python client library
   * Sample notebooks for demonstration
   * Enhanced query capabilities

5. **1.0.0** – Stabilize APIs, optimize performance, release binaries
   * API stabilization
   * Performance optimizations
   * Binary releases for macOS, Linux, Windows
   * Comprehensive documentation

---

## 9 Next steps

1. **Scaffold repo** with `go work`, create `cmd/icebox` and minimal `init` command
   * Set up GitHub repository with proper structure
   * Initialize Go modules and dependencies
   * Implement basic CLI framework using Cobra

2. **Wire embedded catalog**: generate SQLite schema from Iceberg REST spec
   * Create SQLite schema based on REST catalog API
   * Implement catalog interfaces from iceberg-go
   * Add unit tests for catalog operations

3. **Implement `scan -> Arrow -> DuckDB` pipeline**
   * Create small proof of concept first
   * Wire up DuckDB with Arrow conversion
   * Test with sample datasets

4. **Add demo dataset importer + README QuickStart**
   * Create sample datasets (NYC taxi, etc.)
   * Write comprehensive README with examples
   * Implement import command

5. **Harden with integration tests**
   * Test snapshot isolation & time‑travel
   * Test schema evolution and compaction
   * Test with various storage backends

With this blueprint, you can start coding the `init` + `import` flow and have a tangible demo quickly—enough to demonstrate value and gather feedback for further iterations.

---

## 10 Integration with existing ecosystems

### 10.1 DuckDB integration

Icebox will integrate with DuckDB in several ways:

1. **Query execution**: Use DuckDB's SQL engine to query Iceberg tables
2. **Data conversion**: Convert between Arrow and DuckDB formats
3. **Extension possibility**: Potential future DuckDB extension for direct Iceberg access

### 10.2 Spark/Flink compatibility

Ensure Icebox-created tables are compatible with:

1. **Spark**: Test that tables created by Icebox can be read by Spark
2. **Flink**: Ensure compatibility with Flink's Iceberg connector
3. **Trino**: Test compatibility with Trino's Iceberg connector

### 10.3 Cloud provider integration

Add support for major cloud services:

1. **AWS**: S3 for storage, Glue for catalog
2. **GCP**: GCS for storage
3. **Azure**: Azure Blob Storage for storage

---

## 11 Performance considerations

For optimal performance, Icebox will:

1. **Use efficient algorithms** for table operations
2. **Leverage Arrow** for in-memory data representation
3. **Implement caching** of catalog and file metadata
4. **Parallelize operations** where appropriate
5. **Optimize file formats** for common query patterns
6. **Monitor memory usage** to prevent OOM issues with large tables

Performance benchmarks will be created to track progress and identify bottlenecks.
