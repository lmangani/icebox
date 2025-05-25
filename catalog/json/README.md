# Icebox JSON Catalog for Apache Iceberg

This is a JSON catalog implementation for Apache Iceberg that provides a feature-complete catalog solution suitable for sandbox environments.

## Features

### 🏗️ Core Architecture

- **Thread-Safe Operations**: Full concurrent access protection using read-write mutexes
- **Atomic File Operations**: All catalog modifications use atomic write operations with temporary files
- **Data Integrity**: Comprehensive validation of catalog data structure and relationships
- **Graceful Error Handling**: Proper error types with detailed error messages

### 📊 Monitoring & Observability

- **Operation Metrics**: Built-in metrics tracking for all catalog operations
  - Tables created/dropped
  - Namespaces created/dropped
  - Operation errors
  - Cache hits/misses
- **Structured Logging**: Comprehensive logging with operation context
- **Performance Monitoring**: Cache performance and operation timing

### 🚀 Battle Ready Features

- **Caching Layer**: Optional TTL-based caching for improved performance
- **Retry Logic**: Exponential backoff retry mechanism for concurrent modifications
- **Atomic Transactions**: ETag-based optimistic concurrency control
- **Configuration Validation**: Comprehensive validation of configuration parameters
- **Proper UUID Generation**: RFC4122 compliant UUID v4 generation

### 🔧 Advanced Iceberg Support

- **Complete Type System**: Full support for all Iceberg types including:
  - Primitive types (boolean, int, long, float, double, string, date, time, timestamp, timestamptz, binary, uuid)
  - Complex types (struct, list, map, decimal, fixed)
- **Schema Evolution**: Proper schema field ID tracking
- **Metadata Compliance**: Full Iceberg v2 metadata specification compliance
- **Table Registration**: Support for registering existing tables with external metadata

### 📈 Scalability Features

- **Efficient Namespace Hierarchy**: Proper handling of nested namespaces
- **Optimized File I/O**: Buffered writes with proper fsync for durability
- **Memory Efficient**: Smart caching and memory management
- **Concurrent Access**: Safe for high-concurrency environments
