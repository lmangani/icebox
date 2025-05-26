# Avro Import Feature

Icebox supports importing Apache Avro files into Iceberg tables using Apache Arrow Go's native Avro support. This feature provides automatic schema inference and data conversion from Avro to Iceberg format.

## Overview

The Avro import functionality allows you to:

- Import Avro Object Container Files (OCF) into Iceberg tables
- Automatically infer schema from Avro files
- Convert Avro data types to compatible Iceberg types
- Handle both simple and complex nested data structures
- Work with all three supported catalog types (SQLite, REST, JSON)

## Usage

### Command Line Interface

```bash
# Import an Avro file into an Iceberg table
icebox import data.avro --table my_table

# Import with specific namespace
icebox import data.avro --table my_namespace.my_table

# Import with overwrite
icebox import data.avro --table my_table --overwrite

# Dry run to see schema inference
icebox import data.avro --table my_table --dry-run --infer-schema
```

### Programmatic Usage

```go
package main

import (
    "context"
    "github.com/TFMV/icebox/config"
    "github.com/TFMV/icebox/importer"
    "github.com/apache/iceberg-go/table"
)

func main() {
    // Load configuration
    cfg, err := config.LoadConfig("icebox.yaml")
    if err != nil {
        panic(err)
    }

    // Create Avro importer
    avroImporter, err := importer.NewAvroImporter(cfg)
    if err != nil {
        panic(err)
    }
    defer avroImporter.Close()

    // Infer schema from Avro file
    schema, stats, err := avroImporter.InferSchema("data.avro")
    if err != nil {
        panic(err)
    }

    // Import the file
    ctx := context.Background()
    req := importer.ImportRequest{
        ParquetFile:    "data.avro", // Field name is reused for Avro files
        TableIdent:     table.Identifier{"my_namespace", "my_table"},
        NamespaceIdent: table.Identifier{"my_namespace"},
        Overwrite:      false,
    }

    result, err := avroImporter.ImportTable(ctx, req)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Imported %d records\n", result.RecordCount)
}
```

### Factory Pattern Usage

```go
// Use the factory to create importers based on file type
factory := importer.NewImporterFactory(cfg)

// Automatically detect file type and create appropriate importer
avroImporter, importerType, err := factory.CreateImporter("data.avro")
if err != nil {
    panic(err)
}
defer avroImporter.Close()

// Or create by specific type
avroImporter, err := factory.CreateImporterByType(importer.ImporterTypeAvro)
if err != nil {
    panic(err)
}
defer avroImporter.Close()
```

## Data Type Mapping

The Avro importer maps Avro data types to Iceberg types as follows:

| Avro Type | Iceberg Type | Notes |
|-----------|--------------|-------|
| `boolean` | `boolean` | Direct mapping |
| `int` | `int` | 32-bit signed integer |
| `long` | `long` | 64-bit signed integer |
| `float` | `float` | 32-bit floating point |
| `double` | `double` | 64-bit floating point |
| `bytes` | `binary` | Variable-length byte array |
| `string` | `string` | UTF-8 encoded string |
| `fixed` | `fixed` | Fixed-length byte array |
| `enum` | `string` | Converted to string representation |
| `array` | `list` | Variable-length array |
| `map` | `map` | Key-value mapping |
| `record` | `struct` | Nested record structure |
| `union` | Complex mapping | Handled based on union types |

### Complex Type Handling

#### Arrays/Lists

```json
{
  "type": "array",
  "items": "string"
}
```

Maps to Iceberg `list<string>`.

#### Maps

```json
{
  "type": "map",
  "values": "int"
}
```

Maps to Iceberg `map<string, int>`.

#### Records/Structs

```json
{
  "type": "record",
  "name": "Person",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
```

Maps to Iceberg `struct<name: string, age: int>`.

#### Unions

```json
["null", "string"]
```

Maps to nullable Iceberg `string` type.

## Limitations and Known Issues

### Arrow Go Avro Reader Limitations

The current implementation uses Apache Arrow Go v18's Avro support, which has some limitations:

1. **Complex Nested Structures**: Very deeply nested or complex Avro schemas may cause the Arrow Avro reader to fail
2. **Union Type Complexity**: Complex union types with multiple record types may not be fully supported
3. **Schema Evolution**: Limited support for Avro schema evolution features

### Fallback Handling

When the Arrow Avro reader encounters unsupported schemas, the importer provides fallback behavior:

1. **Schema Inference Fallback**: Creates a simple schema with basic string fields
2. **Data Import Fallback**: Creates a table with file metadata for manual processing
3. **Warning Messages**: Clear indication when fallback mode is used

Example fallback output:

```
⚠️  Arrow Avro reader failed (failed to create avro reader: invalid: could not create avro ocfreader), attempting fallback schema inference
⚠️  Arrow Avro reader failed (failed to create avro reader: invalid: could not create avro ocfreader), creating fallback table
```

### Workarounds for Complex Files

For Avro files that cannot be processed by the Arrow reader:

1. **Use External Tools**: Convert complex Avro files to Parquet using tools like Apache Spark
2. **Schema Simplification**: Flatten complex nested structures before import
3. **Manual Processing**: Use the fallback table as a placeholder and process data separately

## Performance Considerations

### Memory Usage

- Large Avro files are loaded entirely into memory during import
- Consider available RAM when importing very large files
- Use streaming approaches for files larger than available memory

### Batch Processing

- The importer processes records in batches for better performance
- Default batch size is optimized for most use cases
- Adjust batch size in `WriteOptions` for specific requirements

### File Size Recommendations

- **Small files** (< 100MB): Direct import works well
- **Medium files** (100MB - 1GB): Monitor memory usage
- **Large files** (> 1GB): Consider splitting or using external tools

## Error Handling

The Avro importer provides comprehensive error handling:

### Common Errors

1. **File Not Found**

```
failed to stat file: no such file or directory
```

2. **Invalid Avro Format**

```
failed to create avro reader: invalid avro file format
```

3. **Schema Conversion Errors**

```
failed to convert field 'fieldname': unsupported avro type
```

4. **Catalog Errors**

```
failed to create table: table already exists
```

### Error Recovery

- Use `--overwrite` flag to replace existing tables
- Check file permissions and paths
- Verify Avro file format with external tools
- Use fallback mode for unsupported schemas

## Testing

### Unit Tests

The Avro importer includes comprehensive unit tests:

- Schema inference testing
- Data type conversion testing
- Error handling testing
- Fallback mechanism testing

### Integration Tests

- Real Avro file processing
- End-to-end import workflows
- Catalog integration testing
- Performance benchmarking

### Test Data

Test files are located in `testdata/`:

- `githubsamplecommits.avro`: Real-world GitHub commits data
- Various schema complexity levels
- Edge case scenarios

## Future Improvements

### Planned Enhancements

1. **Streaming Import**: Support for large files without full memory loading
2. **Schema Evolution**: Better support for Avro schema evolution
3. **Custom Type Mapping**: User-defined type conversion rules
4. **Parallel Processing**: Multi-threaded import for large files
5. **Progress Reporting**: Real-time import progress updates

### Alternative Implementations

1. **Native Avro Parser**: Custom Avro parser for better compatibility
2. **External Tool Integration**: Integration with Apache Spark or other tools
3. **Incremental Import**: Support for updating existing tables

## Contributing

To contribute to the Avro import feature:

1. **Bug Reports**: Report issues with specific Avro files and schemas
2. **Test Cases**: Add test cases for edge cases and complex schemas
3. **Performance**: Optimize memory usage and processing speed
4. **Documentation**: Improve documentation and examples

## See Also

- [Parquet Import Documentation](parquet-import.md)
- [Iceberg Table Management](table-management.md)
- [Configuration Guide](configuration.md)
- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Apache Arrow Go Documentation](https://arrow.apache.org/docs/go/)
