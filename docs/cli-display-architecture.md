# CLI Display Architecture Design

## Executive Summary

This document proposes a modern, modular architecture for CLI display and user interaction in the Icebox project. The current implementation uses manual string formatting and hardcoded display logic scattered across CLI commands. This design introduces a unified, extensible display system that follows modern Go CLI best practices.

## Current State Analysis

### Existing Implementation Issues

1. **Scattered Display Logic**: Table formatting code is duplicated across multiple files (`sql.go`, `table.go`, `catalog_mgmt.go`)
2. **Manual String Formatting**: Hand-crafted Unicode box drawing and column width calculations
3. **Limited Customization**: Hardcoded styles with no theme support
4. **Poor Separation of Concerns**: Display logic mixed with business logic
5. **Inconsistent Formatting**: Different table styles across commands
6. **No Progressive Enhancement**: No graceful degradation for different terminal capabilities

### Current Display Patterns

```go
// Example from sql.go - Manual table formatting
func displayTableFormat(columns []string, rows [][]interface{}) error {
    // Manual width calculation
    widths := make([]int, len(columns))
    // Manual Unicode box drawing
    fmt.Print("┌")
    for i, width := range widths {
        fmt.Print(strings.Repeat("─", width+2))
        if i < len(widths)-1 {
            fmt.Print("┬")
        }
    }
    fmt.Println("┐")
    // ... more manual formatting
}
```

## Proposed Architecture

### 1. Display System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    CLI Commands                             │
├─────────────────────────────────────────────────────────────┤
│                  Display Facade                            │
├─────────────────────────────────────────────────────────────┤
│          Core Types  │  Renderers  │  Themes & Styles       │
├─────────────────────────────────────────────────────────────┤
│                Terminal Capabilities                        │
└─────────────────────────────────────────────────────────────┘
```

### 2. Package Layout (Implemented)

```text
icebox/display/          # Root package – import "github.com/TFMV/icebox/display"
├── display.go           # Main facade + simple fallback renderer
├── types.go             # TableData, TableOptions, TableBuilder (fluent API)
├── capabilities.go      # Terminal capability detection
├── renderers/           # Pluggable renderer back-ends
│   ├── interface.go     # Renderer interface
│   ├── pterm.go         # Rich renderer (uses pterm)
│   └── fallback.go      # Zero-dependency fallback renderer
└── example/             # Stand-alone demo program
```

> **Note:**  The original proposal had separate `table/`, `themes/`, and `adapters/` sub-packages. During the proof-of-concept we folded those definitions into the root `display` package to keep import paths minimal and avoid circular dependencies. If future requirements justify it (e.g., theme files loaded from disk, custom adapters for HTML reports), the code can be split out with zero breaking changes because the public API already abstracts the internals.

### 3. Core Components (as coded)

#### A. Display Facade (`display/display.go`)

Central interface for all display operations:

```go
type Display interface {
    // Table operations
    Table(data TableData) *TableBuilder
    
    // Message operations
    Success(message string, args ...interface{})
    Error(message string, args ...interface{})
    Warning(message string, args ...interface{})
    Info(message string, args ...interface{})
    
    // Progress operations
    Progress(title string) *ProgressBuilder
    
    // Interactive operations
    Confirm(message string) bool
    Select(message string, options []string) (int, error)
    Input(message string) (string, error)
    
    // Output format control
    SetFormat(format OutputFormat) Display
    SetTheme(theme Theme) Display
}
```

#### B. Table System (`display/table/`)

Modern table rendering with multiple backends:

```go
type TableBuilder struct {
    data    TableData
    options TableOptions
    theme   Theme
}

type TableData struct {
    Headers []string
    Rows    [][]interface{}
    Footer  []string
}

type TableOptions struct {
    Format      OutputFormat  // table, csv, json, markdown
    MaxWidth    int
    Pagination  *PaginationOptions
    Sorting     *SortOptions
    Filtering   *FilterOptions
}

// Fluent API
func (tb *TableBuilder) WithPagination(pageSize int) *TableBuilder
func (tb *TableBuilder) WithSorting(column string, desc bool) *TableBuilder
func (tb *TableBuilder) WithTheme(theme Theme) *TableBuilder
func (tb *TableBuilder) Render() error
```

#### C. Renderer Backends (`display/renderers/`)

Multiple rendering implementations:

1. **PTerm Renderer** (`pterm_renderer.go`)
   - Modern Go library with rich features
   - Built-in color support and themes
   - Progress bars, spinners, interactive prompts
   - Cross-platform compatibility

2. **Go-Pretty Renderer** (`gopretty_renderer.go`)
   - Excellent table rendering capabilities
   - Multiple output formats (ASCII, CSV, HTML, Markdown)
   - High performance for large datasets

3. **Fallback Renderer** (`fallback_renderer.go`)
   - Simple text-based rendering
   - No external dependencies
   - Works in any environment

#### D. Theme System (`display/themes/`)

Configurable visual themes:

```go
type Theme struct {
    Name        string
    Colors      ColorScheme
    TableStyle  TableStyle
    Borders     BorderStyle
    Icons       IconSet
}

type ColorScheme struct {
    Primary     Color
    Secondary   Color
    Success     Color
    Warning     Color
    Error       Color
    Info        Color
    Muted       Color
}

// Predefined themes
var (
    DefaultTheme = Theme{...}
    DarkTheme    = Theme{...}
    LightTheme   = Theme{...}
    MinimalTheme = Theme{...}
    NoColorTheme = Theme{...}
)
```

#### E. Output Adapters (`display/adapters/`)

Format-specific output handling:

```go
type OutputAdapter interface {
    SupportsColor() bool
    SupportsUnicode() bool
    MaxWidth() int
    WriteTable(table TableData, options TableOptions) error
    WriteMessage(level MessageLevel, message string) error
}

// Implementations
type TerminalAdapter struct {...}  // Rich terminal output
type PlainTextAdapter struct {...} // Plain text for pipes/files
type JSONAdapter struct {...}      // Structured JSON output
type CSVAdapter struct {...}       // CSV format
```

### 4. Progressive Enhancement Strategy

The system automatically detects terminal capabilities and adjusts output:

```go
type TerminalCapabilities struct {
    SupportsColor   bool
    SupportsUnicode bool
    Width          int
    Height         int
    IsInteractive  bool
    IsPiped        bool
}

func DetectCapabilities() TerminalCapabilities {
    return TerminalCapabilities{
        SupportsColor:   !isCI() && term.IsTerminal(int(os.Stdout.Fd())),
        SupportsUnicode: detectUnicodeSupport(),
        Width:          getTerminalWidth(),
        Height:         getTerminalHeight(),
        IsInteractive:  term.IsTerminal(int(os.Stdin.Fd())),
        IsPiped:        isPiped(),
    }
}
```

### 5. Configuration System

User-configurable display preferences:

```yaml
# ~/.icebox/display.yaml
display:
  theme: "dark"
  format: "table"
  table:
    max_width: 120
    pagination: 50
    unicode_borders: true
  colors:
    enabled: auto  # auto, always, never
  interactive:
    confirm_destructive: true
```

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1-2)

1. **Create display package structure**

   ```
   icebox/display/
   ├── display.go           # Main interface
   ├── capabilities.go      # Terminal detection
   ├── config.go           # Configuration
   ├── table/
   │   ├── builder.go      # Table builder
   │   ├── data.go         # Data structures
   │   └── options.go      # Configuration options
   ├── themes/
   │   ├── theme.go        # Theme interface
   │   ├── default.go      # Default themes
   │   └── loader.go       # Theme loading
   ├── renderers/
   │   ├── interface.go    # Renderer interface
   │   ├── pterm.go        # PTerm implementation
   │   ├── gopretty.go     # Go-Pretty implementation
   │   └── fallback.go     # Simple fallback
   └── adapters/
       ├── terminal.go     # Terminal adapter
       ├── json.go         # JSON adapter
       └── csv.go          # CSV adapter
   ```

2. **Implement core interfaces and basic functionality**

3. **Add PTerm renderer as primary backend**

### Phase 2: CLI Command Migration (Week 3-4)

#### 2.1 Primary CLI Commands (High Priority)

**SQL Command (`cli/sql.go`)** - 76 print statements

- Query result tables (manual Unicode box drawing)
- Engine metrics display
- Table registration status messages
- Query timing and row count information
- Schema display for queries
- CSV and JSON output formats

**Import Command (`cli/import.go`)** - 25 print statements  

- Schema inference display
- File statistics
- Dry run operation preview
- Import progress and results
- Next steps guidance

**Table Management (`cli/table.go`)** - Table listing and management

- Table listing with namespace/table columns
- Table creation/deletion confirmations
- Table metadata display

**Demo Command (`cli/demo.go`)** - 47 print statements

- Dataset listing and descriptions
- Setup progress and results
- Sample query suggestions
- Cleanup operations

#### 2.2 Secondary CLI Commands (Medium Priority)

**Initialization (`cli/init.go`)** - 8 print statements

- Project creation confirmations
- Configuration summary
- Next steps guidance

**UI Command (`cli/ui.go`)** - 15 print statements

- Web UI startup messages
- Server status and URLs
- Shutdown notifications

**Serve Command (`cli/serve.go`)** - 6 print statements

- API server startup messages
- Configuration display
- Server status

**Pack/Unpack (`cli/pack.go`)** - 12 print statements

- Archive creation/extraction progress
- File statistics and verification

#### 2.3 Specialized Commands (Lower Priority)

**Shell Command (`cli/shell.go`)** - Interactive shell status
**Time Travel (`cli/timetravel.go`)** - Query execution display
**Catalog Management (`cli/catalog_mgmt.go`)** - Namespace operations

### Phase 3: Engine and System Component Integration (Week 5-6)

#### 3.1 DuckDB Engine (`engine/duckdb/engine.go`)

**Logger Output Integration** - 25 logger statements

- Engine initialization messages
- Extension loading status
- Query execution logging
- Performance metrics
- Warning and error messages

**Migration Strategy:**

```go
// Current: Direct logger output
e.logger.Printf("DuckDB engine initialized successfully with catalog: %s", e.catalog.Name())

// Proposed: Contextual display integration
if display := getDisplayFromContext(ctx); display != nil {
    display.Success("DuckDB engine initialized with catalog: %s", e.catalog.Name())
} else {
    e.logger.Printf("DuckDB engine initialized successfully with catalog: %s", e.catalog.Name())
}
```

#### 3.2 MinIO Filesystem (`fs/minio/minio.go`)

**Logger Output Integration** - 20 logger statements

- Server startup/shutdown messages
- Health check status
- Bucket creation notifications
- Request logging

**Migration Strategy:**

- Add optional display context to MinIO operations
- Maintain logger fallback for non-CLI usage
- Provide structured status updates for CLI operations

#### 3.3 JSON Catalog (`catalog/json/catalog.go`)

**Logger Output Integration** - 25 logger statements

- Catalog initialization
- Namespace/table operations
- Transaction logging
- Metadata operations

**Migration Strategy:**

- Add display context to catalog operations
- Provide progress feedback for long-running operations
- Maintain detailed logging for debugging

### Phase 4: Advanced Features and Output Format Support (Week 7-8)

#### 4.1 Structured Logging Integration

**Context-Aware Display:**

```go
type DisplayContext struct {
    Display  Display
    Logger   *log.Logger
    Verbose  bool
    Format   OutputFormat
}

func WithDisplay(ctx context.Context, display Display) context.Context {
    return context.WithValue(ctx, displayContextKey, &DisplayContext{
        Display: display,
        Logger:  log.Default(),
    })
}
```

#### 4.2 Progressive Enhancement

**Capability-Based Output:**

- Rich terminal: Full PTerm features with colors and Unicode
- Basic terminal: Simple text with basic formatting
- Piped output: Structured data (JSON/CSV) without decorations
- CI environment: Plain text with minimal formatting

#### 4.3 Configuration Integration

**Display Configuration:**

```yaml
# ~/.icebox/config.yaml
display:
  theme: "dark"
  format: "table"
  verbose: false
  timing: true
  colors: auto
  unicode: auto
```

### Phase 5: Testing and Documentation (Week 9-10)

#### 5.1 Comprehensive Testing Strategy

**Unit Tests:**

- Display component isolation
- Renderer backend testing
- Theme and configuration validation
- Terminal capability detection

**Integration Tests:**

- End-to-end CLI command testing
- Cross-platform compatibility
- Output format validation
- Performance benchmarking

**Compatibility Tests:**

- Different terminal emulators
- CI/CD environments
- Windows/Linux/macOS platforms
- Various shell environments

#### 5.2 Migration Validation

**Before/After Comparison:**

- Output format consistency
- Performance impact measurement
- Feature parity verification
- User experience validation

### Migration Dependencies and Considerations

#### External Dependencies

- **PTerm**: Primary rich terminal library
- **Go-Pretty**: Table rendering and CSV/HTML output
- **golang.org/x/term**: Terminal capability detection

#### Backward Compatibility

- Maintain existing `--format` flags
- Preserve JSON/CSV output formats
- Support existing configuration options
- Gradual migration with feature flags

#### Risk Mitigation

- Fallback renderer for minimal environments
- Comprehensive testing across platforms
- Phased rollout with user feedback
- Rollback capability for each phase

This comprehensive migration plan addresses all identified output dependencies while ensuring a smooth transition to the modern display architecture.

## Benefits

### For Users

1. **Consistent Experience**: Unified look and feel across all commands
2. **Customizable**: Themes and configuration options
3. **Accessible**: Graceful degradation for different environments
4. **Interactive**: Rich prompts and confirmations
5. **Performant**: Optimized rendering for large datasets

### For Developers

1. **Maintainable**: Centralized display logic
2. **Extensible**: Easy to add new output formats
3. **Testable**: Mockable interfaces for testing
4. **Reusable**: Common display patterns across commands
5. **Modern**: Following Go CLI best practices

## Library Recommendations

Based on research, the recommended libraries are:

### Primary: PTerm

- **Pros**: Modern, feature-rich, excellent documentation, active development
- **Cons**: Larger dependency footprint
- **Use Case**: Primary renderer for rich terminal output

### Secondary: Go-Pretty

- **Pros**: Excellent table rendering, multiple output formats, good performance
- **Cons**: Less interactive features
- **Use Case**: Table-focused rendering, CSV/HTML output

### Fallback: Standard Library

- **Pros**: No dependencies, always available
- **Cons**: Limited features
- **Use Case**: CI environments, minimal installations

## Migration Strategy

### Backward Compatibility

1. **Gradual Migration**: Migrate commands one by one
2. **Feature Flags**: Allow users to opt into new display system
3. **Fallback Support**: Maintain old display code during transition
4. **Configuration**: Respect existing format flags (`--format`, `--output`)

### Risk Mitigation

1. **Comprehensive Testing**: Test across different terminal types
2. **User Feedback**: Beta testing with power users
3. **Rollback Plan**: Ability to revert to old display system
4. **Documentation**: Clear migration guides

## Conclusion

This modern CLI display architecture will significantly improve the user experience while making the codebase more maintainable and extensible. The modular design allows for gradual adoption and provides a solid foundation for future enhancements.

The investment in this architecture will pay dividends in:

- Reduced maintenance burden
- Improved user satisfaction
- Easier feature development
- Better testing capabilities
- Professional appearance

## Next Steps

1. **Review and Approval**: Stakeholder review of this design
2. **Proof of Concept**: Implement basic display system with one command
3. **Full Implementation**: Follow the phased implementation plan
4. **User Testing**: Beta testing with select users
5. **Documentation**: Update all relevant documentation

---

*This document serves as the architectural blueprint for modernizing Icebox's CLI display system. It should be reviewed and updated as implementation progresses.*
