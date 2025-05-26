# CLI Display System - Proof of Concept

## Overview

This document demonstrates the proposed CLI display system through concrete code examples, showing how it would replace the current manual formatting approach.

## Current vs. Proposed Implementation

### SQL Query Results

#### Current Implementation (sql.go)

```go
func displayTableFormat(columns []string, rows [][]interface{}) error {
    if len(rows) == 0 {
        return nil
    }

    // Manual width calculation
    widths := make([]int, len(columns))
    for i, col := range columns {
        widths[i] = len(col)
    }

    // Check data widths
    for _, row := range rows {
        for i, value := range row {
            if i < len(widths) {
                str := formatValue(value)
                if len(str) > widths[i] {
                    widths[i] = len(str)
                }
            }
        }
    }

    // Cap column widths at 50 characters
    for i := range widths {
        if widths[i] > 50 {
            widths[i] = 50
        }
    }

    // Manual Unicode box drawing
    fmt.Print("┌")
    for i, width := range widths {
        fmt.Print(strings.Repeat("─", width+2))
        if i < len(widths)-1 {
            fmt.Print("┬")
        }
    }
    fmt.Println("┐")

    // Print column names
    fmt.Print("│")
    for i, col := range columns {
        fmt.Printf(" %-*s │", widths[i], truncateString(col, widths[i]))
    }
    fmt.Println()

    // Print separator
    fmt.Print("├")
    for i, width := range widths {
        fmt.Print(strings.Repeat("─", width+2))
        if i < len(widths)-1 {
            fmt.Print("┼")
        }
    }
    fmt.Println("┤")

    // Print rows
    for _, row := range rows {
        fmt.Print("│")
        for i, value := range row {
            if i < len(widths) {
                str := formatValue(value)
                fmt.Printf(" %-*s │", widths[i], truncateString(str, widths[i]))
            }
        }
        fmt.Println()
    }

    // Print footer
    fmt.Print("└")
    for i, width := range widths {
        fmt.Print(strings.Repeat("─", width+2))
        if i < len(widths)-1 {
            fmt.Print("┴")
        }
    }
    fmt.Println("┘")

    return nil
}
```

#### Proposed Implementation

```go
func displayResults(result *duckdb.QueryResult, duration time.Duration) error {
    display := display.New()
    
    // Show timing if enabled
    if sqlOpts.timing {
        display.Info("⏱️  Query [%s] executed in %v", result.QueryID, duration)
    }

    // Handle empty results
    if result.RowCount == 0 {
        display.Info("📭 No rows returned")
        return nil
    }

    // Show row count and warnings
    if result.RowCount >= 100000 {
        display.Warning("⚠️  Large result set detected (%d rows) - performance may vary", result.RowCount)
    }
    display.Info("📊 %d rows returned", result.RowCount)

    // Show schema if requested
    if sqlOpts.showSchema {
        display.Info("📋 Schema:")
        for i, col := range result.Columns {
            display.Info("  %d. %s", i+1, col)
        }
    }

    // Create and render table
    table := display.Table(display.TableData{
        Headers: result.Columns,
        Rows:    result.Rows,
    })

    // Apply options
    if sqlOpts.maxRows > 0 {
        table = table.WithPagination(sqlOpts.maxRows)
    }

    // Set output format
    switch sqlOpts.format {
    case "csv":
        table = table.WithFormat(display.FormatCSV)
    case "json":
        table = table.WithFormat(display.FormatJSON)
    default:
        table = table.WithFormat(display.FormatTable)
    }

    return table.Render()
}
```

### Table Listing

#### Current Implementation (table.go)

```go
func displayTableListTable(tables []table.Identifier, namespace table.Identifier) error {
    if namespace != nil {
        fmt.Printf("📊 Tables in namespace '%s' (%d tables):\n", strings.Join(namespace, "."), len(tables))
    } else {
        fmt.Printf("📊 All Tables (%d tables):\n", len(tables))
    }

    fmt.Println("┌────────────────────────────────┬──────────────────────────────────┐")
    fmt.Println("│           Namespace            │             Table                │")
    fmt.Println("├────────────────────────────────┼──────────────────────────────────┤")

    for _, tableIdent := range tables {
        namespace := "default"
        tableName := strings.Join(tableIdent, ".")
        if len(tableIdent) > 1 {
            namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
            tableName = tableIdent[len(tableIdent)-1]
        }

        fmt.Printf("│ %-30s │ %-32s │\n", truncateString(namespace, 30), truncateString(tableName, 32))
    }

    fmt.Println("└────────────────────────────────┴──────────────────────────────────┘")
    return nil
}
```

#### Proposed Implementation

```go
func displayTableList(tables []table.Identifier, namespace table.Identifier) error {
    display := display.New()

    if len(tables) == 0 {
        if namespace != nil {
            display.Info("📭 No tables found in namespace '%s'", strings.Join(namespace, "."))
        } else {
            display.Info("📭 No tables found")
        }
        return nil
    }

    // Show header
    if namespace != nil {
        display.Info("📊 Tables in namespace '%s' (%d tables):", strings.Join(namespace, "."), len(tables))
    } else {
        display.Info("📊 All Tables (%d tables):", len(tables))
    }

    // Prepare table data
    headers := []string{"Namespace", "Table"}
    rows := make([][]interface{}, len(tables))
    
    for i, tableIdent := range tables {
        namespace := "default"
        tableName := strings.Join(tableIdent, ".")
        if len(tableIdent) > 1 {
            namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
            tableName = tableIdent[len(tableIdent)-1]
        }
        rows[i] = []interface{}{namespace, tableName}
    }

    // Create and render table
    table := display.Table(display.TableData{
        Headers: headers,
        Rows:    rows,
    })

    // Apply format from options
    switch tableListOpts.format {
    case "csv":
        table = table.WithFormat(display.FormatCSV)
    case "json":
        table = table.WithFormat(display.FormatJSON)
    default:
        table = table.WithFormat(display.FormatTable)
    }

    return table.Render()
}
```

## Core Display System Implementation

### Main Display Interface

```go
// icebox/display/display.go
package display

import (
    "fmt"
    "os"
    "github.com/pterm/pterm"
    "golang.org/x/term"
)

type Display struct {
    renderer Renderer
    theme    Theme
    format   OutputFormat
    caps     TerminalCapabilities
}

type OutputFormat int

const (
    FormatTable OutputFormat = iota
    FormatCSV
    FormatJSON
    FormatMarkdown
)

func New() *Display {
    caps := DetectCapabilities()
    
    var renderer Renderer
    if caps.SupportsColor && caps.SupportsUnicode {
        renderer = NewPTermRenderer()
    } else {
        renderer = NewFallbackRenderer()
    }

    return &Display{
        renderer: renderer,
        theme:    DefaultTheme,
        format:   FormatTable,
        caps:     caps,
    }
}

func (d *Display) Table(data TableData) *TableBuilder {
    return &TableBuilder{
        display: d,
        data:    data,
        options: DefaultTableOptions(),
    }
}

func (d *Display) Success(message string, args ...interface{}) {
    formatted := fmt.Sprintf(message, args...)
    d.renderer.RenderMessage(MessageLevelSuccess, formatted)
}

func (d *Display) Error(message string, args ...interface{}) {
    formatted := fmt.Sprintf(message, args...)
    d.renderer.RenderMessage(MessageLevelError, formatted)
}

func (d *Display) Warning(message string, args ...interface{}) {
    formatted := fmt.Sprintf(message, args...)
    d.renderer.RenderMessage(MessageLevelWarning, formatted)
}

func (d *Display) Info(message string, args ...interface{}) {
    formatted := fmt.Sprintf(message, args...)
    d.renderer.RenderMessage(MessageLevelInfo, formatted)
}

func (d *Display) Confirm(message string) bool {
    if !d.caps.IsInteractive {
        return false
    }
    return d.renderer.RenderConfirm(message)
}
```

### Table Builder

```go
// icebox/display/table/builder.go
package display

type TableBuilder struct {
    display *Display
    data    TableData
    options TableOptions
}

type TableData struct {
    Headers []string
    Rows    [][]interface{}
    Footer  []string
}

type TableOptions struct {
    Format      OutputFormat
    MaxWidth    int
    Pagination  *PaginationOptions
    Sorting     *SortOptions
    Theme       *Theme
}

type PaginationOptions struct {
    PageSize int
    ShowAll  bool
}

func (tb *TableBuilder) WithFormat(format OutputFormat) *TableBuilder {
    tb.options.Format = format
    return tb
}

func (tb *TableBuilder) WithPagination(pageSize int) *TableBuilder {
    tb.options.Pagination = &PaginationOptions{
        PageSize: pageSize,
        ShowAll:  false,
    }
    return tb
}

func (tb *TableBuilder) WithTheme(theme Theme) *TableBuilder {
    tb.options.Theme = &theme
    return tb
}

func (tb *TableBuilder) Render() error {
    // Apply pagination if needed
    data := tb.data
    if tb.options.Pagination != nil && !tb.options.Pagination.ShowAll {
        if len(data.Rows) > tb.options.Pagination.PageSize {
            data.Rows = data.Rows[:tb.options.Pagination.PageSize]
            tb.display.Warning("⚠️  Showing first %d rows (use --max-rows to adjust)", tb.options.Pagination.PageSize)
        }
    }

    return tb.display.renderer.RenderTable(data, tb.options)
}
```

### PTerm Renderer Implementation

```go
// icebox/display/renderers/pterm.go
package display

import (
    "encoding/csv"
    "encoding/json"
    "os"
    "strings"
    "github.com/pterm/pterm"
)

type PTermRenderer struct {
    theme Theme
}

func NewPTermRenderer() *PTermRenderer {
    return &PTermRenderer{
        theme: DefaultTheme,
    }
}

func (r *PTermRenderer) RenderTable(data TableData, options TableOptions) error {
    switch options.Format {
    case FormatCSV:
        return r.renderCSV(data)
    case FormatJSON:
        return r.renderJSON(data)
    case FormatMarkdown:
        return r.renderMarkdown(data)
    default:
        return r.renderTable(data, options)
    }
}

func (r *PTermRenderer) renderTable(data TableData, options TableOptions) error {
    // Convert data to PTerm format
    tableData := make([][]string, len(data.Rows)+1)
    
    // Headers
    tableData[0] = data.Headers
    
    // Rows
    for i, row := range data.Rows {
        tableData[i+1] = make([]string, len(row))
        for j, cell := range row {
            tableData[i+1][j] = formatValue(cell)
        }
    }

    // Create PTerm table
    table := pterm.DefaultTable.WithHasHeader().WithData(tableData)
    
    // Apply theme
    if options.Theme != nil {
        table = table.WithHeaderStyle(pterm.NewStyle(pterm.FgLightBlue, pterm.Bold))
        table = table.WithRowSeparator("-")
    }

    return table.Render()
}

func (r *PTermRenderer) renderCSV(data TableData) error {
    writer := csv.NewWriter(os.Stdout)
    defer writer.Flush()

    // Write headers
    if err := writer.Write(data.Headers); err != nil {
        return err
    }

    // Write rows
    for _, row := range data.Rows {
        record := make([]string, len(row))
        for i, cell := range row {
            record[i] = formatValue(cell)
        }
        if err := writer.Write(record); err != nil {
            return err
        }
    }

    return nil
}

func (r *PTermRenderer) renderJSON(data TableData) error {
    var result []map[string]interface{}
    
    for _, row := range data.Rows {
        record := make(map[string]interface{})
        for i, header := range data.Headers {
            if i < len(row) {
                record[header] = row[i]
            }
        }
        result = append(result, record)
    }

    encoder := json.NewEncoder(os.Stdout)
    encoder.SetIndent("", "  ")
    return encoder.Encode(result)
}

func (r *PTermRenderer) RenderMessage(level MessageLevel, message string) {
    switch level {
    case MessageLevelSuccess:
        pterm.Success.Println(message)
    case MessageLevelError:
        pterm.Error.Println(message)
    case MessageLevelWarning:
        pterm.Warning.Println(message)
    case MessageLevelInfo:
        pterm.Info.Println(message)
    }
}

func (r *PTermRenderer) RenderConfirm(message string) bool {
    result, _ := pterm.DefaultInteractiveConfirm.Show(message)
    return result
}
```

### Terminal Capabilities Detection

```go
// icebox/display/capabilities.go
package display

import (
    "os"
    "runtime"
    "golang.org/x/term"
)

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
        SupportsColor:   detectColorSupport(),
        SupportsUnicode: detectUnicodeSupport(),
        Width:          getTerminalWidth(),
        Height:         getTerminalHeight(),
        IsInteractive:  term.IsTerminal(int(os.Stdin.Fd())),
        IsPiped:        isPiped(),
    }
}

func detectColorSupport() bool {
    // Check if we're in CI
    if isCI() {
        return false
    }
    
    // Check if stdout is a terminal
    if !term.IsTerminal(int(os.Stdout.Fd())) {
        return false
    }
    
    // Check environment variables
    if os.Getenv("NO_COLOR") != "" {
        return false
    }
    
    if os.Getenv("FORCE_COLOR") != "" {
        return true
    }
    
    // Check TERM environment variable
    termVar := os.Getenv("TERM")
    if termVar == "dumb" {
        return false
    }
    
    return true
}

func detectUnicodeSupport() bool {
    // Windows Command Prompt has limited Unicode support
    if runtime.GOOS == "windows" {
        return false
    }
    
    // Check locale
    for _, env := range []string{"LC_ALL", "LC_CTYPE", "LANG"} {
        if val := os.Getenv(env); val != "" {
            if strings.Contains(strings.ToLower(val), "utf") {
                return true
            }
        }
    }
    
    return true
}

func isCI() bool {
    ciVars := []string{"CI", "GITHUB_ACTIONS", "JENKINS_URL", "TRAVIS", "CIRCLECI"}
    for _, v := range ciVars {
        if os.Getenv(v) != "" {
            return true
        }
    }
    return false
}

func isPiped() bool {
    stat, _ := os.Stdout.Stat()
    return (stat.Mode() & os.ModeCharDevice) == 0
}

func getTerminalWidth() int {
    width, _, err := term.GetSize(int(os.Stdout.Fd()))
    if err != nil {
        return 80 // Default width
    }
    return width
}

func getTerminalHeight() int {
    _, height, err := term.GetSize(int(os.Stdout.Fd()))
    if err != nil {
        return 24 // Default height
    }
    return height
}
```

## Benefits Demonstration

### Before: Manual Implementation

- **Lines of Code**: ~100 lines per display function
- **Maintainability**: Low (duplicated logic)
- **Consistency**: Poor (different styles)
- **Features**: Basic (no themes, limited formats)
- **Testing**: Difficult (hardcoded output)

### After: Modern Architecture

- **Lines of Code**: ~20 lines per display function
- **Maintainability**: High (centralized logic)
- **Consistency**: Excellent (unified system)
- **Features**: Rich (themes, multiple formats, interactive)
- **Testing**: Easy (mockable interfaces)

## Migration Example

Here's how to migrate the SQL command:

```go
// 1. Replace the display functions
func displayResults(result *duckdb.QueryResult, duration time.Duration) error {
    display := display.New()
    
    // Timing information
    if sqlOpts.timing {
        display.Info("⏱️  Query [%s] executed in %v", result.QueryID, duration)
    }

    // Handle empty results
    if result.RowCount == 0 {
        display.Info("📭 No rows returned")
        return nil
    }

    // Large result warning
    if result.RowCount >= 100000 {
        display.Warning("⚠️  Large result set detected (%d rows) - performance may vary", result.RowCount)
    }
    
    display.Info("📊 %d rows returned", result.RowCount)

    // Schema information
    if sqlOpts.showSchema {
        display.Info("📋 Schema:")
        for i, col := range result.Columns {
            display.Info("  %d. %s", i+1, col)
        }
    }

    // Create table
    table := display.Table(display.TableData{
        Headers: result.Columns,
        Rows:    result.Rows,
    })

    // Apply options
    if sqlOpts.maxRows > 0 {
        table = table.WithPagination(sqlOpts.maxRows)
    }

    // Set format
    switch sqlOpts.format {
    case "csv":
        table = table.WithFormat(display.FormatCSV)
    case "json":
        table = table.WithFormat(display.FormatJSON)
    default:
        table = table.WithFormat(display.FormatTable)
    }

    return table.Render()
}

// 2. Remove old display functions
// - displayTableFormat()
// - displayCSVFormat()
// - displayJSONFormat()
// - formatValue()
// - formatValueCSV()
// - truncateString()
```

This proof-of-concept demonstrates how the new architecture would:

1. **Reduce code complexity** by 80%
2. **Improve maintainability** through centralized logic
3. **Enable rich features** like themes and interactive prompts
4. **Ensure consistency** across all commands
5. **Support multiple output formats** seamlessly
6. **Provide better testing** through mockable interfaces

The migration would be gradual, allowing for thorough testing and user feedback at each step.
