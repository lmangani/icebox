package display

// TableData represents the data for a table
type TableData struct {
	Headers []string
	Rows    [][]interface{}
	Footer  []string
}

// TableOptions represents configuration options for table rendering
type TableOptions struct {
	Format     OutputFormat
	MaxWidth   int
	Pagination *PaginationOptions
	Sorting    *SortOptions
	Theme      *Theme
}

// PaginationOptions represents pagination configuration
type PaginationOptions struct {
	PageSize int
	ShowAll  bool
}

// SortOptions represents sorting configuration
type SortOptions struct {
	Column     string
	Descending bool
}

// DefaultTableOptions returns default table options
func DefaultTableOptions() TableOptions {
	return TableOptions{
		Format:   FormatTable,
		MaxWidth: 120,
	}
}

// TableBuilder for building tables with fluent API
type TableBuilder struct {
	display *DisplayImpl
	data    TableData
	options TableOptions
}

// WithFormat sets the output format
func (tb *TableBuilder) WithFormat(format OutputFormat) *TableBuilder {
	tb.options.Format = format
	return tb
}

// WithPagination sets pagination options
func (tb *TableBuilder) WithPagination(pageSize int) *TableBuilder {
	tb.options.Pagination = &PaginationOptions{
		PageSize: pageSize,
		ShowAll:  false,
	}
	return tb
}

// WithTheme sets the theme
func (tb *TableBuilder) WithTheme(theme Theme) *TableBuilder {
	tb.options.Theme = &theme
	return tb
}

// Render renders the table
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
