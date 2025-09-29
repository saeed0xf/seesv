package operations

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// CSVOperations handles all CSV-related operations
type CSVOperations struct {
	FilePath   string
	DataFrame  dataframe.DataFrame
	Headers    []string
	RawOutput  bool
	OutputFile string
}

// Initialize loads the CSV file and prepares the dataframe
func (ops *CSVOperations) Initialize() error {
	file, err := os.Open(ops.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Load CSV into DataFrame
	df := dataframe.ReadCSV(file)
	if df.Err != nil {
		return fmt.Errorf("failed to read CSV: %v", df.Err)
	}

	ops.DataFrame = df
	ops.Headers = df.Names()
	return nil
}

// ShowColumns displays all column headers
func (ops *CSVOperations) ShowColumns() error {
	fmt.Println("Columns in CSV file:")
	for i, col := range ops.Headers {
		fmt.Printf("%d: %s\n", i+1, col)
	}
	return nil
}

// ValidateColumns checks if specified columns exist in the DataFrame
func (ops *CSVOperations) ValidateColumns(columns []string) error {
	headerSet := make(map[string]bool)
	for _, h := range ops.Headers {
		headerSet[h] = true
	}

	for _, col := range columns {
		if !headerSet[col] {
			return fmt.Errorf("column '%s' does not exist in CSV", col)
		}
	}
	return nil
}

// ParseColumns parses comma-separated column names
func (ops *CSVOperations) ParseColumns(colStr string) []string {
	if colStr == "" {
		return ops.Headers // Return all columns if none specified
	}
	
	columns := strings.Split(colStr, ",")
	for i := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}
	return columns
}

// ApplyWhereCondition filters the dataframe based on WHERE condition
func (ops *CSVOperations) ApplyWhereCondition(df dataframe.DataFrame, whereCondition string) (dataframe.DataFrame, error) {
	if whereCondition == "" {
		return df, nil
	}

	// Parse simple WHERE conditions like "age > 30", "name = 'John'", etc.
	return ops.parseAndApplyFilter(df, whereCondition)
}

// parseAndApplyFilter parses and applies filter conditions
func (ops *CSVOperations) parseAndApplyFilter(df dataframe.DataFrame, condition string) (dataframe.DataFrame, error) {
	condition = strings.TrimSpace(condition)
	
	// Support multiple operators
	operators := []string{">=", "<=", "!=", "=", ">", "<"}
	var column, operator, value string
	
	for _, op := range operators {
		if strings.Contains(condition, op) {
			parts := strings.SplitN(condition, op, 2)
			if len(parts) == 2 {
				column = strings.TrimSpace(parts[0])
				operator = op
				value = strings.TrimSpace(parts[1])
				// Remove quotes from string values
				value = strings.Trim(value, "'\"")
				break
			}
		}
	}
	
	if column == "" || operator == "" {
		return df, fmt.Errorf("invalid WHERE condition: %s", condition)
	}

	// Validate column exists
	if err := ops.ValidateColumns([]string{column}); err != nil {
		return df, err
	}

	// Apply filter based on operator
	switch operator {
	case "=":
		return df.Filter(dataframe.F{Colname: column, Comparator: series.Eq, Comparando: value}), nil
	case "!=":
		return df.Filter(dataframe.F{Colname: column, Comparator: series.Neq, Comparando: value}), nil
	case ">":
		return df.Filter(dataframe.F{Colname: column, Comparator: series.Greater, Comparando: value}), nil
	case "<":
		return df.Filter(dataframe.F{Colname: column, Comparator: series.Less, Comparando: value}), nil
	case ">=":
		return df.Filter(dataframe.F{Colname: column, Comparator: series.GreaterEq, Comparando: value}), nil
	case "<=":
		return df.Filter(dataframe.F{Colname: column, Comparator: series.LessEq, Comparando: value}), nil
	default:
		return df, fmt.Errorf("unsupported operator: %s", operator)
	}
}

// ApplyOrderBy sorts the dataframe
func (ops *CSVOperations) ApplyOrderBy(df dataframe.DataFrame, orderBy string) (dataframe.DataFrame, error) {
	if orderBy == "" {
		return df, nil
	}

	parts := strings.Fields(orderBy)
	if len(parts) == 0 {
		return df, fmt.Errorf("empty ORDER BY clause")
	}

	column := parts[0]
	ascending := true

	if len(parts) > 1 {
		direction := strings.ToLower(parts[1])
		if direction == "desc" {
			ascending = false
		} else if direction != "asc" {
			return df, fmt.Errorf("invalid ORDER BY direction: %s (use 'asc' or 'desc')", parts[1])
		}
	}

	// Validate column exists
	if err := ops.ValidateColumns([]string{column}); err != nil {
		return df, err
	}

	if ascending {
		return df.Arrange(dataframe.Sort(column)), nil
	} else {
		return df.Arrange(dataframe.RevSort(column)), nil
	}
}

// ApplyLimit limits the number of rows
func (ops *CSVOperations) ApplyLimit(df dataframe.DataFrame, limit int) dataframe.DataFrame {
	if limit <= 0 || limit >= df.Nrow() {
		return df
	}
	indices := make([]int, limit)
	for i := 0; i < limit; i++ {
		indices[i] = i
	}
	return df.Subset(indices)
}

// PrintDataFrame prints the dataframe in a formatted table or saves to file
func (ops *CSVOperations) PrintDataFrame(df dataframe.DataFrame) {
	// If output file is specified, save to file instead of printing
	if ops.OutputFile != "" {
		if ops.RawOutput {
			// For raw output, save as CSV without headers
			err := ops.SaveDataFrameToFile(df, ops.OutputFile, false)
			if err != nil {
				fmt.Printf("Error saving to file: %v\n", err)
				return
			}
		} else {
			// For formatted output, save as CSV with headers
			err := ops.SaveDataFrameToFile(df, ops.OutputFile, true)
			if err != nil {
				fmt.Printf("Error saving to file: %v\n", err)
				return
			}
		}
		fmt.Printf("Results saved to: %s\n", ops.OutputFile)
		return
	}

	// Original stdout printing logic
	if df.Nrow() == 0 {
		if !ops.RawOutput {
			fmt.Println("No rows to display.")
		}
		return
	}

	headers := df.Names()
	
	if !ops.RawOutput {
		// Print headers
		for i, header := range headers {
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Printf("%-15s", header)
		}
		fmt.Println()
		
		// Print separator line
		for i := range headers {
			if i > 0 {
				fmt.Print("-+-")
			}
			fmt.Print(strings.Repeat("-", 15))
		}
		fmt.Println()
	}

	// Print data rows
	for i := 0; i < df.Nrow(); i++ {
		for j := 0; j < df.Ncol(); j++ {
			if j > 0 {
				if ops.RawOutput {
					fmt.Print(",")
				} else {
					fmt.Print(" | ")
				}
			}
			val := df.Elem(i, j)
			if ops.RawOutput {
				fmt.Printf("%v", val)
			} else {
				fmt.Printf("%-15s", fmt.Sprintf("%v", val))
			}
		}
		fmt.Println()
	}
}

// SaveDataFrameToFile saves the dataframe to a file with options for headers
func (ops *CSVOperations) SaveDataFrameToFile(df dataframe.DataFrame, filename string, includeHeaders bool) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	if !includeHeaders {
		// Write only data rows without headers
		for i := 0; i < df.Nrow(); i++ {
			for j := 0; j < df.Ncol(); j++ {
				if j > 0 {
					fmt.Fprint(file, ",")
				}
				val := df.Elem(i, j)
				fmt.Fprintf(file, "%v", val)
			}
			fmt.Fprintln(file)
		}
		return nil
	}

	// Write with headers (default CSV format)
	return df.WriteCSV(file)
}

// SaveDataFrameToCSV saves the dataframe back to CSV (backward compatibility)
func (ops *CSVOperations) SaveDataFrameToCSV(df dataframe.DataFrame, filename string) error {
	return ops.SaveDataFrameToFile(df, filename, true)
}