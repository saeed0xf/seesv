package operations

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// Insert adds a new row to the CSV file
func (ops *CSVOperations) Insert(insertVals string) error {
	if insertVals == "" {
		return fmt.Errorf("INSERT values cannot be empty")
	}

	// Parse the insert values
	values, err := ops.ParseInsertValues(insertVals)
	if err != nil {
		return fmt.Errorf("failed to parse INSERT values: %v", err)
	}

	// Validate that all required columns are provided or have defaults
	if err := ops.ValidateInsertValues(values); err != nil {
		return fmt.Errorf("INSERT validation failed: %v", err)
	}

	// Create a new row with proper column ordering
	newRow := ops.CreateInsertRow(values)

	// Add the new row to the dataframe
	newDF := ops.AppendRowToDataFrame(ops.DataFrame, newRow)

	// Save back to the original file
	if err := ops.SaveDataFrameToCSV(newDF, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save updated CSV: %v", err)
	}

	fmt.Printf("Successfully inserted 1 row into %s\n", ops.FilePath)
	return nil
}

// ParseInsertValues parses INSERT values in format "col1=val1,col2=val2"
func (ops *CSVOperations) ParseInsertValues(insertVals string) (map[string]string, error) {
	values := make(map[string]string)
	
	// Split by comma to get individual column assignments
	assignments := strings.Split(insertVals, ",")
	
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		
		// Split by = to get column and value
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid assignment format: %s (expected col=value)", assignment)
		}
		
		column := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		
		// Remove quotes from value if present
		value = strings.Trim(value, "'\"")
		
		values[column] = value
	}
	
	return values, nil
}

// ValidateInsertValues ensures all required columns are provided
func (ops *CSVOperations) ValidateInsertValues(values map[string]string) error {
	// Check if provided columns exist in CSV
	for column := range values {
		if err := ops.ValidateColumns([]string{column}); err != nil {
			return err
		}
	}
	
	// In a more sophisticated implementation, you might check for:
	// - Required columns (non-nullable)
	// - Data type validation
	// - Constraint validation
	// For now, we'll allow partial inserts and fill missing columns with empty values
	
	return nil
}

// CreateInsertRow creates a properly ordered row for insertion
func (ops *CSVOperations) CreateInsertRow(values map[string]string) []string {
	row := make([]string, len(ops.Headers))
	
	for i, header := range ops.Headers {
		if val, exists := values[header]; exists {
			row[i] = val
		} else {
			// Use empty string for missing columns
			row[i] = ""
		}
	}
	
	return row
}

// AppendRowToDataFrame adds a new row to the dataframe
func (ops *CSVOperations) AppendRowToDataFrame(df dataframe.DataFrame, newRow []string) dataframe.DataFrame {
	// Convert row to series
	seriesList := make([]series.Series, len(newRow))
	for i, val := range newRow {
		seriesList[i] = series.New([]string{val}, series.String, ops.Headers[i])
	}
	
	// Create a new dataframe with the single row
	newRowDF := dataframe.New(seriesList...)
	
	// Concatenate with original dataframe
	return df.Concat(newRowDF)
}

// BatchInsert allows inserting multiple rows (for future enhancement)
func (ops *CSVOperations) BatchInsert(rows []map[string]string) error {
	if len(rows) == 0 {
		return fmt.Errorf("no rows to insert")
	}

	df := ops.DataFrame
	
	// Process each row
	for i, values := range rows {
		// Validate values
		if err := ops.ValidateInsertValues(values); err != nil {
			return fmt.Errorf("row %d validation failed: %v", i+1, err)
		}
		
		// Create and append row
		newRow := ops.CreateInsertRow(values)
		df = ops.AppendRowToDataFrame(df, newRow)
	}

	// Save back to file
	if err := ops.SaveDataFrameToCSV(df, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save updated CSV: %v", err)
	}

	fmt.Printf("Successfully inserted %d rows into %s\n", len(rows), ops.FilePath)
	return nil
}

// InsertFromCSV inserts data from another CSV file (for future enhancement)
func (ops *CSVOperations) InsertFromCSV(sourceFile string) error {
	// Open source CSV file
	srcFile, err := os.Open(sourceFile)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer srcFile.Close()

	// Read source dataframe
	srcDF := dataframe.ReadCSV(srcFile)
	if srcDF.Err != nil {
		return fmt.Errorf("failed to read source CSV: %v", srcDF.Err)
	}

	// Validate that source has compatible columns
	srcHeaders := srcDF.Names()
	for _, header := range srcHeaders {
		if err := ops.ValidateColumns([]string{header}); err != nil {
			return fmt.Errorf("incompatible column in source file: %v", err)
		}
	}

	// Concatenate dataframes
	combinedDF := ops.DataFrame.Concat(srcDF)

	// Save back to original file
	if err := ops.SaveDataFrameToCSV(combinedDF, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save updated CSV: %v", err)
	}

	fmt.Printf("Successfully inserted %d rows from %s into %s\n", srcDF.Nrow(), sourceFile, ops.FilePath)
	return nil
}