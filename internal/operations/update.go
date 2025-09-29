package operations

import (
	"fmt"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// Update modifies existing rows based on WHERE condition
func (ops *CSVOperations) Update(updateVals, whereCond string) error {
	if updateVals == "" {
		return fmt.Errorf("UPDATE values cannot be empty")
	}

	if whereCond == "" {
		return fmt.Errorf("UPDATE requires WHERE condition to prevent accidental mass updates")
	}

	// Parse the update values
	updates, err := ops.ParseUpdateValues(updateVals)
	if err != nil {
		return fmt.Errorf("failed to parse UPDATE values: %v", err)
	}

	// Validate update columns
	updateColumns := make([]string, 0, len(updates))
	for column := range updates {
		updateColumns = append(updateColumns, column)
	}
	if err := ops.ValidateColumns(updateColumns); err != nil {
		return fmt.Errorf("UPDATE validation failed: %v", err)
	}

	// Apply WHERE condition to find rows to update
	df := ops.DataFrame
	filteredDF, err := ops.ApplyWhereCondition(df, whereCond)
	if err != nil {
		return fmt.Errorf("WHERE condition error: %v", err)
	}

	if filteredDF.Nrow() == 0 {
		fmt.Println("No rows match the WHERE condition. No updates performed.")
		return nil
	}

	// Perform the update
	updatedDF, rowsAffected, err := ops.PerformUpdate(df, filteredDF, updates, whereCond)
	if err != nil {
		return fmt.Errorf("failed to perform update: %v", err)
	}

	// Save back to file
	if err := ops.SaveDataFrameToCSV(updatedDF, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save updated CSV: %v", err)
	}

	fmt.Printf("Successfully updated %d rows in %s\n", rowsAffected, ops.FilePath)
	return nil
}

// ParseUpdateValues parses UPDATE values in format "col1=val1,col2=val2"
func (ops *CSVOperations) ParseUpdateValues(updateVals string) (map[string]string, error) {
	updates := make(map[string]string)
	
	// Split by comma to get individual column assignments
	assignments := strings.Split(updateVals, ",")
	
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
		
		updates[column] = value
	}
	
	return updates, nil
}

// PerformUpdate executes the actual update operation
func (ops *CSVOperations) PerformUpdate(originalDF, filteredDF dataframe.DataFrame, updates map[string]string, whereCond string) (dataframe.DataFrame, int, error) {
	rowsAffected := 0
	
	// Create a copy of the original dataframe for modification
	updatedDF := originalDF.Copy()
	
	// Get indices of rows that match the WHERE condition
	matchingIndices := ops.GetMatchingRowIndices(originalDF, whereCond)
	
	// Update each matching row
	for _, rowIndex := range matchingIndices {
		// Update each specified column in this row
		for column, newValue := range updates {
			// Find column index
			columnIndex := -1
			for i, colName := range ops.Headers {
				if colName == column {
					columnIndex = i
					break
				}
			}
			
			if columnIndex >= 0 {
				// Update the value in the dataframe
				updatedDF = ops.UpdateCellValue(updatedDF, rowIndex, columnIndex, newValue)
				rowsAffected++
			}
		}
	}
	
	// Adjust rowsAffected to count unique rows, not individual cell updates
	rowsAffected = len(matchingIndices)
	
	return updatedDF, rowsAffected, nil
}

// GetMatchingRowIndices returns indices of rows that match the WHERE condition
func (ops *CSVOperations) GetMatchingRowIndices(df dataframe.DataFrame, whereCond string) []int {
	filteredDF, err := ops.ApplyWhereCondition(df, whereCond)
	if err != nil {
		return []int{}
	}
	
	var indices []int
	
	// This is a simplified approach - in a production system you'd want 
	// more efficient indexing
	for i := 0; i < df.Nrow(); i++ {
		// Check if this row exists in the filtered dataframe
		if ops.RowExistsInFiltered(df, filteredDF, i) {
			indices = append(indices, i)
		}
	}
	
	return indices
}

// RowExistsInFiltered checks if a row from original DF exists in filtered DF
func (ops *CSVOperations) RowExistsInFiltered(originalDF, filteredDF dataframe.DataFrame, rowIndex int) bool {
	if rowIndex >= originalDF.Nrow() {
		return false
	}
	
	// Create signature of the row to match
	originalRow := make([]string, originalDF.Ncol())
	for j := 0; j < originalDF.Ncol(); j++ {
		originalRow[j] = fmt.Sprintf("%v", originalDF.Elem(rowIndex, j))
	}
	
	// Check if this row signature exists in filtered dataframe
	for i := 0; i < filteredDF.Nrow(); i++ {
		match := true
		for j := 0; j < filteredDF.Ncol(); j++ {
			if fmt.Sprintf("%v", filteredDF.Elem(i, j)) != originalRow[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	
	return false
}

// UpdateCellValue updates a specific cell in the dataframe
func (ops *CSVOperations) UpdateCellValue(df dataframe.DataFrame, rowIndex, colIndex int, newValue string) dataframe.DataFrame {
	// This is a workaround since gota doesn't provide direct cell update
	// We'll rebuild the dataframe with the updated value
	
	// Extract all data
	allData := make([][]string, df.Ncol())
	for j := 0; j < df.Ncol(); j++ {
		columnData := make([]string, df.Nrow())
		for i := 0; i < df.Nrow(); i++ {
			if i == rowIndex && j == colIndex {
				columnData[i] = newValue
			} else {
				columnData[i] = fmt.Sprintf("%v", df.Elem(i, j))
			}
		}
		allData[j] = columnData
	}
	
	// Rebuild dataframe
	seriesList := make([]series.Series, len(allData))
	for j, data := range allData {
		seriesList[j] = series.New(data, series.String, ops.Headers[j])
	}
	
	return dataframe.New(seriesList...)
}

// UpdateWhere performs UPDATE with complex WHERE conditions (future enhancement)
func (ops *CSVOperations) UpdateWhere(updates map[string]string, whereConditions []string) error {
	// This could support multiple WHERE conditions with AND/OR logic
	// For now, we use the simpler single-condition approach
	return fmt.Errorf("complex WHERE conditions not yet implemented")
}

// BulkUpdate performs multiple updates in a single operation (future enhancement)
func (ops *CSVOperations) BulkUpdate(bulkUpdates []struct {
	Updates   map[string]string
	Condition string
}) error {
	totalRowsAffected := 0
	df := ops.DataFrame
	
	for i, update := range bulkUpdates {
		updatedDF, rowsAffected, err := ops.PerformUpdate(df, dataframe.DataFrame{}, update.Updates, update.Condition)
		if err != nil {
			return fmt.Errorf("bulk update %d failed: %v", i+1, err)
		}
		df = updatedDF
		totalRowsAffected += rowsAffected
	}
	
	// Save final result
	if err := ops.SaveDataFrameToCSV(df, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save bulk updated CSV: %v", err)
	}
	
	fmt.Printf("Successfully performed bulk update affecting %d total rows in %s\n", totalRowsAffected, ops.FilePath)
	return nil
}