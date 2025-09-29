package operations

import (
	"fmt"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// Delete removes rows based on WHERE condition
func (ops *CSVOperations) Delete(whereCond string) error {
	if whereCond == "" {
		return fmt.Errorf("DELETE requires WHERE condition to prevent accidental mass deletion")
	}

	// Apply WHERE condition to find rows to delete
	df := ops.DataFrame
	rowsToDelete, err := ops.ApplyWhereCondition(df, whereCond)
	if err != nil {
		return fmt.Errorf("WHERE condition error: %v", err)
	}

	if rowsToDelete.Nrow() == 0 {
		fmt.Println("No rows match the WHERE condition. No deletions performed.")
		return nil
	}

	// Perform the deletion
	remainingDF, rowsDeleted, err := ops.PerformDelete(df, rowsToDelete, whereCond)
	if err != nil {
		return fmt.Errorf("failed to perform delete: %v", err)
	}

	// Save back to file
	if err := ops.SaveDataFrameToCSV(remainingDF, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save updated CSV: %v", err)
	}

	fmt.Printf("Successfully deleted %d rows from %s\n", rowsDeleted, ops.FilePath)
	return nil
}

// PerformDelete executes the actual delete operation
func (ops *CSVOperations) PerformDelete(originalDF, rowsToDelete dataframe.DataFrame, whereCond string) (dataframe.DataFrame, int, error) {
	if rowsToDelete.Nrow() == 0 {
		return originalDF, 0, nil
	}

	// Get indices of rows to keep (opposite of rows to delete)
	indicesToKeep := ops.GetIndicesToKeep(originalDF, whereCond)
	
	if len(indicesToKeep) == 0 {
		// All rows would be deleted, return empty dataframe with same structure
		return ops.CreateEmptyDataFrame(), originalDF.Nrow(), nil
	}

	// Create new dataframe with only the rows to keep
	remainingDF := ops.SubsetByIndices(originalDF, indicesToKeep)
	rowsDeleted := originalDF.Nrow() - remainingDF.Nrow()

	return remainingDF, rowsDeleted, nil
}

// GetIndicesToKeep returns indices of rows that should be kept (not deleted)
func (ops *CSVOperations) GetIndicesToKeep(df dataframe.DataFrame, whereCond string) []int {
	// Get rows that match the WHERE condition (to be deleted)
	rowsToDelete, err := ops.ApplyWhereCondition(df, whereCond)
	if err != nil {
		// If WHERE condition fails, keep all rows
		indices := make([]int, df.Nrow())
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Create a set of row signatures to delete
	deleteSignatures := make(map[string]bool)
	for i := 0; i < rowsToDelete.Nrow(); i++ {
		signature := ops.CreateRowSignature(rowsToDelete, i)
		deleteSignatures[signature] = true
	}

	// Find indices of rows to keep
	var indicesToKeep []int
	for i := 0; i < df.Nrow(); i++ {
		signature := ops.CreateRowSignature(df, i)
		if !deleteSignatures[signature] {
			indicesToKeep = append(indicesToKeep, i)
		}
	}

	return indicesToKeep
}

// CreateRowSignature creates a unique signature for a row
func (ops *CSVOperations) CreateRowSignature(df dataframe.DataFrame, rowIndex int) string {
	var signature strings.Builder
	for j := 0; j < df.Ncol(); j++ {
		if j > 0 {
			signature.WriteString("|")
		}
		signature.WriteString(fmt.Sprintf("%v", df.Elem(rowIndex, j)))
	}
	return signature.String()
}

// SubsetByIndices creates a new dataframe containing only specified row indices
func (ops *CSVOperations) SubsetByIndices(df dataframe.DataFrame, indices []int) dataframe.DataFrame {
	if len(indices) == 0 {
		return ops.CreateEmptyDataFrame()
	}

	// Extract data for specified indices
	allData := make([][]string, df.Ncol())
	for j := 0; j < df.Ncol(); j++ {
		columnData := make([]string, len(indices))
		for i, rowIndex := range indices {
			columnData[i] = fmt.Sprintf("%v", df.Elem(rowIndex, j))
		}
		allData[j] = columnData
	}

	// Create new dataframe
	seriesList := make([]series.Series, len(allData))
	for j, data := range allData {
		seriesList[j] = series.New(data, series.String, ops.Headers[j])
	}

	return dataframe.New(seriesList...)
}

// CreateEmptyDataFrame creates an empty dataframe with the same structure
func (ops *CSVOperations) CreateEmptyDataFrame() dataframe.DataFrame {
	seriesList := make([]series.Series, len(ops.Headers))
	for i, header := range ops.Headers {
		seriesList[i] = series.New([]string{}, series.String, header)
	}
	return dataframe.New(seriesList...)
}

// DeleteAll removes all rows (truncate table equivalent)
func (ops *CSVOperations) DeleteAll() error {
	// Create empty dataframe with same structure
	emptyDF := ops.CreateEmptyDataFrame()
	
	// Save back to file
	if err := ops.SaveDataFrameToCSV(emptyDF, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save truncated CSV: %v", err)
	}

	fmt.Printf("Successfully deleted all rows from %s\n", ops.FilePath)
	return nil
}

// DeleteByRowNumbers deletes rows by their row numbers (future enhancement)
func (ops *CSVOperations) DeleteByRowNumbers(rowNumbers []int) error {
	df := ops.DataFrame
	
	// Validate row numbers
	for _, rowNum := range rowNumbers {
		if rowNum < 1 || rowNum > df.Nrow() {
			return fmt.Errorf("invalid row number: %d (valid range: 1-%d)", rowNum, df.Nrow())
		}
	}

	// Convert to 0-based indices and sort in descending order to avoid index shifting
	indices := make([]int, len(rowNumbers))
	for i, rowNum := range rowNumbers {
		indices[i] = rowNum - 1 // Convert to 0-based
	}

	// Sort indices in descending order
	for i := 0; i < len(indices)-1; i++ {
		for j := i + 1; j < len(indices); j++ {
			if indices[i] < indices[j] {
				indices[i], indices[j] = indices[j], indices[i]
			}
		}
	}

	// Create list of indices to keep
	keepIndices := make([]int, 0, df.Nrow()-len(indices))
	deleteSet := make(map[int]bool)
	for _, idx := range indices {
		deleteSet[idx] = true
	}

	for i := 0; i < df.Nrow(); i++ {
		if !deleteSet[i] {
			keepIndices = append(keepIndices, i)
		}
	}

	// Create new dataframe with remaining rows
	remainingDF := ops.SubsetByIndices(df, keepIndices)

	// Save back to file
	if err := ops.SaveDataFrameToCSV(remainingDF, ops.FilePath); err != nil {
		return fmt.Errorf("failed to save updated CSV: %v", err)
	}

	fmt.Printf("Successfully deleted %d rows from %s\n", len(rowNumbers), ops.FilePath)
	return nil
}

// SafeDelete performs delete with confirmation (future enhancement)
func (ops *CSVOperations) SafeDelete(whereCond string, requireConfirmation bool) error {
	// Preview rows that would be deleted
	rowsToDelete, err := ops.ApplyWhereCondition(ops.DataFrame, whereCond)
	if err != nil {
		return fmt.Errorf("WHERE condition error: %v", err)
	}

	fmt.Printf("The following %d rows would be deleted:\n", rowsToDelete.Nrow())
	// Show max 10 rows for preview
	previewRows := min(10, rowsToDelete.Nrow())
	indices := make([]int, previewRows)
	for i := 0; i < previewRows; i++ {
		indices[i] = i
	}
	ops.PrintDataFrame(rowsToDelete.Subset(indices))

	if rowsToDelete.Nrow() > 10 {
		fmt.Printf("... and %d more rows\n", rowsToDelete.Nrow()-10)
	}

	if requireConfirmation {
		fmt.Print("Continue with deletion? (y/N): ")
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Deletion cancelled.")
			return nil
		}
	}

	// Proceed with actual deletion
	return ops.Delete(whereCond)
}

// Helper function to get minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}