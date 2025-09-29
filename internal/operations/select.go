package operations

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// AggregateFunction represents supported aggregate functions
type AggregateFunction struct {
	Function string // COUNT, SUM, AVG, MIN, MAX
	Column   string
	Alias    string
}

// Select performs SELECT operations with optional WHERE, ORDER BY, LIMIT
func (ops *CSVOperations) Select(selectCols, whereCond, orderBy string, limit int) error {
	df := ops.DataFrame

	// Check if this is an aggregation query
	aggFuncs, isAggregation := ops.ParseAggregations(selectCols)
	
	if isAggregation {
		return ops.HandleAggregation(aggFuncs, whereCond)
	}

	// Parse columns to select
	columns := ops.ParseColumns(selectCols)
	
	// Validate columns exist
	if err := ops.ValidateColumns(columns); err != nil {
		return err
	}

	// Apply WHERE condition
	filteredDF, err := ops.ApplyWhereCondition(df, whereCond)
	if err != nil {
		return fmt.Errorf("WHERE condition error: %v", err)
	}

	// Select specific columns
	if selectCols != "" {
		filteredDF = filteredDF.Select(columns)
	}

	// Apply DISTINCT if requested (basic implementation)
	if strings.Contains(selectCols, "DISTINCT") || strings.Contains(selectCols, "distinct") {
		filteredDF = ops.ApplyDistinct(filteredDF)
	}

	// Apply ORDER BY
	orderedDF, err := ops.ApplyOrderBy(filteredDF, orderBy)
	if err != nil {
		return fmt.Errorf("ORDER BY error: %v", err)
	}

	// Apply LIMIT
	limitedDF := ops.ApplyLimit(orderedDF, limit)

	// Print results
	ops.PrintDataFrame(limitedDF)
	
	if !ops.RawOutput {
		fmt.Printf("\n(%d rows)\n", limitedDF.Nrow())
	}
	return nil
}

// ParseAggregations parses aggregation functions from SELECT clause
func (ops *CSVOperations) ParseAggregations(selectCols string) ([]AggregateFunction, bool) {
	if selectCols == "" {
		return nil, false
	}

	var aggFuncs []AggregateFunction
	cols := strings.Split(selectCols, ",")
	hasAggregation := false

	for _, col := range cols {
		col = strings.TrimSpace(col)
		
		// Check for aggregation functions
		upperCol := strings.ToUpper(col)
		for _, funcName := range []string{"COUNT", "SUM", "AVG", "MIN", "MAX"} {
			if strings.HasPrefix(upperCol, funcName+"(") && strings.HasSuffix(upperCol, ")") {
				hasAggregation = true
				
				// Extract column name from function
				start := strings.Index(upperCol, "(") + 1
				end := strings.LastIndex(upperCol, ")")
				columnName := strings.TrimSpace(col[start:end])
				
				// Handle COUNT(*) special case
				if funcName == "COUNT" && columnName == "*" {
					columnName = ops.Headers[0] // Use first column for count
				}
				
				aggFuncs = append(aggFuncs, AggregateFunction{
					Function: funcName,
					Column:   columnName,
					Alias:    fmt.Sprintf("%s(%s)", funcName, columnName),
				})
				break
			}
		}
	}

	return aggFuncs, hasAggregation
}

// HandleAggregation processes aggregation functions
func (ops *CSVOperations) HandleAggregation(aggFuncs []AggregateFunction, whereCond string) error {
	df := ops.DataFrame

	// Apply WHERE condition first
	filteredDF, err := ops.ApplyWhereCondition(df, whereCond)
	if err != nil {
		return fmt.Errorf("WHERE condition error: %v", err)
	}

	// Calculate aggregations
	results := make(map[string]interface{})
	
	for _, aggFunc := range aggFuncs {
		if err := ops.ValidateColumns([]string{aggFunc.Column}); err != nil {
			return err
		}

		result, err := ops.CalculateAggregation(filteredDF, aggFunc)
		if err != nil {
			return fmt.Errorf("aggregation error: %v", err)
		}
		
		results[aggFunc.Alias] = result
	}

	// Print aggregation results
	ops.PrintAggregationResults(results)
	return nil
}

// CalculateAggregation performs the actual aggregation calculation
func (ops *CSVOperations) CalculateAggregation(df dataframe.DataFrame, aggFunc AggregateFunction) (interface{}, error) {
	col := df.Col(aggFunc.Column)
	
	switch aggFunc.Function {
	case "COUNT":
		return df.Nrow(), nil
		
	case "SUM":
		if col.Type() != series.Float && col.Type() != series.Int {
			return nil, fmt.Errorf("SUM requires numeric column, got %s", col.Type())
		}
		sum := 0.0
		for i := 0; i < col.Len(); i++ {
			if val := col.Elem(i); val != nil {
				if fVal, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64); err == nil {
					sum += fVal
				}
			}
		}
		return sum, nil
		
	case "AVG":
		if col.Type() != series.Float && col.Type() != series.Int {
			return nil, fmt.Errorf("AVG requires numeric column, got %s", col.Type())
		}
		sum := 0.0
		count := 0
		for i := 0; i < col.Len(); i++ {
			if val := col.Elem(i); val != nil {
				if fVal, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64); err == nil {
					sum += fVal
					count++
				}
			}
		}
		if count == 0 {
			return 0.0, nil
		}
		return sum / float64(count), nil
		
	case "MIN":
		if col.Len() == 0 {
			return nil, nil
		}
		min := col.Elem(0)
		for i := 1; i < col.Len(); i++ {
			if val := col.Elem(i); val != nil {
				if col.Type() == series.Float || col.Type() == series.Int {
					if fVal, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64); err == nil {
						if fMin, err := strconv.ParseFloat(fmt.Sprintf("%v", min), 64); err == nil {
							if fVal < fMin {
								min = val
							}
						}
					}
				} else {
					if fmt.Sprintf("%v", val) < fmt.Sprintf("%v", min) {
						min = val
					}
				}
			}
		}
		return min, nil
		
	case "MAX":
		if col.Len() == 0 {
			return nil, nil
		}
		max := col.Elem(0)
		for i := 1; i < col.Len(); i++ {
			if val := col.Elem(i); val != nil {
				if col.Type() == series.Float || col.Type() == series.Int {
					if fVal, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64); err == nil {
						if fMax, err := strconv.ParseFloat(fmt.Sprintf("%v", max), 64); err == nil {
							if fVal > fMax {
								max = val
							}
						}
					}
				} else {
					if fmt.Sprintf("%v", val) > fmt.Sprintf("%v", max) {
						max = val
					}
				}
			}
		}
		return max, nil
		
	default:
		return nil, fmt.Errorf("unsupported aggregation function: %s", aggFunc.Function)
	}
}

// PrintAggregationResults prints aggregation results in a formatted way
func (ops *CSVOperations) PrintAggregationResults(results map[string]interface{}) {
	if ops.RawOutput {
		// Print raw values separated by commas
		first := true
		for _, value := range results {
			if !first {
				fmt.Print(",")
			}
			first = false
			if value == nil {
				fmt.Print("NULL")
			} else {
				switch v := value.(type) {
				case float64:
					if v == float64(int64(v)) {
						fmt.Printf("%.0f", v)
					} else {
						fmt.Printf("%.2f", v)
					}
				default:
					fmt.Printf("%v", value)
				}
			}
		}
		fmt.Println()
	} else {
		fmt.Println("Aggregation Results:")
		fmt.Println(strings.Repeat("-", 30))
		
		for alias, value := range results {
			if value == nil {
				fmt.Printf("%-20s: NULL\n", alias)
			} else {
				// Format numeric values nicely
				switch v := value.(type) {
				case float64:
					if v == float64(int64(v)) {
						fmt.Printf("%-20s: %.0f\n", alias, v)
					} else {
						fmt.Printf("%-20s: %.2f\n", alias, v)
					}
				default:
					fmt.Printf("%-20s: %v\n", alias, value)
				}
			}
		}
	}
}

// ApplyDistinct removes duplicate rows (basic implementation)
func (ops *CSVOperations) ApplyDistinct(df dataframe.DataFrame) dataframe.DataFrame {
	// This is a simplified DISTINCT implementation
	// In a production system, you might want a more efficient algorithm
	seen := make(map[string]bool)
	var indices []int
	
	for i := 0; i < df.Nrow(); i++ {
		// Create a key from all column values in the row
		var rowKey strings.Builder
		for j := 0; j < df.Ncol(); j++ {
			if j > 0 {
				rowKey.WriteString("|")
			}
			rowKey.WriteString(fmt.Sprintf("%v", df.Elem(i, j)))
		}
		
		key := rowKey.String()
		if !seen[key] {
			seen[key] = true
			indices = append(indices, i)
		}
	}
	
	return df.Subset(indices)
}