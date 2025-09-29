# csvql - Perform SQL like queries on CSV files

csvql is a CLI tool written in Go that allows you to perform SQL like operations on CSV files. It supports SELECT, INSERT, UPDATE, DELETE, WHERE, ORDER BY, LIMIT, DISTINCT, and aggregation functions.

## Features

- **SELECT**: Query and filter CSV data with column selection
- **INSERT**: Add new rows to CSV files
- **UPDATE**: Modify existing rows based on conditions  
- **DELETE**: Remove rows matching specified criteria
- **WHERE**: Filter rows using SQL-like conditions
- **ORDER BY**: Sort results in ascending or descending order
- **LIMIT**: Restrict the number of returned rows
- **DISTINCT**: Remove duplicate rows from results
- **Aggregations**: COUNT, SUM, AVG, MIN, MAX functions
- **Column listing**: Display all available columns in CSV files
- **Raw output**: CSV format output for piping and scripting
- **Project Discovery-style CLI**: Clean, organized help menu and single dash flags
- **Error handling**: Comprehensive validation and error messages

## Installation

### Prerequisites
- Go 1.19 or higher

### Build from source
```bash
git clone <repository-url>
cd csvql
go mod tidy
go build -o csvql
```

### Install binary
```bash
# Make the binary available globally
sudo cp csvql /usr/local/bin/
```

## Usage

### Command Line Flags

- `-file <file>` (or `-f`) - CSV input file (required)
- `-select "<cols>"` - SELECT columns (comma-separated)
- `-where "<condition>"` - WHERE condition (SQL-like)  
- `-update "<col=val,...>"` - UPDATE column values
- `-delete` - DELETE rows matching WHERE condition
- `-insert "<col=val,...>"` - INSERT new row
- `-limit <n>` - LIMIT number of rows returned
- `-order "<col> [asc|desc]"` - ORDER BY column with direction
- `-columns` - Show CSV column headers
- `-raw` - Show only table values without column headers (CSV format)
- `-h` (or `-help`) - Show help message

## Examples

### Basic Operations

#### Show column headers
```bash
csvql -file data.csv -columns
```

#### SELECT all columns
```bash
csvql -file data.csv
```

#### SELECT specific columns
```bash
csvql -file data.csv -select "name,age,city"
```

#### SELECT with WHERE condition
```bash
csvql -file data.csv -select "name,age" -where "age > 30"
csvql -file data.csv -select "name,salary" -where "department = 'Engineering'"
```

#### SELECT with ORDER BY
```bash
csvql -file data.csv -select "name,age" -order "age desc"
csvql -file data.csv -select "name,salary" -order "salary asc"
```

#### SELECT with LIMIT
```bash
csvql -file data.csv -select "name,age" -limit 10
```

#### SELECT with multiple conditions
```bash
csvql -file data.csv -select "name,age,salary" -where "age > 25" -order "salary desc" -limit 5
```

#### Raw output (CSV format without headers)
```bash
csvql -file data.csv -select "name,age" -raw
csvql -file data.csv -select "name,salary" -where "age > 30" -raw
```

### Aggregation Functions

#### COUNT rows
```bash
csvql -file data.csv -select "COUNT(*)"
csvql -file data.csv -select "COUNT(id)" -where "status = 'active'"
csvql -file data.csv -select "COUNT(*)" -raw  # Raw output: just the number
```

#### SUM values
```bash
csvql -file data.csv -select "SUM(salary)"
csvql -file data.csv -select "SUM(amount)" -where "category = 'sales'"
```

#### AVG (Average) values
```bash
csvql -file data.csv -select "AVG(age)"
csvql -file data.csv -select "AVG(score)" -where "grade = 'A'"
```

#### MIN and MAX values
```bash
csvql -file data.csv -select "MIN(age), MAX(age)"
csvql -file data.csv -select "MIN(salary), MAX(salary)" -where "department = 'IT'"
csvql -file data.csv -select "MIN(salary), MAX(salary)" -raw  # Raw output: 50000,85000
```

### Data Modification Operations

#### INSERT new row
```bash
csvql -file data.csv -insert "name='John Doe',age=28,city='New York'"
csvql -file users.csv -insert "username='alice',email='alice@example.com',status='active'"
```

#### UPDATE existing rows
```bash
csvql -file data.csv -update "status='inactive'" -where "last_login < '2024-01-01'"
csvql -file users.csv -update "age=29,city='Boston'" -where "name = 'John Doe'"
```

#### DELETE rows
```bash
csvql -file data.csv -delete -where "status = 'inactive'"
csvql -file users.csv -delete -where "age < 18"
```

## WHERE Condition Syntax

The WHERE clause supports the following operators:

- `=` - Equal to
- `!=` - Not equal to
- `>` - Greater than
- `<` - Less than
- `>=` - Greater than or equal to
- `<=` - Less than or equal to

### Examples:
```bash
# String comparisons (with or without quotes)
-where "name = 'John'"
-where "status = active"

# Numeric comparisons  
-where "age > 30"
-where "salary >= 50000"

# Date comparisons (string-based)
-where "created_date > '2024-01-01'"
```

## Sample CSV Files

### employees.csv
```csv
id,name,age,department,salary,hire_date
1,Alice Johnson,28,Engineering,75000,2022-01-15
2,Bob Smith,35,Marketing,65000,2021-03-20
3,Carol Davis,42,Engineering,85000,2020-06-10
4,David Wilson,29,Sales,55000,2023-02-01
5,Eve Brown,31,HR,60000,2021-11-30
```

### Example operations on employees.csv:

```bash
# Show all engineers
csvql -file employees.csv -select "name,salary" -where "department = Engineering"

# Find highest paid employees
csvql -file employees.csv -select "name,salary" -order "salary desc" -limit 3

# Calculate average salary by department  
csvql -file employees.csv -select "AVG(salary)" -where "department = Engineering"

# Add new employee
csvql -file employees.csv -insert "id=6,name='Frank Miller',age=33,department='IT',salary=70000,hire_date='2024-03-01'"

# Give raise to all engineers  
csvql -file employees.csv -update "salary=salary*1.1" -where "department = Engineering"

# Remove employees hired before 2022
csvql -file employees.csv -delete -where "hire_date < '2022-01-01'"

# Export data in raw CSV format for further processing
csvql -file employees.csv -select "name,salary" -where "department = IT" -raw > it_salaries.csv
```

## Error Handling

The tool provides comprehensive error handling for common scenarios:

- **File not found**: Clear message when CSV file doesn't exist
- **Invalid columns**: Validation that specified columns exist in CSV
- **Malformed WHERE conditions**: Syntax validation for filter expressions
- **Type mismatches**: Appropriate error messages for incompatible operations
- **File permissions**: Handles read/write permission issues gracefully

## Advanced Usage

### Backup Before Modifications
Always backup your CSV files before running UPDATE or DELETE operations:

```bash
cp data.csv data.csv.backup
csvql -f data.csv -update "status='processed'" -where "id > 100"
```

### Raw Output Mode
The `-raw` flag outputs data in pure CSV format without headers or formatting, perfect for piping to other tools:

```bash
# Export filtered data to another CSV file
csvql -file data.csv -select "name,salary" -where "department = 'IT'" -raw > it_employees.csv

# Get just the count for scripting
COUNT=$(csvql -file data.csv -select "COUNT(*)" -raw)
echo "Total rows: $COUNT"

# Pipe raw output to other tools
csvql -file sales.csv -select "amount" -where "region = 'North'" -raw | awk '{sum+=$1} END {print sum}'
```

### Complex Queries
For complex operations, you can chain multiple csvql commands:

```bash
# First filter, then aggregate
csvql -file sales.csv -select "*" -where "region = 'North'" > north_sales.csv
csvql -file north_sales.csv -select "SUM(amount), COUNT(*)"
```

## Performance Considerations

- **Large files**: The tool loads the entire CSV into memory. For very large files (>1GB), consider splitting them first
- **Indexing**: No indexing is currently implemented, so WHERE operations scan all rows
- **Memory usage**: Memory usage is approximately 2-3x the size of your CSV file

## Limitations

- **WHERE clauses**: Currently supports simple conditions only (no AND/OR operators)
- **JOIN operations**: Not supported (single table operations only)
- **Data types**: All data is treated as strings, with numeric parsing for aggregations
- **NULL handling**: Empty values are treated as empty strings

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Dependencies

- [GoFlags](https://github.com/projectdiscovery/goflags) - CLI framework with single dash flag support
- [Gota](https://github.com/go-gota/gota) - DataFrame library for CSV processing

## Changelog

### v1.0.0
- Initial release with full SQL-like functionality
- Support for SELECT, INSERT, UPDATE, DELETE operations
- WHERE conditions and ORDER BY sorting
- Aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- Comprehensive error handling and validation