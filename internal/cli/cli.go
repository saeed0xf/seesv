package cli

import (
	"fmt"
	"os"

	"github.com/projectdiscovery/goflags"
	"csvql/internal/operations"
)

// Options represents the CLI configuration
type Options struct {
	File       string `flag:"file" cfgFlagName:"file" description:"CSV input file (required)"`
	Select     string `flag:"select" cfgFlagName:"select" description:"SELECT columns (comma-separated)"`
	Where      string `flag:"where" cfgFlagName:"where" description:"WHERE condition (SQL-like)"`
	Update     string `flag:"update" cfgFlagName:"update" description:"UPDATE column values (col1=val1,col2=val2)"`
	Delete     bool   `flag:"delete" cfgFlagName:"delete" description:"DELETE rows matching WHERE condition"`
	Insert     string `flag:"insert" cfgFlagName:"insert" description:"INSERT new row (col1=val1,col2=val2)"`
	Limit      int    `flag:"limit" cfgFlagName:"limit" description:"LIMIT number of rows returned"`
	Order      string `flag:"order" cfgFlagName:"order" description:"ORDER BY column [asc|desc]"`
	Columns    bool   `flag:"columns" cfgFlagName:"columns" description:"Show CSV column headers"`
	Raw        bool   `flag:"raw" cfgFlagName:"raw" description:"Show only table values without column headers"`
	Output     string `flag:"output" cfgFlagName:"output" description:"Output file to save results"`
	Help       bool   `flag:"h" cfgFlagName:"help" description:"Show help message"`
}

// Execute runs the CLI application
func Execute() error {
	opts := &Options{}
	
	flagSet := goflags.NewFlagSet()
	flagSet.SetDescription("")
	
	// Create flags with single dash - no groups for cleaner help
	flagSet.StringVarP(&opts.File, "file", "f", "", "")
	flagSet.StringVar(&opts.Select, "select", "", "")
	flagSet.StringVar(&opts.Where, "where", "", "")
	flagSet.StringVar(&opts.Update, "update", "", "")
	flagSet.BoolVar(&opts.Delete, "delete", false, "")
	flagSet.StringVar(&opts.Insert, "insert", "", "")
	flagSet.IntVar(&opts.Limit, "limit", 0, "")
	flagSet.StringVar(&opts.Order, "order", "", "")
	flagSet.BoolVar(&opts.Columns, "columns", false, "")
	flagSet.BoolVar(&opts.Raw, "raw", false, "")
	flagSet.StringVarP(&opts.Output, "output", "o", "", "")
	flagSet.BoolVarP(&opts.Help, "help", "h", false, "")

	// Parse flags
	if err := flagSet.Parse(); err != nil {
		return fmt.Errorf("failed to parse flags: %v", err)
	}

	// Show help if requested
	if opts.Help {
		ShowUsage(flagSet)
		return nil
	}

	// Validate required flags
	if opts.File == "" {
		fmt.Fprintln(os.Stderr, "Error: -file flag is required")
		ShowUsage(flagSet)
		return fmt.Errorf("missing required flag: -file")
	}

	return runCSVQL(opts)
}

// ShowUsage displays help information
func ShowUsage(flagSet *goflags.FlagSet) {
	fmt.Println("csvql - SQL-like queries on CSV files")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Printf("  %s [flags]\n", os.Args[0])
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println()
	
	// Input flags
	fmt.Println("INPUT:")
	fmt.Printf("   %-20s %s\n", "-file, -f", "CSV input file (required)")
	fmt.Println()
	
	// Operation flags  
	fmt.Println("OPERATIONS:")
	fmt.Printf("   %-20s %s\n", "-select", "SELECT columns (comma-separated)")
	fmt.Printf("   %-20s %s\n", "-insert", "INSERT new row (col1=val1,col2=val2)")
	fmt.Printf("   %-20s %s\n", "-update", "UPDATE column values (col1=val1,col2=val2)")
	fmt.Printf("   %-20s %s\n", "-delete", "DELETE rows matching WHERE condition")
	fmt.Println()
	
	// Query modifiers
	fmt.Println("QUERY MODIFIERS:")
	fmt.Printf("   %-20s %s\n", "-where", "WHERE condition (SQL-like)")
	fmt.Printf("   %-20s %s\n", "-order", "ORDER BY column [asc|desc]")
	fmt.Printf("   %-20s %s\n", "-limit", "LIMIT number of rows returned")
	fmt.Println()
	
	// Output flags
	fmt.Println("OUTPUT:")
	fmt.Printf("   %-20s %s\n", "-columns", "Show CSV column headers")
	fmt.Printf("   %-20s %s\n", "-raw", "Show only table values without column headers")
	fmt.Printf("   %-20s %s\n", "-output, -o", "Output file to save results")
	fmt.Println()
	
	// Misc flags
	fmt.Printf("   %-20s %s\n", "-h, -help", "Show help message")
	fmt.Println()
	
	// fmt.Println("Examples:")
	// fmt.Printf("  %s -file tests/scope.csv -select \"identifier,max_severity\" -where \"max_severity = critical\"\n", "csvql")
	// fmt.Printf("  %s -file tests/scope.csv -update \"max_severity='high'\" -where \"identifier = '*.example.com'\"\n", "csvql")
	// fmt.Printf("  %s -file tests/scope.csv -delete -where \"eligible_for_bounty = false\"\n", "csvql")
	// fmt.Printf("  %s -file tests/scope.csv -insert \"identifier='*.newdomain.com',asset_type='WILDCARD',max_severity='medium'\"\n", "csvql")
	// fmt.Printf("  %s -file tests/scope.csv -columns\n", "csvql")
	// fmt.Printf("  %s -file tests/scope.csv -select \"identifier,asset_type\" -raw\n", "csvql")
}

func runCSVQL(opts *Options) error {
	// Validate that file exists
	if _, err := os.Stat(opts.File); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", opts.File)
	}

	// Create operations instance
	ops := &operations.CSVOperations{
		FilePath: opts.File,
		RawOutput: opts.Raw,
		OutputFile: opts.Output,
	}

	// Initialize the operations
	if err := ops.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize CSV operations: %v", err)
	}

	// Handle different operations based on flags
	switch {
	case opts.Columns:
		return ops.ShowColumns()
	case opts.Insert != "":
		return ops.Insert(opts.Insert)
	case opts.Update != "":
		return ops.Update(opts.Update, opts.Where)
	case opts.Delete:
		return ops.Delete(opts.Where)
	default:
		// Default to SELECT operation
		return ops.Select(opts.Select, opts.Where, opts.Order, opts.Limit)
	}
}