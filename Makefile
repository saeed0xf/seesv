# Makefile for csvql

.PHONY: build install clean test help

# Build the binary
build:
	go build -o csvql .

# Build and install to /usr/local/bin (requires sudo)
install: build
	sudo cp csvql /usr/local/bin/

# Clean build artifacts
clean:
	rm -f csvql
	go mod tidy

# Run basic tests with sample data
test: build
	@echo "Testing csvql with sample data..."
	@echo "1. Showing columns:"
	./csvql -file employees.csv -columns
	@echo "\n2. SELECT with WHERE:"
	./csvql -file employees.csv -select "name,age,salary" -where "age > 30"
	@echo "\n3. Aggregation:"
	./csvql -file employees.csv -select "COUNT(*), AVG(salary)"
	@echo "\n4. ORDER BY:"
	./csvql -file employees.csv -select "name,salary" -order "salary desc" -limit 3
	@echo "\n5. Raw output:"
	./csvql -file employees.csv -select "name,age" -raw

# Show help
help:
	@echo "Available commands:"
	@echo "  build   - Build the csvql binary"
	@echo "  install - Build and install to /usr/local/bin"
	@echo "  clean   - Clean build artifacts"
	@echo "  test    - Run basic functionality tests"
	@echo "  help    - Show this help message"
	@echo ""
	@echo "For csvql usage help, run: ./csvql -h"