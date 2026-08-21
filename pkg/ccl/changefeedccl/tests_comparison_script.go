package main

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
)

var (
	originalFile = "changefeed_test.go"
	newFiles     = []string{
		"changefeed_basics_test.go",
		"changefeed_schema_test.go",
		"changefeed_robustness_test.go",
		"changefeed_cluster_test.go",
		"changefeed_telemetry_test.go",
	}
)

type TestFunction struct {
	Name string
	Hash string
}

func extractTestFunctions(filename string) (map[string]string, error) {
	src, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	// Parse into an AST
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	tests := make(map[string]string)
	srcStr := string(src)

	ast.Inspect(node, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok {
			if fn.Name != nil && fn.Name.IsExported() &&
				len(fn.Name.Name) > 4 && fn.Name.Name[:4] == "Test" {

				start := fset.Position(fn.Pos()).Offset
				end := fset.Position(fn.End()).Offset
				content := srcStr[start:end]

				// Create SHA256 hash of the content
				hash := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
				tests[fn.Name.Name] = hash
			}
		}
		return true
	})
	return tests, nil
}
func main() {
	// Extract tests from original file
	originalTests, err := extractTestFunctions(originalFile)
	if err != nil {
		fmt.Printf("Error reading original file %s: %v\n", originalFile, err)
		os.Exit(1)
	}
	// Extract tests from all new files
	newTests := make(map[string]string)
	for _, filename := range newFiles {
		tests, err := extractTestFunctions(filename)
		if err != nil {
			fmt.Printf("Error reading file %s: %v\n", filename, err)
			continue
		}

		for name, hash := range tests {
			newTests[name] = hash
		}
	}
	// Compare
	fmt.Printf("Original tests: %d\n", len(originalTests))
	fmt.Printf("New tests: %d\n", len(newTests))
	fmt.Printf("\n")
	// Find missing tests
	var missing []string
	for name := range originalTests {
		if _, exists := newTests[name]; !exists {
			missing = append(missing, name)
		}
	}
	// Find modified tests
	var modified []string
	for name, originalHash := range originalTests {
		if newHash, exists := newTests[name]; exists && originalHash != newHash {
			modified = append(modified, name)
		}
	}
	// Find newly added tests
	var added []string
	for name := range newTests {
		if _, exists := originalTests[name]; !exists {
			added = append(added, name)
		}
	}

	if len(missing) > 0 {
		fmt.Printf("Missing tests (%d):\n", len(missing))
		for _, name := range missing {
			fmt.Printf("  - %s\n", name)
		}
		fmt.Printf("\n")
	}
	if len(modified) > 0 {
		fmt.Printf("Modified tests (%d):\n", len(modified))
		for _, name := range modified {
			fmt.Printf("  - %s\n", name)
		}
		fmt.Printf("\n")
	}
	if len(added) > 0 {
		fmt.Printf("Extra tests (%d):\n", len(added))
		for _, name := range added {
			fmt.Printf("  - %s\n", name)
		}
		fmt.Printf("\n")
	}

	if len(missing) == 0 && len(modified) == 0 && len(added) == 0 {
		fmt.Printf("All original tests match %d new tests added.\n", len(added))
	}
}
