// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// testpyramid analyzes the CockroachDB codebase to generate a test pyramid
// analysis, categorizing tests into E2E, Integration, and Unit tests.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// TestCategory represents the category of a test in the pyramid.
type TestCategory string

const (
	CategoryE2ERoachtest     TestCategory = "E2E-Roachtest"
	CategoryE2ECypress       TestCategory = "E2E-Cypress"
	CategoryE2EAcceptance    TestCategory = "E2E-Acceptance"
	CategoryIntegrationLogic TestCategory = "Integration-LogicTest"
	CategoryIntegrationTC    TestCategory = "Integration-TestCluster"
	CategoryIntegrationTS    TestCategory = "Integration-TestServer"
	CategoryDatadriven       TestCategory = "Datadriven"
	CategoryUnit             TestCategory = "Unit"
	CategoryFrontendUnit     TestCategory = "Frontend-Unit"
)

// PyramidLevel maps categories to pyramid levels for aggregation.
var PyramidLevel = map[TestCategory]string{
	CategoryE2ERoachtest:     "E2E",
	CategoryE2ECypress:       "E2E",
	CategoryE2EAcceptance:    "E2E",
	CategoryIntegrationLogic: "Integration",
	CategoryIntegrationTC:    "Integration",
	CategoryIntegrationTS:    "Integration",
	CategoryDatadriven:       "Datadriven",
	CategoryUnit:             "Unit",
	CategoryFrontendUnit:     "Unit",
}

// TestInfo holds information about a single test.
type TestInfo struct {
	Name       string
	File       string
	Line       int
	Category   TestCategory
	Package    string
	Subpackage string // e.g., "ccl" for enterprise tests
}

// Analyzer scans and categorizes tests in the codebase.
type Analyzer struct {
	rootDir string
	tests   []TestInfo
	fset    *token.FileSet
}

// NewAnalyzer creates a new test analyzer.
func NewAnalyzer(rootDir string) *Analyzer {
	return &Analyzer{
		rootDir: rootDir,
		tests:   make([]TestInfo, 0),
		fset:    token.NewFileSet(),
	}
}

// Analyze runs the full analysis on the codebase.
func (a *Analyzer) Analyze() error {
	// Scan Go test files
	if err := a.scanGoTests(); err != nil {
		return fmt.Errorf("scanning Go tests: %w", err)
	}

	// Scan roachtests (different pattern - uses r.Add())
	if err := a.scanRoachtests(); err != nil {
		return fmt.Errorf("scanning roachtests: %w", err)
	}

	// Scan Cypress tests
	if err := a.scanCypressTests(); err != nil {
		return fmt.Errorf("scanning Cypress tests: %w", err)
	}

	// Scan frontend unit tests
	if err := a.scanFrontendTests(); err != nil {
		return fmt.Errorf("scanning frontend tests: %w", err)
	}

	return nil
}

// scanGoTests scans all Go test files and categorizes test functions.
func (a *Analyzer) scanGoTests() error {
	return filepath.Walk(a.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// Skip vendor, node_modules, and .git directories
		if info.IsDir() {
			name := info.Name()
			if name == "vendor" || name == "node_modules" || name == ".git" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}

		// Only process Go test files, but skip roachtest directory (handled separately)
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}

		// Skip roachtest tests directory - we handle those separately
		if strings.Contains(path, "pkg/cmd/roachtest/tests/") {
			return nil
		}

		return a.analyzeGoTestFile(path)
	})
}

// analyzeGoTestFile parses a Go test file and extracts test functions.
func (a *Analyzer) analyzeGoTestFile(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil // Skip unreadable files
	}

	file, err := parser.ParseFile(a.fset, path, src, parser.ParseComments)
	if err != nil {
		return nil // Skip unparseable files
	}

	// Collect imports
	imports := make(map[string]bool)
	for _, imp := range file.Imports {
		if imp.Path != nil {
			imports[strings.Trim(imp.Path.Value, `"`)] = true
		}
	}

	// Check for logic test files
	isLogicTest := strings.Contains(path, "logictest") ||
		imports["github.com/cockroachdb/cockroach/pkg/sql/logictest"] ||
		imports["github.com/cockroachdb/cockroach/pkg/ccl/logictestccl"]

	// Analyze each test function
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}

		// Check if it's a test function
		if !strings.HasPrefix(fn.Name.Name, "Test") {
			continue
		}

		// Must have *testing.T parameter
		if fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
			continue
		}

		category := a.categorizeTestFunction(fn, imports, isLogicTest, string(src), path)

		relPath, _ := filepath.Rel(a.rootDir, path)
		pkg := filepath.Dir(relPath)
		subpkg := ""
		if strings.Contains(path, "/ccl/") {
			subpkg = "ccl"
		}

		pos := a.fset.Position(fn.Pos())
		a.tests = append(a.tests, TestInfo{
			Name:       fn.Name.Name,
			File:       relPath,
			Line:       pos.Line,
			Category:   category,
			Package:    pkg,
			Subpackage: subpkg,
		})
	}

	return nil
}

// categorizeTestFunction determines the category of a test function.
func (a *Analyzer) categorizeTestFunction(fn *ast.FuncDecl, imports map[string]bool, isLogicTest bool, src string, path string) TestCategory {
	// Acceptance tests use Docker and are E2E tests
	if strings.Contains(path, "pkg/acceptance/") {
		return CategoryE2EAcceptance
	}

	// Logic tests are integration tests
	if isLogicTest && strings.Contains(fn.Name.Name, "Logic") {
		return CategoryIntegrationLogic
	}

	// Check function body for integration markers
	if fn.Body == nil {
		return CategoryUnit
	}

	hasTestCluster := false
	hasTestServer := false
	hasDatadriven := false

	// Walk the function body looking for calls
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		callStr := a.callExprToString(call)

		// Check for TestCluster patterns
		if strings.Contains(callStr, "StartTestCluster") ||
			strings.Contains(callStr, "NewTestCluster") ||
			strings.Contains(callStr, "testcluster.Start") {
			hasTestCluster = true
		}

		// Check for TestServer patterns
		if strings.Contains(callStr, "StartServer") ||
			strings.Contains(callStr, "NewServer") ||
			strings.Contains(callStr, "serverutils.Start") ||
			strings.Contains(callStr, "serverutils.Make") ||
			strings.Contains(callStr, "MakeServer") {
			hasTestServer = true
		}

		// Check for datadriven patterns
		if strings.Contains(callStr, "datadriven.Run") ||
			strings.Contains(callStr, "datadriven.Walk") {
			hasDatadriven = true
		}

		return true
	})

	// Also check imports for stronger signal
	if imports["github.com/cockroachdb/cockroach/pkg/testutils/testcluster"] {
		// Check if testcluster is actually used in this function
		fnSrc := a.getFunctionSource(fn, src)
		if strings.Contains(fnSrc, "testcluster.") || strings.Contains(fnSrc, "StartTestCluster") {
			hasTestCluster = true
		}
	}

	if imports["github.com/cockroachdb/cockroach/pkg/testutils/serverutils"] ||
		imports["github.com/cockroachdb/cockroach/pkg/base"] {
		fnSrc := a.getFunctionSource(fn, src)
		if strings.Contains(fnSrc, "serverutils.") || strings.Contains(fnSrc, "StartServer") {
			hasTestServer = true
		}
	}

	// Categorize based on findings (most specific first)
	if hasTestCluster {
		return CategoryIntegrationTC
	}
	if hasTestServer {
		return CategoryIntegrationTS
	}
	if hasDatadriven {
		return CategoryDatadriven
	}

	return CategoryUnit
}

// callExprToString converts a call expression to a string for pattern matching.
func (a *Analyzer) callExprToString(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		if x, ok := fn.X.(*ast.Ident); ok {
			return x.Name + "." + fn.Sel.Name
		}
		return fn.Sel.Name
	}
	return ""
}

// getFunctionSource extracts the source code of a function.
func (a *Analyzer) getFunctionSource(fn *ast.FuncDecl, src string) string {
	start := a.fset.Position(fn.Pos()).Offset
	end := a.fset.Position(fn.End()).Offset
	if end > len(src) {
		end = len(src)
	}
	if start >= end || start < 0 {
		return ""
	}
	return src[start:end]
}

// scanRoachtests scans the roachtest directory for test registrations.
func (a *Analyzer) scanRoachtests() error {
	roachtestDir := filepath.Join(a.rootDir, "pkg/cmd/roachtest/tests")

	return filepath.Walk(roachtestDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		// Skip _test.go files in roachtest - those are unit tests for the roachtest framework
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}

		return a.analyzeRoachtestFile(path)
	})
}

// analyzeRoachtestFile finds r.Add() registrations in a roachtest file.
func (a *Analyzer) analyzeRoachtestFile(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	srcStr := string(src)
	relPath, _ := filepath.Rel(a.rootDir, path)

	// Look for r.Add() patterns which register roachtests
	// Pattern: r.Add(registry.TestSpec{Name: "..."
	re := regexp.MustCompile(`(?m)r\.Add\(registry\.TestSpec\{[^}]*Name:\s*"([^"]+)"`)
	matches := re.FindAllStringSubmatchIndex(srcStr, -1)

	for _, match := range matches {
		if len(match) >= 4 {
			testName := srcStr[match[2]:match[3]]
			// Calculate line number
			line := 1 + strings.Count(srcStr[:match[0]], "\n")

			a.tests = append(a.tests, TestInfo{
				Name:     testName,
				File:     relPath,
				Line:     line,
				Category: CategoryE2ERoachtest,
				Package:  "pkg/cmd/roachtest/tests",
			})
		}
	}

	// Also look for simpler patterns: r.Add(spec) where spec is defined elsewhere
	// We can detect these by looking for the registerX() function pattern
	// that's common in roachtest files

	return nil
}

// scanCypressTests scans for Cypress test files.
func (a *Analyzer) scanCypressTests() error {
	cypressDir := filepath.Join(a.rootDir, "pkg/ui/workspaces/e2e-tests/cypress")

	return filepath.Walk(cypressDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() || !strings.HasSuffix(path, ".cy.ts") {
			return nil
		}

		return a.analyzeCypressFile(path)
	})
}

// analyzeCypressFile counts it() blocks in a Cypress test file.
func (a *Analyzer) analyzeCypressFile(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	srcStr := string(src)
	relPath, _ := filepath.Rel(a.rootDir, path)

	// Find it() blocks - these are individual test cases
	// Pattern: it("description", ... or it('description', ...
	re := regexp.MustCompile(`(?m)\bit\s*\(\s*["']([^"']+)["']`)
	matches := re.FindAllStringSubmatchIndex(srcStr, -1)

	for _, match := range matches {
		if len(match) >= 4 {
			testName := srcStr[match[2]:match[3]]
			line := 1 + strings.Count(srcStr[:match[0]], "\n")

			a.tests = append(a.tests, TestInfo{
				Name:     testName,
				File:     relPath,
				Line:     line,
				Category: CategoryE2ECypress,
				Package:  "pkg/ui/workspaces/e2e-tests",
			})
		}
	}

	return nil
}

// scanFrontendTests scans for frontend unit tests (.spec.ts/.spec.tsx files).
func (a *Analyzer) scanFrontendTests() error {
	uiDir := filepath.Join(a.rootDir, "pkg/ui/workspaces")

	return filepath.Walk(uiDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			// Skip node_modules
			if info.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip cypress tests (already counted as E2E)
		if strings.Contains(path, "/cypress/") {
			return nil
		}

		if strings.HasSuffix(path, ".spec.ts") || strings.HasSuffix(path, ".spec.tsx") {
			return a.analyzeFrontendTestFile(path)
		}

		return nil
	})
}

// analyzeFrontendTestFile counts it()/test() blocks in a frontend test file.
func (a *Analyzer) analyzeFrontendTestFile(path string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil
	}

	srcStr := string(src)
	relPath, _ := filepath.Rel(a.rootDir, path)

	// Find it() or test() blocks
	re := regexp.MustCompile(`(?m)\b(?:it|test)\s*\(\s*["']([^"']+)["']`)
	matches := re.FindAllStringSubmatchIndex(srcStr, -1)

	for _, match := range matches {
		if len(match) >= 4 {
			testName := srcStr[match[2]:match[3]]
			line := 1 + strings.Count(srcStr[:match[0]], "\n")

			a.tests = append(a.tests, TestInfo{
				Name:     testName,
				File:     relPath,
				Line:     line,
				Category: CategoryFrontendUnit,
				Package:  filepath.Dir(relPath),
			})
		}
	}

	return nil
}

// WriteCSV writes the test data to a CSV file.
func (a *Analyzer) WriteCSV(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Write header
	if err := w.Write([]string{"test_name", "file", "line", "category", "pyramid_level", "package", "subpackage"}); err != nil {
		return err
	}

	// Sort tests by category, then file, then line
	sort.Slice(a.tests, func(i, j int) bool {
		if a.tests[i].Category != a.tests[j].Category {
			return a.tests[i].Category < a.tests[j].Category
		}
		if a.tests[i].File != a.tests[j].File {
			return a.tests[i].File < a.tests[j].File
		}
		return a.tests[i].Line < a.tests[j].Line
	})

	// Write data
	for _, t := range a.tests {
		if err := w.Write([]string{
			t.Name,
			t.File,
			fmt.Sprintf("%d", t.Line),
			string(t.Category),
			PyramidLevel[t.Category],
			t.Package,
			t.Subpackage,
		}); err != nil {
			return err
		}
	}

	return nil
}

// PrintPyramid prints an ASCII visualization of the test pyramid.
func (a *Analyzer) PrintPyramid() {
	// Count tests by category and level
	categoryCount := make(map[TestCategory]int)
	levelCount := make(map[string]int)

	for _, t := range a.tests {
		categoryCount[t.Category]++
		levelCount[PyramidLevel[t.Category]]++
	}

	total := len(a.tests)
	if total == 0 {
		fmt.Println("No tests found.")
		return
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                        COCKROACHDB TEST PYRAMID                              ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println()

	// Print pyramid visualization
	e2eCount := levelCount["E2E"]
	intCount := levelCount["Integration"]
	ddCount := levelCount["Datadriven"]
	unitCount := levelCount["Unit"]

	// Calculate percentages
	e2ePct := float64(e2eCount) / float64(total) * 100
	intPct := float64(intCount) / float64(total) * 100
	ddPct := float64(ddCount) / float64(total) * 100
	unitPct := float64(unitCount) / float64(total) * 100

	// ASCII pyramid - wider at bottom
	fmt.Println("                              ╱╲")
	fmt.Println("                             ╱  ╲")
	fmt.Printf("                            ╱ E2E╲        %6d tests (%5.1f%%)\n", e2eCount, e2ePct)
	fmt.Println("                           ╱──────╲")
	fmt.Println("                          ╱        ╲")
	fmt.Printf("                         ╱Integration╲   %6d tests (%5.1f%%)\n", intCount, intPct)
	fmt.Println("                        ╱────────────╲")
	fmt.Println("                       ╱              ╲")
	fmt.Printf("                      ╱   Datadriven   ╲  %6d tests (%5.1f%%)\n", ddCount, ddPct)
	fmt.Println("                     ╱──────────────────╲")
	fmt.Println("                    ╱                    ╲")
	fmt.Printf("                   ╱        Unit          ╲ %6d tests (%5.1f%%)\n", unitCount, unitPct)
	fmt.Println("                  ╱────────────────────────╲")
	fmt.Println()

	// Print detailed breakdown
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println("║                           DETAILED BREAKDOWN                                 ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println()

	categories := []TestCategory{
		CategoryE2ERoachtest,
		CategoryE2ECypress,
		CategoryE2EAcceptance,
		CategoryIntegrationLogic,
		CategoryIntegrationTC,
		CategoryIntegrationTS,
		CategoryDatadriven,
		CategoryUnit,
		CategoryFrontendUnit,
	}

	maxBar := 50
	maxCount := 0
	for _, c := range categoryCount {
		if c > maxCount {
			maxCount = c
		}
	}

	for _, cat := range categories {
		count := categoryCount[cat]
		pct := float64(count) / float64(total) * 100
		barLen := 0
		if maxCount > 0 {
			barLen = count * maxBar / maxCount
		}
		bar := strings.Repeat("█", barLen)

		fmt.Printf("  %-25s %6d (%5.1f%%) %s\n", cat, count, pct, bar)
	}

	fmt.Println()
	fmt.Printf("  %-25s %6d\n", "TOTAL", total)
	fmt.Println()

	// Print pyramid health assessment
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println("║                           PYRAMID HEALTH                                     ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Println()

	// Ideal pyramid ratios (roughly): 70% unit, 20% integration, 10% e2e
	// Calculate actual ratios
	unitRatio := float64(unitCount+ddCount) / float64(total) * 100 // Include datadriven as "closer to unit"
	intRatio := float64(intCount) / float64(total) * 100
	e2eRatio := float64(e2eCount) / float64(total) * 100

	fmt.Printf("  Unit + Datadriven (ideal ~70%%):  %5.1f%%", unitRatio)
	if unitRatio >= 60 {
		fmt.Println("  ✓")
	} else {
		fmt.Println("  ⚠ (low)")
	}

	fmt.Printf("  Integration (ideal ~20%%):        %5.1f%%", intRatio)
	if intRatio <= 30 {
		fmt.Println("  ✓")
	} else {
		fmt.Println("  ⚠ (high)")
	}

	fmt.Printf("  E2E (ideal ~10%%):                %5.1f%%", e2eRatio)
	if e2eRatio <= 15 {
		fmt.Println("  ✓")
	} else {
		fmt.Println("  ⚠ (high)")
	}

	fmt.Println()
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")
}

func main() {
	rootDir := flag.String("root", ".", "Root directory of the CockroachDB repository")
	outputCSV := flag.String("csv", "test_pyramid.csv", "Output CSV file path")
	flag.Parse()

	// Resolve root directory
	absRoot, err := filepath.Abs(*rootDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving root directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Analyzing tests in: %s\n", absRoot)
	fmt.Println("This may take a minute...")

	analyzer := NewAnalyzer(absRoot)
	if err := analyzer.Analyze(); err != nil {
		fmt.Fprintf(os.Stderr, "Error analyzing tests: %v\n", err)
		os.Exit(1)
	}

	// Write CSV
	if err := analyzer.WriteCSV(*outputCSV); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing CSV: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("CSV written to: %s\n", *outputCSV)

	// Print pyramid
	analyzer.PrintPyramid()
}
