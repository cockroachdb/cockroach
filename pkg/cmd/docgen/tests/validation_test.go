// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
)

// TestMissingBNFFiles checks that all statement specs defined in diagrams.go
// have corresponding BNF files generated.
func TestMissingBNFFiles(t *testing.T) {
	repoRoot, ok := findRepoRoot(t)
	if !ok {
		return // already skipped
	}

	expectedFiles, ok := getExpectedDiagramNames(t, repoRoot)
	if !ok {
		return // already skipped
	}

	bnfDir := filepath.Join(repoRoot, "docs", "generated", "sql", "bnf")

	var missing []string
	for _, name := range expectedFiles {
		bnfPath := filepath.Join(bnfDir, name+".bnf")
		if _, err := os.Stat(bnfPath); os.IsNotExist(err) {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		t.Errorf("Missing BNF files for the following diagram specs:\n%s\n\n"+
			"These specs are defined in pkg/cmd/docgen/diagrams.go but no corresponding .bnf file exists.\n"+
			"Run './dev generate bnf' to generate them, or remove the spec if it's no longer needed.",
			strings.Join(missing, "\n"))
	}
}

// TestMissingHTMLDiagrams checks that all BNF files have corresponding
// HTML diagram files generated.
func TestMissingHTMLDiagrams(t *testing.T) {
	repoRoot, ok := findRepoRoot(t)
	if !ok {
		return
	}

	bnfDir := filepath.Join(repoRoot, "docs", "generated", "sql", "bnf")

	// Get all BNF files
	bnfFiles, err := filepath.Glob(filepath.Join(bnfDir, "*.bnf"))
	if err != nil || len(bnfFiles) == 0 {
		skip.IgnoreLint(t, "No BNF files found - run './dev generate bnf' first")
		return
	}

	// Check corresponding HTML files exist (either in bnfDir or bazel-bin output)
	htmlDirs := []string{
		bnfDir,
		filepath.Join(repoRoot, "bazel-bin", "docs", "generated", "sql", "bnf"),
	}

	var missing []string
	for _, bnfPath := range bnfFiles {
		baseName := strings.TrimSuffix(filepath.Base(bnfPath), ".bnf")
		// HTML files have _stmt suffix removed
		htmlName := strings.Replace(baseName, "_stmt", "", 1)

		found := false
		for _, htmlDir := range htmlDirs {
			htmlPath := filepath.Join(htmlDir, htmlName+".html")
			if _, err := os.Stat(htmlPath); err == nil {
				found = true
				break
			}
		}

		if !found {
			missing = append(missing, baseName)
		}
	}

	if len(missing) > 0 {
		t.Errorf("Missing HTML diagram files for the following BNF files:\n%s\n\n"+
			"Run './dev build docs/generated/sql/bnf:svg' to generate them.",
			strings.Join(missing, "\n"))
	}
}

// TestStmtNamingConflicts checks for _stmt naming conflicts in diagram specs.
// Some specs use names like "foo_stmt" while the HTML output removes the "_stmt"
// suffix, which can cause conflicts.
func TestStmtNamingConflicts(t *testing.T) {
	repoRoot, ok := findRepoRoot(t)
	if !ok {
		return
	}

	expectedFiles, ok := getExpectedDiagramNames(t, repoRoot)
	if !ok {
		return
	}

	// Build a map of names with and without _stmt suffix
	nameMap := make(map[string][]string)
	for _, name := range expectedFiles {
		baseName := strings.TrimSuffix(name, "_stmt")
		nameMap[baseName] = append(nameMap[baseName], name)
	}

	var conflicts []string
	for baseName, names := range nameMap {
		if len(names) > 1 {
			conflicts = append(conflicts, baseName+": "+strings.Join(names, ", "))
		}
	}

	if len(conflicts) > 0 {
		t.Errorf("_stmt naming conflicts detected:\n%s\n\n"+
			"These names will produce conflicting HTML output files. "+
			"Rename one of the specs to avoid the conflict.",
			strings.Join(conflicts, "\n"))
	}
}

// TestReplaceUnlinkMismatch checks for mismatches between replace and unlink
// directives in diagram specs. If a token is replaced, it often should be unlinked
// to prevent broken grammar links.
func TestReplaceUnlinkMismatch(t *testing.T) {
	repoRoot, ok := findRepoRoot(t)
	if !ok {
		return
	}

	diagramsPath := filepath.Join(repoRoot, "pkg", "cmd", "docgen", "diagrams.go")

	content, err := os.ReadFile(diagramsPath)
	if err != nil {
		skip.IgnoreLint(t, "Could not read diagrams.go")
		return
	}

	// Parse the specs to find replace/unlink pairs
	// This is a simplified check - a more thorough check would parse the Go code
	replaceRe := regexp.MustCompile(`replace:\s*map\[string\]string\{([^}]+)\}`)
	unlinkRe := regexp.MustCompile(`unlink:\s*\[\]string\{([^}]+)\}`)

	// Find all spec blocks
	specRe := regexp.MustCompile(`\{\s*name:\s*"([^"]+)"[^}]+\}`)
	matches := specRe.FindAllStringSubmatch(string(content), -1)

	var warnings []string
	for _, match := range matches {
		specBlock := match[0]
		specName := match[1]

		replaceMatch := replaceRe.FindStringSubmatch(specBlock)
		unlinkMatch := unlinkRe.FindStringSubmatch(specBlock)

		if replaceMatch != nil && unlinkMatch == nil {
			// Has replace but no unlink - might be intentional but worth checking
			warnings = append(warnings, specName+": has 'replace' but no 'unlink' directive")
		}
	}

	if len(warnings) > 0 {
		t.Logf("WARNING: Potential replace/unlink mismatches:\n%s\n\n"+
			"Consider adding 'unlink' directives for replaced tokens to prevent broken grammar links.",
			strings.Join(warnings, "\n"))
	}
}

// TestBrokenGrammarLinks checks for broken grammar links in generated HTML diagrams.
func TestBrokenGrammarLinks(t *testing.T) {
	repoRoot, ok := findRepoRoot(t)
	if !ok {
		return
	}

	// Check HTML files for links to non-existent grammar rules
	htmlDirs := []string{
		filepath.Join(repoRoot, "docs", "generated", "sql", "bnf"),
		filepath.Join(repoRoot, "bazel-bin", "docs", "generated", "sql", "bnf"),
	}

	// Get all available grammar rule names
	grammarRules := make(map[string]bool)
	bnfDir := filepath.Join(repoRoot, "docs", "generated", "sql", "bnf")
	bnfFiles, _ := filepath.Glob(filepath.Join(bnfDir, "*.bnf"))
	if len(bnfFiles) == 0 {
		skip.IgnoreLint(t, "No BNF files found")
		return
	}

	for _, f := range bnfFiles {
		name := strings.TrimSuffix(filepath.Base(f), ".bnf")
		grammarRules[name] = true
		// Also add without _stmt suffix
		grammarRules[strings.TrimSuffix(name, "_stmt")] = true
	}

	linkRe := regexp.MustCompile(`xlink:href="sql-grammar\.html#([^"]+)"`)
	var brokenLinks []string

	for _, htmlDir := range htmlDirs {
		htmlFiles, _ := filepath.Glob(filepath.Join(htmlDir, "*.html"))
		for _, htmlFile := range htmlFiles {
			content, err := os.ReadFile(htmlFile)
			if err != nil {
				continue
			}

			matches := linkRe.FindAllStringSubmatch(string(content), -1)
			for _, match := range matches {
				linkedRule := match[1]
				if !grammarRules[linkedRule] {
					brokenLinks = append(brokenLinks,
						filepath.Base(htmlFile)+": broken link to '"+linkedRule+"'")
				}
			}
		}
	}

	if len(brokenLinks) > 0 {
		t.Errorf("Broken grammar links detected:\n%s\n\n"+
			"These HTML files contain links to grammar rules that don't exist. "+
			"Consider using 'unlink' directives in the spec to remove these links.",
			strings.Join(brokenLinks, "\n"))
	}
}

// TestUnusedDiagrams is a placeholder for future functionality.
// Detecting unused diagrams would require cloning the cockroachdb/docs
// repository and scanning for remote_include references. This is not
// currently needed but could be implemented later if diagram cruft
// becomes a problem.
func TestUnusedDiagrams(t *testing.T) {
	skip.IgnoreLint(t, "Unused diagram detection not needed - can be implemented later if required")
}

// TestSKIPDOCSuppressions reports all SKIP DOC annotations in sql.y
func TestSKIPDOCSuppressions(t *testing.T) {
	repoRoot, ok := findRepoRoot(t)
	if !ok {
		return
	}

	sqlYPath := filepath.Join(repoRoot, "pkg", "sql", "parser", "sql.y")

	file, err := os.Open(sqlYPath)
	if err != nil {
		skip.IgnoreLint(t, "Could not open sql.y")
		return
	}
	defer file.Close()

	var suppressions []string
	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if strings.Contains(line, "SKIP DOC") {
			suppressions = append(suppressions, formatSuppression(lineNum, line))
		}
	}

	if len(suppressions) > 0 {
		t.Logf("SKIP DOC suppressions found in sql.y:\n%s\n\n"+
			"These grammar rules are intentionally excluded from documentation.",
			strings.Join(suppressions, "\n"))
	}
}

// Helper functions

func findRepoRoot(t *testing.T) (string, bool) {
	t.Helper()

	// Check for Bazel's BUILD_WORKSPACE_DIRECTORY first (available during `bazel run`)
	if wsDir := os.Getenv("BUILD_WORKSPACE_DIRECTORY"); wsDir != "" {
		return wsDir, true
	}

	// Check for TEST_SRCDIR (available during `bazel test`)
	if testSrcDir := os.Getenv("TEST_SRCDIR"); testSrcDir != "" {
		// The runfiles structure is: TEST_SRCDIR/workspace_name/path
		// We need to find the workspace root
		wsName := os.Getenv("TEST_WORKSPACE")
		if wsName == "" {
			wsName = "com_github_cockroachdb_cockroach"
		}
		wsRoot := filepath.Join(testSrcDir, wsName)
		if _, err := os.Stat(wsRoot); err == nil {
			return wsRoot, true
		}
	}

	// Fallback: Start from current working directory and walk up
	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		// Check if this is the repo root by looking for go.mod
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, true
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			skip.IgnoreLint(t, "Could not find repository root - test must be run from within the repository or with proper Bazel data dependencies")
			return "", false
		}
		dir = parent
	}
}

func getExpectedDiagramNames(t *testing.T, repoRoot string) ([]string, bool) {
	t.Helper()

	buildBazelPath := filepath.Join(repoRoot, "docs", "generated", "sql", "bnf", "BUILD.bazel")

	content, err := os.ReadFile(buildBazelPath)
	if err != nil {
		skip.IgnoreLint(t, "Could not read BUILD.bazel - required files not available")
		return nil, false
	}

	// Parse the FILES list from BUILD.bazel
	filesRe := regexp.MustCompile(`"([a-z_]+)"`)

	var names []string
	inFilesList := false
	for _, line := range strings.Split(string(content), "\n") {
		if strings.Contains(line, "FILES = [") {
			inFilesList = true
			continue
		}
		if inFilesList && strings.Contains(line, "]") {
			break
		}
		if inFilesList {
			match := filesRe.FindStringSubmatch(line)
			if match != nil {
				names = append(names, match[1])
			}
		}
	}

	return names, true
}

func formatSuppression(lineNum int, line string) string {
	// Extract relevant context around SKIP DOC
	line = strings.TrimSpace(line)
	if len(line) > 100 {
		line = line[:100] + "..."
	}
	return fmt.Sprintf("    Line %d: %s", lineNum, line)
}
