// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCategorizeGoTests tests the categorization of Go test functions.
func TestCategorizeGoTests(t *testing.T) {
	// Create a temporary directory structure
	tmpDir := t.TempDir()

	testCases := []struct {
		name         string
		path         string
		content      string
		wantCategory TestCategory
		wantName     string
	}{
		{
			name: "unit test",
			path: "pkg/util/foo_test.go",
			content: `package util

import "testing"

func TestSimple(t *testing.T) {
	if 1 != 1 {
		t.Fatal("math is broken")
	}
}
`,
			wantCategory: CategoryUnit,
			wantName:     "TestSimple",
		},
		{
			name: "test with TestServer",
			path: "pkg/sql/bar_test.go",
			content: `package sql

import (
	"testing"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

func TestWithServer(t *testing.T) {
	s, _, _ := serverutils.StartServer(t, nil)
	defer s.Stopper().Stop(nil)
}
`,
			wantCategory: CategoryIntegrationTS,
			wantName:     "TestWithServer",
		},
		{
			name: "test with TestCluster",
			path: "pkg/kv/baz_test.go",
			content: `package kv

import (
	"testing"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
)

func TestWithCluster(t *testing.T) {
	tc := testcluster.StartTestCluster(t, 3, nil)
	defer tc.Stopper().Stop(nil)
}
`,
			wantCategory: CategoryIntegrationTC,
			wantName:     "TestWithCluster",
		},
		{
			name: "datadriven test",
			path: "pkg/sql/parser/parser_test.go",
			content: `package parser

import (
	"testing"
	"github.com/cockroachdb/datadriven"
)

func TestDataDriven(t *testing.T) {
	datadriven.RunTest(t, "testdata/parse", func(t *testing.T, d *datadriven.TestData) string {
		return ""
	})
}
`,
			wantCategory: CategoryDatadriven,
			wantName:     "TestDataDriven",
		},
		{
			name: "logic test",
			path: "pkg/sql/logictest/tests/local/generated_test.go",
			content: `package local

import "testing"

func TestLogic_aggregate(t *testing.T) {
	// logic test
}
`,
			wantCategory: CategoryIntegrationLogic,
			wantName:     "TestLogic_aggregate",
		},
		{
			name: "acceptance test",
			path: "pkg/acceptance/cli_test.go",
			content: `package acceptance

import "testing"

func TestDockerCLI(t *testing.T) {
	// acceptance test using docker
}
`,
			wantCategory: CategoryE2EAcceptance,
			wantName:     "TestDockerCLI",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create the file in temp directory
			fullPath := filepath.Join(tmpDir, tc.path)
			if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(fullPath, []byte(tc.content), 0644); err != nil {
				t.Fatal(err)
			}

			// Run analyzer
			analyzer := NewAnalyzer(tmpDir)
			if err := analyzer.analyzeGoTestFile(fullPath); err != nil {
				t.Fatal(err)
			}

			// Verify results
			if len(analyzer.tests) != 1 {
				t.Fatalf("expected 1 test, got %d", len(analyzer.tests))
			}

			test := analyzer.tests[0]
			if test.Name != tc.wantName {
				t.Errorf("expected name %q, got %q", tc.wantName, test.Name)
			}
			if test.Category != tc.wantCategory {
				t.Errorf("expected category %q, got %q", tc.wantCategory, test.Category)
			}
		})
	}
}

// TestRoachtestPatternMatching tests detection of roachtest registrations.
func TestRoachtestPatternMatching(t *testing.T) {
	tmpDir := t.TempDir()

	roachtestContent := `package tests

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"

func registerFoo(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "foo/bar",
		Owner: "test-team",
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// test code
		},
	})
	r.Add(registry.TestSpec{
		Name:  "foo/baz",
		Owner: "test-team",
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// test code
		},
	})
}
`

	roachtestDir := filepath.Join(tmpDir, "pkg/cmd/roachtest/tests")
	if err := os.MkdirAll(roachtestDir, 0755); err != nil {
		t.Fatal(err)
	}

	roachtestFile := filepath.Join(roachtestDir, "foo.go")
	if err := os.WriteFile(roachtestFile, []byte(roachtestContent), 0644); err != nil {
		t.Fatal(err)
	}

	analyzer := NewAnalyzer(tmpDir)
	if err := analyzer.analyzeRoachtestFile(roachtestFile); err != nil {
		t.Fatal(err)
	}

	if len(analyzer.tests) != 2 {
		t.Fatalf("expected 2 roachtests, got %d", len(analyzer.tests))
	}

	expectedNames := map[string]bool{"foo/bar": true, "foo/baz": true}
	for _, test := range analyzer.tests {
		if test.Category != CategoryE2ERoachtest {
			t.Errorf("expected category %q, got %q", CategoryE2ERoachtest, test.Category)
		}
		if !expectedNames[test.Name] {
			t.Errorf("unexpected test name: %q", test.Name)
		}
		delete(expectedNames, test.Name)
	}

	if len(expectedNames) > 0 {
		t.Errorf("missing expected tests: %v", expectedNames)
	}
}

// TestCypressPatternMatching tests detection of Cypress test cases.
func TestCypressPatternMatching(t *testing.T) {
	tmpDir := t.TempDir()

	cypressContent := `describe("Health Check", () => {
  it("should load the login page", () => {
    cy.visit("/");
  });

  it("should display error on invalid login", () => {
    cy.get("#username").type("invalid");
  });

  it('handles single quotes too', () => {
    cy.log("test");
  });
});
`

	cypressDir := filepath.Join(tmpDir, "pkg/ui/workspaces/e2e-tests/cypress/e2e")
	if err := os.MkdirAll(cypressDir, 0755); err != nil {
		t.Fatal(err)
	}

	cypressFile := filepath.Join(cypressDir, "health.cy.ts")
	if err := os.WriteFile(cypressFile, []byte(cypressContent), 0644); err != nil {
		t.Fatal(err)
	}

	analyzer := NewAnalyzer(tmpDir)
	if err := analyzer.analyzeCypressFile(cypressFile); err != nil {
		t.Fatal(err)
	}

	if len(analyzer.tests) != 3 {
		t.Fatalf("expected 3 cypress tests, got %d", len(analyzer.tests))
	}

	for _, test := range analyzer.tests {
		if test.Category != CategoryE2ECypress {
			t.Errorf("expected category %q, got %q", CategoryE2ECypress, test.Category)
		}
	}
}

// TestFrontendTestPatternMatching tests detection of frontend unit tests.
func TestFrontendTestPatternMatching(t *testing.T) {
	tmpDir := t.TempDir()

	specContent := `import { render } from "@testing-library/react";

describe("Component", () => {
  it("renders correctly", () => {
    render(<Component />);
  });

  test("handles click events", () => {
    // test code
  });
});
`

	frontendDir := filepath.Join(tmpDir, "pkg/ui/workspaces/cluster-ui/src/components")
	if err := os.MkdirAll(frontendDir, 0755); err != nil {
		t.Fatal(err)
	}

	specFile := filepath.Join(frontendDir, "component.spec.tsx")
	if err := os.WriteFile(specFile, []byte(specContent), 0644); err != nil {
		t.Fatal(err)
	}

	analyzer := NewAnalyzer(tmpDir)
	if err := analyzer.analyzeFrontendTestFile(specFile); err != nil {
		t.Fatal(err)
	}

	if len(analyzer.tests) != 2 {
		t.Fatalf("expected 2 frontend tests, got %d", len(analyzer.tests))
	}

	for _, test := range analyzer.tests {
		if test.Category != CategoryFrontendUnit {
			t.Errorf("expected category %q, got %q", CategoryFrontendUnit, test.Category)
		}
	}
}

// TestCSVOutput tests the CSV output format.
func TestCSVOutput(t *testing.T) {
	tmpDir := t.TempDir()

	analyzer := NewAnalyzer(tmpDir)
	analyzer.tests = []TestInfo{
		{
			Name:       "TestFoo",
			File:       "pkg/foo/foo_test.go",
			Line:       10,
			Category:   CategoryUnit,
			Package:    "pkg/foo",
			Subpackage: "",
		},
		{
			Name:       "TestBar",
			File:       "pkg/ccl/bar/bar_test.go",
			Line:       20,
			Category:   CategoryIntegrationTS,
			Package:    "pkg/ccl/bar",
			Subpackage: "ccl",
		},
	}

	csvPath := filepath.Join(tmpDir, "output.csv")
	if err := analyzer.WriteCSV(csvPath); err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines, got %d", len(lines))
	}

	// Check header
	expectedHeader := "test_name,file,line,category,pyramid_level,package,subpackage"
	if lines[0] != expectedHeader {
		t.Errorf("expected header %q, got %q", expectedHeader, lines[0])
	}

	// Check that both tests are present
	csvContent := string(content)
	if !strings.Contains(csvContent, "TestFoo") {
		t.Error("CSV should contain TestFoo")
	}
	if !strings.Contains(csvContent, "TestBar") {
		t.Error("CSV should contain TestBar")
	}
	if !strings.Contains(csvContent, "ccl") {
		t.Error("CSV should contain ccl subpackage")
	}
}

// TestPyramidLevelMapping tests that all categories map to valid pyramid levels.
func TestPyramidLevelMapping(t *testing.T) {
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

	validLevels := map[string]bool{
		"E2E":         true,
		"Integration": true,
		"Datadriven":  true,
		"Unit":        true,
	}

	for _, cat := range categories {
		level, ok := PyramidLevel[cat]
		if !ok {
			t.Errorf("category %q has no pyramid level mapping", cat)
			continue
		}
		if !validLevels[level] {
			t.Errorf("category %q maps to invalid level %q", cat, level)
		}
	}
}
