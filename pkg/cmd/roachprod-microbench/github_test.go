// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost/issues"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

func TestCreatePostRequest(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "github"), func(t *testing.T, path string) {
		var response cluster.RemoteResponse
		var bk benchmarkKey
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "benchmark":
				d.ScanArgs(t, "name", &bk.name)
				d.ScanArgs(t, "pkg", &bk.pkg)
				d.ScanArgs(t, "args", &response.Args)
				return ""
			case "stdout":
				response.Stdout = d.Input
				return ""
			case "stderr":
				response.Stderr = d.Input
				return ""
			case "post":
				response.Err = errors.New("benchmark failed")
				response.ExitStatus = 1
				response.Duration = time.Second * 10
				response.Metadata = bk
				formatter, req := createBenchmarkPostRequest("", response, false)
				str, err := formatPostRequest(formatter, req)
				if err != nil {
					t.Fatal(err)
				}
				return str
			}
			return ""
		})
	})
}

// formatPostRequest emulates the behavior of the githubpost package.
func TestCreateRegressionPostRequestBasic(t *testing.T) {
	regressions := []regressionInfo{
		{
			benchmarkName:  "BenchmarkScan",
			metricUnit:     "ns/op",
			percentChange:  25.5,
			formattedDelta: "+1.2ms",
		},
		{
			benchmarkName:  "BenchmarkInsert",
			metricUnit:     "ns/op",
			percentChange:  30.0,
			formattedDelta: "+500µs",
		},
	}

	formatter, req := createRegressionPostRequest(
		"pkg/sql",
		regressions,
		"https://docs.google.com/spreadsheets/d/test123",
		"master -> release-24.1",
	)

	// Verify labels
	expectedLabels := []string{"O-microbench", "C-performance"}
	for _, label := range expectedLabels {
		found := false
		for _, l := range req.Labels {
			if l == label {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected label %q not found in %v", label, req.Labels)
		}
	}

	// Verify title contains package name
	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}
	title := formatter.Title(data)
	if !strings.Contains(title, "pkg/sql") {
		t.Errorf("Title should contain package name: %s", title)
	}
	if !strings.Contains(title, "performance regression") {
		t.Errorf("Title should contain 'performance regression': %s", title)
	}

	// Verify message contains key information
	if !strings.Contains(req.Message, "pkg/sql") {
		t.Error("Message should contain package name")
	}
	if !strings.Contains(req.Message, "BenchmarkScan") {
		t.Error("Message should contain benchmark name")
	}
	if !strings.Contains(req.Message, "25.5%") {
		t.Error("Message should contain percentage")
	}
	if !strings.Contains(req.Message, "https://docs.google.com/spreadsheets/d/test123") {
		t.Error("Message should contain sheet link")
	}
	if !strings.Contains(req.Message, "master -> release-24.1") {
		t.Error("Message should contain comparison description")
	}
}

func TestCreateRegressionPostRequestMultiple(t *testing.T) {
	// Test with multiple regressions
	regressions := make([]regressionInfo, 15)
	for i := 0; i < 15; i++ {
		regressions[i] = regressionInfo{
			benchmarkName:  fmt.Sprintf("Benchmark%d", i),
			metricUnit:     "ns/op",
			percentChange:  20.0 + float64(i),
			formattedDelta: fmt.Sprintf("+%dµs", i*100),
		}
	}

	_, req := createRegressionPostRequest(
		"pkg/kv",
		regressions,
		"https://docs.google.com/spreadsheets/d/test456",
		"baseline -> experiment",
	)

	// Should mention total count
	if !strings.Contains(req.Message, "15 benchmark(s)") {
		t.Error("Message should contain total regression count")
	}

	// Should truncate to 10 and show "... and 5 more"
	if !strings.Contains(req.Message, "5 more regression(s)") {
		t.Errorf("Message should indicate truncation. Got: %s", req.Message)
	}

	// First regression should be present
	if !strings.Contains(req.Message, "Benchmark0") {
		t.Error("Message should contain first benchmark")
	}

	// 11th regression should not be listed individually
	if strings.Contains(req.Message, "Benchmark10") {
		t.Error("Message should not list 11th benchmark individually")
	}
}

func TestCreateRegressionPostRequestTruncation(t *testing.T) {
	// Test the exact boundary at 10 regressions
	regressions := make([]regressionInfo, 10)
	for i := 0; i < 10; i++ {
		regressions[i] = regressionInfo{
			benchmarkName:  fmt.Sprintf("Benchmark%d", i),
			metricUnit:     "ns/op",
			percentChange:  20.0,
			formattedDelta: "+100µs",
		}
	}

	_, req := createRegressionPostRequest(
		"pkg/storage",
		regressions,
		"https://docs.google.com/spreadsheets/d/test",
		"test comparison",
	)

	// All 10 should be listed
	for i := 0; i < 10; i++ {
		if !strings.Contains(req.Message, fmt.Sprintf("Benchmark%d", i)) {
			t.Errorf("Message should contain Benchmark%d", i)
		}
	}

	// Should not show truncation message for exactly 10
	if strings.Contains(req.Message, "more regression(s)") {
		t.Error("Should not show truncation message for exactly 10 regressions")
	}
}

func TestCreateRegressionPostRequestFormat(t *testing.T) {
	// Test the full formatted output of a regression issue
	regressions := []regressionInfo{
		{
			benchmarkName:  "BenchmarkScan/rows=1000",
			metricUnit:     "ns/op",
			percentChange:  25.5,
			formattedDelta: "+1.2ms",
		},
		{
			benchmarkName:  "BenchmarkInsert/batch=100",
			metricUnit:     "B/op",
			percentChange:  30.0,
			formattedDelta: "+5.0KB",
		},
	}

	formatter, req := createRegressionPostRequest(
		"pkg/sql/exec",
		regressions,
		"https://docs.google.com/spreadsheets/d/example123",
		"v24.1.0 -> v24.2.0",
	)

	// Verify the full formatted message structure
	output, err := formatPostRequest(formatter, req)
	if err != nil {
		t.Fatalf("Failed to format post request: %v", err)
	}

	// Check title format
	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}
	title := formatter.Title(data)

	// Verify title structure
	if !strings.Contains(title, "pkg/sql/exec") {
		t.Errorf("Title should contain package name, got: %s", title)
	}
	if !strings.Contains(title, "performance regression") {
		t.Errorf("Title should contain 'performance regression', got: %s", title)
	}

	// Verify formatted output contains all key elements
	expectedElements := []string{
		"Performance regressions detected in package pkg/sql/exec",
		"Comparison: v24.1.0 -> v24.2.0",
		"Found 2 benchmark(s) with regressions ≥20%",
		"BenchmarkScan/rows=1000",
		"(ns/op): +1.2ms (25.5%)",
		"BenchmarkInsert/batch=100",
		"(B/op): +5.0KB (30.0%)",
		"Detailed comparison: https://docs.google.com/spreadsheets/d/example123",
	}

	for _, expected := range expectedElements {
		if !strings.Contains(output, expected) {
			t.Errorf("Formatted output should contain %q\nGot:\n%s", expected, output)
		}
	}

	// Verify labels are correct
	if !containsLabel(req.Labels, "O-microbench") {
		t.Errorf("Should have O-microbench label, got: %v", req.Labels)
	}
	if !containsLabel(req.Labels, "C-performance") {
		t.Errorf("Should have C-performance label, got: %v", req.Labels)
	}
}

func containsLabel(labels []string, label string) bool {
	for _, l := range labels {
		if l == label {
			return true
		}
	}
	return false
}

func formatPostRequest(formatter issues.IssueFormatter, req issues.PostRequest) (string, error) {
	// These fields can vary based on the test env so we set them to arbitrary
	// values here.
	req.MentionOnCreate = []string{"@test-eng"}

	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}

	r := &issues.Renderer{}
	if err := formatter.Body(r, data); err != nil {
		return "", err
	}

	var post strings.Builder
	post.WriteString(r.String())

	u, err := url.Parse("https://github.com/cockroachdb/cockroach/issues/new")
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Add("title", formatter.Title(data))
	q.Add("body", post.String())
	// Adding a template parameter is required to be able to view the rendered
	// template on GitHub, otherwise it just takes you to the template selection
	// page.
	q.Add("template", "none")
	u.RawQuery = q.Encode()
	post.WriteString(fmt.Sprintf("Rendered:\n%s", u.String()))

	return post.String(), nil
}
