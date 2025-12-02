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
	"github.com/stretchr/testify/require"
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
	require.Contains(t, req.Labels, "O-microbench")
	require.Contains(t, req.Labels, "C-performance")

	// Verify title contains package name
	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}
	title := formatter.Title(data)
	require.Contains(t, title, "pkg/sql")
	require.Contains(t, title, "performance regression")

	// Verify message contains key information
	require.Contains(t, req.Message, "pkg/sql")
	require.Contains(t, req.Message, "BenchmarkScan")
	require.Contains(t, req.Message, "25.5%")
	require.Contains(t, req.Message, "https://docs.google.com/spreadsheets/d/test123")
	require.Contains(t, req.Message, "master -> release-24.1")
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
	require.Contains(t, req.Message, "15 benchmark(s)")

	// Should truncate to 10 and show "... and 5 more"
	require.Contains(t, req.Message, "5 more regression(s)")

	// First regression should be present
	require.Contains(t, req.Message, "Benchmark0")

	// 11th regression should not be listed individually
	require.NotContains(t, req.Message, "Benchmark10")
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
		require.Contains(t, req.Message, fmt.Sprintf("Benchmark%d", i))
	}

	// Should not show truncation message for exactly 10
	require.NotContains(t, req.Message, "more regression(s)")
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
	require.NoError(t, err)

	// Check title format
	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}
	title := formatter.Title(data)

	// Verify title structure
	require.Contains(t, title, "pkg/sql/exec")
	require.Contains(t, title, "performance regression")

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
		require.Contains(t, output, expected)
	}

	// Verify labels are correct
	require.Contains(t, req.Labels, "O-microbench")
	require.Contains(t, req.Labels, "C-performance")
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
