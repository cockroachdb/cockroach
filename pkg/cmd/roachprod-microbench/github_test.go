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
			benchmarkName:  "pkg/sql→BenchmarkScan",
			metricUnit:     "ns/op",
			percentChange:  25.5,
			formattedDelta: "+1.2ms",
			threshold:      20.0,
		},
		{
			benchmarkName:  "pkg/sql→BenchmarkInsert",
			metricUnit:     "ns/op",
			percentChange:  30.0,
			formattedDelta: "+500µs",
			threshold:      20.0,
		},
	}

	formatter, req, err := createRegressionPostRequest(
		"pkg/sql",
		regressions,
		"v24.1.0 -> master",
	)
	require.NoError(t, err)

	// Verify labels
	require.Contains(t, req.Labels, "O-microbench")
	require.Contains(t, req.Labels, "C-performance")
	require.Contains(t, req.Labels, "branch-release-24.1")

	// Verify title is simplified and canonical
	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}
	title := formatter.Title(data)
	require.Equal(t, "pkg/sql: potential performance regression", title)

	// Verify message contains key information
	require.Contains(t, req.Message, "pkg/sql")
	require.Contains(t, req.Message, "BenchmarkScan")
	require.Contains(t, req.Message, "25.5%")
	require.Contains(t, req.Message, "threshold: 20.0%")
	require.Contains(t, req.Message, "v24.1.0 -> master")

	// Verify dashboard links are included (entire line is a link with checkbox)
	require.Contains(t, req.Message, "https://microbench.testeng.crdb.io/dashboard/?benchmark=BenchmarkScan&package=pkg/sql")
	require.Contains(t, req.Message, "- [ ] [pkg/sql→BenchmarkScan (ns/op): +1.2ms (25.5%, threshold: 20.0%)](https://microbench.testeng.crdb.io/dashboard/")
}

func TestCreateRegressionPostRequestMultiple(t *testing.T) {
	// Test with multiple regressions
	regressions := make([]regressionInfo, 15)
	for i := 0; i < 15; i++ {
		regressions[i] = regressionInfo{
			benchmarkName:  fmt.Sprintf("pkg/kv→Benchmark%d", i),
			metricUnit:     "ns/op",
			percentChange:  20.0 + float64(i),
			formattedDelta: fmt.Sprintf("+%dµs", i*100),
			threshold:      20.0,
		}
	}

	_, req, err := createRegressionPostRequest(
		"pkg/kv",
		regressions,
		"baseline -> experiment",
	)
	require.NoError(t, err)

	// Should mention total count
	require.Contains(t, req.Message, "15 benchmark(s)")

	// First regression should be present
	require.Contains(t, req.Message, "Benchmark0")

	// Last regression should be present too
	require.Contains(t, req.Message, "Benchmark14")
}

func TestCreateRegressionPostRequestFormat(t *testing.T) {
	// Test the full formatted output of a regression issue
	regressions := []regressionInfo{
		{
			benchmarkName:  "pkg/sql/exec→BenchmarkScan/rows=1000",
			metricUnit:     "ns/op",
			percentChange:  25.5,
			formattedDelta: "+1.2ms",
			threshold:      20.0,
		},
		{
			benchmarkName:  "pkg/sql/exec→BenchmarkInsert/batch=100",
			metricUnit:     "B/op",
			percentChange:  30.0,
			formattedDelta: "+5.0KB",
			threshold:      15.0,
		},
	}

	formatter, req, err := createRegressionPostRequest(
		"pkg/sql/exec",
		regressions,
		"v24.1.0 -> v24.2.0",
	)
	require.NoError(t, err)

	// Verify branch label is added based on baseline version
	require.Contains(t, req.Labels, "branch-release-24.1")

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

	// Verify title is simplified and canonical (version context not in title)
	require.Equal(t, "pkg/sql/exec: potential performance regression", title)

	// Verify formatted output contains all key elements
	expectedElements := []string{
		"Performance regressions detected in package pkg/sql/exec",
		"Comparison: v24.1.0 -&gt; v24.2.0",
		"BenchmarkScan/rows=1000",
		"(ns/op): +1.2ms (25.5%, threshold: 20.0%)",
		"BenchmarkInsert/batch=100",
		"(B/op): +5.0KB (30.0%, threshold: 15.0%)",
		// Verify entire line is a markdown checklist item with link
		"- [ ] [pkg/sql/exec→BenchmarkScan/rows=1000 (ns/op): +1.2ms (25.5%, threshold: 20.0%)](https://microbench.testeng.crdb.io/dashboard/?benchmark=BenchmarkScan/rows=1000&amp;package=pkg/sql/exec)",
		"- [ ] [pkg/sql/exec→BenchmarkInsert/batch=100 (B/op): +5.0KB (30.0%, threshold: 15.0%)](https://microbench.testeng.crdb.io/dashboard/?benchmark=BenchmarkInsert/batch=100&amp;package=pkg/sql/exec)",
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

func TestGenerateDashboardLink(t *testing.T) {
	tests := []struct {
		benchmarkName string
		expectedLink  string
		description   string
	}{
		{
			benchmarkName: "pkg/sql/exec→BenchmarkScan/rows=1000-8",
			expectedLink:  "https://microbench.testeng.crdb.io/dashboard/?benchmark=BenchmarkScan/rows=1000-8&package=pkg/sql/exec",
			description:   "benchmark with subtest and GOMAXPROCS",
		},
		{
			benchmarkName: "pkg/storage→MVCCGet-32",
			expectedLink:  "https://microbench.testeng.crdb.io/dashboard/?benchmark=MVCCGet-32&package=pkg/storage",
			description:   "benchmark without subtest",
		},
		{
			benchmarkName: "pkg/kv/kvserver→BenchmarkRaftStorage/entries=100/bytes=1024-16",
			expectedLink:  "https://microbench.testeng.crdb.io/dashboard/?benchmark=BenchmarkRaftStorage/entries=100/bytes=1024-16&package=pkg/kv/kvserver",
			description:   "benchmark with multiple subtest params",
		},
		{
			benchmarkName: "pkg/bench/rttanalysis→Truncate/truncate_2_column_1_rows-32",
			expectedLink:  "https://microbench.testeng.crdb.io/dashboard/?benchmark=Truncate/truncate_2_column_1_rows-32&package=pkg/bench/rttanalysis",
			description:   "real-world example from user",
		},
		{
			benchmarkName: "invalid",
			expectedLink:  "",
			description:   "invalid format returns empty string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			result := generateDashboardLink(tt.benchmarkName)
			require.Equal(t, tt.expectedLink, result, "dashboard link mismatch")
		})
	}
}

func TestExtractBranchLabel(t *testing.T) {
	tests := []struct {
		versionContext string
		expectedLabel  string
		description    string
	}{
		{
			versionContext: "v25.4.0 (4f417924) -> refs/heads/master (47a00e3e)",
			expectedLabel:  "branch-release-25.4",
			description:    "version with commit hash to master",
		},
		{
			versionContext: "v24.1.5 -> v24.2.0",
			expectedLabel:  "branch-release-24.1",
			description:    "version to version comparison",
		},
		{
			versionContext: "release-24.1 -> master",
			expectedLabel:  "branch-release-24.1",
			description:    "release branch to master",
		},
		{
			versionContext: "master -> release-24.2",
			expectedLabel:  "",
			description:    "master as baseline (no label)",
		},
		{
			versionContext: "refs/heads/master -> release-24.1",
			expectedLabel:  "",
			description:    "refs/heads/master as baseline (no label)",
		},
		{
			versionContext: "v23.2.0 -> v23.2.1",
			expectedLabel:  "branch-release-23.2",
			description:    "patch version update",
		},
		{
			versionContext: "invalid",
			expectedLabel:  "",
			description:    "invalid format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			result := extractBranchLabel(tt.versionContext)
			require.Equal(t, tt.expectedLabel, result, "branch label mismatch")
		})
	}
}

func TestParseBenchmarkName(t *testing.T) {
	tests := []struct {
		input       string
		wantPkg     string
		wantFunc    string
		expectError bool
		description string
	}{
		{
			input:       "pkg/ccl/changefeedccl/schemafeed→PauseOrResumePolling/not_schema_locked-32",
			wantPkg:     "pkg/ccl/changefeedccl/schemafeed",
			wantFunc:    "BenchmarkPauseOrResumePolling",
			expectError: false,
			description: "benchmark with subtest and GOMAXPROCS",
		},
		{
			input:       "pkg/storage→MVCCGet/batch=false/versions=1/valueSize=8/numRangeKeys=0-32",
			wantPkg:     "pkg/storage",
			wantFunc:    "BenchmarkMVCCGet",
			expectError: false,
			description: "benchmark with multiple subtest params",
		},
		{
			input:       "pkg/util/num32→Scale-32",
			wantPkg:     "pkg/util/num32",
			wantFunc:    "BenchmarkScale",
			expectError: false,
			description: "benchmark without subtest",
		},
		{
			input:       "pkg/raft→Status/members=1/WithProgress-32",
			wantPkg:     "pkg/raft",
			wantFunc:    "BenchmarkStatus",
			expectError: false,
			description: "benchmark with nested subtests",
		},
		{
			input:       "invalid-format",
			wantPkg:     "",
			wantFunc:    "",
			expectError: true,
			description: "invalid format returns error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			gotPkg, gotFunc, err := parseBenchmarkName(tt.input)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "invalid benchmark name format")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantPkg, gotPkg, "package path mismatch")
				require.Equal(t, tt.wantFunc, gotFunc, "function name mismatch")
			}
		})
	}
}

func TestCreateSingleRegressionPostRequestNotify(t *testing.T) {
	reg := regressionInfo{
		benchmarkName:  "pkg/sql→BenchmarkScan/rows=1000-32",
		metricUnit:     "ns/op",
		percentChange:  25.5,
		formattedDelta: "+1.2ms",
		threshold:      20.0,
	}

	formatter, req, err := createSingleRegressionPostRequest(
		reg, "v24.1.0 -> master", false, /* releaseBlocker */
	)
	require.NoError(t, err)

	// Verify labels: should have O-microbench, C-performance, branch label,
	// but NOT release-blocker.
	require.Contains(t, req.Labels, "O-microbench")
	require.Contains(t, req.Labels, "C-performance")
	require.Contains(t, req.Labels, "branch-release-24.1")
	require.NotContains(t, req.Labels, issues.ReleaseBlockerLabel)

	// Verify title
	data := issues.TemplateData{
		PostRequest:      req,
		Parameters:       req.ExtraParams,
		CondensedMessage: issues.CondensedMessage(req.Message),
		PackageNameShort: req.PackageName,
	}
	title := formatter.Title(data)
	require.Equal(t, "pkg/sql: potential performance regression", title)

	// Verify message contains the single benchmark info and threshold
	require.Contains(t, req.Message, "BenchmarkScan/rows=1000-32")
	require.Contains(t, req.Message, "25.5%, threshold: 20.0%")
}

func TestCreateSingleRegressionPostRequestBlocker(t *testing.T) {
	reg := regressionInfo{
		benchmarkName:  "pkg/kv→BenchmarkGet-32",
		metricUnit:     "ns/op",
		percentChange:  40.0,
		formattedDelta: "+2.5ms",
		threshold:      30.0,
	}

	_, req, err := createSingleRegressionPostRequest(
		reg, "v25.1.0 -> master", true, /* releaseBlocker */
	)
	require.NoError(t, err)

	require.Contains(t, req.Labels, issues.ReleaseBlockerLabel)
	require.Contains(t, req.Labels, "O-microbench")
	require.Contains(t, req.Labels, "C-performance")
	require.Contains(t, req.Labels, "branch-release-25.1")

	// Verify threshold is in the message
	require.Contains(t, req.Message, "threshold: 30.0%")
}

func TestCreateSingleRegressionPostRequestFormat(t *testing.T) {
	reg := regressionInfo{
		benchmarkName:  "pkg/sql/exec→BenchmarkHashJoin/rows=10000-32",
		metricUnit:     "ns/op",
		percentChange:  35.2,
		formattedDelta: "+3.1ms",
		threshold:      10.0,
	}

	formatter, req, err := createSingleRegressionPostRequest(
		reg, "v24.2.0 -> v24.3.0", false,
	)
	require.NoError(t, err)

	// Verify message structure
	require.Contains(t, req.Message, "Performance regression detected for benchmark pkg/sql/exec→BenchmarkHashJoin/rows=10000-32")
	require.Contains(t, req.Message, "Comparison: v24.2.0 -> v24.3.0")

	// Verify dashboard link is included
	require.Contains(t, req.Message, "https://microbench.testeng.crdb.io/dashboard/?benchmark=BenchmarkHashJoin/rows=10000-32&package=pkg/sql/exec")

	// Verify the full formatted output includes threshold
	output, err := formatPostRequest(formatter, req)
	require.NoError(t, err)
	require.Contains(t, output, "BenchmarkHashJoin/rows=10000-32")
	require.Contains(t, output, "(ns/op): +3.1ms (35.2%, threshold: 10.0%)")
}
