// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package benchdoc

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeGoodBenchmarkDocs(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "good_bench_test_go"))
	require.NoError(t, err)

	benchmarks, err := AnalyzeBenchmarkDocs(f, stubPackageResolver, stubTeamResolver, false, nil)
	require.NoError(t, err)
	require.Equal(t, []BenchmarkInfo{
		{
			Name:    "BenchmarkDocTest",
			Package: "pkg_test",
			Team:    "team_test",
			RunArgs: RunArgs{
				Suite:     SuiteManual,
				Count:     10,
				BenchTime: "50x",
				Timeout:   20 * time.Minute,
			},
			CompareArgs: NewCompareArgs(),
		},
	}, benchmarks)
}

func TestAnalyzeBadBenchmarkDocs(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "bad_bench_test_go"))
	require.NoError(t, err)

	_, err = AnalyzeBenchmarkDocs(f, stubPackageResolver, stubTeamResolver, false, nil)
	require.Error(t, err)
}

func parseFile(filename string) (*ast.File, error) {
	tokenSet := token.NewFileSet()
	f, err := parser.ParseFile(tokenSet, filename, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func TestAnalyzeCompareArgsBenchmarkDocs(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "compare_args_bench_test_go"))
	require.NoError(t, err)

	benchmarks, err := AnalyzeBenchmarkDocs(f, stubPackageResolver, stubTeamResolver, false, nil)
	require.NoError(t, err)
	require.Equal(t, []BenchmarkInfo{
		{
			Name:    "BenchmarkAllArgs",
			Package: "pkg_test",
			Team:    "team_test",
			RunArgs: RunArgs{
				Suite:     SuiteManual,
				Count:     10,
				BenchTime: "50x",
				Timeout:   20 * time.Minute,
			},
			CompareArgs: CompareArgs{
				PostIssue: PostIssueBlocker,
				Threshold: 0.3,
			},
		},
		{
			Name:    "BenchmarkCompareArgsOnly",
			Package: "pkg_test",
			Team:    "team_test",
			RunArgs: NewRunArgs(),
			CompareArgs: CompareArgs{
				PostIssue: PostIssueNone,
			},
		},
		{
			Name:    "BenchmarkPostBlockerOnly",
			Package: "pkg_test",
			Team:    "team_test",
			RunArgs: NewRunArgs(),
			CompareArgs: CompareArgs{
				PostIssue: PostIssueBlocker,
			},
		},
		{
			Name:    "BenchmarkThresholdOnly",
			Package: "pkg_test",
			Team:    "team_test",
			RunArgs: NewRunArgs(),
			CompareArgs: CompareArgs{
				PostIssue: PostIssueNone,
				Threshold: 1.0,
			},
		},
		{
			Name:        "BenchmarkNoDoc",
			Package:     "pkg_test",
			Team:        "team_test",
			RunArgs:     NewRunArgs(),
			CompareArgs: NewCompareArgs(),
		},
	}, benchmarks)
}

func TestAnalyzeBadPostBenchmarkDocs(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "bad_post_bench_test_go"))
	require.NoError(t, err)

	_, err = AnalyzeBenchmarkDocs(f, stubPackageResolver, stubTeamResolver, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid post issue value")
}

func TestAnalyzeBadThresholdBenchmarkDocs(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "bad_threshold_bench_test_go"))
	require.NoError(t, err)

	_, err = AnalyzeBenchmarkDocs(f, stubPackageResolver, stubTeamResolver, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "threshold must be in range")
}

func TestAnalyzeBadThresholdParseBenchmarkDocs(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "bad_threshold_parse_bench_test_go"))
	require.NoError(t, err)

	_, err = AnalyzeBenchmarkDocs(f, stubPackageResolver, stubTeamResolver, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid threshold value")
}

func TestBenchmarkInfoJSONRoundTrip(t *testing.T) {
	original := []BenchmarkInfo{
		{
			Name:    "BenchmarkFoo",
			Package: "pkg/foo",
			Team:    "team-foo",
			RunArgs: RunArgs{
				Suite:     "nightly",
				Count:     5,
				BenchTime: "100x",
				Timeout:   10 * time.Minute,
			},
			CompareArgs: CompareArgs{
				PostIssue: PostIssueBlocker,
				Threshold: 0.5,
			},
		},
		{
			Name:    "BenchmarkBar",
			Package: "pkg/bar",
			Team:    "team-bar",
		},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored []BenchmarkInfo
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	require.Equal(t, original, restored)
}

func stubTeamResolver() (string, error) {
	return "team_test", nil
}

func stubPackageResolver() (string, error) {
	return "pkg_test", nil
}
