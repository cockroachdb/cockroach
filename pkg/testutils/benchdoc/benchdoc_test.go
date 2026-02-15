// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package benchdoc

import (
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
			Args: RunArgs{
				Suite:     "manual",
				Count:     10,
				BenchTime: "50x",
				Timeout:   20 * time.Minute,
			},
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

func stubTeamResolver() (string, error) {
	return "team_test", nil
}

func stubPackageResolver() (string, error) {
	return "pkg_test", nil
}
