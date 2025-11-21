// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/codeowners"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeBenchmark(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "benchdoc", "bench_test_go"))
	require.NoError(t, err)
	co, err := codeowners.DefaultLoadCodeOwners()
	require.NoError(t, err)
	benchmarks, err := analyzeBenchmarkAST(co, f, "/abc/pkg", "/abc/pkg/some_pkg/sub_pkg/bench_test_go")
	require.NoError(t, err)
	require.Equal(t, []BenchmarkInfo{
		{
			Name:    "BenchmarkDocTest",
			Package: "pkg/some_pkg/sub_pkg",
			Team:    "unowned",
			Args: RunArgs{
				Suite:     "manual",
				Count:     10,
				BenchTime: "50x",
				Timeout:   20 * time.Minute,
			},
		},
	}, benchmarks)
}

func parseFile(filename string) (*ast.File, error) {
	tokenSet := token.NewFileSet()
	f, err := parser.ParseFile(tokenSet, filename, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	return f, nil
}
