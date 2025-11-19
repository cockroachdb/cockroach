// Copyright 2025 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	f, err := parseFile(datapathutils.TestDataPath(t, "benchdoc", "bench_test_go"))
	require.NoError(t, err)
	benchmarks, err := analyzeAST(f, "", "bench_test.go")
	require.NoError(t, err)
	ast.Inspect(f, func(n ast.Node) bool {
		return true
	})
	require.Equal(t, []BenchmarkInfo{
		{
			Name: "BenchmarkDocTest",
			Team: "unowned",
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
