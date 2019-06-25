// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ParseScalarExpr parses a scalar expression and converts it to a
// tree.TypedExpr.
func ParseScalarExpr(sql string, ivc tree.IndexedVarContainer) (tree.TypedExpr, error) {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		return nil, err
	}

	sema := tree.MakeSemaContext()
	sema.IVarContainer = ivc

	return expr.TypeCheck(&sema, types.Any)
}

// GetTestFiles returns the set of test files that matches the Glob pattern.
func GetTestFiles(tb testing.TB, testdataGlob string) []string {
	paths, err := filepath.Glob(testdataGlob)
	if err != nil {
		tb.Fatal(err)
	}
	if len(paths) == 0 {
		tb.Fatalf("no testfiles found matching: %s", testdataGlob)
	}
	return paths
}

var _ = GetTestFiles
