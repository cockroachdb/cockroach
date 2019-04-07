// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
