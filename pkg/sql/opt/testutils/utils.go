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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// ParseType parses a string describing a type.
func ParseType(typeStr string) (types.T, error) {
	colType, err := parser.ParseType(typeStr)
	if err != nil {
		return nil, err
	}
	return coltypes.CastTargetToDatumType(colType), nil
}

// ParseTypes parses a list of types.
func ParseTypes(colStrs []string) ([]types.T, error) {
	res := make([]types.T, len(colStrs))
	for i, s := range colStrs {
		var err error
		res[i], err = ParseType(s)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// ParseScalarExpr parses a scalar expression and converts it to a
// tree.TypedExpr.
func ParseScalarExpr(sql string, ivc tree.IndexedVarContainer) (tree.TypedExpr, error) {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		return nil, err
	}

	sema := tree.MakeSemaContext(false /* privileged */)
	sema.IVarContainer = ivc

	return expr.TypeCheck(&sema, types.Any)
}

// ExecuteTestDDL parses the given DDL SQL statement and creates objects in the
// test catalog. This is used to test without spinning up a cluster.
func ExecuteTestDDL(tb testing.TB, sql string, catalog *TestCatalog) string {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		tb.Fatalf("%v", err)
	}

	if stmt.StatementType() != tree.DDL {
		tb.Fatalf("statement type is not DDL: %v", stmt.StatementType())
	}

	switch stmt := stmt.(type) {
	case *tree.CreateTable:
		tab := catalog.CreateTable(stmt)
		return tab.String()

	default:
		tb.Fatalf("expected CREATE TABLE statement but found: %v", stmt)
		return ""
	}
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
