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

package optbuilder

// This file is home to TestBuilder, which is similar to the logic tests, except it
// is used for optimizer builder-specific testcases.
//
// Each testfile contains testcases of the form
//   <command>
//   <SQL statement or expression>
//   ----
//   <expected results>
//
// The supported commands are:
//
//  - build
//
//    Builds a memo structure from a SQL query and outputs a representation
//    of the "expression view" of the memo structure.
//
//  - build-scalar
//
//    Builds a memo structure from a SQL scalar expression and outputs a
//    representation of the "expression view" of the memo structure.
//
//  - exec-ddl
//
//    Parses a CREATE TABLE statement, creates a test table, and adds the
//    table to the catalog.
//

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
)

var (
	testDataGlob = flag.String("d", "testdata/[^.]*", "test data glob")
)

func TestBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	paths, err := filepath.Glob(*testDataGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no testfiles found matching: %s", *testDataGlob)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			ctx := context.Background()
			catalog := testutils.NewTestCatalog()

			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				var varTypes []types.T
				var iVarHelper tree.IndexedVarHelper

				for _, arg := range d.CmdArgs {
					key := arg
					val := ""
					if pos := strings.Index(key, "="); pos >= 0 {
						key = arg[:pos]
						val = arg[pos+1:]
					}
					if len(val) > 2 && val[0] == '(' && val[len(val)-1] == ')' {
						val = val[1 : len(val)-1]
					}
					vals := strings.Split(val, ",")
					switch key {
					case "vars":
						varTypes, err = testutils.ParseTypes(vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}

						iVarHelper = tree.MakeTypesOnlyIndexedVarHelper(varTypes)

					default:
						d.Fatalf(t, "unknown argument: %s", key)
					}
				}

				switch d.Cmd {
				case "build":
					stmt, err := parser.ParseOne(d.Input)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					o := xform.NewOptimizer(catalog, xform.OptimizeNone)
					b := NewBuilder(ctx, o.Factory(), stmt)
					root, props, err := b.Build()
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
					exprView := o.Optimize(root, props)
					return exprView.String()

				case "build-scalar":
					typedExpr, err := testutils.ParseScalarExpr(d.Input, &iVarHelper)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					o := xform.NewOptimizer(catalog, xform.OptimizeNone)
					b := newScalarBuilder(o.Factory(), &iVarHelper)

					group, err := buildScalar(b, typedExpr)
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
					exprView := o.Optimize(group, &opt.PhysicalProps{})
					return exprView.String()

				case "exec-ddl":
					stmt, err := parser.ParseOne(d.Input)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					if stmt.StatementType() != tree.DDL {
						d.Fatalf(t, "statement type is not DDL: %v", stmt.StatementType())
					}

					switch stmt := stmt.(type) {
					case *tree.CreateTable:
						tbl := catalog.CreateTable(stmt)
						return tbl.String()

					default:
						d.Fatalf(t, "expected CREATE TABLE statement but found: %v", stmt)
						return ""
					}

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}

// newScalarBuilder constructs a Builder to be used for building scalar
// expressions.
func newScalarBuilder(factory opt.Factory, ivh *tree.IndexedVarHelper) *Builder {
	b := &Builder{factory: factory, colMap: make([]columnProps, 1)}

	b.semaCtx.IVarHelper = ivh
	b.semaCtx.Placeholders = tree.MakePlaceholderInfo()

	return b
}

// buildScalar is a wrapper for Builder.buildScalar which catches panics and
// converts those containing builderErrors back to errors.
func buildScalar(b *Builder, typedExpr tree.TypedExpr) (group opt.GroupID, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(builderError); ok {
				err = r.(builderError)
			} else {
				panic(r)
			}
		}
	}()

	group = b.buildScalar(typedExpr, &scope{builder: b})
	return group, err
}
