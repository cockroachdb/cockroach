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

package optbuilder_test

// This file is home to TestBuilder, which is similar to the logic tests, except it
// is used for optimizer builder-specific testcases.
//
// Each testfile contains testcases of the form
//   <command> [<args>]...
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
// The supported args are:
//
//  - vars=(type1,type2,...)
//
//    Information about IndexedVar columns.
//
//  - allow-unsupported
//
//    Allows building unsupported scalar expressions into UnsupportedExprOp.

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
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

	for _, path := range testutils.GetTestFiles(t, *testDataGlob) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			catalog := testutils.NewTestCatalog()

			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				var varTypes []types.T
				var iVarHelper tree.IndexedVarHelper
				var allowUnsupportedExpr bool
				var err error

				for _, arg := range d.CmdArgs {
					key, vals := arg.Key, arg.Vals
					switch key {
					case "vars":
						varTypes, err = testutils.ParseTypes(vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}

						iVarHelper = tree.MakeTypesOnlyIndexedVarHelper(varTypes)

					case "allow-unsupported":
						allowUnsupportedExpr = true

					default:
						d.Fatalf(t, "unknown argument: %s", key)
					}
				}

				switch d.Cmd {
				case "build":
					tester := testutils.NewOptTester(catalog, d.Input)
					tester.AllowUnsupportedExpr = allowUnsupportedExpr
					ev, err := tester.OptBuild()
					if err != nil {
						return fmt.Sprintf("error: %s\n", strings.TrimSpace(err.Error()))
					}
					return ev.String()

				case "build-scalar":
					typedExpr, err := testutils.ParseScalarExpr(d.Input, iVarHelper.Container())
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					ctx := context.Background()
					semaCtx := tree.MakeSemaContext(false /* privileged */)
					evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

					varNames := make([]string, len(varTypes))
					for i := range varNames {
						varNames[i] = fmt.Sprintf("@%d", i+1)
					}
					// Disable normalization rules: we want the tests to check the result
					// of the build process.
					o := xform.NewOptimizer(&evalCtx)
					o.MaxSteps = xform.OptimizeNone
					b := optbuilder.NewScalar(
						ctx, &semaCtx, &evalCtx, o.Factory(), varNames, varTypes,
					)
					b.AllowUnsupportedExpr = allowUnsupportedExpr
					group, err := b.Build(typedExpr)
					if err != nil {
						return fmt.Sprintf("error: %s\n", strings.TrimSpace(err.Error()))
					}
					exprView := o.Optimize(group, &memo.PhysicalProps{})
					return exprView.String()

				case "exec-ddl":
					return testutils.ExecuteTestDDL(t, d.Input, catalog)

				default:
					d.Fatalf(t, "unsupported command: %s", d.Cmd)
					return ""
				}
			})
		})
	}
}
