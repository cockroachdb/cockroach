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

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestBuilder runs data-driven testcases of the form
//   <command> [<args>]...
//   <SQL statement or expression>
//   ----
//   <expected results>
//
// See OptTester.Handle for supported commands. In addition to those, we
// support:
//
//  - build-scalar [args]
//
//    Builds a memo structure from a SQL scalar expression and outputs a
//    representation of the "expression view" of the memo structure.
//
//    The supported args (in addition to the ones supported by OptTester:
//
//      - vars=(type1,type2,...)
//
//        Information about IndexedVar columns.
//
func TestBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		catalog := testcat.New()

		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			var varTypes []*types.T
			var iVarHelper tree.IndexedVarHelper
			var err error

			tester := opttester.New(catalog, d.Input)
			tester.Flags.ExprFormat = memo.ExprFmtHideMiscProps |
				memo.ExprFmtHideConstraints |
				memo.ExprFmtHideFuncDeps |
				memo.ExprFmtHideRuleProps |
				memo.ExprFmtHideStats |
				memo.ExprFmtHideCost |
				memo.ExprFmtHideQualifications

			switch d.Cmd {
			case "build-scalar":
				for _, arg := range d.CmdArgs {
					key, vals := arg.Key, arg.Vals
					switch key {
					case "vars":
						varTypes, err = exprgen.ParseTypes(vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}

						iVarHelper = tree.MakeTypesOnlyIndexedVarHelper(varTypes)

					default:
						if err := tester.Flags.Set(arg); err != nil {
							d.Fatalf(t, "%s", err)
						}
					}
				}

				typedExpr, err := testutils.ParseScalarExpr(d.Input, iVarHelper.Container())
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				ctx := context.Background()
				semaCtx := tree.MakeSemaContext()
				evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

				var o xform.Optimizer
				o.Init(&evalCtx)
				for i, typ := range varTypes {
					o.Memo().Metadata().AddColumn(fmt.Sprintf("@%d", i+1), typ)
				}
				// Disable normalization rules: we want the tests to check the result
				// of the build process.
				o.DisableOptimizations()
				b := optbuilder.NewScalar(ctx, &semaCtx, &evalCtx, o.Factory())
				b.AllowUnsupportedExpr = tester.Flags.AllowUnsupportedExpr
				err = b.Build(typedExpr)
				if err != nil {
					return fmt.Sprintf("error: %s\n", strings.TrimSpace(err.Error()))
				}
				f := memo.MakeExprFmtCtx(tester.Flags.ExprFormat, o.Memo())
				f.FormatExpr(o.Memo().RootExpr())
				return f.Buffer.String()

			default:
				return tester.RunCommand(t, d)
			}
		})
	})
}
