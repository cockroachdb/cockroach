// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/opttester"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// TestExprIsNeverNull runs data-driven testcases of the form
//   <command> [<args>]...
//   <SQL statement or expression>
//   ----
//   <expected results>
//
// See OptTester.Handle for supported commands. In addition to those, we
// support:
//
//  - scalar-is-not-nullable [args]
//
//    Builds a scalar expression using the input and performs a best-effort
//    check to see if the scalar expression is nullable. It outputs this
//    result as a boolean.
//
//    The supported args (in addition to the ones supported by OptTester):
//
//      - vars=(var1 type1 [not null], var2 type2 [not null],...)
//
func TestExprIsNeverNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/expr", func(t *testing.T, path string) {
		catalog := testcat.New()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tester := opttester.New(catalog, d.Input)
			switch d.Cmd {
			case "scalar-is-not-nullable":
				ctx := context.Background()
				semaCtx := tree.MakeSemaContext()
				evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

				var o xform.Optimizer
				o.Init(&evalCtx, catalog)

				var sv testutils.ScalarVars

				for _, arg := range d.CmdArgs {
					key, vals := arg.Key, arg.Vals
					switch key {
					case "vars":
						err := sv.Init(o.Memo().Metadata(), vals)
						if err != nil {
							d.Fatalf(t, "%v", err)
						}

					default:
						if err := tester.Flags.Set(arg); err != nil {
							d.Fatalf(t, "%s", err)
						}
					}
				}

				expr, err := parser.ParseExpr(d.Input)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				b := optbuilder.NewScalar(ctx, &semaCtx, &evalCtx, o.Factory())
				err = b.Build(expr)
				if err != nil {
					return fmt.Sprintf("error: %s\n", strings.TrimSpace(err.Error()))
				}
				result := memo.ExprIsNeverNull(o.Memo().RootExpr().(opt.ScalarExpr), sv.NotNullCols())
				return fmt.Sprintf("%t\n", result)

			default:
				return tester.RunCommand(t, d)
			}
		})
	})
}
