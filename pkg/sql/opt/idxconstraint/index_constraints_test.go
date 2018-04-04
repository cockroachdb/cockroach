// Copyright 2017 The Cockroach Authors.
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

package idxconstraint_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datadriven"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// The test files support only one command:
//
//   - index-constraints [arg | arg=val | arg=(val1,val2, ...)]...
//
//   Takes a scalar expression, builds a memo for it, and computes index
//   constraints. Arguments:
//
//     - vars=(<type>, ...)
//
//       Sets the types for the index vars in the expression.
//
//     - index=(@<index> [ascending|asc|descending|desc] [not null], ...)
//
//       Information for the index (used by index-constraints). Each column of the
//       index refers to an index var.
//
//     - inverted-index=@<index>
//
//       Information about an inverted index (used by index-constraints). The
//       one column of the inverted index refers to an index var. Only one of
//       "index" and "inverted-index" should be used.
//
//     - nonormalize
//
//       Disable the optimizer normalization rules.
//
//     - semtree-normalize
//
//       Run TypedExpr normalization before building the memo.
//
func TestIndexConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		ctx := context.Background()
		semaCtx := tree.MakeSemaContext(false /* privileged */)
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
			var varTypes []types.T
			var indexCols []opt.OrderingColumn
			var notNullCols opt.ColSet
			var iVarHelper tree.IndexedVarHelper
			var invertedIndex bool
			var normalizeTypedExpr bool
			var err error

			f := norm.NewFactory(&evalCtx)
			md := f.Metadata()

			for _, arg := range d.CmdArgs {
				key, vals := arg.Key, arg.Vals
				switch key {
				case "vars":
					varTypes, err = testutils.ParseTypes(vals)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

					iVarHelper = tree.MakeTypesOnlyIndexedVarHelper(varTypes)
					// Set up the columns in the metadata.
					for i, typ := range varTypes {
						md.AddColumn(fmt.Sprintf("@%d", i+1), typ)
					}

				case "index", "inverted-index":
					if varTypes == nil {
						d.Fatalf(t, "vars must precede index")
					}
					indexCols, notNullCols = parseIndexColumns(t, md, vals)
					if key == "inverted-index" {
						if len(indexCols) > 1 {
							d.Fatalf(t, "inverted index must be on a single column")
						}
						invertedIndex = true
					}

				case "nonormalize":
					f.DisableOptimizations()

				case "semtree-normalize":
					normalizeTypedExpr = true

				default:
					d.Fatalf(t, "unknown argument: %s", key)
				}
			}

			switch d.Cmd {
			case "index-constraints":
				typedExpr, err := testutils.ParseScalarExpr(d.Input, iVarHelper.Container())
				if err != nil {
					d.Fatalf(t, "%v", err)
				}

				if normalizeTypedExpr {
					typedExpr, err = evalCtx.NormalizeExpr(typedExpr)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
				}

				varNames := make([]string, len(varTypes))
				for i := range varNames {
					varNames[i] = fmt.Sprintf("@%d", i+1)
				}
				b := optbuilder.NewScalar(ctx, &semaCtx, &evalCtx, f)
				b.AllowUnsupportedExpr = true
				group, err := b.Build(typedExpr)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				ev := memo.MakeNormExprView(f.Memo(), group)

				var ic idxconstraint.Instance
				ic.Init(ev, indexCols, notNullCols, invertedIndex, &evalCtx, f)
				result := ic.Constraint()
				var buf bytes.Buffer
				for i := 0; i < result.Spans.Count(); i++ {
					fmt.Fprintf(&buf, "%s\n", result.Spans.Get(i))
				}
				remainingFilter := ic.RemainingFilter()
				remEv := memo.MakeNormExprView(f.Memo(), remainingFilter)
				if remEv.Operator() != opt.TrueOp {
					execBld := execbuilder.New(nil /* execFactory */, remEv)
					expr := execBld.BuildScalar(&iVarHelper)
					fmt.Fprintf(&buf, "Remaining filter: %s\n", expr)
				}
				return buf.String()

			default:
				d.Fatalf(t, "unsupported command: %s", d.Cmd)
				return ""
			}
		})
	})
}

func BenchmarkIndexConstraints(b *testing.B) {
	type testCase struct {
		name, varTypes, indexInfo, expr string
	}
	testCases := []testCase{
		{
			name:      "point-lookup",
			varTypes:  "int",
			indexInfo: "@1",
			expr:      "@1 = 1",
		},
		{
			name:      "no-constraints",
			varTypes:  "int, int",
			indexInfo: "@2",
			expr:      "@1 = 1",
		},
		{
			name:      "range",
			varTypes:  "int",
			indexInfo: "@1",
			expr:      "@1 >= 1 AND @1 <= 10",
		},
		{
			name:      "range-2d",
			varTypes:  "int, int",
			indexInfo: "@1, @2",
			expr:      "@1 >= 1 AND @1 <= 10 AND @2 >= 1 AND @2 <= 10",
		},
		{
			name:      "many-columns",
			varTypes:  "int, int, int, int, int",
			indexInfo: "@1, @2, @3, @4, @5",
			expr:      "@1 = 1 AND @2 >= 2 AND @2 <= 4 AND (@3, @4, @5) IN ((3, 4, 5), (6, 7, 8))",
		},
	}
	// Generate a few testcases with many columns with single value constraint.
	// This characterizes scaling w.r.t the number of columns.
	for _, n := range []int{10, 100} {
		var tc testCase
		tc.name = fmt.Sprintf("single-jumbo-span-%d", n)
		for i := 1; i <= n; i++ {
			if i > 1 {
				tc.varTypes += ", "
				tc.indexInfo += ", "
				tc.expr += " AND "
			}
			tc.varTypes += "int"
			tc.indexInfo += fmt.Sprintf("@%d", i)
			tc.expr += fmt.Sprintf("@%d=%d", i, i)
		}
		testCases = append(testCases, tc)
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			varTypes, err := testutils.ParseTypes(strings.Split(tc.varTypes, ", "))
			if err != nil {
				b.Fatal(err)
			}
			f := norm.NewFactory(nil /* evalCtx */)
			md := f.Metadata()
			for i, typ := range varTypes {
				md.AddColumn(fmt.Sprintf("@%d", i+1), typ)
			}
			indexCols, notNullCols := parseIndexColumns(b, md, strings.Split(tc.indexInfo, ", "))

			iVarHelper := tree.MakeTypesOnlyIndexedVarHelper(varTypes)
			typedExpr, err := testutils.ParseScalarExpr(tc.expr, iVarHelper.Container())
			if err != nil {
				b.Fatal(err)
			}

			semaCtx := tree.MakeSemaContext(false /* privileged */)
			evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
			bld := optbuilder.NewScalar(context.Background(), &semaCtx, &evalCtx, f)

			group, err := bld.Build(typedExpr)
			if err != nil {
				b.Fatal(err)
			}
			ev := memo.MakeNormExprView(f.Memo(), group)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var ic idxconstraint.Instance
				ic.Init(ev, indexCols, notNullCols, false /*isInverted */, &evalCtx, f)
				_ = ic.Constraint()
				_ = ic.RemainingFilter()
			}
		})
	}
}

// parseIndexColumns parses descriptions of index columns; each
// string corresponds to an index column and is of the form:
//   @id [ascending|asc|descending|desc] [not null]
func parseIndexColumns(
	tb testing.TB, md *opt.Metadata, colStrs []string,
) (columns []opt.OrderingColumn, notNullCols opt.ColSet) {
	columns = make([]opt.OrderingColumn, len(colStrs))
	for i := range colStrs {
		fields := strings.Fields(colStrs[i])
		if fields[0][0] != '@' {
			tb.Fatal("index column must start with @<index>")
		}
		id, err := strconv.Atoi(fields[0][1:])
		if err != nil {
			tb.Fatal(err)
		}
		columns[i] = opt.MakeOrderingColumn(opt.ColumnID(id), false /* descending */)
		fields = fields[1:]
		for len(fields) > 0 {
			switch strings.ToLower(fields[0]) {
			case "ascending", "asc":
				// ascending is the default.
				fields = fields[1:]
			case "descending", "desc":
				columns[i] = -columns[i]
				fields = fields[1:]

			case "not":
				if len(fields) < 2 || strings.ToLower(fields[1]) != "null" {
					tb.Fatalf("unknown column attribute %s", fields)
				}
				notNullCols.Add(id)
				fields = fields[2:]
			default:
				tb.Fatalf("unknown column attribute %s", fields)
			}
		}
	}
	return columns, notNullCols
}
