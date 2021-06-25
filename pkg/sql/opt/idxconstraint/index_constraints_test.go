// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxconstraint_test

import (
	"bytes"
	"context"
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// The test files support only one command:
//
//   - index-constraints [arg | arg=val | arg=(val1,val2, ...)]...
//
//   Takes a scalar expression, builds a memo for it, and computes index
//   constraints. Arguments:
//
//     - vars=(<column> <type> [not null], ...)
//
//       Information about the columns.
//
//     - index=(<column> [ascending|asc|descending|desc], ...)
//
//       Information for the index (used by index-constraints).
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
		semaCtx := tree.MakeSemaContext()
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var sv testutils.ScalarVars
			var indexCols []opt.OrderingColumn
			var err error

			var f norm.Factory
			f.Init(&evalCtx, nil /* catalog */)
			md := f.Metadata()

			for _, arg := range d.CmdArgs {
				key, vals := arg.Key, arg.Vals
				switch key {
				case "vars":
					err = sv.Init(md, vals)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

				case "index":
					if sv.Cols().Empty() {
						d.Fatalf(t, "vars must precede index")
					}
					indexCols = parseIndexColumns(t, md, vals)

				case "nonormalize":
					f.DisableOptimizations()

				default:
					d.Fatalf(t, "unknown argument: %s", key)
				}
			}

			switch d.Cmd {
			case "index-constraints":
				// Allow specifying optional filters using the "optional:" delimiter.
				var filters, optionalFilters memo.FiltersExpr
				if idx := strings.Index(d.Input, "optional:"); idx >= 0 {
					optional := d.Input[idx+len("optional:"):]
					optionalFilters, err = buildFilters(optional, &semaCtx, &evalCtx, &f)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					d.Input = d.Input[:idx]
				}
				if filters, err = buildFilters(d.Input, &semaCtx, &evalCtx, &f); err != nil {
					d.Fatalf(t, "%v", err)
				}

				var computedCols map[opt.ColumnID]opt.ScalarExpr
				if sv.ComputedCols() != nil {
					computedCols = make(map[opt.ColumnID]opt.ScalarExpr)
					for col, expr := range sv.ComputedCols() {
						b := optbuilder.NewScalar(context.Background(), &semaCtx, &evalCtx, &f)
						if err := b.Build(expr); err != nil {
							d.Fatalf(t, "error building computed column expression: %v", err)
						}
						computedCols[col] = f.Memo().RootExpr().(opt.ScalarExpr)
					}
				}

				var ic idxconstraint.Instance
				ic.Init(
					filters, optionalFilters, indexCols, sv.NotNullCols(), computedCols,
					true /* consolidate */, &evalCtx, &f,
				)
				result := ic.Constraint()
				var buf bytes.Buffer
				for i := 0; i < result.Spans.Count(); i++ {
					fmt.Fprintf(&buf, "%s\n", result.Spans.Get(i))
				}
				remainingFilter := ic.RemainingFilters()
				if !remainingFilter.IsTrue() {
					execBld := execbuilder.New(
						nil /* execFactory */, f.Memo(), nil /* catalog */, &remainingFilter,
						&evalCtx, false, /* allowAutoCommit */
					)
					expr, err := execBld.BuildScalar()
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
					fmtCtx := tree.NewFmtCtx(
						tree.FmtSimple,
						tree.FmtIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
							ctx.WriteString(md.ColumnMeta(opt.ColumnID(idx + 1)).Alias)
						}),
					)
					expr.Format(fmtCtx)
					fmt.Fprintf(&buf, "Remaining filter: %s\n", fmtCtx.String())
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
		name, vars, indexInfo, expr string
	}
	testCases := []testCase{
		{
			name:      "point-lookup",
			vars:      "a int",
			indexInfo: "a",
			expr:      "a = 1",
		},
		{
			name:      "no-constraints",
			vars:      "a int, b int",
			indexInfo: "b",
			expr:      "a = 1",
		},
		{
			name:      "range",
			vars:      "a int",
			indexInfo: "a",
			expr:      "a >= 1 AND a <= 10",
		},
		{
			name:      "range-2d",
			vars:      "a int, b int",
			indexInfo: "a, b",
			expr:      "a >= 1 AND a <= 10 AND b >= 1 AND b <= 10",
		},
		{
			name:      "many-columns",
			vars:      "a int, b int, c int, d int, e int",
			indexInfo: "a, b, c, d, e",
			expr:      "a = 1 AND b >= 2 AND b <= 4 AND (c, d, e) IN ((3, 4, 5), (6, 7, 8))",
		},
	}
	// Generate a few testcases with many columns with single value constraint.
	// This characterizes scaling w.r.t the number of columns.
	for _, n := range []int{10, 100} {
		var tc testCase
		tc.name = fmt.Sprintf("single-jumbo-span-%d", n)
		for i := 1; i <= n; i++ {
			if i > 1 {
				tc.vars += ", "
				tc.indexInfo += ", "
				tc.expr += " AND "
			}
			tc.vars += fmt.Sprintf("x%d int", i)
			tc.indexInfo += fmt.Sprintf("x%d", i)
			tc.expr += fmt.Sprintf("x%d=%d", i, i)
		}
		testCases = append(testCases, tc)
	}

	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			var f norm.Factory
			f.Init(&evalCtx, nil /* catalog */)
			md := f.Metadata()
			var sv testutils.ScalarVars
			err := sv.Init(md, strings.Split(tc.vars, ", "))
			if err != nil {
				b.Fatal(err)
			}
			indexCols := parseIndexColumns(b, md, strings.Split(tc.indexInfo, ", "))

			filters, err := buildFilters(tc.expr, &semaCtx, &evalCtx, &f)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var ic idxconstraint.Instance
				ic.Init(
					filters, nil /* optionalFilters */, indexCols, sv.NotNullCols(),
					nil /* computedCols */, true, /* consolidate */
					&evalCtx, &f,
				)
				_ = ic.Constraint()
				_ = ic.RemainingFilters()
			}
		})
	}
}

// parseIndexColumns parses descriptions of index columns; each
// string corresponds to an index column and is of the form:
//   @id [ascending|asc|descending|desc] [not null]
func parseIndexColumns(tb testing.TB, md *opt.Metadata, colStrs []string) []opt.OrderingColumn {
	findCol := func(alias string) opt.ColumnID {
		for i := 0; i < md.NumColumns(); i++ {
			id := opt.ColumnID(i + 1)
			if md.ColumnMeta(id).Alias == alias {
				return id
			}
		}
		tb.Fatalf("unknown column %s", alias)
		return 0
	}

	columns := make([]opt.OrderingColumn, len(colStrs))
	for i := range colStrs {
		fields := strings.Fields(colStrs[i])
		id := findCol(fields[0])
		columns[i] = opt.MakeOrderingColumn(id, false /* descending */)
		fields = fields[1:]
		for len(fields) > 0 {
			switch strings.ToLower(fields[0]) {
			case "ascending", "asc":
				// ascending is the default.
				fields = fields[1:]
			case "descending", "desc":
				columns[i] = -columns[i]
				fields = fields[1:]

			default:
				tb.Fatalf("unknown column attribute %s", fields)
			}
		}
	}
	return columns
}

func buildFilters(
	input string, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, f *norm.Factory,
) (memo.FiltersExpr, error) {
	if input == "" {
		return memo.TrueFilter, nil
	}
	expr, err := parser.ParseExpr(input)
	if err != nil {
		return memo.FiltersExpr{}, err
	}
	b := optbuilder.NewScalar(context.Background(), semaCtx, evalCtx, f)
	if err := b.Build(expr); err != nil {
		return memo.FiltersExpr{}, err
	}
	root := f.Memo().RootExpr().(opt.ScalarExpr)
	if _, ok := root.(*memo.TrueExpr); ok {
		return memo.TrueFilter, nil
	}
	return memo.FiltersExpr{f.ConstructFiltersItem(root)}, nil
}
