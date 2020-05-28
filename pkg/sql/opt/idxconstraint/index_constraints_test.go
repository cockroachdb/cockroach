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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
		semaCtx := tree.MakeSemaContext()
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var varTypes []*types.T
			var indexCols []opt.OrderingColumn
			var notNullCols opt.ColSet
			var iVarHelper tree.IndexedVarHelper
			var invertedIndex bool
			var err error

			var f norm.Factory
			f.Init(&evalCtx, nil /* catalog */)
			md := f.Metadata()

			for _, arg := range d.CmdArgs {
				key, vals := arg.Key, arg.Vals
				switch key {
				case "vars":
					varTypes, err = exprgen.ParseTypes(vals)
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

				var ic idxconstraint.Instance
				ic.Init(filters, optionalFilters, indexCols, notNullCols, invertedIndex, &evalCtx, &f)
				result := ic.Constraint()
				var buf bytes.Buffer
				for i := 0; i < result.Spans.Count(); i++ {
					fmt.Fprintf(&buf, "%s\n", result.Spans.Get(i))
				}
				remainingFilter := ic.RemainingFilters()
				if !remainingFilter.IsTrue() {
					execBld := execbuilder.New(nil /* execFactory */, f.Memo(), nil /* catalog */, &remainingFilter, &evalCtx)
					expr, err := execBld.BuildScalar(&iVarHelper)
					if err != nil {
						return fmt.Sprintf("error: %v\n", err)
					}
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

	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			varTypes, err := exprgen.ParseTypes(strings.Split(tc.varTypes, ", "))
			if err != nil {
				b.Fatal(err)
			}
			var f norm.Factory
			f.Init(&evalCtx, nil /* catalog */)
			md := f.Metadata()
			for i, typ := range varTypes {
				md.AddColumn(fmt.Sprintf("@%d", i+1), typ)
			}
			indexCols, notNullCols := parseIndexColumns(b, md, strings.Split(tc.indexInfo, ", "))

			filters, err := buildFilters(tc.expr, &semaCtx, &evalCtx, &f)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var ic idxconstraint.Instance
				ic.Init(filters, nil /* optionalFilters */, indexCols, notNullCols, false /*isInverted */, &evalCtx, &f)
				_ = ic.Constraint()
				_ = ic.RemainingFilters()
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
				notNullCols.Add(opt.ColumnID(id))
				fields = fields[2:]
			default:
				tb.Fatalf("unknown column attribute %s", fields)
			}
		}
	}
	return columns, notNullCols
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
