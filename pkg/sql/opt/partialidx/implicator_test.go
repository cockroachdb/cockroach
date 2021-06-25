// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package partialidx_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// The test files in testdata/predicate support only one command:
//
//   - predtest vars=(var1 type1, var2 type2, ...)"
//
//   The vars argument sets the names and types of the variables in the
//   expressions.
//
//   The test input must be in the format:
//
//      [filter expression]
//      =>
//      [predicate expression]
//
//   The "=>" symbol denotes implication. For example, "a => b" tests if
//   expression a implies expression b.
//
func TestImplicator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/implicator", func(t *testing.T, path string) {
		semaCtx := tree.MakeSemaContext()
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var err error

			var f norm.Factory
			f.Init(&evalCtx, nil /* catalog */)
			md := f.Metadata()

			if d.Cmd != "predtest" {
				d.Fatalf(t, "unsupported command: %s\n", d.Cmd)
			}

			var sv testutils.ScalarVars

			for _, arg := range d.CmdArgs {
				key, vals := arg.Key, arg.Vals
				switch key {
				case "vars":
					err := sv.Init(md, vals)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}

				default:
					d.Fatalf(t, "unknown argument: %s\n", key)
				}
			}

			splitInput := strings.Split(d.Input, "=>")
			if len(splitInput) != 2 {
				d.Fatalf(t, "input format must be: [filters] => [predicate]")
			}

			// Build the filters from the first split, everything before "=>".
			filters, err := makeFilters(splitInput[0], sv.Cols(), &semaCtx, &evalCtx, &f)
			if err != nil {
				d.Fatalf(t, "unexpected error while building filters: %v\n", err)
			}

			// Build the predicate from the second split, everything after "=>".
			pred, err := makeFilters(splitInput[1], sv.Cols(), &semaCtx, &evalCtx, &f)
			if err != nil {
				d.Fatalf(t, "unexpected error while building predicate: %v\n", err)
			}

			im := partialidx.Implicator{}
			im.Init(&f, md, &evalCtx)
			remainingFilters, ok := im.FiltersImplyPredicate(filters, pred)
			if !ok {
				return "false"
			}

			var buf bytes.Buffer
			buf.WriteString("true\n└── remaining filters: ")
			if remainingFilters.IsTrue() {
				buf.WriteString("none")
			} else {
				execBld := execbuilder.New(
					nil /* factory */, f.Memo(), nil /* catalog */, &remainingFilters,
					&evalCtx, false, /* allowAutoCommit */
				)
				expr, err := execBld.BuildScalar()
				if err != nil {
					d.Fatalf(t, "unexpected error: %v\n", err)
				}
				fmtCtx := tree.NewFmtCtx(
					tree.FmtSimple,
					tree.FmtIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
						ctx.WriteString(md.ColumnMeta(opt.ColumnID(idx + 1)).Alias)
					}),
				)
				expr.Format(fmtCtx)
				buf.WriteString(fmtCtx.String())
			}
			return buf.String()
		})
	})
}

func BenchmarkImplicator(b *testing.B) {
	type testCase struct {
		name, vars, filters, pred string
	}
	testCases := []testCase{
		{
			name:    "single-exact-match",
			vars:    "a int",
			filters: "a >= 10",
			pred:    "a >= 10",
		},
		{
			name:    "single-inexact-match",
			vars:    "a int",
			filters: "a >= 10",
			pred:    "a > 5",
		},
		{
			name:    "range-inexact-match",
			vars:    "a int, b int",
			filters: "a >= 10 AND a <= 90",
			pred:    "a > 0 AND a < 100",
		},
		{
			name:    "two-var-comparison",
			vars:    "a int, b int",
			filters: "a > b",
			pred:    "b <= a",
		},
		{
			name:    "single-exact-match-extra-filters",
			vars:    "a int, b int, c int, d int, e int",
			filters: "a < 0 AND b > 0 AND c >= 10 AND d = 4 AND @5 = 5",
			pred:    "c >= 10",
		},
		{
			name:    "single-inexact-match-extra-filters",
			vars:    "a int, b int, c int, d int, e int",
			filters: "a < 0 AND b > 0 AND c >= 10 AND d = 4 AND @5 = 5",
			pred:    "c > 0",
		},
		{
			name:    "multi-column-and-exact-match",
			vars:    "a int, b string",
			filters: "a >= 10 AND b = 'foo'",
			pred:    "a >= 10 AND b = 'foo'",
		},
		{
			name:    "multi-column-and-inexact-match",
			vars:    "a int, b string",
			filters: "a >= 10 AND b = 'foo'",
			pred:    "a >= 0 AND b IN ('foo', 'bar')",
		},
		{
			name:    "multi-column-and-two-var-comparisons",
			vars:    "a int, b int, c int, d int",
			filters: "a > b AND c < d",
			pred:    "b <= a AND c != d",
		},
		{
			name:    "multi-column-or-exact-match",
			vars:    "a int, b string",
			filters: "a >= 10 OR b = 'foo'",
			pred:    "a >= 10 OR b = 'foo'",
		},
		{
			name:    "multi-column-or-exact-match-reverse",
			vars:    "a int, b string",
			filters: "a >= 10 OR b = 'foo'",
			pred:    "b = 'foo' OR a >= 10",
		},
		{
			name:    "multi-column-or-inexact-match",
			vars:    "a int, b string",
			filters: "a >= 10 OR b = 'foo'",
			pred:    "a > 0 OR b IN ('foo', 'bar')",
		},
		{
			name:    "in-implies-or",
			vars:    "a int",
			filters: "a IN (1, 2, 3)",
			pred:    "a = 2 OR a IN (1, 3)",
		},
		{
			name:    "and-filters-do-not-imply-pred",
			vars:    "a int, b int, c int, d int, e string",
			filters: "a < 0 AND b > 10 AND c >= 10 AND d = 4 AND e = 'foo'",
			pred:    "b > 0 AND e = 'foo'",
		},
		{
			name:    "or-filters-do-not-imply-pred",
			vars:    "a int, b int, c int, d int, e string",
			filters: "a < 0 OR b > 10 OR c >= 10 OR d = 4 OR e = 'foo'",
			pred:    "b > 0 OR e = 'foo'",
		},
	}
	// Generate a few test cases with many columns to show how performance
	// scales with respect to the number of columns.
	for _, n := range []int{10, 100} {
		tc := testCase{}
		tc.name = fmt.Sprintf("many-columns-exact-match%d", n)
		for i := 1; i <= n; i++ {
			if i > 1 {
				tc.vars += ", "
				tc.filters += " AND "
				tc.pred += " AND "
			}
			tc.vars += fmt.Sprintf("x%d int", i)
			tc.filters += fmt.Sprintf("x%d = %d", i, i)
			tc.pred += fmt.Sprintf("x%d = %d", i, i)
		}
		testCases = append(testCases, tc)

		tc = testCase{}
		tc.name = fmt.Sprintf("many-columns-inexact-match%d", n)
		for i := 1; i <= n; i++ {
			if i > 1 {
				tc.vars += ", "
				tc.filters += " AND "
				tc.pred += " AND "
			}
			tc.vars += fmt.Sprintf("x%d int", i)
			tc.filters += fmt.Sprintf("x%d > %d", i, i)
			tc.pred += fmt.Sprintf("x%d >= %d", i, i)
		}
		testCases = append(testCases, tc)
	}

	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	for _, tc := range testCases {
		var f norm.Factory
		f.Init(&evalCtx, nil /* catalog */)
		md := f.Metadata()

		// Parse the variable types.
		var sv testutils.ScalarVars
		err := sv.Init(md, strings.Split(tc.vars, ", "))
		if err != nil {
			b.Fatal(err)
		}

		// Build the filters.
		filters, err := makeFilters(tc.filters, sv.Cols(), &semaCtx, &evalCtx, &f)
		if err != nil {
			b.Fatalf("unexpected error while building filters: %v\n", err)
		}

		// Build the predicate.
		pred, err := makeFilters(tc.pred, sv.Cols(), &semaCtx, &evalCtx, &f)
		if err != nil {
			b.Fatalf("unexpected error while building predicate: %v\n", err)
		}

		im := partialidx.Implicator{}
		im.Init(&f, md, &evalCtx)
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Reset the implicator every 10 iterations to simulate its
				// cache being used multiple times during repetitive calls to
				// FiltersImplyPredicate during xform rules.
				if i%10 == 0 {
					im.ClearCache()
				}
				_, _ = im.FiltersImplyPredicate(filters, pred)
			}
		})
	}
}

// makeFilters returns a FiltersExpr generated from the input string that is
// normalized within the context of a Select. By normalizing within a Select,
// rules that only match on Selects are applied, such as SimplifySelectFilters.
// This ensures that these test filters mimic the filters that will be created
// during a real query.
func makeFilters(
	input string,
	cols opt.ColSet,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	f *norm.Factory,
) (memo.FiltersExpr, error) {
	filters, err := makeFiltersExpr(input, semaCtx, evalCtx, f)
	if err != nil {
		return nil, err
	}

	// Create an output set of columns for the fake relation with all the
	// columns in the test case.
	colStatsMap := props.ColStatsMap{}
	cols.ForEach(func(col opt.ColumnID) {
		colStat, _ := colStatsMap.Add(opt.MakeColSet(col))
		colStat.DistinctCount = 100
		colStat.NullCount = 10
	})

	// Create a non-zero cardinality to prevent the fake Select from
	// simplifying into a ValuesExpr.
	card := props.Cardinality{Min: 0, Max: 1}

	// Create stats for the fake relation.
	stats := props.Statistics{
		Available:   true,
		RowCount:    1000,
		ColStats:    colStatsMap,
		Selectivity: props.OneSelectivity,
	}

	// Create a fake Select and input so that normalization rules are run.
	p := &props.Relational{OutputCols: cols, Cardinality: card, Stats: stats}
	fakeRel := f.ConstructFakeRel(&memo.FakeRelPrivate{Props: p})
	sel := f.ConstructSelect(fakeRel, filters)

	// If the normalized relational expression is a Select, return the filters.
	if s, ok := sel.(*memo.SelectExpr); ok {
		return s.Filters, nil
	}

	// Otherwise, the filters may be either true or false. Check the cardinality
	// to determine which one.
	if sel.Relational().Cardinality.IsZero() {
		return memo.FiltersExpr{f.ConstructFiltersItem(memo.FalseSingleton)}, nil
	}

	return memo.TrueFilter, nil
}

// makeFiltersExpr returns a FiltersExpr generated from the input string.
func makeFiltersExpr(
	input string, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, f *norm.Factory,
) (memo.FiltersExpr, error) {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		return nil, err
	}

	b := optbuilder.NewScalar(context.Background(), semaCtx, evalCtx, f)
	if err := b.Build(expr); err != nil {
		return nil, err
	}

	root := f.Memo().RootExpr().(opt.ScalarExpr)

	return memo.FiltersExpr{f.ConstructFiltersItem(root)}, nil
}
