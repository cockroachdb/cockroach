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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// The test files in testdata/predicate support only one command:
//
//   - predtest vars=(type1,type2, ...)
//
//   The vars argument sets the type of the variables (e.g. @1, @2) in the
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
			var varTypes []*types.T
			var cols opt.ColSet
			var iVarHelper tree.IndexedVarHelper
			var err error

			var f norm.Factory
			f.Init(&evalCtx, nil /* catalog */)
			md := f.Metadata()

			if d.Cmd != "predtest" {
				d.Fatalf(t, "unsupported command: %s\n", d.Cmd)
			}

			for _, arg := range d.CmdArgs {
				key, vals := arg.Key, arg.Vals
				switch key {
				case "vars":
					varTypes, err = exprgen.ParseTypes(vals)
					if err != nil {
						d.Fatalf(t, "failed to parse vars%v\n", err)
					}

					iVarHelper = tree.MakeTypesOnlyIndexedVarHelper(varTypes)

					// Add the columns to the metadata.
					cols = addColumnsToMetadata(varTypes, md)

				default:
					d.Fatalf(t, "unknown argument: %s\n", key)
				}
			}

			splitInput := strings.Split(d.Input, "=>")
			if len(splitInput) != 2 {
				d.Fatalf(t, "input format must be: [filters] => [predicate]")
			}

			// Build the filters from the first split, everything before "=>".
			filters, err := makeFilters(splitInput[0], cols, &semaCtx, &evalCtx, &f)
			if err != nil {
				d.Fatalf(t, "unexpected error while building filters: %v\n", err)
			}

			// Build the predicate from the second split, everything after "=>".
			pred, err := makePredicate(splitInput[1], &semaCtx, &evalCtx, &f)
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
				execBld := execbuilder.New(nil /* factory */, f.Memo(), nil /* catalog */, &remainingFilters, &evalCtx)
				expr, err := execBld.BuildScalar(&iVarHelper)
				if err != nil {
					d.Fatalf(t, "unexpected error: %v\n", err)
				}
				buf.WriteString(expr.String())
			}
			return buf.String()
		})
	})
}

func BenchmarkImplicator(b *testing.B) {
	type testCase struct {
		name, varTypes, filters, pred string
	}
	testCases := []testCase{
		{
			name:     "single-exact-match",
			varTypes: "int",
			filters:  "@1 >= 10",
			pred:     "@1 >= 10",
		},
		{
			name:     "single-inexact-match",
			varTypes: "int",
			filters:  "@1 >= 10",
			pred:     "@1 > 5",
		},
		{
			name:     "range-inexact-match",
			varTypes: "int, int",
			filters:  "@1 >= 10 AND @1 <= 90",
			pred:     "@1 > 0 AND @1 < 100",
		},
		{
			name:     "single-exact-match-extra-filters",
			varTypes: "int, int, int, int, int",
			filters:  "@1 < 0 AND @2 > 0 AND @3 >= 10 AND @4 = 4 AND @5 = 5",
			pred:     "@3 >= 10",
		},
		{
			name:     "single-inexact-match-extra-filters",
			varTypes: "int, int, int, int, int",
			filters:  "@1 < 0 AND @2 > 0 AND @3 >= 10 AND @4 = 4 AND @5 = 5",
			pred:     "@3 > 0",
		},
		{
			name:     "multi-column-and-exact-match",
			varTypes: "int, string",
			filters:  "@1 >= 10 AND @2 = 'foo'",
			pred:     "@1 >= 10 AND @2 = 'foo'",
		},
		{
			name:     "multi-column-and-inexact-match",
			varTypes: "int, string",
			filters:  "@1 >= 10 AND @2 = 'foo'",
			pred:     "@1 >= 0 AND @2 IN ('foo', 'bar')",
		},
		{
			name:     "multi-column-or-exact-match",
			varTypes: "int, string",
			filters:  "@1 >= 10 OR @2 = 'foo'",
			pred:     "@1 >= 10 OR @2 = 'foo'",
		},
		{
			name:     "multi-column-or-exact-match-reverse",
			varTypes: "int, string",
			filters:  "@1 >= 10 OR @2 = 'foo'",
			pred:     "@2 = 'foo' OR @1 >= 10",
		},
		{
			name:     "multi-column-or-inexact-match",
			varTypes: "int, string",
			filters:  "@1 >= 10 OR @2 = 'foo'",
			pred:     "@1 > 0 OR @2 IN ('foo', 'bar')",
		},
		{
			name:     "and-filters-do-not-imply-pred",
			varTypes: "int, int, int, int, string",
			filters:  "@1 < 0 AND @2 > 10 AND @3 >= 10 AND @4 = 4 AND @5 = 'foo'",
			pred:     "@2 > 0 AND @5 = 'foo'",
		},
		{
			name:     "or-filters-do-not-imply-pred",
			varTypes: "int, int, int, int, string",
			filters:  "@1 < 0 OR @2 > 10 OR @3 >= 10 OR @4 = 4 OR @5 = 'foo'",
			pred:     "@2 > 0 OR @5 = 'foo'",
		},
	}
	// Generate a few test cases with many columns to show how performance
	// scales with respect to the number of columns.
	for _, n := range []int{10, 100} {
		tc := testCase{}
		tc.name = fmt.Sprintf("many-columns-exact-match%d", n)
		for i := 1; i <= n; i++ {
			if i > 1 {
				tc.varTypes += ", "
				tc.filters += " AND "
				tc.pred += " AND "
			}
			tc.varTypes += "int"
			tc.filters += fmt.Sprintf("@%d = %d", i, i)
			tc.pred += fmt.Sprintf("@%d = %d", i, i)
		}
		testCases = append(testCases, tc)

		tc = testCase{}
		tc.name = fmt.Sprintf("many-columns-inexact-match%d", n)
		for i := 1; i <= n; i++ {
			if i > 1 {
				tc.varTypes += ", "
				tc.filters += " AND "
				tc.pred += " AND "
			}
			tc.varTypes += "int"
			tc.filters += fmt.Sprintf("@%d > %d", i, i)
			tc.pred += fmt.Sprintf("@%d >= %d", i, i)
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
		varTypes, err := exprgen.ParseTypes(strings.Split(tc.varTypes, ", "))
		if err != nil {
			b.Fatal(err)
		}

		// Add the variables to the metadata.
		cols := addColumnsToMetadata(varTypes, md)

		// Build the filters.
		filters, err := makeFilters(tc.filters, cols, &semaCtx, &evalCtx, &f)
		if err != nil {
			b.Fatalf("unexpected error while building filters: %v\n", err)
		}

		// Build the predicate.
		pred, err := makePredicate(tc.pred, &semaCtx, &evalCtx, &f)
		if err != nil {
			b.Fatalf("unexpected error while building predicate: %v\n", err)
		}

		im := partialidx.Implicator{}
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Reset the implicator every 10 iterations to simulate its
				// cache being used multiple times during repetitive calls to
				// FiltersImplyPredicate during xform rules.
				if i%10 == 0 {
					im.Init(&f, md, &evalCtx)
				}
				_, _ = im.FiltersImplyPredicate(filters, pred)
			}
		})
	}
}

// addColumnsToMetadata adds a new column to the metadata for each type in the
// given list. It returns the set of column IDs that were added.
func addColumnsToMetadata(varTypes []*types.T, md *opt.Metadata) opt.ColSet {
	cols := opt.ColSet{}
	for i, typ := range varTypes {
		col := md.AddColumn(fmt.Sprintf("@%d", i+1), typ)
		cols.Add(col)
	}
	return cols
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
		Selectivity: 1,
	}

	// Create a fake Select and input so that normalization rules are run.
	p := &props.Relational{OutputCols: cols, Cardinality: card, Stats: stats}
	fakeRel := f.ConstructFakeRel(&memo.FakeRelPrivate{Props: p})
	sel := f.ConstructSelect(fakeRel, filters)

	// If the normalized relational expression is a Select, return the filters.
	if s, ok := sel.(*memo.SelectExpr); ok {
		return s.Filters, nil
	}

	// If the resulting relational expression is not a Select, the Select has
	// been eliminated by normalization rules. This can occur for certain
	// filters, such as "true". We still want to test these cases, so we normalize
	// the filters in the same way that predicate expressions are normalized.
	return f.NormalizePartialIndexPredicate(filters), nil
}

// makePredicate returns a FiltersExpr generated from the input string and
// normalized in the same way that optbuilder normalizes partial index
// predicates.
func makePredicate(
	input string, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, f *norm.Factory,
) (memo.FiltersExpr, error) {
	filters, err := makeFiltersExpr(input, semaCtx, evalCtx, f)
	if err != nil {
		return nil, err
	}

	// Normalize the predicate expression as it is in select.go.
	return f.NormalizePartialIndexPredicate(filters), nil
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
