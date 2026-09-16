// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestPartitionBoundary137994 checks partition metadata comparisons against
// datum ordering, where NULL is a value smaller than every non-NULL value.
// Ordinary SQL tuple comparisons can instead reject a whole prefix when a
// boundary contains NULL, causing partition-derived scans to omit rows.
func TestPartitionBoundary137994(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)

	null, low, high := tree.DNull, tree.NewDInt(-1), tree.NewDInt(1)
	domain := tree.Datums{null, low, high}
	// Enumerate every tuple over this domain, including every NULL position.
	tuples := func(length int) []tree.Datums {
		result := []tree.Datums{nil}
		for range length {
			var next []tree.Datums
			for _, prefix := range result {
				for _, value := range domain {
					next = append(next, append(slices.Clone(prefix), value))
				}
			}
			result = next
		}
		return result
	}
	rows := tuples(3)

	for directions := 0; directions < 8; directions++ {
		columns := []string{"a ASC", "b ASC", "c ASC"}
		for i := range columns {
			if directions&(1<<i) != 0 {
				columns[i] = strings.Replace(columns[i], "ASC", "DESC", 1)
			}
		}
		t.Run(strings.Join(columns, ","), func(t *testing.T) {
			catalog := testcat.New()
			_, err := catalog.ExecuteDDL(fmt.Sprintf(
				"CREATE TABLE t (k INT PRIMARY KEY, a INT, b INT, c INT, INDEX idx (%s))",
				strings.Join(columns, ","),
			))
			if err != nil {
				t.Fatal(err)
			}
			var o Optimizer
			o.Init(ctx, &evalCtx, catalog)
			name := tree.NewUnqualifiedTableName("t")
			tab := catalog.Table(name)
			tabID := o.Memo().Metadata().AddTable(tab, name)
			index := tab.Index(1)
			f, funcs := o.Factory(), &o.explorer.funcs

			makeConstraint := func(filters memo.FiltersExpr) constraint.Constraint {
				ic := funcs.initIdxConstraintForIndex(nil, filters, tabID, index.Ordinal(), 0)
				var result constraint.Constraint
				ic.Constraint(&result)
				return result
			}
			contains := func(cons *constraint.Constraint, row tree.Datums) bool {
				// The primary key is the last column of this non-unique index.
				key := constraint.MakeCompositeKey(append(slices.Clone(row), tree.NewDInt(0))...)
				var point constraint.Span
				point.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
				return cons.ContainsSpan(ctx, &evalCtx, &point)
			}
			matches := func(t *testing.T, expr opt.ScalarExpr, row tree.Datums) bool {
				t.Helper()
				var replace norm.ReplaceFunc
				replace = func(e opt.Expr) opt.Expr {
					if variable, ok := e.(*memo.VariableExpr); ok {
						for i, value := range row {
							if variable.Col == tabID.IndexColumnID(index, i) {
								return f.ConstructConstVal(value, types.Int)
							}
						}
						t.Fatalf("unexpected variable %v", variable.Col)
					}
					return f.Replace(e, replace)
				}
				result := replace(expr)
				switch result.Op() {
				case opt.TrueOp:
					return true
				case opt.FalseOp, opt.NullOp:
					return false
				default:
					t.Fatalf("comparison did not fold to a Boolean: %v", result)
					return false
				}
			}

			for length := 1; length <= 3; length++ {
				for _, boundary := range tuples(length) {
					if !slices.Contains(boundary, null) {
						continue
					}
					for _, comp := range []int{-1, 0, 1} {
						t.Run(fmt.Sprintf("%v/%d", boundary, comp), func(t *testing.T) {
							expr := funcs.columnComparison(tabID, index, boundary, comp)
							cons := makeConstraint(memo.FiltersExpr{f.ConstructFiltersItem(expr)})
							for _, row := range rows {
								cmp := row[:length].Compare(ctx, &evalCtx, boundary)
								want := cmp == 0 && comp == 0 || cmp < 0 && comp < 0 || cmp > 0 && comp > 0
								if got := matches(t, expr, row); got != want {
									t.Fatalf("row %v: expected matching=%v, got %v", row, want, got)
								}
								// Constraints may safely overapproximate scalar predicates,
								// especially when the index has mixed directions.
								if want && !contains(&cons, row) {
									t.Fatalf("constraint %s omits matching row %v", &cons, row)
								}
							}
						})
					}
				}
			}

			// The partition spans and their complement must jointly cover all keys,
			// even for repeated boundaries, nested prefixes, or NULL-free metadata.
			partitionSets := []struct {
				name   string
				values []tree.Datums
			}{
				{"duplicate", []tree.Datums{{high, null}, {high, null}}},
				{"prefix", []tree.Datums{{high}, {high, null}}},
				{"nested-null-prefix", []tree.Datums{{high, null}, {high, null, high}}},
				{"gaps", []tree.Datums{{low, null}, {high, null}}},
				{"leading-null", []tree.Datums{{null, high}, {high, null}}},
				{"non-null-control", []tree.Datums{{low, low}, {high, high}}},
			}
			for _, tc := range partitionSets {
				t.Run(tc.name, func(t *testing.T) {
					partitions := makeConstraint(funcs.inPartitionFilters(tabID, index, slices.Clone(tc.values)))
					between := makeConstraint(funcs.inBetweenFilters(tabID, index, slices.Clone(tc.values)))
					for _, row := range rows {
						if !contains(&partitions, row) && !contains(&between, row) {
							t.Fatalf("partitions %s and complement %s omit row %v", &partitions, &between, row)
						}
					}
				})
			}
		})
	}
}
