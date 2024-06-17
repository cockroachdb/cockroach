// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// HasPlaceholdersOrStableExprs returns true if the given relational expression's subtree has
// at least one placeholder.
func (c *CustomFuncs) HasPlaceholdersOrStableExprs(e memo.RelExpr) bool {
	return e.Relational().HasPlaceholder || e.Relational().VolatilitySet.HasStable()
}

// GenerateParameterizedJoinValuesAndFilters returns a single-row Values
// expression containing placeholders and stable expressions in the given
// filters. It also returns a new set of filters where the placeholders and
// stable expressions have been replaced with variables referencing the columns
// produced by the returned Values expression. If the given filters have no
// placeholders or stable expressions, ok=false is returned.
func (c *CustomFuncs) GenerateParameterizedJoinValuesAndFilters(
	filters memo.FiltersExpr,
) (values memo.RelExpr, newFilters memo.FiltersExpr, ok bool) {
	// Collect all the placeholders and stable expressions in the filters.
	//
	// collectExprs recursively walks the scalar expression and collects
	// placeholders  and stable expressions into the exprs slice.
	var exprs memo.ScalarListExpr
	var seenIndexes intsets.Fast
	var collectExprs func(e opt.Expr)
	collectExprs = func(e opt.Expr) {
		switch t := e.(type) {
		case *memo.PlaceholderExpr:
			idx := int(t.Value.(*tree.Placeholder).Idx)
			// Don't include the same placeholder multiple times.
			if !seenIndexes.Contains(idx) {
				seenIndexes.Add(idx)
				exprs = append(exprs, t)
			}

		case *memo.FunctionExpr:
			// TODO(mgartner): Consider including other expressions that could
			// be stable: casts, assignment casts, UDFCallExprs, unary ops,
			// comparisons, binary ops.
			// TODO(mgartner): Include functions with arguments if they are all
			// constants or placeholders.
			if t.Overload.Volatility == volatility.Stable && len(t.Args) == 0 {
				exprs = append(exprs, t)
			}

		default:
			for i, n := 0, e.ChildCount(); i < n; i++ {
				collectExprs(e.Child(i))
			}
		}
	}

	for i := range filters {
		// Only traverse the scalar expression if it contains a placeholder or a
		// stable expression.
		props := filters[i].ScalarProps()
		if props.HasPlaceholder || props.VolatilitySet.HasStable() {
			collectExprs(filters[i].Condition)
		}
	}

	// If there are no placeholders or stable expressions the filters, there is
	// nothing to do.
	if len(exprs) == 0 {
		return nil, nil, false
	}

	// Create the Values expression with one row and one column for each
	// collected expression.
	cols := make(opt.ColList, len(exprs))
	colIDs := make(map[opt.Expr]opt.ColumnID, len(exprs))
	typs := make([]*types.T, len(exprs))
	for i, e := range exprs {
		var col opt.ColumnID
		switch t := e.(type) {
		case *memo.PlaceholderExpr:
			idx := t.Value.(*tree.Placeholder).Idx
			col = c.e.f.Metadata().AddColumn(fmt.Sprintf("$%d", idx+1), t.DataType())
		default:
			col = c.e.f.Metadata().AddColumn(fmt.Sprintf("stable%d", i), t.DataType())
		}
		cols[i] = col
		colIDs[e] = col
		typs[i] = e.DataType()
	}

	tupleTyp := types.MakeTuple(typs)
	rows := memo.ScalarListExpr{c.e.f.ConstructTuple(exprs, tupleTyp)}
	values = c.e.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: cols,
		ID:   c.e.f.Metadata().NextUniqueID(),
	})

	// Create new filters by replacing the placeholders and stable expression in
	// the filters with variables.
	var replace func(e opt.Expr) opt.Expr
	replace = func(e opt.Expr) opt.Expr {
		if col, ok := colIDs[e]; ok {
			return c.e.f.ConstructVariable(col)
		}
		return c.e.f.Replace(e, replace)
	}

	newFilters = make(memo.FiltersExpr, len(filters))
	for i := range newFilters {
		cond := filters[i].Condition
		if newCond := replace(cond).(opt.ScalarExpr); newCond != cond {
			// Construct a new filter if placeholders were replaced.
			newFilters[i] = c.e.f.ConstructFiltersItem(newCond)
		} else {
			// Otherwise copy the filter.
			newFilters[i] = filters[i]
		}
	}

	return values, newFilters, true
}

// ParameterizedJoinPrivate returns JoinPrivate that disabled join reordering and
// merge join exploration.
func (c *CustomFuncs) ParameterizedJoinPrivate() *memo.JoinPrivate {
	return &memo.JoinPrivate{
		Flags:            memo.DisallowMergeJoin,
		SkipReorderJoins: true,
	}
}
