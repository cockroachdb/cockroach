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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// HasPlaceholders returns true if the given relational expression's subtree has
// at least one placeholder.
func (c *CustomFuncs) HasPlaceholders(e memo.RelExpr) bool {
	return e.Relational().HasPlaceholder
}

// GeneratePlaceholderValuesAndJoinFilters returns a single-row Values
// expression containing placeholders in the given filters. It also returns a
// new set of filters where the placeholders have been replaced with variables
// referencing the columns produced by the returned Values expression. If the
// given filters have no placeholders, ok=false is returned.
func (c *CustomFuncs) GeneratePlaceholderValuesAndJoinFilters(
	filters memo.FiltersExpr,
) (values memo.RelExpr, newFilters memo.FiltersExpr, ok bool) {
	// Collect all the placeholders in the filters.
	//
	// collectPlaceholders recursively walks the scalar expression and collects
	// placeholder expressions into the placeholders slice.
	var placeholders []*memo.PlaceholderExpr
	var seenIndexes intsets.Fast
	var collectPlaceholders func(e opt.Expr)
	collectPlaceholders = func(e opt.Expr) {
		if p, ok := e.(*memo.PlaceholderExpr); ok {
			idx := int(p.Value.(*tree.Placeholder).Idx)
			// Don't include the same placeholder multiple times.
			if !seenIndexes.Contains(idx) {
				seenIndexes.Add(idx)
				placeholders = append(placeholders, p)
			}
			return
		}
		for i, n := 0, e.ChildCount(); i < n; i++ {
			collectPlaceholders(e.Child(i))
		}
	}

	for i := range filters {
		// Only traverse the scalar expression if it contains a placeholder.
		if filters[i].ScalarProps().HasPlaceholder {
			collectPlaceholders(filters[i].Condition)
		}
	}

	// If there are no placeholders in the filters, there is nothing to do.
	if len(placeholders) == 0 {
		return nil, nil, false
	}

	// Create the Values expression with one row and one column for each
	// placeholder.
	cols := make(opt.ColList, len(placeholders))
	colIDs := make(map[tree.PlaceholderIdx]opt.ColumnID, len(placeholders))
	typs := make([]*types.T, len(placeholders))
	exprs := make(memo.ScalarListExpr, len(placeholders))
	for i, p := range placeholders {
		idx := p.Value.(*tree.Placeholder).Idx
		col := c.e.f.Metadata().AddColumn(fmt.Sprintf("$%d", idx+1), p.DataType())
		cols[i] = col
		colIDs[idx] = col
		exprs[i] = p
		typs[i] = p.DataType()
	}

	tupleTyp := types.MakeTuple(typs)
	rows := memo.ScalarListExpr{c.e.f.ConstructTuple(exprs, tupleTyp)}
	values = c.e.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: cols,
		ID:   c.e.f.Metadata().NextUniqueID(),
	})

	// Create new filters by replacing the placeholders in the filters with
	// variables.
	var replace func(e opt.Expr) opt.Expr
	replace = func(e opt.Expr) opt.Expr {
		if p, ok := e.(*memo.PlaceholderExpr); ok {
			idx := p.Value.(*tree.Placeholder).Idx
			col, ok := colIDs[idx]
			if !ok {
				panic(errors.AssertionFailedf("unknown placeholder %d", idx))
			}
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

// GenericJoinPrivate returns JoinPrivate that disabled join reordering and
// merge join exploration.
func (c *CustomFuncs) GenericJoinPrivate() *memo.JoinPrivate {
	return &memo.JoinPrivate{
		Flags:            memo.DisallowMergeJoin,
		SkipReorderJoins: true,
	}
}
