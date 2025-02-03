// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// GenericRulesEnabled returns true if rules for optimizing generic query plans
// are enabled, based on the plan_cache_mode session setting.
func (c *CustomFuncs) GenericRulesEnabled() bool {
	return c.e.evalCtx.SessionData().PlanCacheMode != sessiondatapb.PlanCacheModeForceCustom
}

// HasPlaceholdersOrStableExprs returns true if the given relational expression's subtree has
// at least one placeholder.
func (c *CustomFuncs) HasPlaceholdersOrStableExprs(e memo.RelExpr) bool {
	return e.Relational().HasPlaceholder || e.Relational().VolatilitySet.HasStable()
}

// GenerateParameterizedJoin generates joins to optimize generic query plans
// with unknown placeholder values. See the GenerateParameterizedJoin
// exploration rule description for more details.
func (c *CustomFuncs) GenerateParameterizedJoin(
	grp memo.RelExpr, required *physical.Required, scan *memo.ScanExpr, filters memo.FiltersExpr,
) {
	var exprs memo.ScalarListExpr
	var cols opt.ColList
	placeholderCols := make(map[tree.PlaceholderIdx]opt.ColumnID)

	// replace recursively walks the expression tree and replaces placeholders
	// and stable expressions. It collects the replaced expressions and creates
	// columns representing those expressions. Those expressions and columns
	// will be used in the Values expression created below.
	var replace func(e opt.Expr) opt.Expr
	replace = func(e opt.Expr) opt.Expr {
		switch t := e.(type) {
		case *memo.PlaceholderExpr:
			idx := t.Value.(*tree.Placeholder).Idx
			// Reuse the same column for duplicate placeholder references.
			if col, ok := placeholderCols[idx]; ok {
				return c.e.f.ConstructVariable(col)
			}
			col := c.e.f.Metadata().AddColumn(fmt.Sprintf("$%d", idx+1), t.DataType())
			placeholderCols[idx] = col
			exprs = append(exprs, t)
			cols = append(cols, col)
			return c.e.f.ConstructVariable(col)

		case *memo.FunctionExpr:
			// TODO(mgartner): Consider including other expressions that could
			// be stable: casts, assignment casts, UDFCallExprs, unary ops,
			// comparisons, binary ops.
			// TODO(mgartner): Include functions with arguments if they are all
			// constants or placeholders.
			if t.Overload.Volatility == volatility.Stable && len(t.Args) == 0 {
				col := c.e.f.Metadata().AddColumn("", t.DataType())
				exprs = append(exprs, t)
				cols = append(cols, col)
				return c.e.f.ConstructVariable(col)
			}
		}

		return c.e.f.Replace(e, replace)
	}

	// Replace placeholders and stable expressions in each filter.
	var newFilters memo.FiltersExpr
	for i := range filters {
		cond := filters[i].Condition
		if newCond := replace(cond).(opt.ScalarExpr); newCond != cond {
			if newFilters == nil {
				// Lazily allocate newFilters.
				newFilters = make(memo.FiltersExpr, len(filters))
				copy(newFilters, filters[:i])
			}
			// Construct a new filter if placeholders were replaced.
			newFilters[i] = c.e.f.ConstructFiltersItem(newCond)
		} else if newFilters != nil {
			// Otherwise copy the filter if newFilters has been allocated.
			newFilters[i] = filters[i]
		}
	}

	// If no placeholders or stable expressions were replaced, there is nothing
	// to do.
	if len(exprs) == 0 {
		return
	}

	// Create the Values expression with one row and one column for each
	// replaced expression.
	typs := make([]*types.T, len(exprs))
	for i, e := range exprs {
		typs[i] = e.DataType()
	}
	tupleTyp := types.MakeTuple(typs)
	rows := memo.ScalarListExpr{c.e.f.ConstructTuple(exprs, tupleTyp)}
	values := c.e.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: cols,
		ID:   c.e.f.Metadata().NextUniqueID(),
	})

	// Join the Values expression with the input scan and project away unneeded
	// columns.
	var project memo.ProjectExpr
	project.Input = c.e.f.ConstructInnerJoin(values, scan, newFilters, &memo.JoinPrivate{
		Flags:            memo.DisallowMergeJoin,
		SkipReorderJoins: true,
	})
	project.Passthrough = grp.Relational().OutputCols
	c.e.mem.AddProjectToGroup(&project, grp)
}
