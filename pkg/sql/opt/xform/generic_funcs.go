// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
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

// HasPlaceholders returns true if the given relational expression's subtree has
// at least one placeholder.
func (c *CustomFuncs) HasPlaceholders(e memo.RelExpr) bool {
	return e.Relational().HasPlaceholder
}

// HasPlaceholdersOrStableExprs returns true if the given relational expression's subtree has
// at least one placeholder.
func (c *CustomFuncs) HasPlaceholdersOrStableExprs(e memo.RelExpr) bool {
	return e.Relational().HasPlaceholder || e.Relational().VolatilitySet.HasStable()
}

// GeneratedParameterizedIndexJoin returns true if a parameterized index join
// was not generated for the given expression's group by
// GenerateParameterizedIndexJoin.
func (c *CustomFuncs) GeneratedParameterizedIndexJoin(e memo.RelExpr) bool {
	return e.Relational().IsAvailable(props.GeneratedParameterizedIndexJoin)
}

// GenerateParameterizedIndexJoin returns a single-row Values expression
// containing placeholders and an IndexJoinPrivate to construct and IndexJoin.
// The given filters must constrain all PK columns to placeholder values and must
// not constrain any other columns.
//
// TODO(mgartner): Expand this rule to allow filters that constrain PK columns to
// stable expressions and filters that constrain more than just the PK columns.
// the given filters have no placeholders or stable expressions, ok=false is
// returned.
func (c *CustomFuncs) GenerateParameterizedIndexJoin(
	grp memo.RelExpr, sp *memo.ScanPrivate, filters memo.FiltersExpr,
) (values memo.RelExpr, private *memo.IndexJoinPrivate, ok bool) {
	var pkCols opt.ColSet
	tab := c.e.mem.Metadata().Table(sp.Table)
	pkIndex := tab.Index(cat.PrimaryIndex)
	for i, n := 0, pkIndex.KeyColumnCount(); i < n; i++ {
		col := sp.Table.IndexColumnID(pkIndex, i)
		pkCols.Add(col)
	}

	// Every filter must constrain a PK column to a placeholder.
	var exprs memo.ScalarListExpr
	var cols opt.ColList
	var eqCols opt.ColSet
	for i := range filters {
		eq, ok := filters[i].Condition.(*memo.EqExpr)
		if !ok {
			return nil, nil, false
		}
		v, ok := eq.Left.(*memo.VariableExpr)
		if !ok {
			return nil, nil, false
		}
		if !pkCols.Contains(v.Col) {
			return nil, nil, false
		}
		if _, ok = eq.Right.(*memo.PlaceholderExpr); !ok {
			return nil, nil, false
		}
		eqCols.Add(v.Col)
		cols = append(cols, v.Col)
		exprs = append(exprs, eq.Right)
	}

	// Every PK column must be constrained.
	if !eqCols.Equals(pkCols) {
		return nil, nil, false
	}

	// Create the Values expression with one row and one column for each PK
	// column.
	typs := make([]*types.T, len(exprs))
	for i, e := range exprs {
		typs[i] = e.DataType()
	}
	tupleTyp := types.MakeTuple(typs)
	rows := memo.ScalarListExpr{c.e.f.ConstructTuple(exprs, tupleTyp)}
	values = c.e.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: cols,
		ID:   c.e.f.Metadata().NextUniqueID(),
	})

	private = &memo.IndexJoinPrivate{
		Table:   sp.Table,
		Cols:    sp.Cols,
		Locking: sp.Locking,
	}

	// Set a rule prop to prevent GenerateParameterizedJoin from firing.
	grp.Relational().SetAvailable(props.GeneratedParameterizedIndexJoin)

	return values, private, true
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
		return nil, nil, false
	}

	// Create the Values expression with one row and one column for each
	// replaced expression.
	typs := make([]*types.T, len(exprs))
	for i, e := range exprs {
		typs[i] = e.DataType()
	}
	tupleTyp := types.MakeTuple(typs)
	rows := memo.ScalarListExpr{c.e.f.ConstructTuple(exprs, tupleTyp)}
	values = c.e.f.ConstructValues(rows, &memo.ValuesPrivate{
		Cols: cols,
		ID:   c.e.f.Metadata().NextUniqueID(),
	})

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
