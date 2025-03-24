// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// GenericRulesEnabled returns true if rules for optimizing generic query plans
// are enabled, based on the plan_cache_mode session setting.
func (c *CustomFuncs) GenericRulesEnabled() bool {
	return c.e.evalCtx.SessionData().PlanCacheMode != sessiondatapb.PlanCacheModeForceCustom
}

// HasPlaceholdersOrStableExprs returns true if the given relational expression's subtree has
// at least one placeholder.
// TODO: Move this.
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

func (c *CustomFuncs) PlaceholderScanSpanAndPrivate(
	join *memo.LookupJoinExpr, values *memo.ValuesExpr, row memo.ScalarListExpr,
) (span memo.ScalarListExpr, private *memo.ScanPrivate, ok bool) {
	// TODO: Check for other things we want to prevent.

	// The lookup join must only have key columns.
	if join.LookupExpr != nil {
		return nil, nil, false
	}
	if join.RemoteLookupExpr != nil {
		return nil, nil, false
	}

	// The lookup join must not have any post-lookup filters.
	if join.On != nil {
		return nil, nil, false
	}

	private = &memo.ScanPrivate{
		Table: join.Table,
		Index: join.Index,
		Cols:  join.Relational().OutputCols,
		// Constraint:               nil,
		// InvertedConstraint:       nil,
		// HardLimit:                0,
		// Distribution:             physical.Distribution{},
		// Flags:                    memo.ScanFlags{},
		// Locking:                  opt.Locking{},
		// LocalityOptimized:        false,
		// PartitionConstrainedScan: false,
		// ExactPrefix:              0,
	}

	span = make(memo.ScalarListExpr, len(join.KeyCols))
	for i, keyCol := range join.KeyCols {
		// TODO: Assertion that we found all the columns?
		for j, valCol := range values.Cols {
			if keyCol == valCol {
				span[i] = row[j]
				// TODO: Do I need a type check lke veryifType?
				break
			}
		}
		if span[i] == nil {
			panic(errors.AssertionFailedf("no value for span[%d]", i))
		}
	}

	return span, private, true

	// for i := range span {
	// 	col := tabMeta.MetaID.ColumnID(foundIndex.Column(i).Ordinal())
	// 	for j := range sel.Filters {
	// 		eq := sel.Filters[j].Condition.(*memo.EqExpr)
	// 		if v := eq.Left.(*memo.VariableExpr); v.Col == col {
	// 			if !verifyType(o.mem.Metadata(), col, eq.Right.DataType()) {
	// 				return false, nil
	// 			}
	// 			span[i] = eq.Right
	// 			break
	// 		}
	// 	}
	// 	if span[i] == nil {
	// 		// We checked above that the constrained columns match the index prefix.
	// 		return false, errors.AssertionFailedf("no span value")
	// 	}
	// }
	// placeholderScan := &memo.PlaceholderScanExpr{
	// 	Span:        span,
	// 	ScanPrivate: newPrivate,
	// }
	// placeholderScan = o.mem.AddPlaceholderScanToGroup(placeholderScan, root)
	// o.mem.SetBestProps(placeholderScan, rootPhysicalProps, &physical.Provided{}, memo.Cost{C: 1.0})
	// o.mem.SetRoot(placeholderScan, rootPhysicalProps)
	//
	// if buildutil.CrdbTestBuild && !o.mem.IsOptimized() {
	// 	return false, errors.AssertionFailedf("IsOptimized() should be true")
	// }
	//
	// return true, nil
}
