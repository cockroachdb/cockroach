// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// HasHoistableSubquery returns true if the given scalar expression contains a
// subquery within its subtree that has at least one outer column, and if that
// subquery needs to be hoisted up into its parent query as part of query
// decorrelation.
func (c *CustomFuncs) HasHoistableSubquery(scalar opt.ScalarExpr) bool {
	// Shortcut if the scalar has properties associated with it.
	if scalarPropsExpr, ok := scalar.(memo.ScalarPropsExpr); ok {
		// Don't bother traversing the expression tree if there is no subquery.
		scalarProps := scalarPropsExpr.ScalarProps()
		if !scalarProps.HasSubquery {
			return false
		}

		// Lazily calculate and store the HasHoistableSubquery value.
		if !scalarProps.IsAvailable(props.HasHoistableSubquery) {
			scalarProps.Rule.HasHoistableSubquery = c.deriveHasHoistableSubquery(scalar)
			scalarProps.SetAvailable(props.HasHoistableSubquery)
		}
		return scalarProps.Rule.HasHoistableSubquery
	}

	// Otherwise fall back on full traversal of subtree.
	return c.deriveHasHoistableSubquery(scalar)
}

func (c *CustomFuncs) deriveHasHoistableSubquery(scalar opt.ScalarExpr) bool {
	if c.deriveHasUnhoistableExpr(scalar) {
		// Some expressions disqualify subquery-hoisting entirely.
		return false
	}
	return c.deriveHasHoistableSubqueryImpl(scalar)
}

// deriveHasUnhoistableExpr checks for expressions within the given scalar
// expression which cannot be hoisted. This is necessary beyond existing
// volatility checks because of #97432: when a subquery-hoisting rule is
// triggered, *all* correlated subqueries are hoisted, not just the leak-proof
// subqueries. Therefore, it is necessary for correctness to avoid hoisting
// entirely in the presence of certain expressions.
func (c *CustomFuncs) deriveHasUnhoistableExpr(expr opt.Expr) bool {
	switch t := expr.(type) {
	case *memo.BarrierExpr:
		// An optimization barrier indicates the presence of an expression which
		// cannot be reordered with other expressions.
		return true
	case *memo.UDFCallExpr:
		if t.Def.RoutineLang == tree.RoutineLangPLpgSQL {
			// Hoisting a PL/pgSQL sub-routine could move it out of tail-call
			// position, forcing inefficient nested execution.
			//
			// TODO(#119956): consider relaxing this for routines which aren't already
			// in tail-call position.
			return true
		}
	}
	for i := 0; i < expr.ChildCount(); i++ {
		if c.deriveHasUnhoistableExpr(expr.Child(i)) {
			return true
		}
	}
	return false
}

func (c *CustomFuncs) deriveHasHoistableSubqueryImpl(scalar opt.ScalarExpr) bool {
	switch t := scalar.(type) {
	case *memo.SubqueryExpr:
		return !t.Input.Relational().OuterCols.Empty()

	case *memo.ExistsExpr:
		return !t.Input.Relational().OuterCols.Empty()

	case *memo.ArrayFlattenExpr:
		return !t.Input.Relational().OuterCols.Empty()

	case *memo.AnyExpr:
		// Don't hoist Any when only its Scalar operand is correlated, because it
		// executes much slower. It's better to cache the results of the constant
		// subquery in this case. Note that if an Any is at the top-level of a
		// WHERE clause, it will be transformed to an Exists operator, so this case
		// only occurs when the Any is nested, in a projection, etc.
		return !t.Input.Relational().OuterCols.Empty()

	case *memo.UDFCallExpr:
		// Do not attempt to hoist UDFs.
		return false

	case *memo.EqExpr:
		// Hoist subqueries in expressions like (Eq (Variable) (Subquery)) if:
		//
		//   1. The corresponding session setting is enabled.
		//   2. And, the subquery has not already been hoisted elsewhere in the
		//      expression tree. Hoisting the same subquery twice could result
		//      in query plans where two children of an expression have
		//      intersecting columns (see #114703).
		//
		// TODO(mgartner): We could hoist if we have an IS NOT DISTINCT FROM
		// expression. But it won't currently lead to a lookup join due to
		// #100855 and the plan could be worse, so we avoid it for now.
		if c.f.evalCtx.SessionData().OptimizerHoistUncorrelatedEqualitySubqueries {
			_, isLeftVar := scalar.Child(0).(*memo.VariableExpr)
			subquery, isRightSubquery := scalar.Child(1).(*memo.SubqueryExpr)
			if isLeftVar && isRightSubquery &&
				!c.f.Metadata().IsHoistedUncorrelatedSubquery(subquery) {
				return true
			}
		}
	}

	// Special handling for conditional expressions, which maintain invariants
	// about when input expressions are evaluated.
	switch scalar.Op() {
	case opt.CaseOp, opt.CoalesceOp, opt.IfErrOp:
		return c.deriveConditionalHasHoistableSubquery(scalar)
	}

	// If HasHoistableSubquery is true for any child, then it's true for this
	// expression as well. The exception is conditional expressions, which are
	// handled above.
	for i, n := 0, scalar.ChildCount(); i < n; i++ {
		child := scalar.Child(i).(opt.ScalarExpr)
		if c.deriveHasHoistableSubquery(child) {
			return true
		}
	}
	return false
}

// deriveConditionalHasHoistableSubquery analyzes a conditional expression for
// subqueries that can be "hoisted" into a join and replaced with a simple
// variable reference. It returns true when there is at least one subquery that
// can be hoisted, even if there is a subquery within the conditional expression
// that cannot be hoisted.
func (c *CustomFuncs) deriveConditionalHasHoistableSubquery(scalar opt.ScalarExpr) bool {
	if !c.f.evalCtx.SessionData().OptimizerUseConditionalHoistFix {
		return c.legacyDeriveConditionalHasHoistableSubquery(scalar)
	}
	// Conditional expressions maintain invariants about when input expressions
	// are evaluated. For example, a CASE statement does not evaluate a WHEN
	// branch until its guard condition passes. A child expression that is
	// conditionally evaluated can only be hoisted if it is leak-proof, since
	// hoisting can change whether / how many times an expression is evaluated.
	var sharedProps props.Shared
	deriveCanHoistChild := func(child opt.ScalarExpr, isConditional bool) bool {
		if c.deriveHasHoistableSubquery(child) {
			if isConditional {
				memo.BuildSharedProps(child, &sharedProps, c.f.evalCtx)
				return sharedProps.VolatilitySet.IsLeakproof()
			}
			return true
		}
		return false
	}
	switch t := scalar.(type) {
	case *memo.CaseExpr:
		// The input expression is always evaluated.
		if c.deriveHasHoistableSubquery(t.Input) {
			return true
		}
		for i := range t.Whens {
			whenExpr := t.Whens[i].(*memo.WhenExpr)
			if deriveCanHoistChild(whenExpr.Condition, i > 0) {
				// The first condition is always evaluated. The remaining conditions are
				// conditionally evaluated.
				return true
			}
			// The WHEN branches are conditionally evaluated.
			if deriveCanHoistChild(whenExpr.Value, true /* isConditional */) {
				return true
			}
		}
		// The ELSE branch is conditionally evaluated.
		return deriveCanHoistChild(t.OrElse, true /* isConditional */)
	case *memo.CoalesceExpr:
		// The first argument is always evaluated. The remaining arguments are
		// conditionally evaluated.
		for i := range t.Args {
			if deriveCanHoistChild(t.Args[i], i > 0) {
				return true
			}
		}
		return false
	case *memo.IfErrExpr:
		// The condition expression is always evaluated. The OrElse expressions are
		// conditionally evaluated.
		if c.deriveHasHoistableSubquery(t.Cond) {
			return true
		}
		for i := range t.OrElse {
			if deriveCanHoistChild(t.OrElse[i], true /* isConditional */) {
				return true
			}
		}
		return false
	default:
		panic(errors.AssertionFailedf("unhandled op: %v", scalar.Op()))
	}
}

// legacyDeriveConditionalHasHoistableSubquery contains the logic for analyzing
// conditional expressions from before #97432 was fixed. It differs from
// deriveConditionalHasHoistableSubquery mostly in its layout, but also returns
// false early for a COALESCE with a volatile branch, when later branches may
// still be hoistable.
//
// TODO(drewk): consider deleting this once we have high confidence it won't
// lead to plan regressions.
func (c *CustomFuncs) legacyDeriveConditionalHasHoistableSubquery(scalar opt.ScalarExpr) bool {
	// If HasHoistableSubquery is true for any child, then it's true for this
	// expression as well. The exception is Case/If branches that have side
	// effects. These can only be executed if the branch test evaluates to true,
	// and so it's not possible to hoist out subqueries, since they would then be
	// evaluated when they shouldn't be.
	for i, n := 0, scalar.ChildCount(); i < n; i++ {
		child := scalar.Child(i).(opt.ScalarExpr)
		if c.deriveHasHoistableSubquery(child) {
			var sharedProps props.Shared
			hasHoistableSubquery := true

			// Consider CASE WHEN and ELSE branches:
			//   (Case
			//     $input:*
			//     (When $cond1:* $branch1:*)  # optional
			//     (When $cond2:* $branch2:*)  # optional
			//     $else:*                     # optional
			//   )
			switch t := scalar.(type) {
			case *memo.CaseExpr:
				// Determine whether this is the Else child.
				if child == t.OrElse {
					memo.BuildSharedProps(child, &sharedProps, c.f.evalCtx)
					hasHoistableSubquery = sharedProps.VolatilitySet.IsLeakproof()
				}

			case *memo.CoalesceExpr:
				// The first argument does not need to be leakproof because it
				// is always evaluated.
				for j := 1; j < len(t.Args); j++ {
					memo.BuildSharedProps(t.Args[j], &sharedProps, c.f.evalCtx)
					hasHoistableSubquery = sharedProps.VolatilitySet.IsLeakproof()
					if !hasHoistableSubquery {
						break
					}
				}

			case *memo.WhenExpr:
				if child == t.Value {
					memo.BuildSharedProps(child, &sharedProps, c.f.evalCtx)
					hasHoistableSubquery = sharedProps.VolatilitySet.IsLeakproof()
				}

			case *memo.IfErrExpr:
				// Determine whether this is the Else child. Checking this how the
				// other branches do is tricky because it's a list, but we know that
				// it's at position 1.
				if i == 1 {
					memo.BuildSharedProps(child, &sharedProps, c.f.evalCtx)
					hasHoistableSubquery = sharedProps.VolatilitySet.IsLeakproof()
				}
			}

			if hasHoistableSubquery {
				return true
			}
		}
	}
	return false
}

// HoistSelectSubquery searches the Select operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//	SELECT * FROM xy WHERE (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//	=>
//	SELECT xy.*
//	FROM xy
//	LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//	ON True
//	WHERE u IS NULL
func (c *CustomFuncs) HoistSelectSubquery(
	input memo.RelExpr, filters memo.FiltersExpr,
) memo.RelExpr {
	newFilters := make(memo.FiltersExpr, 0, len(filters))

	var hoister subqueryHoister
	hoister.init(c, input)
	for i := range filters {
		item := &filters[i]
		if item.ScalarProps().Rule.HasHoistableSubquery {
			replaced := hoister.hoistAll(item.Condition)
			if replaced.Op() != opt.TrueOp {
				newFilters = append(newFilters, c.f.ConstructFiltersItem(replaced))
			}
		} else {
			newFilters = append(newFilters, *item)
		}
	}

	sel := c.f.ConstructSelect(hoister.input(), newFilters)
	return c.f.ConstructProject(sel, memo.EmptyProjectionsExpr, c.OutputCols(input))
}

// HoistProjectSubquery searches the Project operator's projections for
// correlated subqueries. Any found queries are hoisted into LeftJoinApply
// or InnerJoinApply operators, depending on subquery cardinality:
//
//	SELECT (SELECT max(u) FROM uv WHERE u=x) AS max FROM xy
//	=>
//	SELECT max
//	FROM xy
//	INNER JOIN LATERAL (SELECT max(u) FROM uv WHERE u=x)
//	ON True
func (c *CustomFuncs) HoistProjectSubquery(
	input memo.RelExpr, projections memo.ProjectionsExpr, passthrough opt.ColSet,
) memo.RelExpr {
	newProjections := make(memo.ProjectionsExpr, 0, len(projections))

	var hoister subqueryHoister
	hoister.init(c, input)
	for i := range projections {
		item := &projections[i]
		if item.ScalarProps().Rule.HasHoistableSubquery {
			replaced := hoister.hoistAll(item.Element)
			newProjections = append(newProjections, c.f.ConstructProjectionsItem(replaced, item.Col))
		} else {
			newProjections = append(newProjections, *item)
		}
	}

	return c.f.ConstructProject(hoister.input(), newProjections, passthrough)
}

// HoistJoinSubquery searches the Join operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//	SELECT y, z
//	FROM xy
//	FULL JOIN yz
//	ON (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//	=>
//	SELECT y, z
//	FROM xy
//	FULL JOIN LATERAL
//	(
//	  SELECT *
//	  FROM yz
//	  LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//	  ON True
//	)
//	ON u IS NULL
func (c *CustomFuncs) HoistJoinSubquery(
	op opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr, private *memo.JoinPrivate,
) memo.RelExpr {
	newFilters := make(memo.FiltersExpr, 0, len(on))

	var hoister subqueryHoister
	hoister.init(c, right)
	for i := range on {
		item := &on[i]
		if item.ScalarProps().Rule.HasHoistableSubquery {
			replaced := hoister.hoistAll(item.Condition)
			if replaced.Op() != opt.TrueOp {
				newFilters = append(newFilters, c.f.ConstructFiltersItem(replaced))
			}
		} else {
			newFilters = append(newFilters, *item)
		}
	}

	join := c.ConstructApplyJoin(op, left, hoister.input(), newFilters, private)
	var passthrough opt.ColSet
	switch op {
	case opt.SemiJoinOp, opt.AntiJoinOp:
		passthrough = c.OutputCols(left)
	default:
		passthrough = c.OutputCols(left).Union(c.OutputCols(right))
	}
	return c.f.ConstructProject(join, memo.EmptyProjectionsExpr, passthrough)
}

// HoistValuesSubquery searches the Values operator's projections for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//	SELECT (VALUES (SELECT u FROM uv WHERE u=x LIMIT 1)) FROM xy
//	=>
//	SELECT
//	(
//	  SELECT vals.*
//	  FROM (VALUES ())
//	  LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//	  ON True
//	  INNER JOIN LATERAL (VALUES (u)) vals
//	  ON True
//	)
//	FROM xy
//
// The dummy VALUES clause with a singleton empty row is added to the tree in
// order to use the hoister, which requires an initial input query. While a
// right join would be slightly better here, this is such a fringe case that
// it's not worth the extra code complication.
func (c *CustomFuncs) HoistValuesSubquery(
	rows memo.ScalarListExpr, private *memo.ValuesPrivate,
) memo.RelExpr {
	newRows := make(memo.ScalarListExpr, 0, len(rows))

	var hoister subqueryHoister
	hoister.init(c, c.ConstructNoColsRow())
	for _, item := range rows {
		newRows = append(newRows, hoister.hoistAll(item))
	}

	values := c.f.ConstructValues(newRows, &memo.ValuesPrivate{
		Cols: private.Cols,
		ID:   c.f.Metadata().NextUniqueID(),
	})
	join := c.f.ConstructInnerJoinApply(hoister.input(), values, memo.TrueFilter, memo.EmptyJoinPrivate)
	outCols := values.Relational().OutputCols
	return c.f.ConstructProject(join, memo.EmptyProjectionsExpr, outCols)
}

// HoistProjectSetSubquery searches the ProjectSet operator's functions for
// correlated subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//	SELECT generate_series
//	FROM xy
//	INNER JOIN LATERAL ROWS FROM
//	(
//	  generate_series(1, (SELECT v FROM uv WHERE u=x))
//	)
//	=>
//	SELECT generate_series
//	FROM xy
//	ROWS FROM
//	(
//	  SELECT generate_series
//	  FROM (VALUES ())
//	  LEFT JOIN LATERAL (SELECT v FROM uv WHERE u=x)
//	  ON True
//	  INNER JOIN LATERAL ROWS FROM (generate_series(1, v))
//	  ON True
//	)
func (c *CustomFuncs) HoistProjectSetSubquery(input memo.RelExpr, zip memo.ZipExpr) memo.RelExpr {
	newZip := make(memo.ZipExpr, 0, len(zip))

	var hoister subqueryHoister
	hoister.init(c, input)
	for i := range zip {
		item := &zip[i]
		if item.ScalarProps().Rule.HasHoistableSubquery {
			replaced := hoister.hoistAll(item.Fn)
			newZip = append(newZip, c.f.ConstructZipItem(replaced, item.Cols))
		} else {
			newZip = append(newZip, *item)
		}
	}

	// The process of hoisting will introduce additional columns, so we introduce
	// a projection to not include those in the output.
	outputCols := c.OutputCols(input).Union(zip.OutputCols())

	projectSet := c.f.ConstructProjectSet(hoister.input(), newZip)
	return c.f.ConstructProject(projectSet, memo.EmptyProjectionsExpr, outputCols)
}

// ConstructNonApplyJoin constructs the non-apply join operator that corresponds
// to the given join operator type.
func (c *CustomFuncs) ConstructNonApplyJoin(
	joinOp opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr, private *memo.JoinPrivate,
) memo.RelExpr {
	switch joinOp {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return c.f.ConstructInnerJoin(left, right, on, private)
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return c.f.ConstructLeftJoin(left, right, on, private)
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return c.f.ConstructSemiJoin(left, right, on, private)
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return c.f.ConstructAntiJoin(left, right, on, private)
	}
	panic(errors.AssertionFailedf("unexpected join operator: %v", redact.Safe(joinOp)))
}

// ConstructApplyJoin constructs the apply join operator that corresponds
// to the given join operator type.
func (c *CustomFuncs) ConstructApplyJoin(
	joinOp opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr, private *memo.JoinPrivate,
) memo.RelExpr {
	switch joinOp {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return c.f.ConstructInnerJoinApply(left, right, on, private)
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return c.f.ConstructLeftJoinApply(left, right, on, private)
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return c.f.ConstructSemiJoinApply(left, right, on, private)
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return c.f.ConstructAntiJoinApply(left, right, on, private)
	}
	panic(errors.AssertionFailedf("unexpected join operator: %v", redact.Safe(joinOp)))
}

// EnsureKey finds the shortest strong key for the input expression. If no
// strong key exists and the input expression is a Scan or a Scan wrapped in a
// Select, EnsureKey returns a new Scan (possibly wrapped in a Select) with the
// preexisting primary key for the table. If the input is not a Scan or
// Select(Scan), EnsureKey wraps the input in an Ordinality operator, which
// provides a key column by uniquely numbering the rows. EnsureKey returns the
// input expression (perhaps augmented with a key column(s) or wrapped by
// Ordinality).
func (c *CustomFuncs) EnsureKey(in memo.RelExpr) memo.RelExpr {
	// Try to add the preexisting primary key if the input is a Scan or Scan
	// wrapped in a Select.
	if res, ok := c.tryFindExistingKey(in); ok {
		return res
	}

	// Otherwise, wrap the input in an Ordinality operator.
	colID := c.f.Metadata().AddColumn("rownum", types.Int)
	private := memo.OrdinalityPrivate{ColID: colID, ForDuplicateRemoval: true}
	return c.f.ConstructOrdinality(in, &private)
}

// tryFindExistingKey attempts to find an existing key for the input expression.
// It may modify the expression in order to project the key column.
func (c *CustomFuncs) tryFindExistingKey(in memo.RelExpr) (_ memo.RelExpr, ok bool) {
	_, hasKey := c.CandidateKey(in)
	if hasKey {
		return in, true
	}
	switch t := in.(type) {
	case *memo.ProjectExpr:
		input, foundKey := c.tryFindExistingKey(t.Input)
		if foundKey {
			return c.f.ConstructProject(input, t.Projections, input.Relational().OutputCols), true
		}

	case *memo.ScanExpr:
		private := t.ScanPrivate
		tableID := private.Table
		table := c.f.Metadata().Table(tableID)
		if !table.IsVirtualTable() {
			keyCols := c.PrimaryKeyCols(tableID)
			private.Cols = private.Cols.Union(keyCols)
			return c.f.ConstructScan(&private), true
		}

	case *memo.SelectExpr:
		input, foundKey := c.tryFindExistingKey(t.Input)
		if foundKey {
			return c.f.ConstructSelect(input, t.Filters), true
		}
	}

	return nil, false
}

// KeyCols returns a column set consisting of the columns that make up the
// candidate key for the input expression (a key must be present).
func (c *CustomFuncs) KeyCols(in memo.RelExpr) opt.ColSet {
	keyCols, ok := c.CandidateKey(in)
	if !ok {
		panic(errors.AssertionFailedf("expected expression to have key"))
	}
	return keyCols
}

// NonKeyCols returns a column set consisting of the output columns of the given
// input, minus the columns that make up its candidate key (which it must have).
func (c *CustomFuncs) NonKeyCols(in memo.RelExpr) opt.ColSet {
	keyCols, ok := c.CandidateKey(in)
	if !ok {
		panic(errors.AssertionFailedf("expected expression to have key"))
	}
	return c.OutputCols(in).Difference(keyCols)
}

// MakeAggCols2 is similar to MakeAggCols, except that it allows two different
// sets of aggregate functions to be added to the resulting Aggregations
// operator, with one set appended to the other, like this:
//
//	(Aggregations
//	  [(ConstAgg (Variable 1)) (ConstAgg (Variable 2)) (FirstAgg (Variable 3))]
//	  [1,2,3]
//	)
func (c *CustomFuncs) MakeAggCols2(
	aggOp opt.Operator, cols opt.ColSet, aggOp2 opt.Operator, cols2 opt.ColSet,
) memo.AggregationsExpr {
	colsLen := cols.Len()
	aggs := make(memo.AggregationsExpr, colsLen+cols2.Len())
	c.makeAggCols(aggOp, cols, aggs)
	c.makeAggCols(aggOp2, cols2, aggs[colsLen:])
	return aggs
}

// AppendAggCols2 constructs a new Aggregations operator containing the
// aggregate functions from an existing Aggregations operator plus an
// additional set of aggregate functions, one for each column in the given set.
// The new functions are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols2(
	aggs memo.AggregationsExpr,
	aggOp opt.Operator,
	cols opt.ColSet,
	aggOp2 opt.Operator,
	cols2 opt.ColSet,
) memo.AggregationsExpr {
	colsLen := cols.Len()
	outAggs := make(memo.AggregationsExpr, len(aggs)+colsLen+cols2.Len())
	copy(outAggs, aggs)

	offset := len(aggs)
	c.makeAggCols(aggOp, cols, outAggs[offset:])
	offset += colsLen
	c.makeAggCols(aggOp2, cols2, outAggs[offset:])

	return outAggs
}

// EnsureCanaryCol checks whether an aggregation which cannot ignore nulls exists.
// If one does, it then checks if there are any non-null columns in the input.
// If there is not one, it synthesizes a new True constant column that is
// not-null. This becomes a kind of "canary" column that other expressions can
// inspect, since any null value in this column indicates that the row was
// added by an outer join as part of null extending.
//
// EnsureCanaryCol returns the input expression, possibly wrapped in a new
// Project if a new column was synthesized.
//
// See the TryDecorrelateScalarGroupBy rule comment for more details.
func (c *CustomFuncs) EnsureCanaryCol(in memo.RelExpr, aggs memo.AggregationsExpr) opt.ColumnID {
	for i := range aggs {
		if !opt.AggregateIgnoresNulls(aggs[i].Agg.Op()) {
			// Look for an existing not null column that is not projected by a
			// passthrough aggregate like ConstAgg.
			id, ok := in.Relational().NotNullCols.Next(0)
			if ok && !aggs.OutputCols().Contains(id) {
				return id
			}

			// Synthesize a new column ID.
			return c.f.Metadata().AddColumn("canary", types.Bool)
		}
	}
	return 0
}

// EnsureCanary makes sure that if canaryCol is set, it is projected by the
// input expression.
//
// See the TryDecorrelateScalarGroupBy rule comment for more details.
func (c *CustomFuncs) EnsureCanary(in memo.RelExpr, canaryCol opt.ColumnID) memo.RelExpr {
	if canaryCol == 0 || c.OutputCols(in).Contains(canaryCol) {
		return in
	}
	result := c.ProjectExtraCol(in, c.f.ConstructTrue(), canaryCol)
	return result
}

// CanaryColSet returns a singleton set containing the canary column if set,
// otherwise the empty set.
func (c *CustomFuncs) CanaryColSet(canaryCol opt.ColumnID) opt.ColSet {
	var colSet opt.ColSet
	if canaryCol != 0 {
		colSet.Add(canaryCol)
	}
	return colSet
}

// AggsCanBeDecorrelated returns true if every aggregate satisfies one of the
// following conditions:
//
//   - It is CountRows (because it will be translated into Count),
//   - It ignores nulls (because nothing extra must be done for it)
//   - It gives NULL on no input (because this is how we translate non-null
//     ignoring aggregates)
//
// TODO(justin): we can lift the third condition if we have a function that
// gives the correct "on empty" value for a given aggregate.
func (c *CustomFuncs) AggsCanBeDecorrelated(aggs memo.AggregationsExpr) bool {
	for i := range aggs {
		agg := aggs[i].Agg
		op := agg.Op()
		if op == opt.AggFilterOp || op == opt.AggDistinctOp {
			// TODO(radu): investigate if we can do better here
			return false
		}
		if !(op == opt.CountRowsOp || opt.AggregateIgnoresNulls(op) || opt.AggregateIsNullOnEmpty(op)) {
			return false
		}
	}

	return true
}

// constructCanaryChecker returns a CASE expression which disambiguates an
// aggregation over a left join having received a NULL column because there
// were no matches on the right side of the join, and having received a NULL
// column because a NULL column was matched against.
func (c *CustomFuncs) constructCanaryChecker(
	aggCanaryVar opt.ScalarExpr, inputCol opt.ColumnID,
) opt.ScalarExpr {
	variable := c.f.ConstructVariable(inputCol)
	return c.f.ConstructCase(
		memo.TrueSingleton,
		memo.ScalarListExpr{
			c.f.ConstructWhen(
				c.f.ConstructIsNot(aggCanaryVar, memo.NullSingleton),
				variable,
			),
		},
		c.f.ConstructNull(variable.DataType()),
	)
}

// TranslateNonIgnoreAggs checks if any of the aggregates being decorrelated
// are unable to ignore nulls. If that is the case, it inserts projections
// which check a "canary" aggregation that determines if an expression actually
// had any things grouped into it or not.
func (c *CustomFuncs) TranslateNonIgnoreAggs(
	newIn memo.RelExpr,
	newAggs memo.AggregationsExpr,
	oldIn memo.RelExpr,
	oldAggs memo.AggregationsExpr,
	canaryCol opt.ColumnID,
) memo.RelExpr {
	var aggCanaryVar opt.ScalarExpr
	passthrough := c.OutputCols(newIn).Copy()
	passthrough.Remove(canaryCol)

	var projections memo.ProjectionsExpr
	for i := range newAggs {
		agg := newAggs[i].Agg
		if !opt.AggregateIgnoresNulls(agg.Op()) {
			if aggCanaryVar == nil {
				if canaryCol == 0 {
					id, ok := oldIn.Relational().NotNullCols.Next(0)
					if !ok {
						panic(errors.AssertionFailedf("expected input expression to have not-null column"))
					}
					canaryCol = id
				}
				aggCanaryVar = c.f.ConstructVariable(canaryCol)
			}

			if !opt.AggregateIsNullOnEmpty(agg.Op()) {
				// If this gets triggered we need to modify constructCanaryChecker to
				// have a special "on-empty" value. This shouldn't get triggered
				// because as of writing the only operation that is false for both
				// AggregateIgnoresNulls and AggregateIsNullOnEmpty is CountRows, and
				// we translate that into Count.
				// TestAllAggsIgnoreNullsOrNullOnEmpty verifies that this assumption is
				// true.
				panic(errors.AssertionFailedf("can't decorrelate with aggregate %s", redact.Safe(agg.Op())))
			}

			if projections == nil {
				projections = make(memo.ProjectionsExpr, 0, len(newAggs)-i)
			}
			projections = append(projections, c.f.ConstructProjectionsItem(
				c.constructCanaryChecker(aggCanaryVar, newAggs[i].Col),
				oldAggs[i].Col,
			))
			passthrough.Remove(newAggs[i].Col)
		}
	}

	if projections == nil {
		return newIn
	}
	return c.f.ConstructProject(newIn, projections, passthrough)
}

// EnsureAggsCanIgnoreNulls scans the aggregate list to aggregation functions that
// don't ignore nulls but can be remapped so that they do:
//   - CountRows functions are are converted to Count functions that operate
//     over a not-null column from the given input expression. The
//     EnsureNotNullIfNeeded method should already have been called in order
//     to guarantee such a column exists.
//   - ConstAgg is remapped to ConstNotNullAgg.
//   - Other aggregates that can use a canary column to detect nulls.
//
// See the TryDecorrelateScalarGroupBy rule comment for more details.
func (c *CustomFuncs) EnsureAggsCanIgnoreNulls(
	in memo.RelExpr, aggs memo.AggregationsExpr,
) memo.AggregationsExpr {
	var newAggs memo.AggregationsExpr
	for i := range aggs {
		newAgg := aggs[i].Agg
		newCol := aggs[i].Col

		switch t := newAgg.(type) {
		case *memo.ConstAggExpr:
			// Translate ConstAgg(...) to ConstNotNullAgg(...).
			newAgg = c.f.ConstructConstNotNullAgg(t.Input)

		case *memo.CountRowsExpr:
			// Translate CountRows() to Count(notNullCol).
			id, ok := in.Relational().NotNullCols.Next(0)
			if !ok {
				panic(errors.AssertionFailedf("expected input expression to have not-null column"))
			}
			notNullColID := id
			newAgg = c.f.ConstructCount(c.f.ConstructVariable(notNullColID))

		default:
			if !opt.AggregateIgnoresNulls(t.Op()) {
				// Allocate id for new intermediate agg column. The column will get
				// mapped back to the original id after the grouping (by the
				// TranslateNonIgnoreAggs method).
				md := c.f.Metadata()
				colMeta := md.ColumnMeta(newCol)
				newCol = md.AddColumn(colMeta.Alias, colMeta.Type)
			}
		}
		if newAggs == nil {
			if newAgg != aggs[i].Agg || newCol != aggs[i].Col {
				newAggs = make(memo.AggregationsExpr, len(aggs))
				copy(newAggs, aggs[:i])
			}
		}
		if newAggs != nil {
			newAggs[i] = c.f.ConstructAggregationsItem(newAgg, newCol)
		}
	}
	if newAggs == nil {
		// No changes.
		return aggs
	}
	return newAggs
}

// AddColsToPartition unions the given set of columns with a window private's
// partition columns.
func (c *CustomFuncs) AddColsToPartition(
	priv *memo.WindowPrivate, cols opt.ColSet,
) *memo.WindowPrivate {
	cpy := *priv
	cpy.Partition = cpy.Partition.Union(cols)
	return &cpy
}

// ConstructAnyCondition builds an expression that compares the given scalar
// expression with the first (and only) column of the input rowset, using the
// given comparison operator.
func (c *CustomFuncs) ConstructAnyCondition(
	input memo.RelExpr, scalar opt.ScalarExpr, private *memo.SubqueryPrivate,
) opt.ScalarExpr {
	inputVar := c.referenceSingleColumn(input)
	return c.ConstructBinary(private.Cmp, scalar, inputVar)
}

// ConvertSubToExistsPrivate converts the given SubqueryPrivate to an
// ExistsPrivate.
func (c *CustomFuncs) ConvertSubToExistsPrivate(sub *memo.SubqueryPrivate) *memo.ExistsPrivate {
	col := c.f.Metadata().AddColumn("exists", types.Bool)
	return &memo.ExistsPrivate{
		LazyEvalProjectionCol: col,
		SubqueryPrivate:       *sub,
	}
}

// ConstructBinary builds a dynamic binary expression, given the binary
// operator's type and its two arguments.
func (c *CustomFuncs) ConstructBinary(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	return c.f.DynamicConstruct(op, left, right).(opt.ScalarExpr)
}

// ConstructNoColsRow returns a Values operator having a single row with zero
// columns.
func (c *CustomFuncs) ConstructNoColsRow() memo.RelExpr {
	return c.f.ConstructNoColsRow()
}

// referenceSingleColumn returns a Variable operator that refers to the one and
// only column that is projected by the input expression.
func (c *CustomFuncs) referenceSingleColumn(in memo.RelExpr) opt.ScalarExpr {
	colID := in.Relational().OutputCols.SingleColumn()
	return c.f.ConstructVariable(colID)
}

// subqueryHoister searches scalar expression trees looking for correlated
// subqueries which will be pulled up and joined to a higher level relational
// query. See the  hoistAll comment for more details on how this is done.
type subqueryHoister struct {
	c       *CustomFuncs
	f       *Factory
	mem     *memo.Memo
	hoisted memo.RelExpr
	scratch props.Shared
}

func (r *subqueryHoister) init(c *CustomFuncs, input memo.RelExpr) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*r = subqueryHoister{
		c:       c,
		f:       c.f,
		mem:     c.mem,
		hoisted: input,
	}
}

// input returns a single expression tree that contains the input expression
// provided to the init method, but wrapped with any subqueries hoisted out of
// the scalar expression tree. See the hoistAll comment for more details.
func (r *subqueryHoister) input() memo.RelExpr {
	return r.hoisted
}

// hoistAll searches the given subtree for each correlated Subquery, Exists, or
// Any operator, and lifts its subquery operand out of the scalar context and
// joins it with a higher-level relational expression. The original subquery
// operand is replaced by a Variable operator that refers to the first (and
// only) column of the hoisted relational expression.
//
// hoistAll returns the root of a new expression tree that incorporates the new
// Variable operators. The hoisted subqueries can be accessed via the input
// method. Each removed subquery wraps the one before, with the input query at
// the base. Each subquery adds a single column to its input and uses a
// JoinApply operator to ensure that it has no effect on the cardinality of its
// input. For example:
//
//	SELECT *
//	FROM xy
//	WHERE
//	  (SELECT u FROM uv WHERE u=x LIMIT 1) IS NOT NULL
//	  OR EXISTS(SELECT * FROM jk WHERE j=x)
//	=>
//	SELECT xy.*
//	FROM xy
//	LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//	ON True
//	INNER JOIN LATERAL
//	(
//	  SELECT (CONST_AGG(True) IS NOT NULL) AS exists FROM jk WHERE j=x
//	)
//	ON True
//	WHERE u IS NOT NULL OR exists
//
// The choice of whether to use LeftJoinApply or InnerJoinApply depends on the
// cardinality of the hoisted subquery. If zero rows can be returned from the
// subquery, then LeftJoinApply must be used in order to preserve the
// cardinality of the input expression. Otherwise, InnerJoinApply can be used
// instead. In either case, the wrapped subquery must never return more than one
// row, so as not to change the cardinality of the result.
//
// See the comments for constructGroupByExists and constructGroupByAny for more
// details on how EXISTS and ANY subqueries are hoisted, including usage of the
// CONST_AGG function.
func (r *subqueryHoister) hoistAll(scalar opt.ScalarExpr) opt.ScalarExpr {
	// Match correlated subqueries.
	switch scalar.Op() {
	case opt.SubqueryOp, opt.ExistsOp, opt.AnyOp, opt.ArrayFlattenOp:
		subquery := scalar.Child(0).(memo.RelExpr)
		// According to the implementation of deriveHasHoistableSubquery,
		// Exists, Any, and ArrayFlatten expressions are only hoistable if they
		// are correlated. Uncorrelated subquery expressions are hoistable if
		// the corresponding session setting is enabled, they are part of an
		// equality expression with a variable, and they have not already been
		// hoisted elsewhere in the expression tree.
		if subquery.Relational().OuterCols.Empty() {
			uncorrelatedHoistAllowed := scalar.Op() == opt.SubqueryOp &&
				r.f.evalCtx.SessionData().OptimizerHoistUncorrelatedEqualitySubqueries &&
				!r.f.Metadata().IsHoistedUncorrelatedSubquery(scalar)
			if !uncorrelatedHoistAllowed {
				break
			}
			// Mark the subquery as being hoisted.
			r.f.Metadata().AddHoistedUncorrelatedSubquery(scalar)
		}

		switch t := scalar.(type) {
		case *memo.ExistsExpr:
			subquery = r.constructGroupByExists(subquery)

		case *memo.AnyExpr:
			subquery = r.constructGroupByAny(t.Scalar, t.Cmp, t.Input)
		}

		// Hoist the subquery into a single expression that can be accessed via
		// the subqueries method.
		subqueryProps := subquery.Relational()
		if subqueryProps.Cardinality.CanBeZero() {
			// Zero cardinality allowed, so must use left outer join to preserve
			// outer row (padded with nulls) in case the subquery returns zero rows.
			r.hoisted = r.f.ConstructLeftJoinApply(r.hoisted, subquery, memo.TrueFilter, memo.EmptyJoinPrivate)
		} else {
			// Zero cardinality not allowed, so inner join suffices. Inner joins
			// are preferable to left joins since null handling is much simpler
			// and they allow the optimizer more choices.
			r.hoisted = r.f.ConstructInnerJoinApply(r.hoisted, subquery, memo.TrueFilter, memo.EmptyJoinPrivate)
		}

		// Replace the Subquery operator with a Variable operator referring to
		// the output column of the hoisted query.
		var colID opt.ColumnID
		switch t := scalar.(type) {
		case *memo.ArrayFlattenExpr:
			colID = t.RequestedCol
		default:
			colID = subqueryProps.OutputCols.SingleColumn()
		}
		return r.f.ConstructVariable(colID)

	case opt.CaseOp, opt.CoalesceOp, opt.IfErrOp:
		if r.f.evalCtx.SessionData().OptimizerUseConditionalHoistFix {
			return r.constructConditionalExpr(scalar)
		}
	}

	return r.f.Replace(scalar, func(nd opt.Expr) opt.Expr {
		// Recursively hoist subqueries in each scalar child that contains them.
		// Skip relational children, since only subquery scalar operators have a
		// relational child, and either:
		//
		//   1. The child is correlated, and therefore was handled above by hoisting
		//      and rewriting (and therefore won't ever get here),
		//
		//   2. Or the child is uncorrelated, and therefore should be skipped, since
		//      uncorrelated subqueries are not hoisted.
		//
		if scalarChild, ok := nd.(opt.ScalarExpr); ok {
			return r.hoistAll(scalarChild)
		}
		return nd
	}).(opt.ScalarExpr)
}

// constructConditionalExpr handles the special case of hoisting subqueries
// within a conditional expression, which must maintain invariants as to when
// its children are evaluated. For example, a branch of a CASE statement can
// only be evaluated if its guard condition passes.
func (r *subqueryHoister) constructConditionalExpr(scalar opt.ScalarExpr) opt.ScalarExpr {
	maybeHoistChild := func(child opt.ScalarExpr, isConditional bool) opt.ScalarExpr {
		if isConditional {
			// This child expression is conditionally evaluated, so it can only be
			// hoisted if it does not have side effects.
			r.scratch = props.Shared{}
			memo.BuildSharedProps(child, &r.scratch, r.f.evalCtx)
			if !r.scratch.VolatilitySet.IsLeakproof() {
				return child
			}
		}
		return r.hoistAll(child)
	}

	switch t := scalar.(type) {
	case *memo.CaseExpr:
		// The input expression is unconditionally evaluated.
		newInput := r.hoistAll(t.Input)
		newWhens := make(memo.ScalarListExpr, len(t.Whens))
		for i := range t.Whens {
			// WHEN conditions other than the first are conditionally evaluated. The
			// branches are always conditionally evaluated.
			whenExpr := t.Whens[i].(*memo.WhenExpr)
			newCond := maybeHoistChild(whenExpr.Condition, i > 0)
			newVal := maybeHoistChild(whenExpr.Value, true /* isConditional */)
			newWhens[i] = r.f.ConstructWhen(newCond, newVal)
		}
		// The ELSE branch is conditionally evaluated.
		newOrElse := maybeHoistChild(t.OrElse, true /* isConditional */)
		return r.f.ConstructCase(newInput, newWhens, newOrElse)
	case *memo.CoalesceExpr:
		// The first argument is unconditionally evaluated; the rest are
		// conditional.
		newArgs := make(memo.ScalarListExpr, len(t.Args))
		for i, arg := range t.Args {
			newArgs[i] = maybeHoistChild(arg, i > 0)
		}
		return r.f.ConstructCoalesce(newArgs)
	case *memo.IfErrExpr:
		// The condition expression is always evaluated. The OrElse expressions are
		// conditionally evaluated.
		newCond := r.hoistAll(t.Cond)
		newOrElse := make(memo.ScalarListExpr, len(t.OrElse))
		for i := range t.OrElse {
			newOrElse[i] = maybeHoistChild(t.OrElse[i], true /* isConditional */)
		}
		return r.f.ConstructIfErr(newCond, newOrElse, t.ErrCode)
	}
	panic(errors.AssertionFailedf("unexpected op: %s", scalar.Op()))
}

// constructGroupByExists transforms a scalar Exists expression like this:
//
//	EXISTS(SELECT * FROM a WHERE a.x=b.x)
//
// into a scalar GroupBy expression that returns a one row, one column relation:
//
//	SELECT (CONST_AGG(True) IS NOT NULL) AS exists
//	FROM (SELECT * FROM a WHERE a.x=b.x)
//
// The expression uses an internally-defined CONST_AGG aggregation function,
// since it's able to short-circuit on the first non-null it encounters. The
// above expression is equivalent to:
//
//	SELECT COUNT(True) > 0 FROM (SELECT * FROM a WHERE a.x=b.x)
//
// CONST_AGG (and COUNT) always return exactly one boolean value in the context
// of a scalar GroupBy expression. Because its operand is always True, the only
// way the final expression is False is when the input set is empty (since
// CONST_AGG returns NULL, which IS NOT NULL maps to False).
//
// However, later on, the TryDecorrelateScalarGroupBy rule will push a left join
// into the GroupBy, and null values produced by the join will flow into the
// CONST_AGG which will need to be changed to a CONST_NOT_NULL_AGG (which is
// defined to ignore those nulls so that its result will be unaffected).
func (r *subqueryHoister) constructGroupByExists(subquery memo.RelExpr) memo.RelExpr {
	var canaryColTyp *types.T
	var canaryColID opt.ColumnID
	var subqueryWithCanary memo.RelExpr
	if subquery.Relational().NotNullCols.Empty() {
		canaryColTyp = types.Bool
		canaryColID = r.f.Metadata().AddColumn("canary", types.Bool)
		subqueryWithCanary = r.f.ConstructProject(
			subquery,
			memo.ProjectionsExpr{r.f.ConstructProjectionsItem(memo.TrueSingleton, canaryColID)},
			opt.ColSet{},
		)
	} else {
		canaryColID, _ = subquery.Relational().NotNullCols.Next(0)
		canaryColTyp = r.mem.Metadata().ColumnMeta(canaryColID).Type
		subqueryWithCanary = r.f.ConstructProject(
			subquery,
			memo.ProjectionsExpr{},
			opt.MakeColSet(canaryColID),
		)
	}
	aggColID := r.f.Metadata().AddColumn("canary_agg", canaryColTyp)
	existsColID := r.f.Metadata().AddColumn("exists", types.Bool)

	return r.f.ConstructProject(
		r.f.ConstructScalarGroupBy(
			subqueryWithCanary,
			memo.AggregationsExpr{r.f.ConstructAggregationsItem(
				r.f.ConstructConstAgg(r.f.ConstructVariable(canaryColID)),
				aggColID,
			)},
			memo.EmptyGroupingPrivate,
		),
		memo.ProjectionsExpr{r.f.ConstructProjectionsItem(
			r.f.ConstructIsNot(
				r.f.ConstructVariable(aggColID),
				memo.NullSingleton,
			),
			existsColID,
		)},
		opt.ColSet{},
	)
}

// constructGroupByAny transforms a scalar Any expression into a scalar GroupBy
// expression that returns a one row, one column relation. See
// CustomFuncs.ConstructGroupByAny for more details.
func (r *subqueryHoister) constructGroupByAny(
	scalar opt.ScalarExpr, cmp opt.Operator, input memo.RelExpr,
) memo.RelExpr {
	// When the scalar value is not a simple variable or constant expression,
	// then cache its value using a projection, since it will be referenced
	// multiple times.
	if scalar.Op() != opt.VariableOp && !opt.IsConstValueOp(scalar) {
		typ := scalar.DataType()
		scalarColID := r.f.Metadata().AddColumn("scalar", typ)
		r.hoisted = r.c.ProjectExtraCol(r.hoisted, scalar, scalarColID)
		scalar = r.f.ConstructVariable(scalarColID)
	}
	return r.c.ConstructGroupByAny(scalar, cmp, input)
}

// ConstructGroupByAny transforms a scalar Any expression like this:
//
//	z = ANY(SELECT x FROM xy)
//
// into a scalar GroupBy expression that returns a one row, one column relation
// that is equivalent to this:
//
//	SELECT
//	  CASE
//	    WHEN bool_or(notnull) AND z IS NOT Null THEN True
//	    ELSE bool_or(notnull) IS NULL THEN False
//	    ELSE Null
//	  END
//	FROM
//	(
//	  SELECT x IS NOT Null AS notnull
//	  FROM xy
//	  WHERE (z=x) IS NOT False
//	)
//
// BOOL_OR returns true if any input is true, else false if any input is false,
// else null. This is a mismatch with ANY, which returns true if any input is
// true, else null if any input is null, else false. In addition, the expression
// needs to be easy to decorrelate, which means that the outer column reference
// ("z" in the example) should not be part of a projection (since projections
// are difficult to hoist above left joins). The following procedure solves the
// mismatch between BOOL_OR and ANY, as well as avoids correlated projections:
//
//  1. Filter out false comparison rows with an initial filter. The result of
//     ANY does not change, no matter how many false rows are added or removed.
//     This step has the effect of mapping a set containing only false
//     comparison rows to the empty set (which is desirable).
//
//  2. Step #1 leaves only true and null comparison rows. A null comparison row
//     occurs when either the left or right comparison operand is null (Any
//     only allows comparison operators that propagate nulls). Map each null
//     row to a false row, but only in the case where the right operand is null
//     (i.e. the operand that came from the subquery). The case where the left
//     operand is null will be handled later.
//
//  3. Use the BOOL_OR aggregation function on the true/false values from step
//     #2. If there is at least one true value, then BOOL_OR returns true. If
//     there are no values (the empty set case), then BOOL_OR returns null.
//     Because of the previous steps, this indicates that the original set
//     contained only false values (or no values at all).
//
//  4. A True result from BOOL_OR is ambiguous. It could mean that the
//     comparison returned true for one of the rows in the group. Or, it could
//     mean that the left operand was null. The CASE statement ensures that
//     True is only returned if the left operand was not null.
//
//  5. In addition, the CASE statement maps a null return value to false, and
//     false to null. This matches ANY behavior.
//
// The following is a table showing the various interesting cases:
//
//	      | subquery  | before        | after   | after
//	  z   | x values  | BOOL_OR       | BOOL_OR | CASE
//	------+-----------+---------------+---------+-------
//	  1   | (1)       | (true)        | true    | true
//	  1   | (1, null) | (true, false) | true    | true
//	  1   | (1, 2)    | (true)        | true    | true
//	  1   | (null)    | (false)       | false   | null
//	 null | (1)       | (true)        | true    | null
//	 null | (1, null) | (true, false) | true    | null
//	 null | (null)    | (false)       | false   | null
//	  2   | (1)       | (empty)       | null    | false
//	*any* | (empty)   | (empty)       | null    | false
//
// It is important that the set given to BOOL_OR does not contain any null
// values (the reason for step #2). Null is reserved for use by the
// TryDecorrelateScalarGroupBy rule, which will push a left join into the
// GroupBy. Null values produced by the left join will simply be ignored by
// BOOL_OR, and so cannot be used for any other purpose.
func (c *CustomFuncs) ConstructGroupByAny(
	scalar opt.ScalarExpr, cmp opt.Operator, input memo.RelExpr,
) memo.RelExpr {
	inputVar := c.f.funcs.referenceSingleColumn(input)
	notNullColID := c.f.Metadata().AddColumn("notnull", types.Bool)
	aggColID := c.f.Metadata().AddColumn("bool_or", types.Bool)
	aggVar := c.f.ConstructVariable(aggColID)
	caseColID := c.f.Metadata().AddColumn("case", types.Bool)

	var scalarNotNull opt.ScalarExpr
	if scalar.DataType().Family() == types.TupleFamily {
		scalarNotNull = c.f.ConstructIsTupleNotNull(scalar)
	} else {
		scalarNotNull = c.f.ConstructIsNot(scalar, memo.NullSingleton)
	}

	var inputNotNull opt.ScalarExpr
	if inputVar.DataType().Family() == types.TupleFamily {
		inputNotNull = c.f.ConstructIsTupleNotNull(inputVar)
	} else {
		inputNotNull = c.f.ConstructIsNot(inputVar, memo.NullSingleton)
	}

	return c.f.ConstructProject(
		c.f.ConstructScalarGroupBy(
			c.f.ConstructProject(
				c.f.ConstructSelect(
					input,
					memo.FiltersExpr{c.f.ConstructFiltersItem(
						c.f.ConstructIsNot(
							c.f.funcs.ConstructBinary(cmp, scalar, inputVar),
							memo.FalseSingleton,
						),
					)},
				),
				memo.ProjectionsExpr{c.f.ConstructProjectionsItem(
					inputNotNull,
					notNullColID,
				)},
				opt.ColSet{},
			),
			memo.AggregationsExpr{c.f.ConstructAggregationsItem(
				c.f.ConstructBoolOr(
					c.f.ConstructVariable(notNullColID),
				),
				aggColID,
			)},
			memo.EmptyGroupingPrivate,
		),
		memo.ProjectionsExpr{c.f.ConstructProjectionsItem(
			c.f.ConstructCase(
				c.f.ConstructTrue(),
				memo.ScalarListExpr{
					c.f.ConstructWhen(
						c.f.ConstructAnd(
							aggVar,
							scalarNotNull,
						),
						c.f.ConstructTrue(),
					),
					c.f.ConstructWhen(
						c.f.ConstructIs(aggVar, memo.NullSingleton),
						c.f.ConstructFalse(),
					),
				},
				c.f.ConstructNull(types.Bool),
			),
			caseColID,
		)},
		opt.ColSet{},
	)
}

// CanMaybeRemapOuterCols performs a best-effort check to minimize the cases
// where TryRemapOuterCols is called unnecessarily.
func (c *CustomFuncs) CanMaybeRemapOuterCols(input memo.RelExpr, filters memo.FiltersExpr) bool {
	// The usages of ComputeEquivClosureNoCopy are ok because of the copy below.
	outerCols := input.Relational().OuterCols.Copy()
	equivGroup := input.Relational().FuncDeps.ComputeEquivClosureNoCopy(outerCols)
	for i := range filters {
		if equivGroup.Intersects(input.Relational().OutputCols) {
			return true
		}
		equivGroup = filters[i].ScalarProps().FuncDeps.ComputeEquivClosureNoCopy(equivGroup)
	}
	return equivGroup.Intersects(input.Relational().OutputCols)
}

// TryRemapOuterCols attempts to replace outer column references in the given
// expression with equivalent non-outer columns using equalities from the given
// filters. It accomplishes this by traversing the operator tree for each outer
// column with the set of equivalent non-outer columns, wherever it would be
// valid to push down a filter on those non-outer columns. If a reference to the
// outer column is discovered during this traversal, it is valid to replace it
// with one of the non-outer columns in the set.
func (c *CustomFuncs) TryRemapOuterCols(
	expr memo.RelExpr, filters memo.FiltersExpr,
) (remapped memo.RelExpr, wasRemapped bool) {
	outerCols := expr.Relational().OuterCols
	remapped = expr
	for col, ok := outerCols.Next(0); ok; col, ok = outerCols.Next(col + 1) {
		// substituteCols is the set of input columns for which it may be possible to
		// push a filter constraining the column to be equal to an outer column.
		// Doing so would allow the column to be substituted for the outer column.
		substituteCols := expr.Relational().FuncDeps.ComputeEquivGroup(col)
		for i := range filters {
			// ComputeEquivClosureNoCopy is ok here because ComputeEquivGroup builds
			// a new ColSet.
			substituteCols = filters[i].ScalarProps().FuncDeps.ComputeEquivClosureNoCopy(substituteCols)
		}
		substituteCols.DifferenceWith(outerCols)
		remapped = c.tryRemapOuterCols(remapped, col, substituteCols).(memo.RelExpr)
	}
	wasRemapped = remapped != expr
	return remapped, wasRemapped
}

// tryRemapOuterCols handles the traversal and outer-column replacement for
// TryRemapOuterCols. It returns the replacement expression, which may be
// unchanged if remapping was not possible.
func (c *CustomFuncs) tryRemapOuterCols(
	expr opt.Expr, outerCol opt.ColumnID, substituteCols opt.ColSet,
) opt.Expr {
	if substituteCols.Empty() {
		// It is not possible to remap any references to the current outer
		// column within this expression.
		return expr
	}
	switch t := expr.(type) {
	case *memo.VariableExpr:
		if t.Col == outerCol {
			md := c.mem.Metadata()
			outerColTyp := md.ColumnMeta(outerCol).Type
			replaceCol, ok := substituteCols.Next(0)
			for ; ok; replaceCol, ok = substituteCols.Next(replaceCol + 1) {
				if outerColTyp.Identical(md.ColumnMeta(replaceCol).Type) {
					// Only perform the replacement if the types are identical.
					// This outer-column reference can be remapped.
					return c.f.ConstructVariable(replaceCol)
				}
			}
		}
	case memo.RelExpr:
		if !t.Relational().OuterCols.Contains(outerCol) {
			// This expression does not reference the current outer column.
			return t
		}
		// Modifications to substituteCols may be necessary in order to allow
		// outer-column remapping within (the children of) a RelExpr. Note that
		// getSubstituteColsRelExpr copies substituteCols before modifying it, so
		// different branches of the traversal don't interact with the same ColSet.
		substituteCols = c.getSubstituteColsRelExpr(t, substituteCols)
	case opt.ScalarExpr:
		// Any substitute columns that reach a ScalarExpr are valid candidates
		// for outer-column replacement. No changes to substituteCols required.
	}
	replaceFn := func(e opt.Expr) opt.Expr {
		return c.tryRemapOuterCols(e, outerCol, substituteCols)
	}
	return c.f.Replace(expr, replaceFn)
}

// getSubstituteColsRelExpr modifies the given set of substitute columns to
// reflect the set of columns for which an equality with an outer column could
// be pushed through the given expression. The logic of getSubstituteColsRelExpr
// mirrors that of the filter push-down rules in select.opt and join.opt.
// TODO(drewk): null-rejection has to push down a 'col IS NOT NULL' filter -
// we should be able to share logic. Doing so would remove the issue of rule
// cycles. Any other rules that reuse this logic should reconsider the
// simplification made in getSubstituteColsSetOp.
//
// NOTE: care must be taken for operators that may aggregate or "group" rows.
// If rows for which the outer-column equality holds are grouped together with
// those for which it does not, the result set will be incorrect (see #130001).
//
// getSubstituteColsRelExpr copies substituteCols before performing any
// modifications, so the original ColSet is not mutated.
func (c *CustomFuncs) getSubstituteColsRelExpr(
	expr memo.RelExpr, substituteCols opt.ColSet,
) opt.ColSet {
	// Remove any columns that are not in the output of this expression.
	// Non-output columns can be in substituteCols after a recursive call
	// into the input of an expression that either has multiple relational
	// inputs (e.g. Joins) or can synthesize columns (e.g. Projects).
	//
	// Note that substituteCols is copied here, so subsequent mutations can be
	// performed in place.
	substituteCols = substituteCols.Intersection(expr.Relational().OutputCols)

	// Depending on the expression, further modifications to substituteCols
	// may be necessary.
	switch t := expr.(type) {
	case *memo.SelectExpr:
		// [MergeSelects]
		// No restrictions on push-down for the cols in substituteCols.
	case *memo.ProjectExpr, *memo.ProjectSetExpr:
		// [PushSelectIntoProject]
		// [PushSelectIntoProjectSet]
		// Filter push-down candidates can only reference input columns.
		inputCols := t.Child(0).(memo.RelExpr).Relational().OutputCols
		substituteCols.IntersectionWith(inputCols)
	case *memo.InnerJoinExpr, *memo.InnerJoinApplyExpr:
		// [MergeSelectInnerJoin]
		// [PushFilterIntoJoinLeft]
		// [PushFilterIntoJoinRight]
		// No restrictions on push-down for the cols in substituteCols.
	case *memo.LeftJoinExpr, *memo.LeftJoinApplyExpr, *memo.SemiJoinExpr,
		*memo.SemiJoinApplyExpr, *memo.AntiJoinExpr, *memo.AntiJoinApplyExpr:
		// [PushSelectIntoJoinLeft]
		// [PushSelectCondLeftIntoJoinLeftAndRight]
		// NOTE: These join variants do perform "grouping" operations, but only on
		// the right input, for which we do not push down the equality.
		substituteCols = getSubstituteColsLeftSemiAntiJoin(t, substituteCols)
	case *memo.GroupByExpr, *memo.DistinctOnExpr:
		// [PushSelectIntoGroupBy]
		// Filters must refer only to grouping columns. This ensures that the rows
		// that satisfy the outer-column equality are grouped separately from those
		// that do not. The rows that do not satisfy the equality will therefore not
		// affect the values of the rows that do, and they will be filtered out
		// later, ensuring that the transformation does not change the result set.
		// See also #130001.
		//
		// NOTE: this is more restrictive than PushSelectIntoGroupBy, which also
		// allows references to ConstAgg columns.
		private := t.Private().(*memo.GroupingPrivate)
		substituteCols.IntersectionWith(private.GroupingCols)
	case *memo.UnionExpr, *memo.UnionAllExpr, *memo.IntersectExpr,
		*memo.IntersectAllExpr, *memo.ExceptExpr, *memo.ExceptAllExpr:
		// [PushFilterIntoSetOp]
		// NOTE: the distinct variants (Union, Intersect, Except) de-duplicate
		// across all columns, so the requirement that filters only reference
		// grouping columns is always satisfied. See the comment for DistinctOn
		// above.
		substituteCols = getSubstituteColsSetOp(t, substituteCols)
	default:
		// Filter push-down through this expression is not supported.
		substituteCols = opt.ColSet{}
	}
	return substituteCols
}

func getSubstituteColsLeftSemiAntiJoin(join memo.RelExpr, substituteCols opt.ColSet) opt.ColSet {
	// It is always valid to push an equality between an outer and non-outer
	// left column into the left input of a LeftJoin, SemiJoin, or AntiJoin. If
	// one of the join filters constrains that left column to be equal to a right
	// column, it is also possible to remap and push the equality into the right
	// input. See the PushSelectCondLeftIntoJoinLeftAndRight rule for more info.
	//
	// We can satisfy these requirements by first restricting substituteCols
	// to left input columns, then extending it with right input columns
	// that are held equivalent by the join filters.
	left := join.Child(0).(memo.RelExpr)
	on := join.Child(2).(*memo.FiltersExpr)
	substituteCols.IntersectionWith(left.Relational().OutputCols)
	for i := range *on {
		// The usage of ComputeEquivClosureNoCopy is ok because
		// getSubstituteColsRelExpr copies the set.
		substituteCols = (*on)[i].ScalarProps().FuncDeps.ComputeEquivClosureNoCopy(substituteCols)
	}
	return substituteCols
}

func getSubstituteColsSetOp(set memo.RelExpr, substituteCols opt.ColSet) opt.ColSet {
	// Because TryRemapOuterCols is the equivalent of pushing down an
	// equality filter between an input column and an outer column, we don't
	// have to worry about composite sensitivity here (see CanMapOnSetOp).
	// Map the output columns contained in substituteCols to the columns from
	// both inputs.
	var newSubstituteCols opt.ColSet
	private := set.Private().(*memo.SetPrivate)
	for i, outCol := range private.OutCols {
		if substituteCols.Contains(outCol) {
			newSubstituteCols.Add(private.LeftCols[i])
			newSubstituteCols.Add(private.RightCols[i])
		}
	}
	return newSubstituteCols
}

// MakeCoalesceProjectionsForUnion builds a series of projections that coalesce
// columns from the left and right inputs of a union, projecting the result
// using the union operator's output columns.
func (c *CustomFuncs) MakeCoalesceProjectionsForUnion(
	setPrivate *memo.SetPrivate,
) memo.ProjectionsExpr {
	projections := make(memo.ProjectionsExpr, len(setPrivate.OutCols))
	for i := range setPrivate.OutCols {
		projections[i] = c.f.ConstructProjectionsItem(
			c.f.ConstructCoalesce(memo.ScalarListExpr{
				c.f.ConstructVariable(setPrivate.LeftCols[i]),
				c.f.ConstructVariable(setPrivate.RightCols[i]),
			}),
			setPrivate.OutCols[i],
		)
	}
	return projections
}

// MakeAnyNotNullScalarGroupBy wraps the input expression in a ScalarGroupBy
// that aggregates the input columns with AnyNotNull functions.
func (c *CustomFuncs) MakeAnyNotNullScalarGroupBy(input memo.RelExpr) memo.RelExpr {
	return c.f.ConstructScalarGroupBy(
		input,
		c.MakeAggCols(opt.AnyNotNullAggOp, input.Relational().OutputCols),
		memo.EmptyGroupingPrivate,
	)
}
