// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
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
	}

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
					hasHoistableSubquery = sharedProps.VolatilitySet.IsLeakProof()
				}

			case *memo.WhenExpr:
				if child == t.Value {
					memo.BuildSharedProps(child, &sharedProps, c.f.evalCtx)
					hasHoistableSubquery = sharedProps.VolatilitySet.IsLeakProof()
				}

			case *memo.IfErrExpr:
				// Determine whether this is the Else child. Checking this how the
				// other branches do is tricky because it's a list, but we know that
				// it's at position 1.
				if i == 1 {
					memo.BuildSharedProps(child, &sharedProps, c.f.evalCtx)
					hasHoistableSubquery = sharedProps.VolatilitySet.IsLeakProof()
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
//   SELECT * FROM xy WHERE (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//   =>
//   SELECT xy.*
//   FROM xy
//   LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//   ON True
//   WHERE u IS NULL
//
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
//   SELECT (SELECT max(u) FROM uv WHERE u=x) AS max FROM xy
//   =>
//   SELECT max
//   FROM xy
//   INNER JOIN LATERAL (SELECT max(u) FROM uv WHERE u=x)
//   ON True
//
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
//   SELECT y, z
//   FROM xy
//   FULL JOIN yz
//   ON (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//   =>
//   SELECT y, z
//   FROM xy
//   FULL JOIN LATERAL
//   (
//     SELECT *
//     FROM yz
//     LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//     ON True
//   )
//   ON u IS NULL
//
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
	passthrough := c.OutputCols(left).Union(c.OutputCols(right))
	return c.f.ConstructProject(join, memo.EmptyProjectionsExpr, passthrough)
}

// HoistValuesSubquery searches the Values operator's projections for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
//
//   SELECT (VALUES (SELECT u FROM uv WHERE u=x LIMIT 1)) FROM xy
//   =>
//   SELECT
//   (
//     SELECT vals.*
//     FROM (VALUES ())
//     LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//     ON True
//     INNER JOIN LATERAL (VALUES (u)) vals
//     ON True
//   )
//   FROM xy
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
//   SELECT generate_series
//   FROM xy
//   INNER JOIN LATERAL ROWS FROM
//   (
//     generate_series(1, (SELECT v FROM uv WHERE u=x))
//   )
//   =>
//   SELECT generate_series
//   FROM xy
//   ROWS FROM
//   (
//     SELECT generate_series
//     FROM (VALUES ())
//     LEFT JOIN LATERAL (SELECT v FROM uv WHERE u=x)
//     ON True
//     INNER JOIN LATERAL ROWS FROM (generate_series(1, v))
//     ON True
//   )
//
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
	_, ok := c.CandidateKey(in)
	if ok {
		return in
	}

	// Try to add the preexisting primary key if the input is a Scan or Scan
	// wrapped in a Select.
	if res, ok := c.TryAddKeyToScan(in); ok {
		return res
	}

	// Otherwise, wrap the input in an Ordinality operator.
	colID := c.f.Metadata().AddColumn("rownum", types.Int)
	private := memo.OrdinalityPrivate{ColID: colID}
	return c.f.ConstructOrdinality(in, &private)
}

// TryAddKeyToScan checks whether the input expression is a non-virtual table
// Scan, either alone or wrapped in a Select. If so, it returns a new Scan
// (possibly wrapped in a Select) augmented with the preexisting primary key
// for the table.
func (c *CustomFuncs) TryAddKeyToScan(in memo.RelExpr) (_ memo.RelExpr, ok bool) {
	augmentScan := func(scan *memo.ScanExpr) (_ memo.RelExpr, ok bool) {
		private := scan.ScanPrivate
		tableID := private.Table
		table := c.f.Metadata().Table(tableID)
		if !table.IsVirtualTable() {
			keyCols := c.PrimaryKeyCols(tableID)
			private.Cols = private.Cols.Union(keyCols)
			return c.f.ConstructScan(&private), true
		}
		return nil, false
	}

	switch t := in.(type) {
	case *memo.ScanExpr:
		if res, ok := augmentScan(t); ok {
			return res, true
		}

	case *memo.SelectExpr:
		if scan, ok := t.Input.(*memo.ScanExpr); ok {
			if res, ok := augmentScan(scan); ok {
				return c.f.ConstructSelect(res, t.Filters), true
			}
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
//   (Aggregations
//     [(ConstAgg (Variable 1)) (ConstAgg (Variable 2)) (FirstAgg (Variable 3))]
//     [1,2,3]
//   )
//
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
//   * It is CountRows (because it will be translated into Count),
//   * It ignores nulls (because nothing extra must be done for it)
//   * It gives NULL on no input (because this is how we translate non-null
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

// ConstructBinary builds a dynamic binary expression, given the binary
// operator's type and its two arguments.
func (c *CustomFuncs) ConstructBinary(op opt.Operator, left, right opt.ScalarExpr) opt.ScalarExpr {
	return c.f.DynamicConstruct(op, left, right).(opt.ScalarExpr)
}

// ConstructNoColsRow returns a Values operator having a single row with zero
// columns.
func (c *CustomFuncs) ConstructNoColsRow() memo.RelExpr {
	return c.f.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
		Cols: opt.ColList{},
		ID:   c.f.Metadata().NextUniqueID(),
	})
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
//   SELECT *
//   FROM xy
//   WHERE
//     (SELECT u FROM uv WHERE u=x LIMIT 1) IS NOT NULL
//     OR EXISTS(SELECT * FROM jk WHERE j=x)
//   =>
//   SELECT xy.*
//   FROM xy
//   LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//   ON True
//   INNER JOIN LATERAL
//   (
//     SELECT (CONST_AGG(True) IS NOT NULL) AS exists FROM jk WHERE j=x
//   )
//   ON True
//   WHERE u IS NOT NULL OR exists
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
		if subquery.Relational().OuterCols.Empty() {
			break
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

// constructGroupByExists transforms a scalar Exists expression like this:
//
//   EXISTS(SELECT * FROM a WHERE a.x=b.x)
//
// into a scalar GroupBy expression that returns a one row, one column relation:
//
//   SELECT (CONST_AGG(True) IS NOT NULL) AS exists
//   FROM (SELECT * FROM a WHERE a.x=b.x)
//
// The expression uses an internally-defined CONST_AGG aggregation function,
// since it's able to short-circuit on the first non-null it encounters. The
// above expression is equivalent to:
//
//   SELECT COUNT(True) > 0 FROM (SELECT * FROM a WHERE a.x=b.x)
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
	trueColID := r.f.Metadata().AddColumn("true", types.Bool)
	aggColID := r.f.Metadata().AddColumn("true_agg", types.Bool)
	existsColID := r.f.Metadata().AddColumn("exists", types.Bool)

	return r.f.ConstructProject(
		r.f.ConstructScalarGroupBy(
			r.f.ConstructProject(
				subquery,
				memo.ProjectionsExpr{r.f.ConstructProjectionsItem(memo.TrueSingleton, trueColID)},
				opt.ColSet{},
			),
			memo.AggregationsExpr{r.f.ConstructAggregationsItem(
				r.f.ConstructConstAgg(r.f.ConstructVariable(trueColID)),
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

// constructGroupByAny transforms a scalar Any expression like this:
//
//   z = ANY(SELECT x FROM xy)
//
// into a scalar GroupBy expression that returns a one row, one column relation
// that is equivalent to this:
//
//   SELECT
//     CASE
//       WHEN bool_or(notnull) AND z IS NOT Null THEN True
//       ELSE bool_or(notnull) IS NULL THEN False
//       ELSE Null
//     END
//   FROM
//   (
//     SELECT x IS NOT Null AS notnull
//     FROM xy
//     WHERE (z=x) IS NOT False
//   )
//
// BOOL_OR returns true if any input is true, else false if any input is false,
// else null. This is a mismatch with ANY, which returns true if any input is
// true, else null if any input is null, else false. In addition, the expression
// needs to be easy to decorrelate, which means that the outer column reference
// ("z" in the example) should not be part of a projection (since projections
// are difficult to hoist above left joins). The following procedure solves the
// mismatch between BOOL_OR and ANY, as well as avoids correlated projections:
//
//   1. Filter out false comparison rows with an initial filter. The result of
//      ANY does not change, no matter how many false rows are added or removed.
//      This step has the effect of mapping a set containing only false
//      comparison rows to the empty set (which is desirable).
//
//   2. Step #1 leaves only true and null comparison rows. A null comparison row
//      occurs when either the left or right comparison operand is null (Any
//      only allows comparison operators that propagate nulls). Map each null
//      row to a false row, but only in the case where the right operand is null
//      (i.e. the operand that came from the subquery). The case where the left
//      operand is null will be handled later.
//
//   3. Use the BOOL_OR aggregation function on the true/false values from step
//      #2. If there is at least one true value, then BOOL_OR returns true. If
//      there are no values (the empty set case), then BOOL_OR returns null.
//      Because of the previous steps, this indicates that the original set
//      contained only false values (or no values at all).
//
//   4. A True result from BOOL_OR is ambiguous. It could mean that the
//      comparison returned true for one of the rows in the group. Or, it could
//      mean that the left operand was null. The CASE statement ensures that
//      True is only returned if the left operand was not null.
//
//   5. In addition, the CASE statement maps a null return value to false, and
//      false to null. This matches ANY behavior.
//
// The following is a table showing the various interesting cases:
//
//         | subquery  | before        | after   | after
//     z   | x values  | BOOL_OR       | BOOL_OR | CASE
//   ------+-----------+---------------+---------+-------
//     1   | (1)       | (true)        | true    | true
//     1   | (1, null) | (true, false) | true    | true
//     1   | (1, 2)    | (true)        | true    | true
//     1   | (null)    | (false)       | false   | null
//    null | (1)       | (true)        | true    | null
//    null | (1, null) | (true, false) | true    | null
//    null | (null)    | (false)       | false   | null
//     2   | (1)       | (empty)       | null    | false
//   *any* | (empty)   | (empty)       | null    | false
//
// It is important that the set given to BOOL_OR does not contain any null
// values (the reason for step #2). Null is reserved for use by the
// TryDecorrelateScalarGroupBy rule, which will push a left join into the
// GroupBy. Null values produced by the left join will simply be ignored by
// BOOL_OR, and so cannot be used for any other purpose.
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

	inputVar := r.f.funcs.referenceSingleColumn(input)
	notNullColID := r.f.Metadata().AddColumn("notnull", types.Bool)
	aggColID := r.f.Metadata().AddColumn("bool_or", types.Bool)
	aggVar := r.f.ConstructVariable(aggColID)
	caseColID := r.f.Metadata().AddColumn("case", types.Bool)

	return r.f.ConstructProject(
		r.f.ConstructScalarGroupBy(
			r.f.ConstructProject(
				r.f.ConstructSelect(
					input,
					memo.FiltersExpr{r.f.ConstructFiltersItem(
						r.f.ConstructIsNot(
							r.f.funcs.ConstructBinary(cmp, scalar, inputVar),
							memo.FalseSingleton,
						),
					)},
				),
				memo.ProjectionsExpr{r.f.ConstructProjectionsItem(
					r.f.ConstructIsNot(inputVar, memo.NullSingleton),
					notNullColID,
				)},
				opt.ColSet{},
			),
			memo.AggregationsExpr{r.f.ConstructAggregationsItem(
				r.f.ConstructBoolOr(
					r.f.ConstructVariable(notNullColID),
				),
				aggColID,
			)},
			memo.EmptyGroupingPrivate,
		),
		memo.ProjectionsExpr{r.f.ConstructProjectionsItem(
			r.f.ConstructCase(
				r.f.ConstructTrue(),
				memo.ScalarListExpr{
					r.f.ConstructWhen(
						r.f.ConstructAnd(
							aggVar,
							r.f.ConstructIsNot(scalar, memo.NullSingleton),
						),
						r.f.ConstructTrue(),
					),
					r.f.ConstructWhen(
						r.f.ConstructIs(aggVar, memo.NullSingleton),
						r.f.ConstructFalse(),
					),
				},
				r.f.ConstructNull(types.Bool),
			),
			caseColID,
		)},
		opt.ColSet{},
	)
}
