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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// NullsAreDistinct returns true if the given distinct operator treats NULL
// values as not equal to one another (i.e. distinct). UpsertDistinctOp treats
// NULL values as distinct, whereas DistinctOp does not.
func (c *CustomFuncs) NullsAreDistinct(distinctOp opt.Operator) bool {
	return distinctOp == opt.UpsertDistinctOnOp
}

// RemoveGroupingCols returns a new grouping private struct with the given
// columns removed from the grouping column set.
func (c *CustomFuncs) RemoveGroupingCols(
	private *memo.GroupingPrivate, cols opt.ColSet,
) *memo.GroupingPrivate {
	p := *private
	p.GroupingCols = private.GroupingCols.Difference(cols)
	return &p
}

// AppendAggCols constructs a new Aggregations operator containing the aggregate
// functions from an existing Aggregations operator plus an additional set of
// aggregate functions, one for each column in the given set. The new functions
// are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols(
	aggs memo.AggregationsExpr, aggOp opt.Operator, cols opt.ColSet,
) memo.AggregationsExpr {
	outAggs := make(memo.AggregationsExpr, len(aggs)+cols.Len())
	copy(outAggs, aggs)
	c.makeAggCols(aggOp, cols, outAggs[len(aggs):])
	return outAggs
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

// makeAggCols is a helper method that constructs a new aggregate function of
// the given operator type for each column in the given set. The resulting
// aggregates are written into outElems and outColList. As an example, for
// columns (1,2) and operator ConstAggOp, makeAggCols will set the following:
//
//   outElems[0] = (ConstAggOp (Variable 1))
//   outElems[1] = (ConstAggOp (Variable 2))
//
//   outColList[0] = 1
//   outColList[1] = 2
//
func (c *CustomFuncs) makeAggCols(
	aggOp opt.Operator, cols opt.ColSet, outAggs memo.AggregationsExpr,
) {
	// Append aggregate functions wrapping a Variable reference to each column.
	i := 0
	for id, ok := cols.Next(0); ok; id, ok = cols.Next(id + 1) {
		varExpr := c.f.ConstructVariable(id)

		var outAgg opt.ScalarExpr
		switch aggOp {
		case opt.ConstAggOp:
			outAgg = c.f.ConstructConstAgg(varExpr)

		case opt.AnyNotNullAggOp:
			outAgg = c.f.ConstructAnyNotNullAgg(varExpr)

		case opt.FirstAggOp:
			outAgg = c.f.ConstructFirstAgg(varExpr)

		default:
			panic(errors.AssertionFailedf("unrecognized aggregate operator type: %v", log.Safe(aggOp)))
		}

		outAggs[i] = c.f.ConstructAggregationsItem(outAgg, id)
		i++
	}
}

// CanRemoveAggDistinctForKeys returns true if the given aggregate function
// where its input column, together with the grouping columns, form a key. In
// this case, the wrapper AggDistinct can be removed.
func (c *CustomFuncs) CanRemoveAggDistinctForKeys(
	input memo.RelExpr, private *memo.GroupingPrivate, agg opt.ScalarExpr,
) bool {
	if agg.ChildCount() == 0 {
		return false
	}
	inputFDs := &input.Relational().FuncDeps
	variable := agg.Child(0).(*memo.VariableExpr)
	cols := c.AddColToSet(private.GroupingCols, variable.Col)
	return inputFDs.ColsAreStrictKey(cols)
}

// ReplaceAggregationsItem returns a new list that is a copy of the given list,
// except that the given search item has been replaced by the given replace
// item. If the list contains the search item multiple times, then only the
// first instance is replaced. If the list does not contain the item, then the
// method panics.
func (c *CustomFuncs) ReplaceAggregationsItem(
	aggs memo.AggregationsExpr, search *memo.AggregationsItem, replace opt.ScalarExpr,
) memo.AggregationsExpr {
	newAggs := make([]memo.AggregationsItem, len(aggs))
	for i := range aggs {
		if search == &aggs[i] {
			copy(newAggs, aggs[:i])
			newAggs[i] = c.f.ConstructAggregationsItem(replace, search.Col)
			copy(newAggs[i+1:], aggs[i+1:])
			return newAggs
		}
	}
	panic(errors.AssertionFailedf("item to replace is not in the list: %v", search))
}

// HasNoGroupingCols returns true if the GroupingCols in the private are empty.
func (c *CustomFuncs) HasNoGroupingCols(private *memo.GroupingPrivate) bool {
	return private.GroupingCols.Empty()
}

// GroupingInputOrdering returns the Ordering in the private.
func (c *CustomFuncs) GroupingInputOrdering(private *memo.GroupingPrivate) physical.OrderingChoice {
	return private.Ordering
}

// ConstructProjectionFromDistinctOn converts a DistinctOn to a projection; this
// is correct when input groupings have at most one row (i.e. the input is
// already distinct). Note that DistinctOn can only have aggregations of type
// FirstAgg or ConstAgg.
func (c *CustomFuncs) ConstructProjectionFromDistinctOn(
	input memo.RelExpr, groupingCols opt.ColSet, aggs memo.AggregationsExpr,
) memo.RelExpr {
	// Always pass through grouping columns.
	passthrough := groupingCols.Copy()

	var projections memo.ProjectionsExpr
	for i := range aggs {
		varExpr := memo.ExtractAggFirstVar(aggs[i].Agg)
		inputCol := varExpr.Col
		outputCol := aggs[i].Col
		if inputCol == outputCol {
			passthrough.Add(inputCol)
		} else {
			projections = append(projections, c.f.ConstructProjectionsItem(varExpr, aggs[i].Col))
		}
	}
	return c.f.ConstructProject(input, projections, passthrough)
}

// DuplicateUpsertErrText returns the error text used when duplicate input rows
// to the Upsert operator are detected.
func (c *CustomFuncs) DuplicateUpsertErrText() string {
	return sqlbase.DuplicateUpsertErrText
}

// AreValuesDistinct returns true if a constant Values operator input contains
// only rows that are already distinct with respect to the given grouping
// columns. The Values operator can be wrapped by Select, Project, and/or
// LeftJoin operators.
//
// If nullsAreDistinct is true, then NULL values are treated as not equal to one
// another, and therefore rows containing a NULL value in any grouping column
// are always distinct.
func (c *CustomFuncs) AreValuesDistinct(
	input memo.RelExpr, groupingCols opt.ColSet, nullsAreDistinct bool,
) bool {
	switch t := input.(type) {
	case *memo.ValuesExpr:
		return c.areRowsDistinct(t.Rows, t.Cols, groupingCols, nullsAreDistinct)

	case *memo.SelectExpr:
		return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)

	case *memo.ProjectExpr:
		// Pass through call to input if grouping on passthrough columns.
		if groupingCols.SubsetOf(t.Input.Relational().OutputCols) {
			return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)
		}

	case *memo.LeftJoinExpr:
		// Pass through call to left input if grouping on its columns. Also,
		// ensure that the left join does not cause duplication of left rows.
		leftCols := t.Left.Relational().OutputCols
		rightCols := t.Right.Relational().OutputCols
		if !groupingCols.SubsetOf(leftCols) {
			break
		}

		// If any set of key columns (lax or strict) from the right input are
		// equality joined to columns in the left input, then the left join will
		// never cause duplication of left rows.
		var eqCols opt.ColSet
		for i := range t.On {
			condition := t.On[i].Condition
			ok, _, rightColID := memo.ExtractJoinEquality(leftCols, rightCols, condition)
			if ok {
				eqCols.Add(rightColID)
			}
		}
		if !t.Right.Relational().FuncDeps.ColsAreLaxKey(eqCols) {
			// Not joining on a right input key.
			break
		}

		return c.AreValuesDistinct(t.Left, groupingCols, nullsAreDistinct)

	case *memo.UpsertDistinctOnExpr:
		// Pass through call to input if grouping on passthrough columns.
		if groupingCols.SubsetOf(t.Input.Relational().OutputCols) {
			return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)
		}
	}
	return false
}

// areRowsDistinct returns true if the given rows are unique on the given
// grouping columns. If nullsAreDistinct is true, then NULL values are treated
// as unique, and therefore a row containing a NULL value in any grouping column
// is always distinct from every other row.
func (c *CustomFuncs) areRowsDistinct(
	rows memo.ScalarListExpr, cols opt.ColList, groupingCols opt.ColSet, nullsAreDistinct bool,
) bool {
	var seen map[string]bool
	var encoded []byte
	for _, scalar := range rows {
		row := scalar.(*memo.TupleExpr)

		// Reset scratch bytes.
		encoded = encoded[:0]

		forceDistinct := false
		for i, colID := range cols {
			if !groupingCols.Contains(colID) {
				// This is not a grouping column, so ignore.
				continue
			}

			// Try to extract constant value from column. Call IsConstValueOp first,
			// since this code doesn't handle the tuples and arrays that
			// ExtractConstDatum can return.
			col := row.Elems[i]
			if !opt.IsConstValueOp(col) {
				// At least one grouping column in at least one row is not constant,
				// so can't determine whether the rows are distinct.
				return false
			}
			datum := memo.ExtractConstDatum(col)

			// If this is an UpsertDistinctOn operator, then treat NULL values as
			// always distinct.
			if nullsAreDistinct && datum == tree.DNull {
				forceDistinct = true
				break
			}

			// Encode the datum using the key encoding format. The encodings for
			// multiple column datums are simply appended to one another.
			var err error
			encoded, err = sqlbase.EncodeTableKey(encoded, datum, encoding.Ascending)
			if err != nil {
				// Assume rows are not distinct if an encoding error occurs.
				return false
			}
		}

		if seen == nil {
			seen = make(map[string]bool, len(rows))
		}

		// Determine whether key has already been seen.
		key := string(encoded)
		if _, ok := seen[key]; ok && !forceDistinct {
			// Found duplicate.
			return false
		}

		// Add the key to the seen map.
		seen[key] = true
	}

	return true
}
