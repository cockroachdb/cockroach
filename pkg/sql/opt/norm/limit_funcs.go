// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// LimitGeMaxRows returns true if the given constant limit value is greater than
// or equal to the max number of rows returned by the input expression.
func (c *CustomFuncs) LimitGeMaxRows(limit tree.Datum, input memo.RelExpr) bool {
	limitVal := int64(*limit.(*tree.DInt))
	maxRows := input.Relational().Cardinality.Max
	return limitVal >= 0 && maxRows < math.MaxUint32 && limitVal >= int64(maxRows)
}

// TryPushLimitOrOffsetIntoGroupByInput attempts to push a Limit or Offset to a
// child/descendant expression of a grouping operator. This is possible only if
// the following conditions are satisfied:
//
//  1. The descendant expression can project the grouping and ordering columns.
//
//  2. The grouping columns form a key on the descendant expression.
//
//  3. Grouping column tuples (e.g. (a, b) pairs if grouping by "a" and "b") may
//     be duplicated between the descendant expression and the grouping
//     operator, but they cannot be filtered, and new tuples cannot be added.
//
// What does it mean to "duplicate" a tuple vs adding a new one? In this
// context, to duplicate a tuple is to join it with multiple others so that the
// number of rows grows, but the (unique) set of tuple values across the chosen
// columns remains the same. Select operators can filter tuples, while Union
// operators can add new ones. Join operators can filter or duplicate tuples,
// but do not introduce new ones. This definition is the same as that used by
// props.JoinMultiplicity.
//
// NOTE: outer joins can also add new tuples to the set of grouping columns via
// null-extension. Therefore, we cannot push the limit/offset through the
// null-extended input(s) of an outer join.
//
// Why is this correct? Since we disallow any intermediate operators that may
// filter or add tuples from the descendant expression (condition 3), the set of
// groups that reaches the grouping operator will correspond directly to the set
// of rows emitted by the descendant expression. The grouping operator will
// de-duplicate the rows by a key on the descendant expression (condition 2), so
// the resulting cardinality will be exactly the same as that of the child
// expression. Therefore, a limit on the grouping operator's output can be
// applied to the output of the child expression.
func (c *CustomFuncs) TryPushLimitOrOffsetIntoGroupByInput(
	limitOp opt.Operator,
	input memo.RelExpr,
	groupingCols opt.ColSet,
	limitOffsetVal opt.ScalarExpr,
	limitOffsetOrdering props.OrderingChoice,
) (memo.RelExpr, bool) {
	var replace ReplaceFunc
	replace = func(expr opt.Expr) opt.Expr {
		relExpr, ok := expr.(memo.RelExpr)
		if !ok {
			return expr
		}
		if !groupingCols.SubsetOf(relExpr.Relational().OutputCols) {
			// Skip expressions that do not project the grouping columns.
			return expr
		}
		if !c.OrderingCanProjectCols(limitOffsetOrdering, relExpr.Relational().OutputCols) {
			// It is impossible to satisfy the limit with this expression or its
			// children.
			return expr
		}
		if relExpr.Relational().FuncDeps.ColsAreStrictKey(groupingCols) {
			// The grouping columns form a key on this expression, and the ordering
			// can be satisfied. Wrap this expression with a Limit or Offset.
			return c.f.DynamicConstruct(limitOp, relExpr, limitOffsetVal, &limitOffsetOrdering)
		}
		// Attempt to traverse further down the tree. Do not traverse past
		// expressions that can filter rows, such as Select, or those that add rows
		// such as Union.
		switch t := relExpr.(type) {
		case *memo.ProjectExpr, *memo.OrdinalityExpr, *memo.WindowExpr:
			// These expressions never filter or add rows, though they may add
			// columns. We check that the grouping columns are projected by candidate
			// expressions above.
			return c.f.Replace(t, replace)
		case *memo.LeftJoinExpr:
			// Left joins preserve rows from the left input. If they preserved rows
			// from the right input, we would have simplified the join, so no need
			// to check the right input.
			if newLeft := replace(t.Left); newLeft != t.Left {
				// The right input may have been modified, so we need to reconstruct
				// the join.
				return c.f.ConstructLeftJoin(newLeft.(memo.RelExpr), t.Right, t.On, &t.JoinPrivate)
			}
		case *memo.InnerJoinExpr:
			// InnerJoin can preserve rows conditionally from either input, depending
			// on the filters.
			left, right := t.Child(0).(memo.RelExpr), t.Child(1).(memo.RelExpr)
			on, private := t.Child(2).(*memo.FiltersExpr), t.Private().(*memo.JoinPrivate)
			if c.JoinPreservesLeftRows(t) {
				// The join does not filter rows from the left child.
				if newLeft := replace(left); newLeft != left {
					return c.f.ConstructInnerJoin(newLeft.(memo.RelExpr), right, *on, private)
				}
			}
			if c.JoinPreservesRightRows(t) {
				// The join does not filter rows from the right child.
				if newRight := replace(right); newRight != right {
					return c.f.ConstructInnerJoin(left, newRight.(memo.RelExpr), *on, private)
				}
			}
		}
		// The expression may filter or add rows.
		//
		// We cannot push the limit for FullJoin, since it may null-extend rows
		// from either side. We don't include SemiJoin or AntiJoin since they do not
		// project columns from the right input, and would be removed if they
		// matched all rows from the left input.
		return expr
	}
	replaced := replace(input)
	return replaced.(memo.RelExpr), replaced != input
}
