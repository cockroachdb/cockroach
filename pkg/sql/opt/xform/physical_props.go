// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// canProvidePhysicalProps returns true if the given expression can provide the
// required physical properties. The optimizer calls the canProvide methods to
// determine whether an expression provides a required physical property. If it
// does not, then the optimizer inserts an enforcer operator that is able to
// provide it.
//
// Some operators, like Select and Project, may not directly provide a required
// physical property, but do "pass through" the requirement to their input.
// Operators that do this should return true from the appropriate canProvide
// method and then pass through that property in the buildChildPhysicalProps
// method.
func (o *Optimizer) canProvidePhysicalProps(e memo.RelExpr, required *props.Physical) bool {
	// All operators can provide the Presentation property, so no need to check
	// for that.
	return o.canProvideOrdering(e, &required.Ordering)
}

// canProvideOrdering returns true if the given expression can provide the
// required ordering property. The required ordering is assumed to have already
// been reduced using functional dependency analysis.
func (o *Optimizer) canProvideOrdering(e memo.RelExpr, required *props.OrderingChoice) bool {
	if required.Any() {
		return true
	}

	switch e.Op() {
	case opt.SelectOp:
		// Select operator can always pass through ordering to its input.
		return true

	case opt.ProjectOp, opt.IndexJoinOp, opt.LookupJoinOp:
		// These operators can pass through their ordering if the ordering
		// depends only on columns present in the input.
		return o.isOrderingBoundBy(e.Child(0).(memo.RelExpr), required)

	case opt.ScanOp:
		// Scan naturally orders according to the order of the scanned index.
		ok, _ := e.(*memo.ScanExpr).CanProvideOrdering(o.mem.Metadata(), required)
		return ok

	case opt.RowNumberOp:
		return e.(*memo.RowNumberExpr).CanProvideOrdering(required)

	case opt.MergeJoinOp:
		return e.(*memo.MergeJoinExpr).CanProvideOrdering(required)

	case opt.LimitOp, opt.OffsetOp, opt.DistinctOnOp:
		// These operators require a certain ordering of their input, but can also
		// pass through a stronger ordering.
		return required.Intersects(o.internalOrdering(e))

	case opt.GroupByOp:
		// Similar to Limit, GroupBy may require a certain ordering of its input,
		// but can also pass through a stronger ordering on the grouping columns.
		groupBy := e.(*memo.GroupByExpr)
		if !required.CanProjectCols(groupBy.GroupingCols) {
			return false
		}
		return required.Intersects(&groupBy.Ordering)

	case opt.ScalarGroupByOp:
		// ScalarGroupBy always has exactly one result; any required ordering should
		// have been simplified to Any (unless normalization rules are disabled).
	}

	return false
}

// buildChildPhysicalProps returns the set of physical properties required of
// the nth child, based upon the properties required of the parent. For example,
// the Project operator passes through any ordering requirement to its child,
// but provides any presentation requirement.
//
// The childProps argument is allocated once by the caller and can be reused
// repeatedly as physical properties are derived for each child. On each call,
// buildChildPhysicalProps updates the childProps argument.
func (o *Optimizer) buildChildPhysicalProps(
	parent memo.RelExpr, nth int, parentProps *props.Physical,
) *props.Physical {
	// Fast path taken in common case when no ordering property is required of
	// parent and the operator itself does not require any ordering.
	if parentProps == props.MinPhysProps {
		switch parent.Op() {
		case opt.LimitOp, opt.OffsetOp,
			opt.ExplainOp,
			opt.RowNumberOp,
			opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp,
			opt.MergeJoinOp:
			// These operations can require an ordering of some child even if there is
			// no ordering requirement on themselves.
		default:
			return props.MinPhysProps
		}
	}

	var childProps props.Physical

	switch parent.Op() {
	case opt.SelectOp:
		if nth == 0 {
			childProps.Ordering = parentProps.Ordering
		}

	case opt.ProjectOp, opt.IndexJoinOp, opt.LookupJoinOp:
		// These ops may need to remove ordering columns that are not output
		// by their input expression.
		if nth == 0 {
			childProps.Ordering = parentProps.Ordering
			childOutCols := parent.Child(0).(memo.RelExpr).Relational().OutputCols
			if !childProps.Ordering.SubsetOfCols(childOutCols) {
				childProps.Ordering = childProps.Ordering.Copy()
				childProps.Ordering.ProjectCols(childOutCols)
			}

			// Project can prune input columns, which can cause its FD set to be
			// pruned as well. Check the ordering to see if it can be simplified
			// with respect to the input FD set.
			if project, ok := parent.(*memo.ProjectExpr); ok {
				fdset := &project.Input.Relational().FuncDeps
				if childProps.Ordering.CanSimplify(fdset) {
					childProps.Ordering = childProps.Ordering.Copy()
					childProps.Ordering.Simplify(fdset)
				}
			}
		}

	case opt.RowNumberOp, opt.ScalarGroupByOp:
		// These ops require the ordering in their private.
		if nth == 0 {
			childProps.Ordering = *o.internalOrdering(parent)
		}

	case opt.LimitOp, opt.OffsetOp, opt.DistinctOnOp:
		if nth == 0 {
			// These ops require the ordering in their private, but can pass through a
			// stronger ordering. For example:
			//   SELECT * FROM (SELECT x, y FROM t ORDER BY x LIMIT 10) ORDER BY x,y
			// In this case the internal ordering is x+, but we can pass through x+,y+
			// to satisfy both orderings.
			childProps.Ordering = parentProps.Ordering.Intersection(o.internalOrdering(parent))
		}

	case opt.GroupByOp:
		if nth == 0 {
			// Similar to Limit, GroupBy may require a certain ordering of its input,
			// but can also pass through a stronger ordering on the grouping columns.
			groupBy := parent.(*memo.GroupByExpr)
			parentOrdering := parentProps.Ordering
			if !parentOrdering.SubsetOfCols(groupBy.GroupingCols) {
				parentOrdering = parentOrdering.Copy()
				parentOrdering.ProjectCols(groupBy.GroupingCols)
			}

			childProps.Ordering = parentOrdering.Intersection(&groupBy.Ordering)

			// The FD set of the input doesn't "pass through" to the GroupBy FD set;
			// check the ordering to see if it can be simplified with respect to the
			// input FD set.
			childProps.Ordering.Simplify(&groupBy.Input.Relational().FuncDeps)
		}

	case opt.ExplainOp:
		if nth == 0 {
			childProps = *parent.(*memo.ExplainExpr).Props
		}

	case opt.MergeJoinOp:
		if nth == 0 || nth == 1 {
			merge := parent.(*memo.MergeJoinExpr)
			if nth == 0 {
				childProps.Ordering = merge.LeftOrdering
			} else {
				childProps.Ordering = merge.RightOrdering
			}
		}
		// ************************* WARNING *************************
		//  If you add a new case here, check if it needs to be added
		//     to the exception list in the fast path above.
		// ************************* WARNING *************************
	}

	// RaceEnabled ensures that checks are run on every change (as part of make
	// testrace) while keeping the check code out of non-test builds.
	if util.RaceEnabled && !childProps.Ordering.Any() {
		outCols := parent.Child(nth).(memo.RelExpr).Relational().OutputCols
		if !childProps.Ordering.SubsetOfCols(outCols) {
			panic(fmt.Sprintf("OrderingChoice refers to non-output columns (op: %s)", parent.Op()))
		}
	}

	// If properties haven't changed, no need to re-intern them.
	if childProps.Equals(parentProps) {
		return parentProps
	}

	return o.mem.InternPhysicalProps(&childProps)
}

// internalOrdering returns the internal OrderingChoice stored in the private
// (for operators that have it).
func (o *Optimizer) internalOrdering(nd memo.RelExpr) *props.OrderingChoice {
	switch t := nd.(type) {
	case *memo.LimitExpr:
		return &t.Ordering
	case *memo.OffsetExpr:
		return &t.Ordering
	case *memo.RowNumberExpr:
		return &t.Ordering
	case *memo.GroupByExpr:
		return &t.Ordering
	case *memo.ScalarGroupByExpr:
		return &t.Ordering
	case *memo.DistinctOnExpr:
		return &t.Ordering
	default:
		return nil
	}
}

// isOrderingBoundBy returns whether or not input provides all columns present
// in ordering.
func (o *Optimizer) isOrderingBoundBy(input memo.RelExpr, ordering *props.OrderingChoice) bool {
	inputCols := input.Relational().OutputCols
	return ordering.CanProjectCols(inputCols)
}
