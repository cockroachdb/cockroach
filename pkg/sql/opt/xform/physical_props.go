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
func (o *Optimizer) canProvidePhysicalProps(eid memo.ExprID, required memo.PhysicalPropsID) bool {
	requiredProps := o.mem.LookupPhysicalProps(required)
	return o.canProvidePresentation(eid, requiredProps.Presentation) &&
		o.canProvideOrdering(eid, &requiredProps.Ordering)
}

// canProvidePresentation returns true if the given expression can provide the
// required presentation property. Currently, all relational operators are
// capable of doing this.
func (o *Optimizer) canProvidePresentation(eid memo.ExprID, required props.Presentation) bool {
	if required.Any() {
		return true
	}
	mexpr := o.mem.Expr(eid)
	if !mexpr.IsRelational() {
		panic("presentation property doesn't apply to non-relational operators")
	}

	// All operators can provide the Presentation property.
	return true
}

// canProvideOrdering returns true if the given expression can provide the
// required ordering property. The required ordering is assumed to have already
// been reduced using functional dependency analysis.
func (o *Optimizer) canProvideOrdering(eid memo.ExprID, required *props.OrderingChoice) bool {
	if required.Any() {
		return true
	}
	mexpr := o.mem.Expr(eid)
	if !mexpr.IsRelational() {
		panic("ordering property doesn't apply to non-relational operators")
	}

	switch mexpr.Operator() {
	case opt.SelectOp:
		// Select operator can always pass through ordering to its input.
		return true

	case opt.ProjectOp:
		// Project operators can pass through their ordering if the ordering
		// depends only on columns present in the input.
		return o.isOrderingBoundBy(required, mexpr.AsProject().Input())

	case opt.IndexJoinOp, opt.LookupJoinOp:
		// Index and Lookup Join operators can pass through their ordering if the
		// ordering depends only on columns present in the input.
		return o.isOrderingBoundBy(required, mexpr.ChildGroup(o.mem, 0))

	case opt.ScanOp:
		// Scan naturally orders according to the order of the scanned index.
		def := mexpr.Private(o.mem).(*memo.ScanOpDef)
		ok, _ := def.CanProvideOrdering(o.mem.Metadata(), required)
		return ok

	case opt.RowNumberOp:
		def := mexpr.Private(o.mem).(*memo.RowNumberDef)
		return def.CanProvideOrdering(required)

	case opt.MergeJoinOp:
		mergeOn := o.mem.NormExpr(mexpr.ChildGroup(o.mem, 2))
		def := mergeOn.Private(o.mem).(*memo.MergeOnDef)
		return def.CanProvideOrdering(required)

	case opt.LimitOp, opt.OffsetOp, opt.DistinctOnOp:
		// These operators require a certain ordering of their input, but can also
		// pass through a stronger ordering.
		return required.Intersects(o.internalOrdering(mexpr))

	case opt.GroupByOp:
		// Similar to Limit, GroupBy may require a certain ordering of its input,
		// but can also pass through a stronger ordering on the grouping columns.
		def := mexpr.Private(o.mem).(*memo.GroupByDef)
		if !required.CanProjectCols(def.GroupingCols) {
			return false
		}
		return required.Intersects(&def.Ordering)

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
func (o *Optimizer) buildChildPhysicalProps(
	parent memo.ExprID, required memo.PhysicalPropsID, nth int,
) memo.PhysicalPropsID {
	mexpr := o.mem.Expr(parent)

	// Fast path taken in common case when no properties are required of
	// parent and the operator itself does not require any properties.
	if required == memo.MinPhysPropsID {
		switch mexpr.Operator() {
		case opt.LimitOp, opt.OffsetOp,
			opt.ExplainOp,
			opt.RowNumberOp,
			opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp,
			opt.MergeJoinOp:
			// These operations can require an ordering of some child even if there is
			// no ordering requirement on themselves.
		default:
			return memo.MinPhysPropsID
		}
	}

	parentProps := o.mem.LookupPhysicalProps(required)

	var childProps props.Physical

	// Presentation property is provided by all the relational operators, so
	// don't add it to childProps.

	// Ordering property.
	switch mexpr.Operator() {
	case opt.SelectOp:
		if nth == 0 {
			childProps.Ordering = parentProps.Ordering
		}
	case opt.ProjectOp, opt.IndexJoinOp, opt.LookupJoinOp:
		if nth == 0 {
			childProps.Ordering = parentProps.Ordering
			if mexpr.Operator() == opt.ProjectOp {
				o.optimizeProjectOrdering(mexpr.AsProject(), &childProps)
			}
			childLogicalProps := o.mem.GroupProperties(mexpr.ChildGroup(o.mem, nth))
			childOutCols := childLogicalProps.Relational.OutputCols
			if !childProps.Ordering.SubsetOfCols(childOutCols) {
				childProps.Ordering = childProps.Ordering.Copy()
				childProps.Ordering.ProjectCols(childOutCols)
			}
		}

	case opt.RowNumberOp, opt.ScalarGroupByOp:
		// This op requires the ordering in its private.
		if nth == 0 {
			childProps.Ordering = *o.internalOrdering(mexpr)
		}

	case opt.LimitOp, opt.OffsetOp, opt.DistinctOnOp:
		if nth == 0 {
			// These ops require the ordering in their private, but can pass through a
			// stronger ordering. For example:
			//   SELECT * FROM (SELECT x, y FROM t ORDER BY x LIMIT 10) ORDER BY x,y
			// In this case the internal ordering is x+, but we can pass through x+,y+
			// to satisfy both orderings.
			childProps.Ordering = parentProps.Ordering.Intersection(o.internalOrdering(mexpr))
		}

	case opt.GroupByOp:
		if nth == 0 {
			// Similar to Limit, GroupBy may require a certain ordering of its input,
			// but can also pass through a stronger ordering on the grouping columns.
			def := mexpr.Private(o.mem).(*memo.GroupByDef)
			parentOrdering := parentProps.Ordering
			if !parentOrdering.SubsetOfCols(def.GroupingCols) {
				parentOrdering = parentOrdering.Copy()
				parentOrdering.ProjectCols(def.GroupingCols)
			}

			childProps.Ordering = parentOrdering.Intersection(&def.Ordering)
		}

	case opt.ExplainOp:
		if nth == 0 {
			childProps = o.mem.LookupPrivate(mexpr.AsExplain().Def()).(*memo.ExplainOpDef).Props
		}

	case opt.MergeJoinOp:
		if nth == 0 || nth == 1 {
			mergeOn := o.mem.NormExpr(mexpr.AsMergeJoin().MergeOn())
			def := o.mem.LookupPrivate(mergeOn.AsMergeOn().Def()).(*memo.MergeOnDef)
			if nth == 0 {
				childProps.Ordering = def.LeftOrdering
			} else {
				childProps.Ordering = def.RightOrdering
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
		props := o.mem.GroupProperties(mexpr.ChildGroup(o.mem, nth))
		if !childProps.Ordering.SubsetOfCols(props.Relational.OutputCols) {
			panic(fmt.Sprintf("OrderingChoice refers to non-output columns (op: %s)", mexpr.Operator()))
		}
	}

	// If properties haven't changed, no need to re-intern them.
	if childProps.Equals(parentProps) {
		return required
	}

	return o.mem.InternPhysicalProps(&childProps)
}

// internalOrdering returns the internal OrderingChoice stored in the private
// (for operators that have it).
func (o *Optimizer) internalOrdering(mexpr *memo.Expr) *props.OrderingChoice {
	private := mexpr.Private(o.mem)
	switch mexpr.Operator() {
	case opt.LimitOp, opt.OffsetOp:
		return private.(*props.OrderingChoice)

	case opt.RowNumberOp:
		return &private.(*memo.RowNumberDef).Ordering

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		return &private.(*memo.GroupByDef).Ordering

	default:
		return nil
	}
}

// isOrderingBoundBy returns whether or not input provides all columns present
// in ordering.
func (o *Optimizer) isOrderingBoundBy(ordering *props.OrderingChoice, input memo.GroupID) bool {
	inputCols := o.mem.GroupProperties(input).Relational.OutputCols
	return ordering.CanProjectCols(inputCols)
}

func (o *Optimizer) optimizeProjectOrdering(project *memo.ProjectExpr, physical *props.Physical) {
	// [SimplifyProjectOrdering]
	// SimplifyProjectOrdering tries to update the ordering required of a Project
	// operator's input expression, to make it less restrictive.
	relational := o.mem.GroupProperties(project.Input()).Relational
	if physical.Ordering.CanSimplify(&relational.FuncDeps) {
		if o.matchedRule == nil || o.matchedRule(opt.SimplifyProjectOrdering) {
			physical.Ordering = physical.Ordering.Copy()
			physical.Ordering.Simplify(&relational.FuncDeps)

			if o.appliedRule != nil {
				o.appliedRule(opt.SimplifyProjectOrdering, project.Input(), 0, 0)
			}
		}
	}
}
