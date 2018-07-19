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

	if !requiredProps.Presentation.Any() {
		if !o.canProvidePresentation(eid, requiredProps.Presentation) {
			return false
		}
	}

	if !requiredProps.Ordering.Any() {
		if !o.canProvideOrdering(eid, &requiredProps.Ordering) {
			return false
		}
	}

	return true
}

// canProvidePresentation returns true if the given expression can provide the
// required presentation property. Currently, all relational operators are
// capable of doing this.
func (o *Optimizer) canProvidePresentation(eid memo.ExprID, required props.Presentation) bool {
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
		return def.CanProvideOrdering(o.mem.Metadata(), required)

	case opt.RowNumberOp:
		def := o.mem.LookupPrivate(mexpr.AsRowNumber().Def()).(*memo.RowNumberDef)
		return def.CanProvideOrdering(required)

	case opt.LimitOp:
		// Limit can provide the same ordering it requires of its input.
		provided := o.mem.LookupPrivate(mexpr.AsLimit().Ordering()).(*props.OrderingChoice)
		return provided.SubsetOf(required)

	case opt.OffsetOp:
		// Offset can provide the same ordering it requires of its input.
		provided := o.mem.LookupPrivate(mexpr.AsOffset().Ordering()).(*props.OrderingChoice)
		return provided.SubsetOf(required)
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
			opt.GroupByOp, opt.ScalarGroupByOp,
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

	case opt.LimitOp, opt.OffsetOp, opt.RowNumberOp, opt.GroupByOp, opt.ScalarGroupByOp:
		// These ops require the ordering in their private.
		if nth == 0 {
			var ordering *props.OrderingChoice
			switch mexpr.Operator() {
			case opt.LimitOp:
				ordering = o.mem.LookupPrivate(mexpr.AsLimit().Ordering()).(*props.OrderingChoice)
			case opt.OffsetOp:
				ordering = o.mem.LookupPrivate(mexpr.AsOffset().Ordering()).(*props.OrderingChoice)
			case opt.RowNumberOp:
				def := o.mem.LookupPrivate(mexpr.AsRowNumber().Def()).(*memo.RowNumberDef)
				ordering = &def.Ordering
			case opt.GroupByOp, opt.ScalarGroupByOp:
				def := mexpr.Private(o.mem).(*memo.GroupByDef)
				ordering = &def.Ordering
			}
			childProps.Ordering = *ordering
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
