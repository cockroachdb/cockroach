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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// physicalPropsBuilder determines what physical properties a given expression
// provides and constructs the physical properties required of its children
// based on those properties. The optimizer calls the canProvide methods to
// determine whether an expression provides a required physical property. If it
// does not, then the optimizer inserts an enforcer operator that is able to
// provide it. Some operators, like Select and Project, may not directly
// provide a required physical property, but do "pass through" the requirement
// to their input. Operators that do this should return true from the
// appropriate canProvide method and then pass through that property in the
// buildChildProps method.
type physicalPropsBuilder struct {
	mem *memo.Memo
}

// canProvide returns true if the given expression can provide the required
// physical properties.
func (b physicalPropsBuilder) canProvide(eid memo.ExprID, required memo.PhysicalPropsID) bool {
	requiredProps := b.mem.LookupPhysicalProps(required)

	if requiredProps.Presentation.Defined() {
		if !b.canProvidePresentation(eid, requiredProps.Presentation) {
			return false
		}
	}

	if requiredProps.Ordering.Defined() {
		if !b.canProvideOrdering(eid, requiredProps.Ordering) {
			return false
		}
	}

	return true
}

// canProvidePresentation returns true if the given expression can provide the
// required presentation property. Currently, all relational operators are
// capable of doing this.
func (b physicalPropsBuilder) canProvidePresentation(
	eid memo.ExprID, required props.Presentation,
) bool {
	mexpr := b.mem.Expr(eid)
	if !mexpr.IsRelational() {
		panic("presentation property doesn't apply to non-relational operators")
	}

	// All operators can provide the Presentation property.
	return true
}

// canProvideOrdering returns true if the given expression can provide the
// required ordering property.
func (b physicalPropsBuilder) canProvideOrdering(eid memo.ExprID, required props.Ordering) bool {
	// TODO(justin): we should trim any ordering in `required` which contains a
	// key to its shortest prefix which still contains a key.
	mexpr := b.mem.Expr(eid)
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
		return b.orderingBoundBy(required, mexpr.AsProject().Input())

	case opt.LookupJoinOp:
		// Index Join operators can pass through their ordering if the ordering
		// depends only on columns present in the input.
		return b.orderingBoundBy(required, mexpr.AsLookupJoin().Input())

	case opt.ScanOp:
		// Scan naturally orders according to the order of the scanned index.
		def := mexpr.Private(b.mem).(*memo.ScanOpDef)
		return def.CanProvideOrdering(b.mem.Metadata(), required)

	case opt.RowNumberOp:
		def := b.mem.LookupPrivate(mexpr.AsRowNumber().Def()).(*memo.RowNumberDef)
		return def.CanProvideOrdering(required)

	case opt.LimitOp:
		// Limit can provide the same ordering it requires of its input.
		return b.mem.LookupPrivate(mexpr.AsLimit().Ordering()).(props.Ordering).Provides(required)

	case opt.OffsetOp:
		// Offset can provide the same ordering it requires of its input.
		return b.mem.LookupPrivate(mexpr.AsOffset().Ordering()).(props.Ordering).Provides(required)
	}

	return false
}

// orderingBoundBy returns whether or not input provides all columns present in
// ordering.
func (b physicalPropsBuilder) orderingBoundBy(ordering props.Ordering, input memo.GroupID) bool {
	inputCols := b.mem.GroupProperties(input).Relational.OutputCols
	for _, colOrder := range ordering {
		if colOrder < 0 {
			// Descending order, so recover original index.
			colOrder = -colOrder
		}
		if !inputCols.Contains(int(colOrder)) {
			return false
		}
	}
	return true
}

// buildChildProps returns the set of physical properties required of the nth
// child, based upon the properties required of the parent. For example, the
// Project operator passes through any ordering requirement to its child, but
// provides any presentation requirement.
func (b physicalPropsBuilder) buildChildProps(
	parent memo.ExprID, required memo.PhysicalPropsID, nth int,
) memo.PhysicalPropsID {
	mexpr := b.mem.Expr(parent)

	if required == memo.MinPhysPropsID {
		switch mexpr.Operator() {
		case opt.LimitOp, opt.OffsetOp, opt.ExplainOp, opt.ShowTraceOp, opt.RowNumberOp:
		default:
			// Fast path taken in common case when no properties are required of
			// parent and the operator itself does not require any properties.
			return memo.MinPhysPropsID
		}
	}

	parentProps := b.mem.LookupPhysicalProps(required)

	var childProps props.Physical

	// Presentation property is provided by all the relational operators, so
	// don't add it to childProps.

	// Ordering property.
	switch mexpr.Operator() {
	case opt.SelectOp, opt.ProjectOp, opt.LookupJoinOp:
		if nth == 0 {
			// Pass through the ordering.
			childProps.Ordering = parentProps.Ordering
		}

	case opt.LimitOp, opt.OffsetOp, opt.RowNumberOp:
		// Limit/Offset/RowNumber require the ordering in their private.
		if nth == 0 {
			var ordering props.Ordering
			switch mexpr.Operator() {
			case opt.LimitOp:
				ordering = b.mem.LookupPrivate(mexpr.AsLimit().Ordering()).(props.Ordering)
			case opt.OffsetOp:
				ordering = b.mem.LookupPrivate(mexpr.AsOffset().Ordering()).(props.Ordering)
			case opt.RowNumberOp:
				def := b.mem.LookupPrivate(mexpr.AsRowNumber().Def()).(*memo.RowNumberDef)
				ordering = def.Ordering
			}
			childProps.Ordering = ordering
		}

	case opt.GroupByOp:
		if nth == 0 {
			def := mexpr.Private(b.mem).(*memo.GroupByDef)
			childProps.Ordering = def.Ordering
		}

	case opt.ExplainOp:
		if nth == 0 {
			childProps = b.mem.LookupPrivate(mexpr.AsExplain().Def()).(*memo.ExplainOpDef).Props
		}

	case opt.ShowTraceOp:
		if nth == 0 {
			childProps = b.mem.LookupPrivate(mexpr.AsShowTrace().Def()).(*memo.ShowTraceOpDef).Props
		}
	}

	// If properties haven't changed, no need to re-intern them.
	if childProps.Equals(parentProps) {
		return required
	}

	return b.mem.InternPhysicalProps(&childProps)
}
