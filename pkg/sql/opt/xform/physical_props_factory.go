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
)

// physicalPropsFactory determines what physical properties a given expression
// provides and constructs the physical properties required of its children
// based on those properties. The optimizer calls the canProvide methods to
// determine whether an expression provides a required physical property. If it
// does not, then the optimizer inserts an enforcer operator that is able to
// provide it. Some operators, like Select and Project, may not directly
// provide a required physical property, but do "pass through" the requirement
// to their input. Operators that do this should return true from the
// appropriate canProvide method and then pass through that property in the
// constructChildProps method.
// NOTE: The factory is defined as an empty struct with methods rather than as
//       functions in order to keep the methods grouped and self-contained.
type physicalPropsFactory struct {
	mem *memo.Memo
}

// canProvide returns true if the given expression can provide the required
// physical properties.
func (f physicalPropsFactory) canProvide(eid memo.ExprID, required memo.PhysicalPropsID) bool {
	requiredProps := f.mem.LookupPhysicalProps(required)

	if requiredProps.Presentation.Defined() {
		if !f.canProvidePresentation(eid, requiredProps.Presentation) {
			return false
		}
	}

	if requiredProps.Ordering.Defined() {
		if !f.canProvideOrdering(eid, requiredProps.Ordering) {
			return false
		}
	}

	return true
}

// canProvidePresentation returns true if the given expression can provide the
// required presentation property. Currently, all relational operators are
// capable of doing this.
func (f physicalPropsFactory) canProvidePresentation(
	eid memo.ExprID, required memo.Presentation,
) bool {
	mexpr := f.mem.Expr(eid)
	if !mexpr.IsRelational() {
		panic("presentation property doesn't apply to non-relational operators")
	}

	// All operators can provide the Presentation property.
	return true
}

// canProvideOrdering returns true if the given expression can provide the
// required ordering property.
func (f physicalPropsFactory) canProvideOrdering(eid memo.ExprID, required memo.Ordering) bool {
	mexpr := f.mem.Expr(eid)
	if !mexpr.IsRelational() {
		panic("ordering property doesn't apply to non-relational operators")
	}

	switch mexpr.Operator() {
	case opt.SelectOp:
		// Select operator can always pass through ordering to its input.
		return true

	case opt.ProjectOp:
		// Project operator can pass through ordering if it operates only on
		// input columns.
		input := mexpr.AsProject().Input()
		inputCols := f.mem.GroupProperties(input).Relational.OutputCols
		for _, colOrder := range required {
			if colOrder < 0 {
				// Descending order, so recover original index.
				colOrder = -colOrder
			}
			if !inputCols.Contains(int(colOrder)) {
				return false
			}
		}
		return true

	case opt.ScanOp:
		// Scan naturally orders according to the order of the scanned index.
		def := mexpr.Private(f.mem).(*memo.ScanOpDef)
		return def.CanProvideOrdering(f.mem.Metadata(), required)

	case opt.LimitOp:
		// Limit can provide the same ordering it requires of its input.
		return f.mem.LookupPrivate(mexpr.AsLimit().Ordering()).(memo.Ordering).Provides(required)

	case opt.OffsetOp:
		// Offset can provide the same ordering it requires of its input.
		return f.mem.LookupPrivate(mexpr.AsOffset().Ordering()).(memo.Ordering).Provides(required)
	}

	return false
}

// constructChildProps returns the set of physical properties required of the
// nth child, based upon the properties required of the parent. For example,
// the Project operator passes through any ordering requirement to its child,
// but provides any presentation requirement. This method is heavily called
// during ExprView traversal, so performance is important.
func (f physicalPropsFactory) constructChildProps(
	parent memo.ExprID, required memo.PhysicalPropsID, nth int,
) memo.PhysicalPropsID {
	mexpr := f.mem.Expr(parent)

	if required == memo.MinPhysPropsID {
		switch mexpr.Operator() {
		case opt.LimitOp, opt.OffsetOp:
		default:
			// Fast path taken in common case when no properties are required of
			// parent and the operator itself does not require any properties.
			return memo.MinPhysPropsID
		}
	}

	parentProps := f.mem.LookupPhysicalProps(required)

	var childProps memo.PhysicalProps

	// Presentation property is provided by all the relational operators, so
	// don't add it to childProps.

	// Ordering property.
	switch mexpr.Operator() {
	case opt.SelectOp, opt.ProjectOp:
		if nth == 0 {
			// Pass through the ordering.
			childProps.Ordering = parentProps.Ordering
		}

	case opt.LimitOp, opt.OffsetOp:
		// Limit/Offset require the ordering in their private.
		if nth == 0 {
			var ordering memo.PrivateID
			if mexpr.Operator() == opt.LimitOp {
				ordering = mexpr.AsLimit().Ordering()
			} else {
				ordering = mexpr.AsOffset().Ordering()
			}
			childProps.Ordering = f.mem.LookupPrivate(ordering).(memo.Ordering)
		}
	}

	// If properties haven't changed, no need to re-intern them.
	if childProps.Equals(parentProps) {
		return required
	}

	return f.mem.InternPhysicalProps(&childProps)
}
