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
type physicalPropsFactory struct{}

// canProvide returns true if the given expression can provide the required
// physical properties. Don't access child expressions using the view (child
// groups are OK), because they are not guaranteed to be available yet, since
// physical properties are required starting at the root of the tree and
// proceeding downwards (top-down).
func (f physicalPropsFactory) canProvide(ev ExprView, required opt.PhysicalPropsID) bool {
	requiredProps := ev.mem.lookupPhysicalProps(required)

	if requiredProps.Presentation.Defined() {
		if !f.canProvidePresentation(ev, requiredProps.Presentation) {
			return false
		}
	}

	if requiredProps.Ordering.Defined() {
		if !f.canProvideOrdering(ev, requiredProps.Ordering) {
			return false
		}
	}

	return true
}

// canProvidePresentation returns true if the given expression can provide the
// required presentation property. Currently, all relational operators are
// capable of doing this.
func (f physicalPropsFactory) canProvidePresentation(ev ExprView, required opt.Presentation) bool {
	if !ev.IsRelational() {
		panic("presentation property doesn't apply to non-relational operators")
	}

	// All operators can provide the Presentation property.
	return true
}

// canProvideOrdering returns true if the given expression can provide the
// required ordering property.
func (f physicalPropsFactory) canProvideOrdering(ev ExprView, required opt.Ordering) bool {
	if !ev.IsRelational() {
		panic("ordering property doesn't apply to non-relational operators")
	}

	switch ev.Operator() {
	case opt.SelectOp:
		// Select operator can always pass through ordering to its input.
		return true

	case opt.ProjectOp:
		// Project operator can pass through ordering if it operates only on
		// input columns.
		inputCols := ev.lookupChildGroup(0).logical.Relational.OutputCols
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
		// Scan naturally orders according to the primary index.
		def := ev.Private().(*opt.ScanOpDef)
		primary := ev.Metadata().Table(def.Table).Primary()

		// Scan can project subset of columns.
		ordering := make(opt.Ordering, 0, primary.ColumnCount())
		for i := 0; i < primary.ColumnCount(); i++ {
			primaryCol := primary.Column(i)
			colIndex := ev.Metadata().TableColumn(def.Table, primaryCol.Ordinal)
			if def.Cols.Contains(int(colIndex)) {
				orderingCol := opt.MakeOrderingColumn(colIndex, primaryCol.Descending)
				ordering = append(ordering, orderingCol)
			}
		}

		return ordering.Provides(required)
	}

	return false
}

// constructChildProps returns the set of physical properties required of the
// nth child, based upon the properties required of the parent. For example,
// the Project operator passes through any ordering requirement to its child,
// but provides any presentation requirement. This method is heavily called
// during ExprView traversal, so performance is important.
func (f physicalPropsFactory) constructChildProps(ev ExprView, nth int) opt.PhysicalPropsID {
	// Fast path taken in common case when no properties are required of parent;
	// in that case, no properties are required of children. This will change in
	// the future when certain operators like MergeJoin require input properties
	// from their children regardless of what's required of them.
	if ev.required == opt.MinPhysPropsID {
		return opt.MinPhysPropsID
	}

	parentProps := ev.Physical()

	childProps := *parentProps
	var changed bool

	// Presentation property is provided by all the relational operators, so
	// don't add it to childProps.
	if childProps.Presentation.Defined() {
		childProps.Presentation = nil
		changed = true
	}

	// Check for operators that might pass through the ordering property to
	// input.
	if childProps.Ordering.Defined() {
		switch ev.Operator() {
		case opt.SelectOp, opt.ProjectOp:
			if nth == 0 {
				break
			}
			fallthrough

		default:
			childProps.Ordering = nil
			changed = true
		}
	}

	// If properties haven't changed, no need to re-intern them.
	if !changed {
		return ev.required
	}

	return ev.mem.internPhysicalProps(&childProps)
}
