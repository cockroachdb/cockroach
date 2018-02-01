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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

//go:generate optgen -out expr.og.go exprs ../ops/scalar.opt ../ops/relational.opt ../ops/enforcer.opt

// ExprView provides a view of a single tree in the memo's forest of query plan
// trees (see comment in memo.go for more details about the memo forest). Given
// a particular root memo group and a set of required physical properties,
// there is one designated lowest cost tree in the memo's forest that provides
// those properties. ExprView overlays the memo groups with a view of that
// single tree, exposing methods like ChildCount and Child that enable
// efficient traversal over that one tree in the forest. ExprView is 24 bytes
// on a 64-bit machine, and is immutable after construction, so it can be
// constructed on the stack and passed by value. In addition, it is lazily
// constructed only when needed (generally when the Child method is called).
//
// As an example, consider a memo forest with two alternate join orders. There
// are two trees in this forest:
//
//   3: [inner-join [1 2]] [inner-join [2 1]]
//   2: [scan b]
//   1: [scan a]
//
// If the second tree provides the same required properties as the first, but
// is lower cost, then the ExprView overlays a view that is equivalent to the
// following standalone tree:
//
//   +-----------------+
//   | inner-join b, a |
//   +-----------------+
//      |     |
//      |     |   +--------+
//      |     +---| scan a |
//      |         +--------+
//      |
//      |    +--------+
//      +----| scan b |
//           +--------+
//
// Don't reorder fields without checking the size of ExprView stays under 24
// bytes.
type ExprView struct {
	// mem references the memo which holds the expression forest.
	mem *memo

	// loc locates a group within the memo, and a memo expression within that
	// group.
	loc memoLoc

	// op is the type of the expression (select, inner-join, and, or, etc).
	op opt.Operator

	// required are the physical properties that this expression must provide.
	required opt.PhysicalPropsID
}

// makeExprView creates a new ExprView instance that references the expression
// in the specified group that satisfies the required properties at the lowest
// cost.
// NOTE: While the minPhysPropsID property set always works, other property
//       sets will only work once the optimizer has been invoked with that set,
//       so that it's had an opportunity to compute the lowest cost path.
func makeExprView(mem *memo, group opt.GroupID, required opt.PhysicalPropsID) ExprView {
	mgrp := mem.lookupGroup(group)

	if required == opt.MinPhysPropsID {
		return ExprView{
			mem:      mem,
			loc:      memoLoc{group: group, expr: normExprID},
			op:       mgrp.lookupExpr(normExprID).op,
			required: required,
		}
	}

	panic("physical property sets other than min are not yet implemented")
}

// Operator returns the type of the expression.
func (ev *ExprView) Operator() opt.Operator {
	return ev.op
}

// Logical returns the set of logical properties that this expression provides.
func (ev *ExprView) Logical() *LogicalProps {
	return &ev.mem.lookupGroup(ev.loc.group).logical
}

// ChildCount returns the number of expressions that are inputs to this
// parent expression.
func (ev *ExprView) ChildCount() int {
	return childCountLookup[ev.op](ev)
}

// Child returns the nth expression that is an input to this parent expression.
// It panics if the requested child does not exist.
func (ev *ExprView) Child(nth int) ExprView {
	group := ev.ChildGroup(nth)
	if ev.required == opt.MinPhysPropsID {
		return makeExprView(ev.mem, group, opt.MinPhysPropsID)
	}

	panic("physical properties other than min are not yet implemented")
}

// ChildGroup returns the memo group containing the nth child of this parent
// expression.
func (ev *ExprView) ChildGroup(nth int) opt.GroupID {
	return childGroupLookup[ev.op](ev, nth)
}

// Private returns any private data associated with this expression, or nil if
// there is none.
func (ev *ExprView) Private() interface{} {
	return ev.mem.lookupPrivate(ev.privateID())
}

// String returns a string representation of this expression for testing and
// debugging.
func (ev *ExprView) String() string {
	tp := treeprinter.New()
	ev.format(tp)
	return tp.String()
}

func (ev *ExprView) privateID() opt.PrivateID {
	return privateLookup[ev.op](ev)
}

func (ev *ExprView) format(tp treeprinter.Node) {
	if ev.IsScalar() {
		ev.formatScalar(tp)
	} else {
		ev.formatRelational(tp)
	}
}

func (ev *ExprView) formatScalar(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", ev.op)
	ev.formatPrivate(&buf, ev.Private())

	tp = tp.Child(buf.String())
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		child.format(tp)
	}
}

func (ev *ExprView) formatPrivate(buf *bytes.Buffer, private interface{}) {
	switch ev.op {
	case opt.VariableOp:
		colIndex := private.(opt.ColumnIndex)
		private = ev.mem.metadata.ColumnLabel(colIndex)

	case opt.ProjectionsOp:
		// Projections private data is similar to its output columns, so
		// don't print it again.
		private = nil
	}

	if private != nil {
		fmt.Fprintf(buf, ": %v", private)
	}
}

func (ev *ExprView) formatRelational(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", ev.op)

	logicalProps := ev.Logical()

	tp = tp.Child(buf.String())

	buf.Reset()

	// Write the output columns. Fall back to writing output columns in column
	// index order, with best guess label.
	logicalProps.formatOutputCols(ev.mem, tp)

	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		child.format(tp)
	}
}
