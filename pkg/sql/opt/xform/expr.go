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

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// ColMap provides a 1:1 mapping from one column index to another. It is used
// by operators that need to match columns from its inputs.
type ColMap map[ColumnIndex]ColumnIndex

// Expr virtualizes one of the trees in the memo forest (see comment in memo.go
// for more details), making it both simple and efficient to traverse that tree
// and examine its properties. Given a particular root memo group and a set of
// required physical properties, there is exactly one lowest cost tree in the
// memo forest that provides those properties. Expr overlays the memo groups
// with a view of that single tree, exposing methods like ChildCount and Child
// that enable traversal over that one tree in the forest. It does this
// efficiently by avoiding creation of new objects. Expr is 24 bytes on a
// 64-bit machine, and is immutable after construction, so it can be passed by
// value.
//
// As an example, consider a memo forest with two alternate join orders. There
// are two trees in this forest:
//
//    +-----------+  +-----------+
// 3: | join 1, 2 |  | join 2, 1 |
//    +-----------+  +-----------+
//    +--------+
// 2: | scan b |
//    +--------+
//    +--------+
// 1: | scan a |
//    +--------+
//
// If the second tree provides the same required properties as the first, but
// is lower cost, then the Expr virtualizes its "extraction" so that it is
// equivalent to the following standalone tree:
//
//   +-----------+
//   | join b, a |
//   +-----------+
//      |     |
//      |     |   +--------+
//      |     +---| scan a |
//      |         +--------+
//      |
//      |    +--------+
//      +----| scan b |
//           +--------+
//
// Don't reorder fields without checking the size of Expr stays under 24 bytes.
type Expr struct {
	// mem references the memo which holds the expression forest.
	mem *memo

	// loc locates a group within the memo, and a memo expression within that
	// group.
	loc memoLoc

	// op is the type of the expression (select, inner-join, and, or, etc).
	op Operator

	// required are the physical properties that this expression must provide.
	required physicalPropsID
}

// makeExpr creates a new Expr instance that references the expression in the
// specified group that satisfies the required properties at the lowest cost.
func makeExpr(mem *memo, group GroupID, required physicalPropsID) Expr {
	mgrp := mem.lookupGroup(group)

	if required == minPhysPropsID {
		return Expr{
			mem:      mem,
			loc:      memoLoc{group: group, expr: normExprID},
			op:       mgrp.lookupExpr(normExprID).op,
			required: required,
		}
	}

	panic("physical property sets other than min are not yet implemented")
}

// Operator returns the type of the expression.
func (e *Expr) Operator() Operator {
	return e.op
}

// Logical returns the set of logical properties that this expression provides.
func (e *Expr) Logical() *LogicalProps {
	return &e.mem.lookupGroup(e.loc.group).logical
}

// Physical returns the physical properties required of this expression, such
// as the ordering of result rows.
func (e *Expr) Physical() *PhysicalProps {
	return &PhysicalProps{}
}

// ChildCount returns the number of expressions that are inputs to this
// parent expression.
func (e *Expr) ChildCount() int {
	return childCountLookup[e.op](e)
}

// Child returns the nth expression that is an input to this parent expression.
// It panics if the requested child does not exist.
func (e *Expr) Child(nth int) Expr {
	group := e.ChildGroup(nth)
	if e.required == minPhysPropsID {
		return makeExpr(e.mem, group, minPhysPropsID)
	}

	panic("physical properties other than min are not yet implemented")
}

// ChildGroup returns the memo group containing the nth child of this parent
// expression.
func (e *Expr) ChildGroup(nth int) GroupID {
	return childGroupLookup[e.op](e, nth)
}

// Private returns any private data associated with this expression, or nil if
// there is none.
func (e *Expr) Private() interface{} {
	return e.mem.lookupPrivate(e.privateID())
}

// String returns a string representation of this expression for testing and
// debugging.
func (e *Expr) String() string {
	tp := treeprinter.New()
	e.format(tp)
	return tp.String()
}

func (e *Expr) getChildGroups() []GroupID {
	children := make([]GroupID, e.ChildCount())
	for i := 0; i < e.ChildCount(); i++ {
		children[i] = e.ChildGroup(i)
	}
	return children
}

func (e *Expr) privateID() PrivateID {
	return privateLookup[e.op](e)
}

func (e *Expr) format(tp treeprinter.Node) {
	if e.IsScalar() {
		e.formatScalar(tp)
	} else {
		e.formatRelational(tp)
	}
}

func (e *Expr) formatScalar(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", e.op)
	e.formatPrivate(&buf, e.Private())

	tp = tp.Child(buf.String())
	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		child.format(tp)
	}
}

func (e *Expr) formatPrivate(buf *bytes.Buffer, private interface{}) {
	switch e.op {
	case VariableOp:
		colIndex := private.(ColumnIndex)
		private = e.mem.metadata.ColumnLabel(colIndex)

	case ProjectionsOp:
		// Projections private data is similar to its output columns, so
		// don't print it again.
		private = nil
	}

	if private != nil {
		fmt.Fprintf(buf, ": %v", private)
	}
}

func (e *Expr) formatRelational(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", e.op)

	logicalProps := e.Logical()

	tp = tp.Child(buf.String())

	buf.Reset()

	// Write the output columns. Fall back to writing output columns in column
	// index order, with best guess label.
	logicalProps.formatOutputCols(e.mem, tp)

	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		child.format(tp)
	}
}
