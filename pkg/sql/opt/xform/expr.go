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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
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
// cost. NormPhysPropsID is a special set of required properties that yields a
// view over the normalized, logical expression tree. This will contain no
// enforcers and is not necessarily the lowest cost expression.
//
// Think of a PhysicalPropsID as selecting one tree out of the memo forest.
// That tree can consist of all normalized expressions (i.e. first expression
// in each group), or it can consist of lowest cost expressions (i.e.
// expressions from bestExprs), or any other criteria we decide is useful.
//
// NOTE: While the NormPhysPropsID property set always works, other property
//       sets will only work once the optimizer has been invoked with that set,
//       so that it's had an opportunity to compute the lowest cost path.
func makeExprView(mem *memo, group opt.GroupID, required opt.PhysicalPropsID) ExprView {
	mgrp := mem.lookupGroup(group)

	if required == opt.NormPhysPropsID {
		// NormPhysPropsID is the special id used to traverse the expression
		// tree before it's been fully explored and costed (and bestExprs has
		// been populated). Create the ExprView over the group's normalized
		// expression, which is always the first expression in the group.
		return ExprView{
			mem:      mem,
			loc:      makeNormLoc(group),
			op:       mgrp.lookupExpr(normExprID).op,
			required: required,
		}
	}

	// Find the lowest cost expression in the group that provides the required
	// properties. That becomes the root of the expression tree.
	best := mgrp.lookupBestExpr(required)
	return ExprView{
		mem:      mem,
		loc:      memoLoc{group: group, expr: best.lowest},
		op:       best.op,
		required: required,
	}
}

// Operator returns the type of the expression.
func (ev ExprView) Operator() opt.Operator {
	return ev.op
}

// Logical returns the set of logical properties that this expression provides.
func (ev ExprView) Logical() *LogicalProps {
	return &ev.mem.lookupGroup(ev.loc.group).logical
}

// Physical returns the physical properties required of this expression, such
// as the ordering of result rows. Note that Physical does not return the
// properties *provided* by this expression, but those *required* of it by its
// parent expression, or by the ExprView creator.
func (ev ExprView) Physical() *opt.PhysicalProps {
	return ev.mem.lookupPhysicalProps(ev.required)
}

// Group returns the memo group containing this expression.
func (ev ExprView) Group() opt.GroupID {
	return ev.loc.group
}

// Child returns the nth expression that is an input to this parent expression.
// It panics if the requested child does not exist.
func (ev ExprView) Child(nth int) ExprView {
	group := ev.ChildGroup(nth)

	if ev.required == opt.NormPhysPropsID {
		// NormPhysPropsID is the special id used to traverse the expression
		// tree before it's been fully explored and costed (and bestExprs has
		// been populated).
		return makeExprView(ev.mem, group, opt.NormPhysPropsID)
	}

	required := ev.mem.physPropsFactory.constructChildProps(ev, nth)
	return makeExprView(ev.mem, group, required)
}

// Private returns any private data associated with this expression, or nil if
// there is none.
func (ev ExprView) Private() interface{} {
	return ev.mem.lookupPrivate(ev.privateID())
}

// Metadata returns the metadata that's specific to this expression tree. Some
// operator types refer to the metadata in their private fields. For example,
// the Scan operator holds a metadata table index.
func (ev ExprView) Metadata() *opt.Metadata {
	return ev.mem.metadata
}

// String returns a string representation of this expression for testing and
// debugging.
func (ev ExprView) String() string {
	tp := treeprinter.New()
	ev.format(tp)
	return tp.String()
}

func (ev ExprView) lookupChildGroup(nth int) *memoGroup {
	return ev.mem.lookupGroup(ev.ChildGroup(nth))
}

func (ev ExprView) format(tp treeprinter.Node) {
	if ev.IsScalar() {
		ev.formatScalar(tp)
	} else {
		ev.formatRelational(tp)
	}
}

func (ev ExprView) formatScalar(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", ev.op)
	ev.formatPrivate(&buf, ev.Private())

	// Don't panic if scalar properties don't yet exist when printing
	// expression.
	scalar := ev.Logical().Scalar
	if scalar == nil {
		buf.WriteString(" [type=undefined]")
	} else {
		showType := true
		switch ev.Operator() {
		case opt.ProjectionsOp, opt.AggregationsOp:
			// Don't show the type of these ops because they are simply tuple
			// types of their children's types, and the types of children are
			// already listed.
			showType = false
		}

		hasOuterCols := !ev.Logical().Scalar.OuterCols.Empty()

		if showType || hasOuterCols {
			buf.WriteString(" [")
			if showType {
				fmt.Fprintf(&buf, "type=%s", scalar.Type)
				if hasOuterCols {
					buf.WriteString(", ")
				}
			}
			if hasOuterCols {
				fmt.Fprintf(&buf, "outer=%s", scalar.OuterCols)
			}
			buf.WriteString("]")

		}
	}

	tp = tp.Child(buf.String())
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		child.format(tp)
	}
}

func (ev ExprView) formatPrivate(buf *bytes.Buffer, private interface{}) {
	switch ev.op {
	case opt.VariableOp:
		colIndex := private.(opt.ColumnIndex)
		private = ev.mem.metadata.ColumnLabel(colIndex)

	case opt.NullOp:
		// Private is redundant with logical type property.
		private = nil

	case opt.ProjectionsOp, opt.AggregationsOp:
		// The private data of these ops was already used to print the output
		// columns for their containing op (Project or GroupBy), so no need to
		// print again.
		private = nil
	}

	if private != nil {
		fmt.Fprintf(buf, ": %v", private)
	}
}

func (ev ExprView) formatRelational(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", ev.op)

	switch ev.Operator() {
	case opt.ScanOp:
		tblIndex := ev.Private().(*opt.ScanOpDef).Table
		fmt.Fprintf(&buf, " %s", ev.Metadata().Table(tblIndex).TabName())
	}

	logProps := ev.Logical()
	physProps := ev.Physical()

	tp = tp.Child(buf.String())
	buf.Reset()

	// If a particular column presentation is required of the expression, then
	// print columns using that information.
	if physProps.Presentation.Defined() {
		ev.formatPresentation(physProps.Presentation, tp)
	} else {
		// Special handling to improve the columns display for certain ops.
		switch ev.Operator() {
		case opt.ProjectOp:
			// Get the list of columns from the ProjectionsOp, which has the
			// natural order.
			colList := *ev.Child(1).Private().(*opt.ColList)
			logProps.formatColList("columns:", colList, ev.Metadata(), tp)

		case opt.ValuesOp:
			colList := *ev.Private().(*opt.ColList)
			logProps.formatColList("columns:", colList, ev.Metadata(), tp)

		case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
			opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
			colMap := ev.Private().(*opt.SetOpColMap)
			logProps.formatColList("columns:", colMap.Out, ev.Metadata(), tp)

		default:
			// Fall back to writing output columns in column index order, with
			// best guess label.
			logProps.formatOutputCols(ev.Metadata(), tp)
		}
	}

	switch ev.Operator() {
	// Special-case handling for GroupBy private; print grouping columns in
	// addition to full set of columns.
	case opt.GroupByOp:
		groupingColSet := *ev.Private().(*opt.ColSet)
		logProps.formatColSet("grouping columns:", groupingColSet, ev.Metadata(), tp)

	// Special-case handling for set operators to show the left and right
	// input columns that correspond to the output columns.
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		colMap := ev.Private().(*opt.SetOpColMap)
		logProps.formatColList("left columns:", colMap.Left, ev.Metadata(), tp)
		logProps.formatColList("right columns:", colMap.Right, ev.Metadata(), tp)
	}

	if physProps.Ordering.Defined() {
		tp.Childf("ordering: %s", physProps.Ordering.String())
	}

	for i := 0; i < ev.ChildCount(); i++ {
		ev.Child(i).format(tp)
	}
}

func (ev ExprView) formatPresentation(presentation opt.Presentation, tp treeprinter.Node) {
	logProps := ev.Logical()

	var buf bytes.Buffer
	buf.WriteString("columns:")
	for _, col := range presentation {
		logProps.formatCol(col.Label, col.Index, ev.Metadata(), &buf)
	}
	tp.Child(buf.String())
}

// opLayout describes the "layout" of each op's children. It contains multiple
// fields:
//
//  - baseCount (bits 0,1):
//      number of children, excluding any list; the children are in
//      state[0] through state[baseCount-1], unless the op is an enforcer.
//
//  - list (bits 2,3):
//      0 if op has no list, otherwise 1 + position of the list in state
//      (specifically Offset=state[list-1] Length=state[list]).
//
//  - priv (bits 4,5):
//      0 if op has no private, otherwise 1 + position of the private in state.
//
//  - isEnforcer (bit 7): if set, this op is an enforcer; which always has one
//      child (which is the same group). In this case baseCount is 1 (even
//      though the child is implicit and not stored in state[0]).
//
// The table of values (opLayoutTable) is generated by optgen.
type opLayout uint8

func (val opLayout) baseCount() uint8 {
	return uint8(val) & 3
}

func (val opLayout) list() uint8 {
	return (uint8(val) >> 2) & 3
}

func (val opLayout) priv() uint8 {
	return (uint8(val) >> 4) & 3
}

func (val opLayout) isEnforcer() bool {
	return (uint8(val) >> 7) != 0
}

func makeOpLayout(baseCount, list, priv, isEnforcer uint8) opLayout {
	return opLayout(baseCount | (list << 2) | (priv << 4) | (isEnforcer << 7))
}

// ChildCount returns the number of expressions that are inputs to this
// parent expression.
func (ev ExprView) ChildCount() int {
	layout := opLayoutTable[ev.op]
	baseCount := layout.baseCount()
	list := layout.list()
	if list == 0 {
		return int(baseCount)
	}
	e := ev.mem.lookupExpr(ev.loc)
	return int(baseCount) + int(e.state[list])
}

// ChildGroup returns the memo group containing the nth child of this parent
// expression.
func (ev ExprView) ChildGroup(n int) opt.GroupID {
	layout := opLayoutTable[ev.op]
	baseCount := layout.baseCount()
	e := ev.mem.lookupExpr(ev.loc)
	if n < int(baseCount) {
		if layout.isEnforcer() {
			// Enforcers have a single child which is the same group they're in.
			return ev.loc.group
		}
		return opt.GroupID(e.state[n])
	}
	n -= int(baseCount)
	list := layout.list()
	if list != 0 && n < int(e.state[list]) {
		listID := opt.ListID{Offset: e.state[list-1], Length: e.state[list]}
		return ev.mem.lookupList(listID)[n]
	}
	panic("child index out of range")
}

func (ev ExprView) privateID() opt.PrivateID {
	priv := opLayoutTable[ev.op].priv()
	if priv == 0 {
		return 0
	}
	e := ev.mem.lookupExpr(ev.loc)
	return opt.PrivateID(e.state[priv-1])
}
