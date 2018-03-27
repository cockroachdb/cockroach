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

package memo

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// ExprView provides a view of a single tree in the memo's forest of query plan
// trees (see comment in memo.go for more details about the memo forest). For
// a root memo group and a set of required physical properties, there is one
// designated lowest cost tree (called the "best" expression) in the memo's
// forest that provides those properties. ExprView overlays the memo groups
// with a view of that single tree, exposing methods like ChildCount and Child
// that enable efficient traversal over that one tree in the forest. ExprView
// is 16 bytes on a 64-bit machine, and is immutable after construction, so it
// can be constructed on the stack and passed by value. In addition, it is
// lazily constructed only when needed (generally when the Child method is
// called).
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
// Don't reorder fields without checking the impact on the size of ExprView.
type ExprView struct {
	// mem references the memo which holds the expression forest.
	mem *Memo

	// group is the identifier of the memo group to which the expression belongs.
	group GroupID

	// op is the type of the expression.
	op opt.Operator

	// best references the group's lowest cost expression. If best is the
	// special normBestOrdinal, then this ExprView is traversing the normalized
	// expression tree rather than the lowest cost tree.
	best bestOrdinal
}

// MakeExprView creates a new ExprView instance that references the given
// expression, which is the lowest cost expression in its group for a
// particular set of physical properties. Note that the group must have already
// been optimized with respect to that set of properties. Children of this
// expression will in turn be the lowest cost expressions in their respective
// groups, and so on.
func MakeExprView(mem *Memo, best BestExprID) ExprView {
	mgrp := mem.group(best.group)
	be := mgrp.bestExpr(best.ordinal)
	return ExprView{mem: mem, group: best.group, op: be.op, best: best.ordinal}
}

// MakeNormExprView constructs an ExprView that traverses the normalized
// logical expression tree, rather than the lowest cost physical tree. The
// normalized expression tree will contain no enforcer expressions and physical
// properties are not available. This view is useful when testing or debugging,
// and to traverse the tree before it's been fully explored and costed (and
// bestExprs have been populated). See the struct comment in factory.go for
// more details about the normalized expression tree.
func MakeNormExprView(mem *Memo, group GroupID) ExprView {
	op := mem.NormExpr(group).op
	return ExprView{mem: mem, group: group, op: op, best: normBestOrdinal}
}

// Operator returns the type of the expression.
func (ev ExprView) Operator() opt.Operator {
	return ev.op
}

// Logical returns the set of logical properties that this expression provides.
func (ev ExprView) Logical() *LogicalProps {
	return &ev.mem.group(ev.group).logical
}

// Physical returns the physical properties required of this expression, such
// as the ordering of result rows. Note that Physical does not return the
// properties *provided* by this expression, but those *required* of it by its
// parent expression, or by the ExprView creator.
func (ev ExprView) Physical() *PhysicalProps {
	if ev.best == normBestOrdinal {
		panic("physical properties are not available when traversing the normalized tree")
	}
	return ev.mem.LookupPhysicalProps(ev.lookupBestExpr().required)
}

// Group returns the memo group containing this expression.
func (ev ExprView) Group() GroupID {
	return ev.group
}

// Child returns the nth expression that is an input to this parent expression.
// It panics if the requested child does not exist.
func (ev ExprView) Child(nth int) ExprView {
	if ev.best == normBestOrdinal {
		// normBestOrdinal is the special BestExpr index that indicates traversal
		// of the normalized expression tree, regardless of whether it's the
		// lowest cost.
		group := ev.ChildGroup(nth)
		return MakeNormExprView(ev.mem, group)
	}
	return MakeExprView(ev.mem, ev.lookupBestExpr().Child(nth))
}

// ChildCount returns the number of expressions that are inputs to this
// parent expression.
func (ev ExprView) ChildCount() int {
	if ev.best == normBestOrdinal {
		return ev.mem.NormExpr(ev.group).ChildCount()
	}
	return ev.lookupBestExpr().ChildCount()
}

// ChildGroup returns the memo group containing the nth child of this parent
// expression.
func (ev ExprView) ChildGroup(nth int) GroupID {
	if ev.best == normBestOrdinal {
		return ev.mem.NormExpr(ev.group).ChildGroup(ev.mem, nth)
	}
	return ev.lookupBestExpr().Child(nth).group
}

// Private returns any private data associated with this expression, or nil if
// there is none.
func (ev ExprView) Private() interface{} {
	if ev.best == normBestOrdinal {
		return ev.mem.NormExpr(ev.group).Private(ev.mem)
	}
	return ev.mem.Expr(ev.lookupBestExpr().eid).Private(ev.mem)
}

// Metadata returns the metadata that's specific to this expression tree. Some
// operator types refer to the metadata in their private fields. For example,
// the Scan operator holds a metadata table index.
func (ev ExprView) Metadata() *opt.Metadata {
	return ev.mem.metadata
}

func (ev ExprView) lookupChildGroup(nth int) *group {
	return ev.mem.group(ev.ChildGroup(nth))
}

func (ev ExprView) lookupBestExpr() *BestExpr {
	return ev.mem.group(ev.group).bestExpr(ev.best)
}

// --------------------------------------------------------------------
// String representation.
// --------------------------------------------------------------------

// ExprFmtFlags controls which properties of the expression are shown in
// formatted output.
type ExprFmtFlags int

// HasFlags tests whether the given flags are all set.
func (f ExprFmtFlags) HasFlags(subset ExprFmtFlags) bool {
	return f&subset == subset
}

const (
	// ExprFmtShowAll shows all properties of the expression.
	ExprFmtShowAll ExprFmtFlags = 0

	// ExprFmtHideOuterCols does not show outer columns in the output.
	ExprFmtHideOuterCols ExprFmtFlags = 1 << iota

	// ExprFmtHideStats does not show statistics in the output.
	ExprFmtHideStats

	// ExprFmtHideCost does not show expression cost in the output.
	ExprFmtHideCost

	// ExprFmtHideConstraints does not show inferred constraints in the output.
	ExprFmtHideConstraints

	// ExprFmtHideAll shows only the most basic properties of the expression.
	ExprFmtHideAll ExprFmtFlags = (1 << iota) - 1
)

// String returns a string representation of this expression for testing and
// debugging. The output shows all properties of the expression.
func (ev ExprView) String() string {
	return ev.FormatString(ExprFmtShowAll)
}

// FormatString returns a string representation of this expression for testing
// and debugging. The given flags control which properties are shown.
func (ev ExprView) FormatString(flags ExprFmtFlags) string {
	tp := treeprinter.New()
	ev.format(tp, flags)
	return tp.String()
}

// format constructs a treeprinter view of this expression for testing and
// debugging. The given flags control which properties are added.
func (ev ExprView) format(tp treeprinter.Node, flags ExprFmtFlags) {
	if ev.IsScalar() {
		ev.formatScalar(tp, flags)
	} else {
		ev.formatRelational(tp, flags)
	}
}

func (ev ExprView) formatRelational(tp treeprinter.Node, flags ExprFmtFlags) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", ev.op)

	switch ev.Operator() {
	case opt.ScanOp:
		ev.mem.formatScanPrivate(&buf, ev.Private().(*ScanOpDef), true /* short */)
	}

	var physProps *PhysicalProps
	if ev.best == normBestOrdinal {
		physProps = &PhysicalProps{}
	} else {
		physProps = ev.Physical()
	}

	logProps := ev.Logical()

	tp = tp.Child(buf.String())
	buf.Reset()

	// If a particular column presentation is required of the expression, then
	// print columns using that information.
	if physProps.Presentation.Defined() {
		ev.formatPresentation(tp, physProps.Presentation)
	} else {
		// Special handling to improve the columns display for certain ops.
		switch ev.Operator() {
		case opt.ProjectOp:
			// Get the list of columns from the ProjectionsOp, which has the
			// natural order.
			colList := ev.Child(1).Private().(opt.ColList)
			logProps.FormatColList(tp, ev.Metadata(), "columns:", colList)

		case opt.ValuesOp:
			colList := ev.Private().(opt.ColList)
			logProps.FormatColList(tp, ev.Metadata(), "columns:", colList)

		case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
			opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
			colMap := ev.Private().(*SetOpColMap)
			logProps.FormatColList(tp, ev.Metadata(), "columns:", colMap.Out)

		default:
			// Fall back to writing output columns in column id order, with
			// best guess label.
			logProps.FormatColSet(tp, ev.Metadata(), "columns:", logProps.Relational.OutputCols)
		}
	}

	switch ev.Operator() {
	// Special-case handling for GroupBy private; print grouping columns in
	// addition to full set of columns.
	case opt.GroupByOp:
		groupingColSet := ev.Private().(opt.ColSet)
		logProps.FormatColSet(tp, ev.Metadata(), "grouping columns:", groupingColSet)

	// Special-case handling for set operators to show the left and right
	// input columns that correspond to the output columns.
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		colMap := ev.Private().(*SetOpColMap)
		logProps.FormatColList(tp, ev.Metadata(), "left columns:", colMap.Left)
		logProps.FormatColList(tp, ev.Metadata(), "right columns:", colMap.Right)

	case opt.ScanOp:
		def := ev.Private().(*ScanOpDef)
		if def.Constraint != nil {
			tp.Childf("constraint: %s", def.Constraint)
		}
		if def.HardLimit > 0 {
			tp.Childf("limit: %d", def.HardLimit)
		}
	}

	if !flags.HasFlags(ExprFmtHideStats) {
		tp.Childf("stats: [rows=%d]", logProps.Relational.Stats.RowCount)
	}

	if !flags.HasFlags(ExprFmtHideCost) && ev.best != normBestOrdinal {
		tp.Childf("cost: %.2f", ev.lookupBestExpr().cost)
	}

	if physProps.Ordering.Defined() {
		tp.Childf("ordering: %s", physProps.Ordering.String())
	}

	for i := 0; i < ev.ChildCount(); i++ {
		ev.Child(i).format(tp, flags)
	}
}

func (ev ExprView) formatScalar(tp treeprinter.Node, flags ExprFmtFlags) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", ev.op)
	ev.formatScalarPrivate(&buf, ev.Private())

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
		hasOuterCols := !flags.HasFlags(ExprFmtHideOuterCols) && !scalar.OuterCols.Empty()
		hasConstraints := !flags.HasFlags(ExprFmtHideConstraints) &&
			scalar.Constraints != nil &&
			!scalar.Constraints.IsUnconstrained()

		if showType || hasOuterCols || hasConstraints {
			buf.WriteString(" [")
			if showType {
				fmt.Fprintf(&buf, "type=%s", scalar.Type)
				if hasOuterCols || hasConstraints {
					buf.WriteString(", ")
				}
			}
			if hasOuterCols {
				fmt.Fprintf(&buf, "outer=%s", scalar.OuterCols)
				if hasConstraints {
					buf.WriteString(", ")
				}
			}
			if hasConstraints {
				fmt.Fprintf(&buf, "constraints=(%s", scalar.Constraints)
				if scalar.TightConstraints {
					buf.WriteString("; tight")
				}
				buf.WriteString(")")
			}
			buf.WriteString("]")
		}
	}

	tp = tp.Child(buf.String())
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		child.format(tp, flags)
	}
}

func (ev ExprView) formatScalarPrivate(buf *bytes.Buffer, private interface{}) {
	switch ev.op {
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
		buf.WriteRune(':')
		ev.mem.formatPrivate(buf, private)
	}
}

func (ev ExprView) formatPresentation(tp treeprinter.Node, presentation Presentation) {
	logProps := ev.Logical()

	var buf bytes.Buffer
	buf.WriteString("columns:")
	for _, col := range presentation {
		logProps.FormatCol(&buf, ev.Metadata(), col.Label, col.ID)
	}
	tp.Child(buf.String())
}

// MatchesTupleOfConstants returns true if the expression is a TupleOp with
// ConstValue children.
func MatchesTupleOfConstants(ev ExprView) bool {
	if ev.Operator() != opt.TupleOp {
		return false
	}
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		if !child.IsConstValue() {
			return false
		}
	}
	return true
}
