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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	if best.ordinal == normBestOrdinal {
		// Handle MakeNormExprView case.
		return ExprView{mem: mem, group: best.group, op: mgrp.normExpr.op, best: best.ordinal}
	}
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
	return MakeExprView(mem, BestExprID{group: group, ordinal: normBestOrdinal})
}

// Operator returns the type of the expression.
func (ev ExprView) Operator() opt.Operator {
	return ev.op
}

// Logical returns the set of logical properties that this expression provides.
func (ev ExprView) Logical() *props.Logical {
	return &ev.mem.group(ev.group).logical
}

// Physical returns the physical properties required of this expression, such
// as the ordering of result rows. Note that Physical does not return the
// properties *provided* by this expression, but those *required* of it by its
// parent expression, or by the ExprView creator.
func (ev ExprView) Physical() *props.Physical {
	if ev.best == normBestOrdinal {
		panic("physical properties are not available when traversing the normalized tree")
	}
	return ev.mem.LookupPhysicalProps(ev.bestExpr().required)
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
	return MakeExprView(ev.mem, ev.bestExpr().Child(nth))
}

// ChildCount returns the number of expressions that are inputs to this
// parent expression.
func (ev ExprView) ChildCount() int {
	if ev.best == normBestOrdinal {
		return ev.mem.NormExpr(ev.group).ChildCount()
	}
	return ev.bestExpr().ChildCount()
}

// ChildGroup returns the memo group containing the nth child of this parent
// expression.
func (ev ExprView) ChildGroup(nth int) GroupID {
	if ev.best == normBestOrdinal {
		return ev.mem.NormExpr(ev.group).ChildGroup(ev.mem, nth)
	}
	return ev.bestExpr().Child(nth).group
}

// Private returns any private data associated with this expression, or nil if
// there is none.
func (ev ExprView) Private() interface{} {
	if ev.best == normBestOrdinal {
		return ev.mem.NormExpr(ev.group).Private(ev.mem)
	}
	return ev.mem.Expr(ev.bestExpr().eid).Private(ev.mem)
}

// Metadata returns the metadata that's specific to this expression tree. Some
// operator types refer to the metadata in their private fields. For example,
// the Scan operator holds a metadata table index.
func (ev ExprView) Metadata() *opt.Metadata {
	return ev.mem.metadata
}

// Cost returns the cost of executing this expression tree, as estimated by the
// optimizer. It is not available when the ExprView is traversing the normalized
// expression tree.
func (ev ExprView) Cost() Cost {
	if ev.best == normBestOrdinal {
		panic("Cost is not available when traversing the normalized tree")
	}
	return ev.mem.bestExpr(BestExprID{group: ev.group, ordinal: ev.best}).cost
}

// Replace invokes the given callback function for each child memo group of the
// expression. The callback function can return the unchanged group, or it can
// construct a new group and return that instead. Replace will assemble all of
// the changed and unchanged children into an expression of the same type having
// those children. If the expression is different from the original, then
// Replace will memoize the new expression and return an ExprView over it.
// Callers can use this method as a building block when searching and replacing
// expressions in a tree.
func (ev ExprView) Replace(evalCtx *tree.EvalContext, replace ReplaceChildFunc) ExprView {
	if ev.best != normBestOrdinal {
		panic("Replace can only be used when traversing the normalized tree")
	}
	existingExpr := ev.mem.NormExpr(ev.group)
	newExpr := existingExpr.Replace(ev.mem, replace)
	newGroup := ev.mem.MemoizeNormExpr(evalCtx, newExpr)
	return MakeNormExprView(ev.mem, newGroup)
}

func (ev ExprView) childGroup(nth int) *group {
	return ev.mem.group(ev.ChildGroup(nth))
}

func (ev ExprView) bestExpr() *BestExpr {
	return ev.mem.group(ev.group).bestExpr(ev.best)
}

func (ev ExprView) bestExprID() BestExprID {
	return BestExprID{group: ev.group, ordinal: ev.best}
}

// --------------------------------------------------------------------
// String representation.
// --------------------------------------------------------------------

// String returns a string representation of this expression for testing and
// debugging. The output shows all properties of the expression.
func (ev ExprView) String() string {
	return ev.FormatString(opt.ExprFmtShowAll)
}

// FormatString returns a string representation of this expression for testing
// and debugging. The given flags control which properties are shown.
func (ev ExprView) FormatString(flags opt.ExprFmtFlags) string {
	f := opt.MakeExprFmtCtx(ev.Metadata(), flags)
	tp := treeprinter.New()
	ev.format(&f, tp)
	return tp.String()
}

// format constructs a treeprinter view of this expression for testing and
// debugging. The given flags control which properties are added.
func (ev ExprView) format(f *opt.ExprFmtCtx, tp treeprinter.Node) {
	if ExprFmtInterceptor != nil && ExprFmtInterceptor(f, tp, ev) {
		return
	}
	if ev.IsScalar() {
		ev.formatScalar(f, tp)
	} else {
		ev.formatRelational(f, tp)
	}
}

func (ev ExprView) formatRelational(f *opt.ExprFmtCtx, tp treeprinter.Node) {
	var buf bytes.Buffer
	formatter := ev.mem.makeExprFormatter(&buf)

	// Special cases for merge-join and lookup-join: we want the type of the join
	// to show up first.
	switch ev.Operator() {
	case opt.MergeJoinOp:
		def := ev.Child(2).Private().(*MergeOnDef)
		fmt.Fprintf(&buf, "%v (merge)", def.JoinType)

	case opt.LookupJoinOp:
		def := ev.Private().(*LookupJoinDef)
		fmt.Fprintf(&buf, "%v (lookup", def.JoinType)
		formatter.formatPrivate(def, formatNormal)
		buf.WriteByte(')')

	case opt.ScanOp, opt.VirtualScanOp, opt.IndexJoinOp, opt.ShowTraceForSessionOp:
		fmt.Fprintf(&buf, "%v", ev.op)
		formatter.formatPrivate(ev.Private(), formatNormal)

	default:
		fmt.Fprintf(&buf, "%v", ev.op)
	}

	var physProps *props.Physical
	if ev.best == normBestOrdinal {
		physProps = &props.Physical{}
	} else {
		physProps = ev.Physical()
	}

	logProps := ev.Logical()

	tp = tp.Child(buf.String())

	// If a particular column presentation is required of the expression, then
	// print columns using that information.
	if !physProps.Presentation.Any() {
		ev.formatPresentation(f, tp, physProps.Presentation)
	} else {
		// Special handling to improve the columns display for certain ops.
		switch ev.Operator() {
		case opt.ProjectOp:
			// We want the synthesized column IDs to map 1-to-1 to the projections,
			// and the pass-through columns at the end.

			// Get the list of columns from the ProjectionsOp, which has the natural
			// order.
			def := ev.Child(1).Private().(*ProjectionsOpDef)
			colList := append(opt.ColList(nil), def.SynthesizedCols...)
			// Add pass-through columns.
			def.PassthroughCols.ForEach(func(i int) {
				colList = append(colList, opt.ColumnID(i))
			})

			logProps.FormatColList(f, tp, "columns:", colList)

		case opt.ValuesOp:
			colList := ev.Private().(opt.ColList)
			logProps.FormatColList(f, tp, "columns:", colList)

		case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
			opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
			colMap := ev.Private().(*SetOpColMap)
			logProps.FormatColList(f, tp, "columns:", colMap.Out)

		default:
			// Fall back to writing output columns in column id order, with
			// best guess label.
			logProps.FormatColSet(f, tp, "columns:", logProps.Relational.OutputCols)
		}
	}

	switch ev.Operator() {
	// Special-case handling for GroupBy private; print grouping columns and
	// ordering in addition to full set of columns.
	case opt.GroupByOp, opt.ScalarGroupByOp:
		def := ev.Private().(*GroupByDef)
		groupingColSet := def.GroupingCols
		ordering := def.Ordering
		if !groupingColSet.Empty() {
			logProps.FormatColSet(f, tp, "grouping columns:", groupingColSet)
		}
		if !ordering.Any() {
			tp.Childf("ordering: %s", ordering.String())
		}

	// Special-case handling for set operators to show the left and right
	// input columns that correspond to the output columns.
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		colMap := ev.Private().(*SetOpColMap)
		logProps.FormatColList(f, tp, "left columns:", colMap.Left)
		logProps.FormatColList(f, tp, "right columns:", colMap.Right)

	case opt.ScanOp:
		def := ev.Private().(*ScanOpDef)
		if def.Constraint != nil {
			tp.Childf("constraint: %s", def.Constraint)
		}
		if def.HardLimit > 0 {
			tp.Childf("limit: %d", def.HardLimit)
		}

	case opt.LookupJoinOp:
		def := ev.Private().(*LookupJoinDef)
		buf.Reset()
		idxCols := make(opt.ColList, len(def.KeyCols))
		idx := ev.mem.metadata.Table(def.Table).Index(def.Index)
		for i := range idxCols {
			idxCols[i] = ev.mem.metadata.TableColumn(def.Table, idx.Column(i).Ordinal)
		}
		tp.Childf("key columns: %v = %v", def.KeyCols, idxCols)
	}

	if !f.HasFlags(opt.ExprFmtHideOuterCols) && !logProps.Relational.OuterCols.Empty() {
		tp.Childf("outer: %s", logProps.Relational.OuterCols.String())
	}

	if !f.HasFlags(opt.ExprFmtHideRowCard) {
		if logProps.Relational.Cardinality != props.AnyCardinality {
			// Suppress cardinality for Scan ops if it's redundant with Limit field.
			if ev.Operator() != opt.ScanOp || ev.Private().(*ScanOpDef).HardLimit == 0 {
				tp.Childf("cardinality: %s", logProps.Relational.Cardinality)
			}
		}
	}

	if !f.HasFlags(opt.ExprFmtHideStats) {
		tp.Childf("stats: %s", &logProps.Relational.Stats)
	}

	if !f.HasFlags(opt.ExprFmtHideCost) && ev.best != normBestOrdinal {
		tp.Childf("cost: %.9g", ev.bestExpr().cost)
	}

	// Format functional dependencies.
	if !f.HasFlags(opt.ExprFmtHideFuncDeps) {
		// Show the key separately from the rest of the FDs. Do this by copying
		// the FD to the stack (fast shallow copy), and then calling ClearKey.
		fd := logProps.Relational.FuncDeps
		key, ok := fd.Key()
		if ok {
			tp.Childf("key: %s", key)
		}

		fd.ClearKey()
		if !fd.Empty() {
			tp.Childf("fd: %s", fd)
		}
	}

	if !physProps.Ordering.Any() {
		tp.Childf("ordering: %s", physProps.Ordering.String())
	}

	if !f.HasFlags(opt.ExprFmtHideRuleProps) {
		r := &logProps.Relational.Rule
		if !r.PruneCols.Empty() {
			tp.Childf("prune: %s", r.PruneCols.String())
		}
		if !r.RejectNullCols.Empty() {
			tp.Childf("reject-nulls: %s", r.RejectNullCols.String())
		}
		if len(r.InterestingOrderings) > 0 {
			tp.Childf("interesting orderings: %s", r.InterestingOrderings.String())
		}
	}

	for i := 0; i < ev.ChildCount(); i++ {
		ev.Child(i).format(f, tp)
	}
}

func (ev ExprView) formatScalar(f *opt.ExprFmtCtx, tp treeprinter.Node) {
	// Omit empty ProjectionsOp and AggregationsOp.
	if (ev.op == opt.ProjectionsOp || ev.op == opt.AggregationsOp) &&
		ev.ChildCount() == 0 {
		return
	}
	if ev.op == opt.MergeOnOp {
		tp = tp.Childf("%v", ev.op)
		def := ev.Private().(*MergeOnDef)
		tp.Childf("left ordering: %s", def.LeftEq)
		tp.Childf("right ordering: %s", def.RightEq)
	} else {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%v", ev.op)

		ev.formatScalarPrivate(&buf, ev.Private())
		ev.FormatScalarProps(f, &buf)
		tp = tp.Child(buf.String())
	}
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		child.format(f, tp)
	}
}

// FormatScalarProps writes out a string representation of the scalar
// properties (with a preceding space); for example:
//  " [type=bool, outer=(1), constraints=(/1: [/1 - /1]; tight)]"
func (ev ExprView) FormatScalarProps(f *opt.ExprFmtCtx, buf *bytes.Buffer) {
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
		hasOuterCols := !f.HasFlags(opt.ExprFmtHideOuterCols) && !scalar.OuterCols.Empty()
		hasConstraints := !f.HasFlags(opt.ExprFmtHideConstraints) &&
			scalar.Constraints != nil &&
			!scalar.Constraints.IsUnconstrained()
		hasFuncDeps := !f.HasFlags(opt.ExprFmtHideFuncDeps) && !scalar.FuncDeps.Empty()

		if showType || hasOuterCols || hasConstraints || hasFuncDeps {
			buf.WriteString(" [")
			if showType {
				fmt.Fprintf(buf, "type=%s", scalar.Type)
				if hasOuterCols || hasConstraints || hasFuncDeps {
					buf.WriteString(", ")
				}
			}
			if hasOuterCols {
				fmt.Fprintf(buf, "outer=%s", scalar.OuterCols)
				if hasConstraints || hasFuncDeps {
					buf.WriteString(", ")
				}
			}
			if hasConstraints {
				fmt.Fprintf(buf, "constraints=(%s", scalar.Constraints)
				if scalar.TightConstraints {
					buf.WriteString("; tight")
				}
				buf.WriteString(")")
				if hasFuncDeps {
					buf.WriteString(", ")
				}
			}
			if hasFuncDeps {
				fmt.Fprintf(buf, "fd=%s", scalar.FuncDeps)
			}
			buf.WriteString("]")
		}
	}

}

func (ev ExprView) formatScalarPrivate(buf *bytes.Buffer, private interface{}) {
	switch ev.op {
	case opt.NullOp, opt.TupleOp:
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
		formatter := ev.mem.makeExprFormatter(buf)
		formatter.formatPrivate(private, formatNormal)
	}
}

func (ev ExprView) formatPresentation(
	f *opt.ExprFmtCtx, tp treeprinter.Node, presentation props.Presentation,
) {
	logProps := ev.Logical()

	var buf bytes.Buffer
	buf.WriteString("columns:")
	for _, col := range presentation {
		logProps.FormatCol(f, &buf, col.Label, col.ID)
	}
	tp.Child(buf.String())
}

// HasOnlyConstChildren returns true if all children of ev are constant values
// (tuples of constant values are considered constant values).
func HasOnlyConstChildren(ev ExprView) bool {
	for i := 0; i < ev.ChildCount(); i++ {
		child := ev.Child(i)
		switch {
		case child.IsConstValue():
		case child.Operator() == opt.TupleOp && HasOnlyConstChildren(child):
		default:
			return false
		}
	}
	return true
}

// MatchesTupleOfConstants returns true if the expression is a TupleOp with
// constant values (a nested tuple of constant values is considered constant).
func MatchesTupleOfConstants(ev ExprView) bool {
	return ev.Operator() == opt.TupleOp && HasOnlyConstChildren(ev)
}

// ExprFmtInterceptor is a callback that can be set to a custom formatting
// function. If the function returns true, the normal formatting code is bypassed.
var ExprFmtInterceptor func(f *opt.ExprFmtCtx, tp treeprinter.Node, ev ExprView) bool
