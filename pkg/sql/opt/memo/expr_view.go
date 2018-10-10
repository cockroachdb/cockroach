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
	return ExprView{mem: mem, group: group, op: mem.NormExpr(group).Operator(), best: normBestOrdinal}
}

// Operator returns the type of the expression.
func (ev ExprView) Operator() opt.Operator {
	return ev.op
}

// Memo returns the Memo structure over which the ExprView operates.
func (ev ExprView) Memo() *Memo {
	return ev.mem
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
		group := ev.mem.NormExpr(ev.group).ChildGroup(ev.mem, nth)
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
	return ev.mem.Metadata()
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
	return ev.mem.MemoizeNormExpr(evalCtx, newExpr)
}

func (ev ExprView) childGroup(nth int) *group {
	return ev.mem.group(ev.ChildGroup(nth))
}

func (ev ExprView) bestExpr() *BestExpr {
	return ev.mem.group(ev.group).bestExpr(ev.best)
}

// --------------------------------------------------------------------
// String representation.
// --------------------------------------------------------------------

// String returns a string representation of this expression for testing and
// debugging. The output shows all properties of the expression, except for
// fully qualified names (when there are no ambiguities).
func (ev ExprView) String() string {
	return ev.FormatString(ExprFmtHideQualifications)
}

// FormatString returns a string representation of this expression for testing
// and debugging. The given flags control which properties are shown.
func (ev ExprView) FormatString(flags ExprFmtFlags) string {
	f := MakeExprFmtCtx(&bytes.Buffer{}, flags, ev.mem)
	tp := treeprinter.New()
	ev.format(&f, tp, false /* showProps */)
	return tp.String()
}

// RequestColStat causes a column statistic to be calculated on the expression.
// This is used for testing.
func (ev ExprView) RequestColStat(evalCtx *tree.EvalContext, cols opt.ColSet) {
	var sb statisticsBuilder
	sb.init(evalCtx, ev.Metadata())
	sb.colStat(cols, ev)
}

// HasOnlyConstChildren returns true if all children of ev are constant values
// (tuples of constant values are considered constant values).
func HasOnlyConstChildren(ev ExprView) bool {
	for i, n := 0, ev.ChildCount(); i < n; i++ {
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
