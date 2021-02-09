// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
)

// FmtFlags controls how the memo output is formatted.
type FmtFlags int

const (
	// FmtPretty performs a breadth-first topological sort on the memo groups,
	// and shows the root group at the top of the memo.
	FmtPretty FmtFlags = iota
)

type group struct {
	first  opt.Expr
	states []*groupState
}

type memoFormatter struct {
	buf   *bytes.Buffer
	flags FmtFlags

	o *Optimizer

	// groups reachable from the root, generated in breadth-first order.
	groups []group

	// groupIdx remembers the group index (in the groups slice) for the first
	// expression in each group.
	groupIdx map[opt.Expr]int
}

func makeMemoFormatter(o *Optimizer, flags FmtFlags) memoFormatter {
	return memoFormatter{buf: &bytes.Buffer{}, flags: flags, o: o}
}

func (mf *memoFormatter) format() string {
	m := mf.o.mem

	// Assign group numbers to every expression in the memo.
	mf.groupIdx = make(map[opt.Expr]int)
	mf.numberMemo(m.RootExpr())

	// Populate the group states.
	mf.populateStates()

	// Format the memo using treeprinter.
	tp := treeprinter.New()
	desc := "not optimized"
	if mf.o.mem.IsOptimized() {
		desc = "optimized"
	}
	tpRoot := tp.Childf(
		"memo (%s, ~%dKB, required=%s)",
		desc, mf.o.mem.MemoryEstimate()/1024, m.RootProps(),
	)

	for i, e := range mf.groups {
		mf.buf.Reset()
		rel, ok := e.first.(memo.RelExpr)
		if !ok {
			mf.formatExpr(e.first)
			tpRoot.Childf("G%d: %s", i+1, mf.buf.String())
			continue
		}
		mf.formatGroup(rel)
		tpChild := tpRoot.Childf("G%d: %s", i+1, mf.buf.String())
		for _, s := range e.states {
			mf.buf.Reset()
			c := tpChild.Childf("%s", s.required)
			mf.formatBest(s.best, s.required)
			c.Childf("best: %s", mf.buf.String())
			c.Childf("cost: %.2f", s.cost)
		}
	}

	return tp.String()
}

func (mf *memoFormatter) group(expr opt.Expr) int {
	res, ok := mf.groupIdx[firstExpr(expr)]
	if !ok {
		panic(errors.AssertionFailedf("unknown group for %s", expr))
	}
	return res
}

// numberMemo does a breadth-first search of the memo (starting at the root of
// the expression tree), creates the groups and sets groupIdx for all
// expressions.
func (mf *memoFormatter) numberMemo(root opt.Expr) {
	mf.numberExpr(root)

	// Do a breadth-first search (groups acts as our queue).
	for i := 0; i < len(mf.groups); i++ {
		// next returns the next expression in the group.
		next := func(e opt.Expr) opt.Expr {
			rel, ok := e.(memo.RelExpr)
			if !ok {
				return nil
			}
			return rel.NextExpr()
		}

		for e := mf.groups[i].first; e != nil; e = next(e) {
			for i := 0; i < e.ChildCount(); i++ {
				mf.numberExpr(e.Child(i))
			}
		}
	}
}

// numberExpr sets groupIdx for the expression, creating a new group if necessary.
func (mf *memoFormatter) numberExpr(expr opt.Expr) {
	// Don't include list item expressions, as they don't communicate useful
	// information.
	skipItemOp := func(expr opt.Expr) opt.Expr {
		if opt.IsListItemOp(expr) {
			return expr.Child(0)
		}
		return expr
	}

	expr = firstExpr(skipItemOp(expr))

	// Handle special case of list expressions, which are not interned and stored
	// as byval slices in parent expressions: search the existing groups to detect
	// duplicate lists.
	if opt.IsListOp(expr) {
		for i := range mf.groups {
			g := mf.groups[i].first
			if g.Op() == expr.Op() && g.ChildCount() == expr.ChildCount() {
				eq := true
				for j := 0; j < g.ChildCount(); j++ {
					if firstExpr(skipItemOp(g.Child(j))) != firstExpr(skipItemOp(expr.Child(j))) {
						eq = false
						break
					}
				}
				if eq {
					mf.groupIdx[expr] = i
					return
				}
			}
		}
		// Not a duplicate; fall through to add a new group.
	}

	if _, ok := mf.groupIdx[expr]; !ok {
		mf.groupIdx[expr] = len(mf.groups)
		mf.groups = append(mf.groups, group{first: expr})
	}
}

func (mf *memoFormatter) populateStates() {
	for groupStateKey, groupState := range mf.o.stateMap {
		if !groupState.fullyOptimized {
			continue
		}
		groupIdx, ok := mf.groupIdx[groupStateKey.group]
		if !ok {
			// This group was not reachable from the root; ignore.
			continue
		}
		mf.groups[groupIdx].states = append(mf.groups[groupIdx].states, groupState)
	}

	// Sort the states to get deterministic results.
	for groupIdx := range mf.groups {
		s := mf.groups[groupIdx].states
		sort.Slice(s, func(i, j int) bool {
			p1, p2 := s[i].required, s[j].required
			// Sort no required props last.
			if !p1.Defined() {
				return false
			}
			if !p2.Defined() {
				return true
			}
			// Sort props with presentations before those without.
			if !p1.Presentation.Any() && p2.Presentation.Any() {
				return true
			}
			if p1.Presentation.Any() && !p2.Presentation.Any() {
				return false
			}
			// Finally, just sort by string.
			return p1.String() < p2.String()
		})
	}

}

// formatGroup prints out (to mf.buf) all members of the group); e.g:
//    (limit G2 G3 ordering=-1) (scan a,rev,cols=(1,3),lim=10(rev))
func (mf *memoFormatter) formatGroup(first memo.RelExpr) {
	for member := first; member != nil; member = member.NextExpr() {
		if member != first {
			mf.buf.WriteByte(' ')
		}
		mf.formatExpr(member)
	}
}

// formatExpr prints out (to mf.buf) a single expression; e.g:
//    (filters G6 G7)
func (mf *memoFormatter) formatExpr(e opt.Expr) {
	fmt.Fprintf(mf.buf, "(%s", e.Op())
	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		if opt.IsListItemOp(child) {
			child = child.Child(0)
		}
		fmt.Fprintf(mf.buf, " G%d", mf.group(child)+1)
	}
	mf.formatPrivate(e, &physical.Required{})
	mf.buf.WriteString(")")
}

func (mf *memoFormatter) formatBest(best memo.RelExpr, required *physical.Required) {
	fmt.Fprintf(mf.buf, "(%s", best.Op())

	for i := 0; i < best.ChildCount(); i++ {
		fmt.Fprintf(mf.buf, " G%d", mf.group(best.Child(i))+1)

		// Print properties required of the child if they are interesting.
		childReq := BuildChildPhysicalProps(mf.o.mem, best, i, required)
		if childReq.Defined() {
			fmt.Fprintf(mf.buf, "=\"%s\"", childReq)
		}
	}

	mf.formatPrivate(best, required)
	mf.buf.WriteString(")")
}

func (mf *memoFormatter) formatPrivate(e opt.Expr, physProps *physical.Required) {
	private := e.Private()
	if private == nil {
		return
	}

	// Remap special-case privates.
	switch t := e.(type) {
	case *memo.CastExpr:
		private = t.Typ.SQLString()
	}

	// Start by using private expression formatting.
	m := mf.o.mem
	nf := memo.MakeExprFmtCtxBuffer(mf.buf, memo.ExprFmtHideAll, m, nil /* catalog */)
	memo.FormatPrivate(&nf, private, physProps)

	// Now append additional information that's useful in the memo case.
	switch t := e.(type) {
	case *memo.ScanExpr:
		tab := m.Metadata().Table(t.Table)
		if tab.ColumnCount() != t.Cols.Len() {
			fmt.Fprintf(mf.buf, ",cols=%s", t.Cols)
		}
		if t.Constraint != nil {
			fmt.Fprintf(mf.buf, ",constrained")
		}
		if t.InvertedConstraint != nil {
			fmt.Fprintf(mf.buf, ",constrained inverted")
		}
		if t.HardLimit.IsSet() {
			fmt.Fprintf(mf.buf, ",lim=%s", t.HardLimit)
		}

	case *memo.IndexJoinExpr:
		fmt.Fprintf(mf.buf, ",cols=%s", t.Cols)

	case *memo.LookupJoinExpr:
		if len(t.KeyCols) > 0 {
			fmt.Fprintf(mf.buf, ",keyCols=%v,outCols=%s", t.KeyCols, t.Cols)
		} else {
			fmt.Fprintf(mf.buf, ",outCols=%s", t.Cols)
		}

	case *memo.ExplainExpr:
		propsStr := t.Props.String()
		if propsStr != "" {
			fmt.Fprintf(mf.buf, " %s", propsStr)
		}

	case *memo.ProjectExpr:
		t.Passthrough.ForEach(func(i opt.ColumnID) {
			fmt.Fprintf(mf.buf, " %s", m.Metadata().ColumnMeta(i).Alias)
		})
	}
}

func firstExpr(expr opt.Expr) opt.Expr {
	if rel, ok := expr.(memo.RelExpr); ok {
		return rel.FirstExpr()
	}
	return expr
}
