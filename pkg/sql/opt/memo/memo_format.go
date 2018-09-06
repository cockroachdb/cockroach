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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type memoFmtCtx struct {
	buf       *bytes.Buffer
	flags     FmtFlags
	ordering  []GroupID
	numbering []GroupID
}

func (m *Memo) format(f *memoFmtCtx) string {
	// If requested, we topological sort the memo with respect to its root group.
	// Otherwise, we simply print out all the groups in the order and with the
	// names they're referred to physically.
	if f.flags.HasFlags(FmtRaw) {
		// In this case we renumber with the identity mapping, so the groups have
		// the same numbers as they're represented with internally.
		f.ordering = make([]GroupID, len(m.groups)-1)
		for i := range m.groups[1:] {
			f.ordering[i] = GroupID(i + 1)
		}
	} else {
		f.ordering = m.sortGroups(m.RootGroup())
	}

	// We renumber the groups so that they're still printed in order from 1..N.
	f.numbering = make([]GroupID, len(m.groups))
	for i := range f.ordering {
		f.numbering[f.ordering[i]] = GroupID(i + 1)
	}

	tp := treeprinter.New()

	var root treeprinter.Node
	if m.isOptimized() {
		root = tp.Childf("memo (optimized, ~%dKB)", m.MemoryEstimate()/1024)
	} else {
		root = tp.Childf("memo (not optimized, ~%dKB)", m.MemoryEstimate()/1024)
	}

	for i := range f.ordering {
		mgrp := &m.groups[f.ordering[i]]

		f.buf.Reset()
		for ord := 0; ord < mgrp.exprCount(); ord++ {
			if ord != 0 {
				f.buf.WriteByte(' ')
			}
			m.formatExpr(f, mgrp.expr(ExprOrdinal(ord)))
		}
		child := root.Childf("G%d: %s", i+1, f.buf.String())
		m.formatBestExprSet(f, child, mgrp)
	}

	// If showing raw memo, then add header text to point to root expression if
	// it's available.
	if f.flags.HasFlags(FmtRaw) {
		rootProps := m.LookupPhysicalProps(m.RootProps())
		return fmt.Sprintf("root: G%d, %s\n%s", f.numbering[m.RootGroup()], rootProps, tp.String())
	}
	return tp.String()
}

func (m *Memo) formatExpr(f *memoFmtCtx, e *Expr) {
	fmt.Fprintf(f.buf, "(%s", e.op)
	for i := 0; i < e.ChildCount(); i++ {
		fmt.Fprintf(f.buf, " G%d", f.numbering[e.ChildGroup(m, i)])
	}
	m.formatPrivate(f, e.Private(m), &props.Physical{})
	f.buf.WriteString(")")
}

func (m *Memo) formatPrivate(f *memoFmtCtx, private interface{}, physProps *props.Physical) {
	if private == nil {
		return
	}

	// Start by using private expression formatting.
	ef := MakeExprFmtCtx(f.buf, ExprFmtHideAll, m)
	formatPrivate(&ef, private, physProps)

	// Now append additional information that's useful in the memo case.
	switch t := private.(type) {
	case *ScanOpDef:
		tab := m.metadata.Table(t.Table)
		if tab.ColumnCount() != t.Cols.Len() {
			fmt.Fprintf(f.buf, ",cols=%s", t.Cols)
		}
		if t.Constraint != nil {
			fmt.Fprintf(f.buf, ",constrained")
		}
		if t.HardLimit.IsSet() {
			fmt.Fprintf(f.buf, ",lim=%s", t.HardLimit)
		}

	case *IndexJoinDef:
		fmt.Fprintf(f.buf, ",cols=%s", t.Cols)

	case *LookupJoinDef:
		fmt.Fprintf(f.buf, ",keyCols=%v,lookupCols=%s", t.KeyCols, t.LookupCols)

	case *ExplainOpDef:
		propsStr := t.Props.String()
		if propsStr != "" {
			fmt.Fprintf(f.buf, " %s", propsStr)
		}

	case *ProjectionsOpDef:
		t.PassthroughCols.ForEach(func(i int) {
			fmt.Fprintf(f.buf, " %s", m.metadata.ColumnLabel(opt.ColumnID(i)))
		})
	}
}

type bestExprSort struct {
	required PhysicalPropsID
	display  string
	best     *BestExpr
}

func (m *Memo) formatBestExprSet(f *memoFmtCtx, tp treeprinter.Node, mgrp *group) {
	// Don't show best expressions for scalar groups because they're not too
	// interesting.
	if mgrp.isScalarGroup() {
		return
	}

	cnt := mgrp.bestExprCount()
	if cnt == 0 {
		// No best expressions to show.
		return
	}

	// Sort the bestExprs by required properties.
	beSort := make([]bestExprSort, 0, cnt)
	for i := 0; i < cnt; i++ {
		best := mgrp.bestExpr(bestOrdinal(i))
		beSort = append(beSort, bestExprSort{
			required: best.required,
			display:  m.LookupPhysicalProps(best.required).String(),
			best:     best,
		})
	}

	sort.Slice(beSort, func(i, j int) bool {
		// Always order the root required properties first.
		if mgrp.id == m.RootGroup() {
			if beSort[i].required == beSort[i].best.required {
				return true
			}
		}
		return strings.Compare(beSort[i].display, beSort[j].display) < 0
	})

	for _, sort := range beSort {
		f.buf.Reset()
		child := tp.Childf("\"%s\"", sort.display)
		m.formatBestExpr(f, sort.best)
		child.Childf("best: %s", f.buf.String())
		child.Childf("cost: %.2f", sort.best.cost)
	}
}

func (m *Memo) formatBestExpr(f *memoFmtCtx, be *BestExpr) {
	fmt.Fprintf(f.buf, "(%s", be.op)

	for i := 0; i < be.ChildCount(); i++ {
		bestChild := be.Child(i)
		fmt.Fprintf(f.buf, " G%d", f.numbering[bestChild.group])

		// Print properties required of the child if they are interesting.
		required := m.bestExpr(bestChild).required
		if required != MinPhysPropsID {
			fmt.Fprintf(f.buf, "=\"%s\"", m.LookupPhysicalProps(required).String())
		}
	}

	m.formatPrivate(f, be.Private(m), m.LookupPhysicalProps(be.Required()))
	f.buf.WriteString(")")
}

// sortGroups sorts groups reachable from the root by doing a BFS topological
// sort.
func (m *Memo) sortGroups(root GroupID) (groups []GroupID) {
	indegrees := m.getIndegrees(root)

	res := make([]GroupID, 0, len(m.groups))
	queue := []GroupID{root}

	for len(queue) > 0 {
		var next GroupID
		next, queue = queue[0], queue[1:]
		res = append(res, next)

		// When we visit a group, we conceptually remove it from the dependency
		// graph, so all of its dependencies have their indegree reduced by one.
		// Any dependencies which have no more dependents can now be visited and
		// are added to the queue.
		m.forEachDependency(&m.groups[next], func(dep GroupID) {
			indegrees[dep]--
			if indegrees[dep] == 0 {
				queue = append(queue, dep)
			}
		})
	}

	// If there remains any group with nonzero indegree, we had a cycle.
	for i := range indegrees {
		if indegrees[i] != 0 {
			// The memo should never have a cycle.
			panic("memo had a cycle, use raw-memo to print")
		}
	}

	return res
}

// forEachDependency runs fn for each child group of g.
func (m *Memo) forEachDependency(g *group, fn func(GroupID)) {
	for i := 0; i < g.exprCount(); i++ {
		e := g.expr(ExprOrdinal(i))
		for c := 0; c < e.ChildCount(); c++ {
			fn(e.ChildGroup(m, c))
		}
	}
}

// getIndegrees returns the indegree of each group reachable from the root.
func (m *Memo) getIndegrees(root GroupID) (indegrees []int) {
	indegrees = make([]int, len(m.groups))
	m.computeIndegrees(root, make([]bool, len(m.groups)), indegrees)
	return indegrees
}

// computedIndegrees computes the indegree (number of dependents) of each group
// reachable from id.  It also populates reachable with true for all reachable
// ids.
func (m *Memo) computeIndegrees(id GroupID, reachable []bool, indegrees []int) {
	if id <= 0 || reachable[id] {
		return
	}
	reachable[id] = true

	m.forEachDependency(&m.groups[id], func(dep GroupID) {
		indegrees[dep]++
		m.computeIndegrees(dep, reachable, indegrees)
	})
}
