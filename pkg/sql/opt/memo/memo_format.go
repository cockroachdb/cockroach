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
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type exprFormatter struct {
	mem *Memo
	buf *bytes.Buffer
}

type memoFormatter struct {
	exprFormatter
	ordering  []GroupID
	numbering []GroupID
}

func (m *Memo) makeExprFormatter(buf *bytes.Buffer) exprFormatter {
	return exprFormatter{
		mem: m,
		buf: buf,
	}
}

func (m *Memo) makeMemoFormatter(root GroupID) memoFormatter {
	f := memoFormatter{
		exprFormatter: exprFormatter{
			mem: m,
			buf: &bytes.Buffer{},
		},
	}

	// If a root was specified, we topological sort from it.  Otherwise, we
	// simply print out all the groups in the order and with the names they're
	// referred to physically. We can do this because the GroupID 0 never refers
	// to a valid group.
	if root != 0 {
		f.ordering = f.sortGroups(root)
	} else {
		// In this case we renumber with the identity mapping, so the groups have
		// the same numbers as they're represented with internally.
		f.ordering = make([]GroupID, len(m.groups)-1)
		for i := range m.groups[1:] {
			f.ordering[i] = GroupID(i + 1)
		}
	}

	// We renumber the groups so that they're still printed in order from 1..N.
	f.numbering = make([]GroupID, len(m.groups))
	for i := range f.ordering {
		f.numbering[f.ordering[i]] = GroupID(i + 1)
	}

	return f
}

func (f memoFormatter) Format() string {
	tp := treeprinter.New()
	root := tp.Child("memo")

	for i := range f.ordering {
		mgrp := &f.mem.groups[f.ordering[i]]

		f.buf.Reset()
		for ord := 0; ord < mgrp.exprCount(); ord++ {
			if ord != 0 {
				f.buf.WriteByte(' ')
			}
			f.formatExpr(mgrp.expr(ExprOrdinal(ord)))
		}
		child := root.Childf("G%d: %s", i+1, f.buf.String())
		f.formatBestExprSet(child, mgrp)
	}

	return tp.String()
}

func (f *memoFormatter) formatExpr(e *Expr) {
	fmt.Fprintf(f.buf, "(%s", e.op)
	for i := 0; i < e.ChildCount(); i++ {
		fmt.Fprintf(f.buf, " G%d", f.numbering[e.ChildGroup(f.mem, i)])
	}
	f.formatPrivate(e.Private(f.mem))
	f.buf.WriteString(")")
}

type bestExprSort struct {
	required PhysicalPropsID
	display  string
	best     *BestExpr
}

func (f *memoFormatter) formatBestExprSet(tp treeprinter.Node, mgrp *group) {
	// Sort the bestExprs by required properties.
	cnt := mgrp.bestExprCount()
	beSort := make([]bestExprSort, 0, cnt)
	for i := 0; i < cnt; i++ {
		best := mgrp.bestExpr(bestOrdinal(i))
		beSort = append(beSort, bestExprSort{
			required: best.required,
			display:  f.mem.LookupPhysicalProps(best.required).String(),
			best:     best,
		})
	}

	sort.Slice(beSort, func(i, j int) bool {
		return strings.Compare(beSort[i].display, beSort[j].display) < 0
	})

	for _, sort := range beSort {
		f.buf.Reset()

		// Don't show best expressions for scalar groups because they're not too
		// interesting.
		if !isScalarLookup[sort.best.op] {
			child := tp.Childf("\"%s\" [cost=%.2f]", sort.display, sort.best.cost)
			f.formatBestExpr(sort.best)
			child.Childf("best: %s", f.buf.String())
		}
	}
}

func (f *memoFormatter) formatBestExpr(be *BestExpr) {
	fmt.Fprintf(f.buf, "(%s", be.op)

	for i := 0; i < be.ChildCount(); i++ {
		bestChild := be.Child(i)
		fmt.Fprintf(f.buf, " G%d", f.numbering[bestChild.group])

		// Print properties required of the child if they are interesting.
		required := f.mem.bestExpr(bestChild).required
		if required != MinPhysPropsID {
			fmt.Fprintf(f.buf, "=\"%s\"", f.mem.LookupPhysicalProps(required).Fingerprint())
		}
	}

	f.formatPrivate(be.Private(f.mem))
	f.buf.WriteString(")")
}

func (f *exprFormatter) formatPrivate(private interface{}) {
	if private != nil {
		switch t := private.(type) {
		case nil:

		case *ScanOpDef:
			f.formatScanPrivate(t, false /* short */)

		case opt.ColumnID:
			fmt.Fprintf(f.buf, " %s", f.mem.metadata.ColumnLabel(t))

		case opt.ColSet, opt.ColList:
			// Don't show anything, because it's mostly redundant.

		default:
			fmt.Fprintf(f.buf, " %s", private)
		}
	}
}

func (f *exprFormatter) formatScanPrivate(def *ScanOpDef, short bool) {
	// Don't output name of index if it's the primary index.
	tab := f.mem.metadata.Table(def.Table)
	if def.Index == opt.PrimaryIndex {
		fmt.Fprintf(f.buf, " %s", tab.TabName())
	} else {
		fmt.Fprintf(f.buf, " %s@%s", tab.TabName(), tab.Index(def.Index).IdxName())
	}

	// Add additional fields when short=false.
	if !short {
		if def.Constraint != nil {
			fmt.Fprintf(f.buf, ",constrained")
		}
		if def.HardLimit > 0 {
			fmt.Fprintf(f.buf, ",lim=%d", def.HardLimit)
		}
	}
}

// iterDeps runs f for each child group of g.
func (f *memoFormatter) iterDeps(g *group, fn func(GroupID)) {
	for i := 0; i < g.exprCount(); i++ {
		e := g.expr(ExprOrdinal(i))
		for c := 0; c < e.ChildCount(); c++ {
			fn(e.ChildGroup(f.mem, c))
		}
	}
}

// sortGroups sorts groups reachable from the root by doing a BFS topological
// sort.
func (f *memoFormatter) sortGroups(root GroupID) (groups []GroupID) {
	indegrees := f.getIndegrees(root)

	res := make([]GroupID, 0, len(f.mem.groups))
	queue := []GroupID{root}

	for len(queue) > 0 {
		var next GroupID
		next, queue = queue[0], queue[1:]
		res = append(res, next)

		// When we visit a group, we conceptually remove it from the dependency
		// graph, so all of its dependencies have their indegree reduced by one.
		// Any dependencies which have no more dependents can now be visited and
		// are added to the queue.
		f.iterDeps(&f.mem.groups[next], func(dep GroupID) {
			indegrees[dep]--
			if indegrees[dep] == 0 {
				queue = append(queue, dep)
			}
		})
	}

	// If there remains any group with nonzero indegree, we had a cycle.
	for i := range indegrees {
		if indegrees[i] != 0 {
			// TODO(justin): we should handle this case, but there's no good way to
			// test it yet. Cross this bridge when we come to it (use raw-memo to
			// bypass this sorting procedure if this is causing you trouble).
			panic("memo had a cycle, use raw-memo")
		}
	}

	return res
}

// getIndegrees returns the indegree of each group reachable from the root.
func (f *memoFormatter) getIndegrees(root GroupID) (indegrees []int) {
	indegrees = make([]int, len(f.mem.groups))
	f.computeIndegrees(root, make([]bool, len(f.mem.groups)), indegrees)
	return indegrees
}

// computedIndegrees computes the indegree (number of dependents) of each group
// reachable from id.  It also populates reachable with true for all reachable
// ids.
func (f *memoFormatter) computeIndegrees(id GroupID, reachable []bool, indegrees []int) {
	if id <= 0 || reachable[id] {
		return
	}
	reachable[id] = true

	f.iterDeps(&f.mem.groups[id], func(dep GroupID) {
		indegrees[dep]++
		f.computeIndegrees(dep, reachable, indegrees)
	})
}
