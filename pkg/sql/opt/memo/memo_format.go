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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type exprFormatter struct {
	mem *Memo
	buf *bytes.Buffer
}

type memoFormatter struct {
	exprFormatter
	flags     FmtFlags
	ordering  []GroupID
	numbering []GroupID
}

// formatMode controls the formatting depending on the context.
type formatMode int8

const (
	// formatNormal is used when we are printing expressions.
	formatNormal formatMode = iota
	// formatMemo is used when we are printing the memo.
	formatMemo
)

func (m *Memo) makeExprFormatter(buf *bytes.Buffer) exprFormatter {
	return exprFormatter{
		mem: m,
		buf: buf,
	}
}

func (m *Memo) makeMemoFormatter(flags FmtFlags) memoFormatter {
	return memoFormatter{
		exprFormatter: exprFormatter{
			mem: m,
			buf: &bytes.Buffer{},
		},
		flags: flags,
	}
}

func (f *memoFormatter) format() string {
	// If requested, we topological sort the memo with respect to its root group.
	// Otherwise, we simply print out all the groups in the order and with the
	// names they're referred to physically.
	if f.flags.HasFlags(FmtRaw) || !f.mem.isOptimized() {
		// In this case we renumber with the identity mapping, so the groups have
		// the same numbers as they're represented with internally.
		f.ordering = make([]GroupID, len(f.mem.groups)-1)
		for i := range f.mem.groups[1:] {
			f.ordering[i] = GroupID(i + 1)
		}
	} else {
		f.ordering = f.sortGroups(f.mem.root.group)
	}

	// We renumber the groups so that they're still printed in order from 1..N.
	f.numbering = make([]GroupID, len(f.mem.groups))
	for i := range f.ordering {
		f.numbering[f.ordering[i]] = GroupID(i + 1)
	}

	tp := treeprinter.New()

	var root treeprinter.Node
	if f.mem.isOptimized() {
		root = tp.Child("memo (optimized)")
	} else {
		root = tp.Child("memo (not optimized)")
	}

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

	// If showing raw memo, then add header text to point to root expression if
	// it's available.
	if f.flags.HasFlags(FmtRaw) && f.mem.isOptimized() {
		ev := f.mem.Root()
		return fmt.Sprintf("root: G%d, %s\n%s", f.numbering[ev.Group()], ev.Physical(), tp.String())
	}
	return tp.String()
}

func (f *memoFormatter) formatExpr(e *Expr) {
	fmt.Fprintf(f.buf, "(%s", e.op)
	for i := 0; i < e.ChildCount(); i++ {
		fmt.Fprintf(f.buf, " G%d", f.numbering[e.ChildGroup(f.mem, i)])
	}
	f.formatPrivate(e.Private(f.mem), &props.Physical{}, formatMemo)
	f.buf.WriteString(")")
}

type bestExprSort struct {
	required PhysicalPropsID
	display  string
	best     *BestExpr
}

func (f *memoFormatter) formatBestExprSet(tp treeprinter.Node, mgrp *group) {
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
			display:  f.mem.LookupPhysicalProps(best.required).String(),
			best:     best,
		})
	}

	sort.Slice(beSort, func(i, j int) bool {
		// Always order the root required properties first.
		if mgrp.id == f.mem.root.group {
			if beSort[i].required == beSort[i].best.required {
				return true
			}
		}
		return strings.Compare(beSort[i].display, beSort[j].display) < 0
	})

	for _, sort := range beSort {
		f.buf.Reset()
		child := tp.Childf("\"%s\"", sort.display)
		f.formatBestExpr(sort.best)
		child.Childf("best: %s", f.buf.String())
		child.Childf("cost: %.2f", sort.best.cost)
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
			fmt.Fprintf(f.buf, "=\"%s\"", f.mem.LookupPhysicalProps(required).String())
		}
	}

	f.formatPrivate(be.Private(f.mem), f.mem.LookupPhysicalProps(be.Required()), formatMemo)
	f.buf.WriteString(")")
}

// forEachDependency runs f for each child group of g.
func (f *memoFormatter) forEachDependency(g *group, fn func(GroupID)) {
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
		f.forEachDependency(&f.mem.groups[next], func(dep GroupID) {
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

	f.forEachDependency(&f.mem.groups[id], func(dep GroupID) {
		indegrees[dep]++
		f.computeIndegrees(dep, reachable, indegrees)
	})
}

func (f exprFormatter) formatPrivate(
	private interface{}, physProps *props.Physical, mode formatMode,
) {
	if private == nil {
		return
	}
	switch t := private.(type) {
	case *ScanOpDef:
		// Don't output name of index if it's the primary index.
		tab := f.mem.metadata.Table(t.Table)
		if t.Index == opt.PrimaryIndex {
			fmt.Fprintf(f.buf, " %s", tab.Name().TableName)
		} else {
			fmt.Fprintf(f.buf, " %s@%s", tab.Name().TableName, tab.Index(t.Index).IdxName())
		}
		if _, reverse := t.CanProvideOrdering(f.mem.metadata, &physProps.Ordering); reverse {
			f.buf.WriteString(",rev")
		}
		if mode == formatMemo {
			if tab.ColumnCount() != t.Cols.Len() {
				fmt.Fprintf(f.buf, ",cols=%s", t.Cols)
			}
			if t.Constraint != nil {
				fmt.Fprintf(f.buf, ",constrained")
			}
			if t.HardLimit.IsSet() {
				fmt.Fprintf(f.buf, ",lim=%s", t.HardLimit)
			}
		}

	case *VirtualScanOpDef:
		tab := f.mem.metadata.Table(t.Table)
		fmt.Fprintf(f.buf, " %s", tab.Name())

	case *RowNumberDef:
		if !t.Ordering.Any() {
			fmt.Fprintf(f.buf, " ordering=%s", t.Ordering)
		}

	case *GroupByDef:
		fmt.Fprintf(f.buf, " cols=%s", t.GroupingCols.String())
		if !t.Ordering.Any() {
			fmt.Fprintf(f.buf, ",ordering=%s", t.Ordering)
		}

	case opt.ColumnID:
		fmt.Fprintf(f.buf, " %s", f.mem.metadata.ColumnLabel(t))

	case *IndexJoinDef:
		tab := f.mem.metadata.Table(t.Table)
		fmt.Fprintf(f.buf, " %s", tab.Name().TableName)
		if mode == formatMemo {
			fmt.Fprintf(f.buf, ",cols=%s", t.Cols)
		}

	case *LookupJoinDef:
		tab := f.mem.metadata.Table(t.Table)
		if t.Index == opt.PrimaryIndex {
			fmt.Fprintf(f.buf, " %s", tab.Name().TableName)
		} else {
			fmt.Fprintf(f.buf, " %s@%s", tab.Name().TableName, tab.Index(t.Index).IdxName())
		}
		if mode == formatMemo {
			fmt.Fprintf(f.buf, ",keyCols=%v,lookupCols=%s", t.KeyCols, t.LookupCols)
		}

	case *ExplainOpDef:
		if mode == formatMemo {
			propsStr := t.Props.String()
			if propsStr != "" {
				fmt.Fprintf(f.buf, " %s", propsStr)
			}
		}

	case *MergeOnDef:
		fmt.Fprintf(f.buf, " %s,%s,%s", t.JoinType, t.LeftEq, t.RightEq)

	case opt.ColSet, opt.ColList:
		// Don't show anything, because it's mostly redundant.

	case *ProjectionsOpDef:
		// In normal mode, the information is mostly redundant. It is helpful to
		// display these columns in memo mode though.
		if mode == formatMemo {
			t.PassthroughCols.ForEach(func(i int) {
				fmt.Fprintf(f.buf, " %s", f.mem.metadata.ColumnLabel(opt.ColumnID(i)))
			})
		}

	case *props.OrderingChoice:
		if !t.Any() {
			fmt.Fprintf(f.buf, " ordering=%s", t)
		}

	case *SetOpColMap, types.T:
		// Don't show anything, because it's mostly redundant.

	default:
		fmt.Fprintf(f.buf, " %v", private)
	}
}
