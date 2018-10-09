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
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type memoFmtCtx struct {
	buf       *bytes.Buffer
	flags     FmtFlags
	ordering  []opt.Expr
	numbering map[opt.Expr]int
	interner  interner
}

func (m *Memo) format(f *memoFmtCtx) string {
	// First assign group numbers to every expression in the memo.
	f.numbering = make(map[opt.Expr]int)
	m.numberMemo(f, m.RootExpr())

	// Now recursively format the memo using treeprinter.
	tp := treeprinter.New()

	var tpRoot treeprinter.Node
	if m.IsOptimized() {
		tpRoot = tp.Childf("memo (optimized, ~%dKB)", m.MemoryEstimate()/1024)
	} else {
		tpRoot = tp.Childf("memo (not optimized, ~%dKB)", m.MemoryEstimate()/1024)
	}

	for i, e := range f.ordering {
		f.buf.Reset()
		if rel, ok := e.(RelExpr); ok {
			m.formatGroup(f, rel)
			tpChild := tpRoot.Childf("G%d: %s", i+1, f.buf.String())
			m.formatBestSet(f, rel, tpChild)
		} else {
			m.formatExpr(f, e)
			tpRoot.Childf("G%d: %s", i+1, f.buf.String())
		}
	}

	return tp.String()
}

// numberMemo numbers all memo groups breadth-first, starting from the root of
// expression tree.
func (m *Memo) numberMemo(f *memoFmtCtx, root opt.Expr) {
	f.numbering[root] = 0
	f.ordering = append(f.ordering, root)

	// Keep looping as long as expressions keep getting added to the ordering
	// slice.
	start := 0
	for end := len(f.ordering); start != end; start, end = end, len(f.ordering) {
		// Loop over expressions added during the previous pass.
		for i := start; i < end; i++ {
			e := f.ordering[i]

			if member, ok := e.(RelExpr); ok {
				// Iterate over members in memo group.
				for member = member.FirstExpr(); member != nil; member = member.NextExpr() {
					f.numbering[member] = i
					m.numberChildren(f, member, i)
				}
			} else {
				m.numberChildren(f, e, i)
			}
		}
	}
}

// numberChildren assigns numbers to child groups of the given expression and
// adds them to the numbering and ordering slices.
func (m *Memo) numberChildren(f *memoFmtCtx, e opt.Expr, number int) {
	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)

		// Don't include list item expressions, as they don't communicate useful
		// information.
		if opt.IsListItemOp(child) {
			child = child.Child(0)
		}

		// Handle special case of list expressions, which are not interned and
		// stored as byval slices in parent expressions. Intern separately in
		// order to detect duplicate lists.
		if opt.IsListOp(child) {
			interned := f.interner.InternExpr(child)
			if interned != child {
				// List has already been seen before, so give it same number.
				f.numbering[child] = f.numbering[interned]
				continue
			}
		}

		// Use the first expression in each group to ensure no more than one
		// expression per group is added to the ordering list.
		grp := child
		if rel, ok := grp.(RelExpr); ok {
			grp = rel.FirstExpr()
		}

		if existing, ok := f.numbering[grp]; ok {
			f.numbering[child] = existing
		} else {
			f.numbering[child] = len(f.ordering)
			f.ordering = append(f.ordering, child)
		}
	}
}

func (m *Memo) formatGroup(f *memoFmtCtx, best RelExpr) {
	first := best.FirstExpr()
	for member := first; member != nil; member = member.NextExpr() {
		if member != first {
			f.buf.WriteByte(' ')
		}
		m.formatExpr(f, member)
	}
}

func (m *Memo) formatExpr(f *memoFmtCtx, e opt.Expr) {
	fmt.Fprintf(f.buf, "(%s", e.Op())
	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		if opt.IsListItemOp(child) {
			child = child.Child(0)
		}
		fmt.Fprintf(f.buf, " G%d", f.numbering[child]+1)
	}
	m.formatPrivate(f, e, &props.Physical{})
	f.buf.WriteString(")")
}

func (m *Memo) formatBestSet(f *memoFmtCtx, best RelExpr, tp treeprinter.Node) {
	for {
		physical := best.Physical()
		if physical == nil {
			break
		}

		f.buf.Reset()
		var tpChild treeprinter.Node
		if physical.Defined() {
			tpChild = tp.Childf("%s", physical)
		} else {
			tpChild = tp.Childf("[]")
		}
		m.formatBest(f, best)
		tpChild.Childf("best: %s", f.buf.String())
		tpChild.Childf("cost: %.2f", best.Cost())

		if !opt.IsEnforcerOp(best) {
			break
		}

		best = best.Child(0).(RelExpr)
	}
}

func (m *Memo) formatBest(f *memoFmtCtx, best RelExpr) {
	fmt.Fprintf(f.buf, "(%s", best.Op())

	for i := 0; i < best.ChildCount(); i++ {
		bestChild := best.Child(i)
		fmt.Fprintf(f.buf, " G%d", f.numbering[bestChild]+1)

		// Print properties required of the child if they are interesting.
		if rel, ok := bestChild.(RelExpr); ok {
			required := rel.Physical()
			if required != nil && required.Defined() {
				fmt.Fprintf(f.buf, "=\"%s\"", required)
			}
		}
	}

	m.formatPrivate(f, best, best.Physical())
	f.buf.WriteString(")")
}

func (m *Memo) formatPrivate(f *memoFmtCtx, e opt.Expr, physProps *props.Physical) {
	private := e.Private()
	if private == nil {
		return
	}

	// Start by using private expression formatting.
	nf := MakeExprFmtCtxBuffer(f.buf, ExprFmtHideAll, m)
	formatPrivate(&nf, private, physProps)

	// Now append additional information that's useful in the memo case.
	switch t := e.(type) {
	case *ScanExpr:
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

	case *IndexJoinExpr:
		fmt.Fprintf(f.buf, ",cols=%s", t.Cols)

	case *LookupJoinExpr:
		fmt.Fprintf(f.buf, ",keyCols=%v,outCols=%s", t.KeyCols, t.Cols)

	case *ExplainExpr:
		propsStr := t.Props.String()
		if propsStr != "" {
			fmt.Fprintf(f.buf, " %s", propsStr)
		}

	case *ProjectExpr:
		t.Passthrough.ForEach(func(i int) {
			fmt.Fprintf(f.buf, " %s", m.metadata.ColumnLabel(opt.ColumnID(i)))
		})
	}
}
