// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"container/heap"
	"fmt"
	"sort"
	"strconv"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// ValuesClause constructs a valuesNode from a VALUES expression.
func (p *planner) ValuesClause(n *parser.ValuesClause) (planNode, *roachpb.Error) {
	v := &valuesNode{
		rows: make([]parser.DTuple, 0, len(n.Tuples)),
	}

	for num, tupleOrig := range n.Tuples {
		if num == 0 {
			v.columns = make([]ResultColumn, 0, len(tupleOrig.Exprs))
		} else if a, e := len(tupleOrig.Exprs), len(v.columns); a != e {
			return nil, roachpb.NewUErrorf("VALUES lists must all be the same length, %d for %d", a, e)
		}

		// We must not modify the ValuesClause node in-place (or the changes will leak if we retry this
		// transaction).
		tuple := parser.Tuple{Exprs: parser.Exprs(append([]parser.Expr(nil), tupleOrig.Exprs...))}

		for i := range tuple.Exprs {
			var pErr *roachpb.Error
			tuple.Exprs[i], pErr = p.expandSubqueries(tuple.Exprs[i], 1)
			if pErr != nil {
				return nil, pErr
			}
			var err error
			typ, err := tuple.Exprs[i].TypeCheck(p.evalCtx.Args)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			tuple.Exprs[i], err = p.parser.NormalizeExpr(p.evalCtx, tuple.Exprs[i])
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			if num == 0 {
				v.columns = append(v.columns, ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ == parser.DNull {
				v.columns[i].Typ = typ
			} else if typ != parser.DNull && !typ.TypeEqual(v.columns[i].Typ) {
				return nil, roachpb.NewUErrorf("VALUES list type mismatch, %s for %s", typ.Type(), v.columns[i].Typ.Type())
			}
		}
		data, err := tuple.Eval(p.evalCtx)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		vals, ok := data.(parser.DTuple)
		if !ok {
			return nil, roachpb.NewUErrorf("expected a tuple, but found %T", data)
		}
		v.rows = append(v.rows, vals)
	}

	return v, nil
}

type valuesNode struct {
	columns  []ResultColumn
	ordering columnOrdering
	rows     []parser.DTuple

	nextRow       int           // The index of the next row.
	invertSorting bool          // Inverts the sorting predicate.
	tmpValues     parser.DTuple // Used to store temporary values.
}

func (n *valuesNode) Columns() []ResultColumn {
	return n.columns
}

func (n *valuesNode) Ordering() orderingInfo {
	return orderingInfo{}
}

func (n *valuesNode) Values() parser.DTuple {
	return n.rows[n.nextRow-1]
}

func (*valuesNode) MarkDebug(_ explainMode) {}

func (n *valuesNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: n.nextRow - 1,
		key:    fmt.Sprintf("%d", n.nextRow-1),
		value:  n.rows[n.nextRow-1].String(),
		output: debugValueRow,
	}
}

func (n *valuesNode) Next() bool {
	if n.nextRow >= len(n.rows) {
		return false
	}
	n.nextRow++
	return true
}

func (*valuesNode) PErr() *roachpb.Error {
	return nil
}

func (n *valuesNode) Len() int {
	return len(n.rows)
}

func (n *valuesNode) Less(i, j int) bool {
	// TODO(pmattis): An alternative to this type of field-based comparison would
	// be to construct a sort-key per row using encodeTableKey(). Using a
	// sort-key approach would likely fit better with a disk-based sort.
	ra, rb := n.rows[i], n.rows[j]
	return n.invertSorting != n.ValuesLess(ra, rb)
}

// ValuesLess returns the comparison result between the two provided DTuples
// in the context of the valuesNode ordering.
func (n *valuesNode) ValuesLess(ra, rb parser.DTuple) bool {
	for _, c := range n.ordering {
		var da, db parser.Datum
		if c.direction == encoding.Ascending {
			da = ra[c.colIdx]
			db = rb[c.colIdx]
		} else {
			da = rb[c.colIdx]
			db = ra[c.colIdx]
		}
		// TODO(pmattis): This is assuming that the datum types are compatible. I'm
		// not sure this always holds as `CASE` expressions can return different
		// types for a column for different rows. Investigate how other RDBMs
		// handle this.
		if c := da.Compare(db); c < 0 {
			return true
		} else if c > 0 {
			return false
		}
	}
	return true
}

func (n *valuesNode) Swap(i, j int) {
	n.rows[i], n.rows[j] = n.rows[j], n.rows[i]
}

var _ heap.Interface = (*valuesNode)(nil)

// Push implements the heap.Interface interface.
func (n *valuesNode) Push(x interface{}) {
	n.rows = append(n.rows, n.tmpValues)
}

// PushValues pushes the given DTuple value into the heap representation
// of the valuesNode.
func (n *valuesNode) PushValues(values parser.DTuple) {
	// Avoid passing slice through interface{} to avoid allocation.
	n.tmpValues = values
	heap.Push(n, nil)
}

// Pop implements the heap.Interface interface.
func (n *valuesNode) Pop() interface{} {
	idx := len(n.rows) - 1
	// Returning a pointer to avoid an allocation when storing the slice in an interface{}.
	x := &(n.rows)[idx]
	n.rows = n.rows[:idx]
	return x
}

// PopValues pops the top DTuple value off the heap representation
// of the valuesNode.
func (n *valuesNode) PopValues() parser.DTuple {
	x := heap.Pop(n)
	return *x.(*parser.DTuple)
}

// SortAll sorts all values in the valuesNode.rows slice.
func (n *valuesNode) SortAll() {
	n.invertSorting = false
	sort.Sort(n)
}

// InitMaxHeap initializes the valuesNode.rows slice as a max-heap.
func (n *valuesNode) InitMaxHeap() {
	n.invertSorting = true
	heap.Init(n)
}

// InitMaxHeap initializes the valuesNode.rows slice as a min-heap.
func (n *valuesNode) InitMinHeap() {
	n.invertSorting = false
	heap.Init(n)
}

func (n *valuesNode) ExplainPlan() (name, description string, children []planNode) {
	name = "values"
	pluralize := func(n int) string {
		if n == 1 {
			return ""
		}
		return "s"
	}
	description = fmt.Sprintf("%d column%s, %d row%s",
		len(n.columns), pluralize(len(n.columns)),
		len(n.rows), pluralize(len(n.rows)))
	return name, description, nil
}

func (*valuesNode) SetLimitHint(_ int64, _ bool) {}
