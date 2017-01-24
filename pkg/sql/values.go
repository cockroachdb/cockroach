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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

type valuesNode struct {
	n        *parser.ValuesClause
	p        *planner
	columns  ResultColumns
	ordering sqlbase.ColumnOrdering
	tuples   [][]parser.TypedExpr
	rows     *RowContainer

	// rowsPopped is used for heaps, it indicates the number of rows that were
	// "popped". These rows are still part of the underlying RowContainer, in the
	// range [rows.Len()-n.rowsPopped, rows.Len).
	rowsPopped int

	desiredTypes []parser.Type // This can be removed when we only type check once.

	nextRow       int           // The index of the next row.
	invertSorting bool          // Inverts the sorting predicate.
	tmpValues     parser.DTuple // Used to store temporary values.
	err           error         // Used to propagate errors during heap operations.
}

func (p *planner) newContainerValuesNode(columns ResultColumns, capacity int) *valuesNode {
	return &valuesNode{
		columns: columns,
		rows:    NewRowContainer(p.session.TxnState.makeBoundAccount(), columns, capacity),
	}
}

func (p *planner) ValuesClause(
	n *parser.ValuesClause, desiredTypes []parser.Type,
) (planNode, error) {
	v := &valuesNode{
		p:            p,
		n:            n,
		desiredTypes: desiredTypes,
	}
	if len(n.Tuples) == 0 {
		return v, nil
	}

	numCols := len(n.Tuples[0].Exprs)

	v.tuples = make([][]parser.TypedExpr, 0, len(n.Tuples))
	tupleBuf := make([]parser.TypedExpr, len(n.Tuples)*numCols)

	v.columns = make(ResultColumns, 0, numCols)

	for num, tuple := range n.Tuples {
		if a, e := len(tuple.Exprs), numCols; a != e {
			return nil, fmt.Errorf("VALUES lists must all be the same length, %d for %d", a, e)
		}

		// Chop off prefix of tupleBuf and limit its capacity.
		tupleRow := tupleBuf[:numCols:numCols]
		tupleBuf = tupleBuf[numCols:]

		for i, expr := range tuple.Exprs {
			if err := p.parser.AssertNoAggregationOrWindowing(
				expr, "VALUES", p.session.SearchPath,
			); err != nil {
				return nil, err
			}

			desired := parser.TypeAny
			if len(desiredTypes) > i {
				desired = desiredTypes[i]
			}
			typedExpr, err := p.analyzeExpr(expr, nil, parser.IndexedVarHelper{}, desired, false, "")
			if err != nil {
				return nil, err
			}

			typ := typedExpr.ResolvedType()
			if num == 0 {
				v.columns = append(v.columns, ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ == parser.TypeNull {
				v.columns[i].Typ = typ
			} else if typ != parser.TypeNull && !typ.Equivalent(v.columns[i].Typ) {
				return nil, fmt.Errorf("VALUES list type mismatch, %s for %s", typ, v.columns[i].Typ)
			}

			tupleRow[i] = typedExpr
		}
		v.tuples = append(v.tuples, tupleRow)
	}

	return v, nil
}

func (n *valuesNode) Start() error {
	if n.n == nil {
		return nil
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so its expressions need evaluting.
	// This may run subqueries.
	n.rows = NewRowContainer(n.p.session.TxnState.makeBoundAccount(), n.columns, len(n.n.Tuples))

	row := make([]parser.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			if n.columns[i].omitted {
				row[i] = parser.DNull
			} else {
				var err error
				row[i], err = typedExpr.Eval(&n.p.evalCtx)
				if err != nil {
					return err
				}
			}
		}
		if _, err := n.rows.AddRow(row); err != nil {
			return err
		}
	}

	return nil
}

func (n *valuesNode) Columns() ResultColumns {
	return n.columns
}

func (n *valuesNode) Ordering() orderingInfo {
	return orderingInfo{}
}

func (n *valuesNode) Values() parser.DTuple {
	return n.rows.At(n.nextRow - 1)
}

func (*valuesNode) MarkDebug(_ explainMode) {}

func (n *valuesNode) DebugValues() debugValues {
	val := n.rows.At(n.nextRow - 1)
	return debugValues{
		rowIdx: n.nextRow - 1,
		key:    fmt.Sprintf("%d", n.nextRow-1),
		value:  val.String(),
		output: debugValueRow,
	}
}

func (n *valuesNode) Next() (bool, error) {
	if n.nextRow >= n.rows.Len() {
		return false, nil
	}
	n.nextRow++
	return true, nil
}

func (n *valuesNode) Close() {
	if n.rows != nil {
		n.rows.Close()
		n.rows = nil
	}
}

func (n *valuesNode) Len() int {
	return n.rows.Len() - n.rowsPopped
}

func (n *valuesNode) Less(i, j int) bool {
	// TODO(pmattis): An alternative to this type of field-based comparison would
	// be to construct a sort-key per row using encodeTableKey(). Using a
	// sort-key approach would likely fit better with a disk-based sort.
	ra, rb := n.rows.At(i), n.rows.At(j)
	return n.invertSorting != n.ValuesLess(ra, rb)
}

// ValuesLess returns the comparison result between the two provided DTuples
// in the context of the valuesNode ordering.
func (n *valuesNode) ValuesLess(ra, rb parser.DTuple) bool {
	for _, c := range n.ordering {
		var da, db parser.Datum
		if c.Direction == encoding.Ascending {
			da = ra[c.ColIdx]
			db = rb[c.ColIdx]
		} else {
			da = rb[c.ColIdx]
			db = ra[c.ColIdx]
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
	n.rows.Swap(i, j)
}

var _ heap.Interface = (*valuesNode)(nil)

// Push implements the heap.Interface interface.
func (n *valuesNode) Push(x interface{}) {
	_, n.err = n.rows.AddRow(n.tmpValues)
}

// PushValues pushes the given DTuple value into the heap representation
// of the valuesNode.
func (n *valuesNode) PushValues(values parser.DTuple) error {
	// Avoid passing slice through interface{} to avoid allocation.
	n.tmpValues = values
	heap.Push(n, nil)
	return n.err
}

// Pop implements the heap.Interface interface.
func (n *valuesNode) Pop() interface{} {
	if n.rowsPopped >= n.rows.Len() {
		panic("no more rows to pop")
	}
	n.rowsPopped++
	// Returning a DTuple as an interface{} involves an allocation. Luckily, the
	// value of Pop is only used for the return value of heap.Pop, which we can
	// avoid using.
	return nil
}

// PopValues pops the top DTuple value off the heap representation
// of the valuesNode.
func (n *valuesNode) PopValues() parser.DTuple {
	heap.Pop(n)
	// Return the last popped row.
	return n.rows.At(n.rows.Len() - n.rowsPopped)
}

// ResetLen resets the length to that of the underlying row container. This
// resets the effect that popping values had on the valuesNode's visible length.
func (n *valuesNode) ResetLen() {
	n.rowsPopped = 0
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

// InitMinHeap initializes the valuesNode.rows slice as a min-heap.
func (n *valuesNode) InitMinHeap() {
	n.invertSorting = false
	heap.Init(n)
}
