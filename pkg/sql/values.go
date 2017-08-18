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

package sql

import (
	"container/heap"
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func newValuesListLenErr(exp, got int) error {
	return errors.Errorf("VALUES lists must all be the same length, expected %d columns, found %d",
		exp, got)
}

type valuesNode struct {
	n        *parser.ValuesClause
	p        *planner
	columns  sqlbase.ResultColumns
	ordering sqlbase.ColumnOrdering
	tuples   [][]parser.TypedExpr
	rows     *sqlbase.RowContainer

	// isConst is set if the valuesNode only contains constant expressions (no
	// subqueries). In this case, rows will be evaluated during the first call
	// to planNode.Start and memoized for future consumption. A valuesNode with
	// isConst = true can serve its values multiple times. See valuesNode.Reset.
	isConst bool

	// rowsPopped is used for heaps, it indicates the number of rows that were
	// "popped". These rows are still part of the underlying sqlbase.RowContainer, in the
	// range [rows.Len()-n.rowsPopped, rows.Len).
	rowsPopped int

	nextRow       int  // The index of the next row.
	invertSorting bool // Inverts the sorting predicate.
}

func (p *planner) newContainerValuesNode(columns sqlbase.ResultColumns, capacity int) *valuesNode {
	return &valuesNode{
		p:       p,
		columns: columns,
		rows: sqlbase.NewRowContainer(
			p.session.TxnState.makeBoundAccount(), sqlbase.ColTypeInfoFromResCols(columns), capacity,
		),
		isConst: true,
	}
}

func (p *planner) ValuesClause(
	ctx context.Context, n *parser.ValuesClause, desiredTypes []parser.Type,
) (planNode, error) {
	v := &valuesNode{
		p:       p,
		n:       n,
		isConst: true,
	}
	if len(n.Tuples) == 0 {
		return v, nil
	}

	numCols := len(n.Tuples[0].Exprs)

	v.tuples = make([][]parser.TypedExpr, 0, len(n.Tuples))
	tupleBuf := make([]parser.TypedExpr, len(n.Tuples)*numCols)

	v.columns = make(sqlbase.ResultColumns, 0, numCols)

	defer func(prev bool) { p.hasSubqueries = prev }(p.hasSubqueries)
	p.hasSubqueries = false

	for num, tuple := range n.Tuples {
		if a, e := len(tuple.Exprs), numCols; a != e {
			return nil, newValuesListLenErr(e, a)
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
			typedExpr, err := p.analyzeExpr(ctx, expr, nil, parser.IndexedVarHelper{}, desired, false, "")
			if err != nil {
				return nil, err
			}

			typ := typedExpr.ResolvedType()
			if num == 0 {
				v.columns = append(v.columns, sqlbase.ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ == parser.TypeNull {
				v.columns[i].Typ = typ
			} else if typ != parser.TypeNull && !typ.Equivalent(v.columns[i].Typ) {
				return nil, fmt.Errorf("VALUES list type mismatch, %s for %s", typ, v.columns[i].Typ)
			}

			tupleRow[i] = typedExpr
		}
		v.tuples = append(v.tuples, tupleRow)
	}

	// TODO(nvanbenschoten): if v.isConst, we should be able to evaluate n.rows
	// ahead of time. This requires changing the contract for planNode.Close such
	// that it must always be called unless an error is returned from a planNode
	// constructor. This would simplify the Close contract, but would make some
	// code (like in planner.SelectClause) more messy.
	v.isConst = !p.hasSubqueries
	return v, nil
}

// Start implements the planNode interface.
func (n *valuesNode) Start(params runParams) error {
	if n.rows != nil {
		if !n.isConst {
			log.Fatalf(params.ctx, "valuesNode evaluted twice")
		}
		return nil
	}

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so its expressions need evaluting.
	// This may run subqueries.
	n.rows = sqlbase.NewRowContainer(
		n.p.session.TxnState.makeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(n.columns),
		len(n.n.Tuples),
	)

	row := make([]parser.Datum, len(n.columns))
	for _, tupleRow := range n.tuples {
		for i, typedExpr := range tupleRow {
			if n.columns[i].Omitted {
				row[i] = parser.DNull
			} else {
				var err error
				row[i], err = typedExpr.Eval(&n.p.evalCtx)
				if err != nil {
					return err
				}
			}
		}
		if _, err := n.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}

	return nil
}

func (n *valuesNode) Values() parser.Datums {
	return n.rows.At(n.nextRow - 1)
}

func (n *valuesNode) Next(runParams) (bool, error) {
	if n.nextRow >= n.rows.Len() {
		return false, nil
	}
	n.nextRow++
	return true, nil
}

// Reset resets the valuesNode processing state without requiring recomputation
// of the values tuples if the valuesNode is processed again. Reset can only
// be called if valuesNode.isConst.
func (n *valuesNode) Reset(ctx context.Context) {
	if !n.isConst {
		log.Fatalf(ctx, "valuesNode.Reset can only be called on constant valuesNodes")
	}
	n.nextRow = 0
}

func (n *valuesNode) Close(ctx context.Context) {
	if n.rows != nil {
		n.rows.Close(ctx)
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

// ValuesLess returns the comparison result between the two provided Datums slices
// in the context of the valuesNode ordering.
func (n *valuesNode) ValuesLess(ra, rb parser.Datums) bool {
	return sqlbase.CompareDatums(n.ordering, &n.p.evalCtx, ra, rb) < 0
}

func (n *valuesNode) Swap(i, j int) {
	n.rows.Swap(i, j)
}

var _ heap.Interface = (*valuesNode)(nil)

// Push implements the heap.Interface interface.
func (n *valuesNode) Push(x interface{}) {
}

// PushValues pushes the given Datums value into the heap representation
// of the valuesNode.
func (n *valuesNode) PushValues(ctx context.Context, values parser.Datums) error {
	_, err := n.rows.AddRow(ctx, values)
	heap.Push(n, nil)
	return err
}

// Pop implements the heap.Interface interface.
func (n *valuesNode) Pop() interface{} {
	if n.rowsPopped >= n.rows.Len() {
		panic("no more rows to pop")
	}
	n.rowsPopped++
	// Returning a Datums as an interface{} involves an allocation. Luckily, the
	// value of Pop is only used for the return value of heap.Pop, which we can
	// avoid using.
	return nil
}

// PopValues pops the top Datums value off the heap representation
// of the valuesNode.
func (n *valuesNode) PopValues() parser.Datums {
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
func (n *valuesNode) SortAll(cancelChecker *sqlbase.CancelChecker) {
	n.invertSorting = false
	sqlbase.Sort(n, cancelChecker)
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
