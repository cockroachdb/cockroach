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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

type valuesNode struct {
	n        *parser.ValuesClause
	p        *planner
	columns  []ResultColumn
	ordering columnOrdering
	rows     []parser.DTuple

	desiredTypes []parser.Datum // This can be removed when we only type check once.

	nextRow       int           // The index of the next row.
	invertSorting bool          // Inverts the sorting predicate.
	tmpValues     parser.DTuple // Used to store temporary values.
}

func (p *planner) ValuesClause(n *parser.ValuesClause, desiredTypes []parser.Datum) (planNode, error) {
	v := &valuesNode{
		p:            p,
		n:            n,
		desiredTypes: desiredTypes,
	}
	if len(n.Tuples) == 0 {
		return v, nil
	}

	numCols := len(n.Tuples[0].Exprs)
	v.columns = make([]ResultColumn, 0, numCols)

	for num, tuple := range n.Tuples {
		if a, e := len(tuple.Exprs), numCols; a != e {
			return nil, fmt.Errorf("VALUES lists must all be the same length, %d for %d", a, e)
		}

		for i, expr := range tuple.Exprs {
			// TODO(knz): We need to expand subqueries two times, once here
			// and once in Start() below, until the logic for select is split
			// between makePlan and Start(). This is because a first call is
			// needed for typechecking, and a separate call is needed to
			// select indexes.
			expr, err := p.expandSubqueries(expr, 1)
			if err != nil {
				return nil, err
			}
			desired := parser.NoTypePreference
			if len(desiredTypes) > i {
				desired = desiredTypes[i]
			}
			typedExpr, err := parser.TypeCheck(expr, p.evalCtx.Args, desired)
			if err != nil {
				return nil, err
			}
			typedExpr, err = p.parser.NormalizeExpr(p.evalCtx, typedExpr)
			if err != nil {
				return nil, err
			}

			typ := typedExpr.ReturnType()
			if num == 0 {
				v.columns = append(v.columns, ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ == parser.DNull {
				v.columns[i].Typ = typ
			} else if typ != parser.DNull && !typ.TypeEqual(v.columns[i].Typ) {
				return nil, fmt.Errorf("VALUES list type mismatch, %s for %s", typ.Type(), v.columns[i].Typ.Type())
			}
		}
	}

	return v, nil
}

func (n *valuesNode) expandPlan() error {
	if n.n == nil {
		return nil
	}

	n.rows = make([]parser.DTuple, 0, len(n.n.Tuples))

	// This node is coming from a SQL query (as opposed to sortNode and
	// others that create a valuesNode internally for storing results
	// from other planNodes), so it needs to evaluate expressions.

	numCols := len(n.columns)
	rowBuf := make(parser.DTuple, len(n.n.Tuples)*numCols)

	for _, tuple := range n.n.Tuples {
		// Chop off prefix of rowBuf and limit its capacity.
		row := rowBuf[:numCols:numCols]
		rowBuf = rowBuf[numCols:]

		for i, expr := range tuple.Exprs {
			// TODO(knz): see comment above about expandSubqueries in ValuesClause().
			expr, err := n.p.expandSubqueries(expr, 1)
			if err != nil {
				return err
			}
			desired := parser.NoTypePreference
			if len(n.desiredTypes) > i {
				desired = n.desiredTypes[i]
			}
			typedExpr, err := parser.TypeCheck(expr, n.p.evalCtx.Args, desired)
			if err != nil {
				return err
			}
			typedExpr, err = n.p.parser.NormalizeExpr(n.p.evalCtx, typedExpr)
			if err != nil {
				return err
			}

			row[i], err = typedExpr.Eval(n.p.evalCtx)
			if err != nil {
				return err
			}
		}
		n.rows = append(n.rows, row)
	}
	return nil
}

func (n *valuesNode) Start() error {
	return nil
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

func (*valuesNode) Err() error {
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

func (n *valuesNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	name = "values"
	description = fmt.Sprintf("%d column%s",
		len(n.columns), util.Pluralize(int64(len(n.columns))))
	return name, description, nil
}

func (n *valuesNode) ExplainTypes(regTypes func(string, string)) {
	if n.n != nil {
		for i, tuple := range n.rows {
			regTypes(fmt.Sprintf("tuple %d", i), parser.AsStringWithFlags(&tuple, parser.FmtShowTypes))
		}
	}
}

func (*valuesNode) SetLimitHint(_ int64, _ bool) {}
