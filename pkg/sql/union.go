// Copyright 2016 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// unionNode is a planNode whose rows are the result of one of three set
// operations (UNION, INTERSECT, or EXCEPT) on left and right. There are two
// variations of each set operation: distinct, which always returns unique
// results, and all, which does no uniqueing.
//
// Ordering of rows is expected to be handled externally to unionNode.
// TODO(dan): In the long run, this is insufficient. If we know both left and
// right are ordered the same way, we can do the set logic without the map
// state. Additionally, if the unionNode has an ordering then we can hint it
// down to left and right and force the condition for this first optimization.
//
// All six of the operations can be completed without cacheing rows by
// iterating one side then the other and keeping counts of unique rows
// in a map. The logic is common for all six. However, because EXCEPT
// needs to iterate the right side first, the common code always reads
// the right operand first. Meanwhile, we invert the operands for the
// non-EXCEPT cases in order to preserve the appearance of the
// original specified order.
//
// The emit logic for each op is represented by implementors of the
// unionNodeEmit interface. The emitRight method is called for each row output
// by the right side and passed a hashable representation of the row. If it
// returns true, the row is emitted. After all right rows are examined, then
// each left row is passed to emitLeft in the same way.
//
// An example: intersectNodeEmitAll
// VALUES (1), (1), (1), (2), (2) INTERSECT ALL VALUES (1), (3), (1)
// ----
// 1
// 1
// There are three 1s on the left and two 1s on the right, so we emit 1, 1.
// Nothing else is in both.
//  emitRight: For each row, increment the map entry.
//  emitLeft: For each row, if the row is not present in the map, it was not in
//    both, don't emit. Otherwise, if the count for the row was > 0, emit and
//    decrement the entry. Otherwise, the row was on the right, but we've
//    already emitted as many as were on the right, don't emit.
type unionNode struct {
	// right and left are the data source operands.
	// right is read first, to populate the `emit` field.
	right, left planNode
	// columns contains the metadata for the results of this node.
	columns sqlbase.ResultColumns
	// inverted, when true, indicates that the right plan corresponds to
	// the left operand in the input SQL syntax, and vice-versa.
	inverted bool
	// emitAll is a performance optimization for UNION ALL. When set
	// the union logic avoids the `emit` logic entirely.
	emitAll bool
	// emit contains the rows seen on the right so far and performs the
	// selection/filtering logic.
	emit unionNodeEmit

	// unionType is the type of operation (UNION, INTERSECT, EXCEPT)
	unionType tree.UnionType
	// all indicates if the operation is the ALL or DISTINCT version
	all bool

	run unionRun
}

// Union constructs a planNode from a UNION/INTERSECT/EXCEPT expression.
func (p *planner) Union(
	ctx context.Context, n *tree.UnionClause, desiredTypes []types.T,
) (planNode, error) {
	left, err := p.newPlan(ctx, n.Left, desiredTypes)
	if err != nil {
		return nil, err
	}
	right, err := p.newPlan(ctx, n.Right, desiredTypes)
	if err != nil {
		return nil, err
	}

	return p.newUnionNode(n.Type, n.All, left, right)
}

func (p *planner) newUnionNode(
	typ tree.UnionType, all bool, left, right planNode,
) (planNode, error) {
	var emitAll = false
	var emit unionNodeEmit
	switch typ {
	case tree.UnionOp:
		if all {
			emitAll = true
		} else {
			emit = make(unionNodeEmitDistinct)
		}
	case tree.IntersectOp:
		if all {
			emit = make(intersectNodeEmitAll)
		} else {
			emit = make(intersectNodeEmitDistinct)
		}
	case tree.ExceptOp:
		if all {
			emit = make(exceptNodeEmitAll)
		} else {
			emit = make(exceptNodeEmitDistinct)
		}
	default:
		return nil, errors.Errorf("%v is not supported", typ)
	}

	leftColumns := planColumns(left)
	rightColumns := planColumns(right)
	if len(leftColumns) != len(rightColumns) {
		return nil, fmt.Errorf("each %v query must have the same number of columns: %d vs %d",
			typ, len(leftColumns), len(rightColumns))
	}
	unionColumns := append(sqlbase.ResultColumns(nil), leftColumns...)
	for i := 0; i < len(unionColumns); i++ {
		l := leftColumns[i]
		r := rightColumns[i]
		// TODO(dan): This currently checks whether the types are exactly the same,
		// but Postgres is more lenient:
		// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
		if !(l.Typ.Equivalent(r.Typ) || l.Typ == types.Unknown || r.Typ == types.Unknown) {
			return nil, fmt.Errorf("%v types %s and %s cannot be matched", typ, l.Typ, r.Typ)
		}
		if l.Hidden != r.Hidden {
			return nil, fmt.Errorf("%v types cannot be matched", typ)
		}
		if l.Typ == types.Unknown {
			unionColumns[i].Typ = r.Typ
		}
	}

	inverted := false
	if typ != tree.ExceptOp {
		// The logic below reads the rows from the right operand first,
		// because for EXCEPT in particular this is what we need to match.
		// However for the other operators (UNION, INTERSECT) it is
		// actually confusing to see the right values come up first in the
		// results. So invert this here, to reduce surprise by users.
		left, right = right, left
		inverted = true
	}

	node := &unionNode{
		right:     right,
		left:      left,
		columns:   unionColumns,
		inverted:  inverted,
		emitAll:   emitAll,
		emit:      emit,
		unionType: typ,
		all:       all,
	}
	return node, nil
}

// unionRun contains the run-time state of unionNode during local execution.
type unionRun struct {
	// scratch is a preallocated buffer for formatting the key of the
	// current row on the right.
	scratch []byte
}

func (n *unionNode) startExec(params runParams) error {
	n.run.scratch = make([]byte, 0)
	return nil
}

func (n *unionNode) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}
	if n.right != nil {
		return n.readRight(params)
	}
	if n.left != nil {
		return n.readLeft(params)
	}
	return false, nil
}

func (n *unionNode) Values() tree.Datums {
	if n.right != nil {
		return n.right.Values()
	}
	if n.left != nil {
		return n.left.Values()
	}
	return nil
}

func (n *unionNode) Close(ctx context.Context) {
	if n.right != nil {
		n.right.Close(ctx)
		n.right = nil
	}
	if n.left != nil {
		n.left.Close(ctx)
		n.left = nil
	}
}

func (n *unionNode) readRight(params runParams) (bool, error) {
	next, err := n.right.Next(params)
	for ; next; next, err = n.right.Next(params) {
		if n.emitAll {
			return true, nil
		}
		n.run.scratch = n.run.scratch[:0]
		if n.run.scratch, err = sqlbase.EncodeDatums(n.run.scratch, n.right.Values()); err != nil {
			return false, err
		}
		// TODO(dan): Sending the entire encodeDTuple to be stored in the map would
		// use a lot of memory for big rows or big resultsets. Consider using a hash
		// of the bytes instead.
		if n.emit.emitRight(n.run.scratch) {
			return true, nil
		}
	}
	if err != nil {
		return false, err
	}

	n.right.Close(params.ctx)
	n.right = nil
	return n.readLeft(params)
}

func (n *unionNode) readLeft(params runParams) (bool, error) {
	next, err := n.left.Next(params)
	for ; next; next, err = n.left.Next(params) {
		if n.emitAll {
			return true, nil
		}
		n.run.scratch = n.run.scratch[:0]
		if n.run.scratch, err = sqlbase.EncodeDatums(n.run.scratch, n.left.Values()); err != nil {
			return false, err
		}
		if n.emit.emitLeft(n.run.scratch) {
			return true, nil
		}
	}
	if err != nil {
		return false, err
	}
	n.left.Close(params.ctx)
	n.left = nil
	return false, nil
}

// unionNodeEmit represents the emitter logic for one of the six combinations of
// UNION/INTERSECT/EXCEPT and ALL/DISTINCT. As right and then left are iterated,
// state is kept and used to compute the set operation as well as distinctness.
type unionNodeEmit interface {
	emitRight([]byte) bool
	emitLeft([]byte) bool
}

type unionNodeEmitDistinct map[string]int
type intersectNodeEmitAll map[string]int
type intersectNodeEmitDistinct map[string]int
type exceptNodeEmitAll map[string]int
type exceptNodeEmitDistinct map[string]int

// NB: the compiler optimizes out the string allocation in
// `myMap[string(myBytes)]`. See:
// https://github.com/golang/go/commit/f5f5a8b6209f84961687d993b93ea0d397f5d5bf
func (e unionNodeEmitDistinct) emitRight(b []byte) bool {
	_, ok := e[string(b)]
	e[string(b)] = 1
	return !ok
}

func (e unionNodeEmitDistinct) emitLeft(b []byte) bool {
	_, ok := e[string(b)]
	e[string(b)] = 1
	return !ok
}

func (e intersectNodeEmitAll) emitRight(b []byte) bool {
	e[string(b)]++
	return false
}

func (e intersectNodeEmitAll) emitLeft(b []byte) bool {
	if v, ok := e[string(b)]; ok && v > 0 {
		e[string(b)]--
		return true
	}
	return false
}

func (e intersectNodeEmitDistinct) emitRight(b []byte) bool {
	e[string(b)]++
	return false
}

func (e intersectNodeEmitDistinct) emitLeft(b []byte) bool {
	if v, ok := e[string(b)]; ok && v > 0 {
		e[string(b)] = 0
		return true
	}
	return false
}

func (e exceptNodeEmitAll) emitRight(b []byte) bool {
	e[string(b)]++
	return false
}

func (e exceptNodeEmitAll) emitLeft(b []byte) bool {
	if v, ok := e[string(b)]; ok && v > 0 {
		e[string(b)]--
		return false
	}
	return true
}

func (e exceptNodeEmitDistinct) emitRight(b []byte) bool {
	e[string(b)]++
	return false
}

func (e exceptNodeEmitDistinct) emitLeft(b []byte) bool {
	if _, ok := e[string(b)]; !ok {
		e[string(b)] = 0
		return true
	}
	return false
}
