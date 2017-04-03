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
//
// Author: Dan Harrison (daniel.harrison@gmail.com)

package sql

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// UnionClause constructs a planNode from a UNION/INTERSECT/EXCEPT expression.
func (p *planner) UnionClause(
	ctx context.Context, n *parser.UnionClause, desiredTypes []parser.Type, autoCommit bool,
) (planNode, error) {
	var emitAll = false
	var emit unionNodeEmit
	switch n.Type {
	case parser.UnionOp:
		if n.All {
			emitAll = true
		} else {
			emit = make(unionNodeEmitDistinct)
		}
	case parser.IntersectOp:
		if n.All {
			emit = make(intersectNodeEmitAll)
		} else {
			emit = make(intersectNodeEmitDistinct)
		}
	case parser.ExceptOp:
		if n.All {
			emit = make(exceptNodeEmitAll)
		} else {
			emit = make(exceptNodeEmitDistinct)
		}
	default:
		return nil, errors.Errorf("%v is not supported", n.Type)
	}

	left, err := p.newPlan(ctx, n.Left, desiredTypes, autoCommit)
	if err != nil {
		return nil, err
	}
	right, err := p.newPlan(ctx, n.Right, desiredTypes, autoCommit)
	if err != nil {
		return nil, err
	}

	leftColumns := left.Columns()
	rightColumns := right.Columns()
	if len(leftColumns) != len(rightColumns) {
		return nil, fmt.Errorf("each %v query must have the same number of columns: %d vs %d", n.Type, len(left.Columns()), len(right.Columns()))
	}
	for i := 0; i < len(leftColumns); i++ {
		l := leftColumns[i]
		r := rightColumns[i]
		// TODO(dan): This currently checks whether the types are exactly the same,
		// but Postgres is more lenient:
		// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
		if !l.Typ.Equivalent(r.Typ) {
			return nil, fmt.Errorf("%v types %s and %s cannot be matched", n.Type, l.Typ, r.Typ)
		}
		if l.hidden != r.hidden {
			return nil, fmt.Errorf("%v types cannot be matched", n.Type)
		}
	}

	node := &unionNode{
		right:     right,
		left:      left,
		rightDone: false,
		leftDone:  false,
		emitAll:   emitAll,
		emit:      emit,
		scratch:   make([]byte, 0),
	}
	return node, nil
}

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
// All six of the operations can be completed without cacheing rows by iterating
// one side then the other and keeping counts of unique rows in a map. Because
// EXCEPT needs to iterate the right side first, and the other two don't care,
// we always read right before left.
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
	right, left planNode
	rightDone   bool
	leftDone    bool
	emitAll     bool // emitAll is a performance optimization for UNION ALL.
	emit        unionNodeEmit
	scratch     []byte
	explain     explainMode
	debugVals   debugValues
}

func (n *unionNode) Columns() ResultColumns { return n.left.Columns() }
func (n *unionNode) Ordering() orderingInfo { return orderingInfo{} }

func (n *unionNode) Spans(ctx context.Context) (reads, writes roachpb.Spans, err error) {
	leftReads, leftWrites, err := n.left.Spans(ctx)
	if err != nil {
		return nil, nil, err
	}
	rightReads, rightWrites, err := n.right.Spans(ctx)
	if err != nil {
		return nil, nil, err
	}
	return append(leftReads, rightReads...), append(leftWrites, rightWrites...), nil
}

func (n *unionNode) Values() parser.Datums {
	switch {
	case !n.rightDone:
		return n.right.Values()
	case !n.leftDone:
		return n.left.Values()
	default:
		return nil
	}
}

func (n *unionNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.left.MarkDebug(mode)
	n.right.MarkDebug(mode)
}

func (n *unionNode) DebugValues() debugValues {
	return n.debugVals
}

func (n *unionNode) readRight(ctx context.Context) (bool, error) {
	next, err := n.right.Next(ctx)
	for ; next; next, err = n.right.Next(ctx) {
		if n.explain == explainDebug {
			n.debugVals = n.right.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Pass through any non-row debug info.
				return true, nil
			}
		}

		if n.emitAll {
			return true, nil
		}
		n.scratch = n.scratch[:0]
		if n.scratch, err = sqlbase.EncodeDatums(n.scratch, n.right.Values()); err != nil {
			return false, err
		}
		// TODO(dan): Sending the entire encodeDTuple to be stored in the map would
		// use a lot of memory for big rows or big resultsets. Consider using a hash
		// of the bytes instead.
		if n.emit.emitRight(n.scratch) {
			return true, nil
		}
		if n.explain == explainDebug {
			// Mark the row as filtered out.
			n.debugVals.output = debugValueFiltered
			return true, nil
		}
	}
	if err != nil {
		return false, err
	}

	n.rightDone = true
	n.right.Close(ctx)
	return n.readLeft(ctx)
}

func (n *unionNode) readLeft(ctx context.Context) (bool, error) {
	next, err := n.left.Next(ctx)
	for ; next; next, err = n.left.Next(ctx) {
		if n.explain == explainDebug {
			n.debugVals = n.left.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Pass through any non-row debug info.
				return true, nil
			}
		}

		if n.emitAll {
			return true, nil
		}
		n.scratch = n.scratch[:0]
		if n.scratch, err = sqlbase.EncodeDatums(n.scratch, n.left.Values()); err != nil {
			return false, err
		}
		if n.emit.emitLeft(n.scratch) {
			return true, nil
		}
		if n.explain == explainDebug {
			// Mark the row as filtered out.
			n.debugVals.output = debugValueFiltered
			return true, nil
		}
	}
	if err != nil {
		return false, err
	}
	n.leftDone = true
	n.left.Close(ctx)
	return false, nil
}

func (n *unionNode) Start(ctx context.Context) error {
	if err := n.right.Start(ctx); err != nil {
		return err
	}
	return n.left.Start(ctx)
}

func (n *unionNode) Next(ctx context.Context) (bool, error) {
	switch {
	case !n.rightDone:
		return n.readRight(ctx)
	case !n.leftDone:
		return n.readLeft(ctx)
	default:
		return false, nil
	}
}

func (n *unionNode) Close(ctx context.Context) {
	switch {
	case !n.rightDone:
		n.right.Close(ctx)
	case !n.leftDone:
		n.left.Close(ctx)
	}
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
