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
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// Union constructs a planNode from a UNION/INTERSECT/EXCEPT expression.
func (p *planner) Union(n *parser.Union, autoCommit bool) (planNode, *roachpb.Error) {
	var emit unionNodeEmit
	// TODO(dan): These constants are available in parser, but unexported. Export them?
	if n.Type == "UNION" {
		if n.All {
			emit = unionNodeEmitAll{}
		} else {
			emit = make(unionNodeEmitDistinct)
		}
	} else if n.Type == "INTERSECT" {
		if n.All {
			emit = make(intersectNodeEmitAll)
		} else {
			emit = make(intersectNodeEmitDistinct)
		}
	} else if n.Type == "EXCEPT" {
		if n.All {
			emit = make(exceptNodeEmitAll)
		} else {
			emit = make(exceptNodeEmitDistinct)
		}
	} else {
		// TODO(dan): This is really an internal error, since the parser shouldn't have accepted it.
		return nil, roachpb.NewErrorf("%s is not supported", n.Type)
	}
	left, err := p.makePlan(n.Left, autoCommit)
	if err != nil {
		return nil, err
	}
	right, err := p.makePlan(n.Right, autoCommit)
	if err != nil {
		return nil, err
	}
	leftColumns := left.Columns()
	rightColumns := right.Columns()
	if len(leftColumns) != len(rightColumns) {
		return nil, roachpb.NewErrorf("each UNION query must have the same number of columns: %d vs %d", len(left.Columns()), len(right.Columns()))
	}
	columns := make([]ResultColumn, len(leftColumns))
	for i := 0; i < len(leftColumns); i++ {
		l := leftColumns[i]
		r := rightColumns[i]
		// TODO(dan): How does r.Hidden play into this?
		// TODO(dan): This currently checks whether the types are exactly the same, but SQL is more
		// lenient: http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
		if l.Typ.Compare(r.Typ) != 0 {
			return nil, roachpb.NewErrorf("UNION types %s and %s cannot be matched", l.Typ.String(), r.Typ.String())
		}
		columns[i] = l
	}

	node := &unionNode{
		columns: columns,
		err:     nil,
		left:    left,
		right:   right,
		state:   unionNodeRight,
		emit:    emit,
	}

	// TODO(dan): Support ORDER BY. It's currently blocked on
	// https://github.com/cockroachdb/cockroach/issues/4734. Once that's done, it should be possible
	// to read all the values from node, put them in a valuesNode, and sort it.
	return node, nil
}

// unionNodeState represents which SELECT we're reading from.
type unionNodeState int

const (
	unionNodeRight unionNodeState = iota
	unionNodeLeft
	unionNodeDone
)

// unionNode is a planNode whose rows are the result of one of three set operations (UNION,
// INTERSECT, or EXCEPT) on left and right. There are two variations of each set operation:
// distinct, which always returns unique results, and all, which does no uniqueing. Ordering of rows
// is unavailable.
type unionNode struct {
	columns     []ResultColumn
	err         *roachpb.Error
	left, right planNode
	state       unionNodeState
	emit        unionNodeEmit
}

func (n *unionNode) Columns() []ResultColumn { return n.columns }
func (n *unionNode) Ordering() orderingInfo  { return orderingInfo{} }
func (n *unionNode) PErr() *roachpb.Error    { return n.err }
func (n *unionNode) Values() parser.DTuple {
	if n.state == unionNodeLeft {
		return n.left.Values()
	} else if n.state == unionNodeRight {
		return n.right.Values()
	}
	return nil
}

func (n *unionNode) ExplainPlan() (name, description string, children []planNode) {
	// TODO(dan): Look at some other descriptions and see what this is.
	return "union", "-", []planNode{n.left, n.right}
}

func (*unionNode) DebugValues() debugValues {
	// TODO(dan): Look at some other debugValues and see what this is.
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}

func (n *unionNode) readRight() bool {
	hasNext := n.right.Next()
	if !hasNext {
		n.state = unionNodeLeft
		return n.readLeft()
	}
	if n.emit.emitRight(n.right.Values()) {
		return true
	}
	return n.readRight()
}

func (n *unionNode) readLeft() bool {
	hasNext := n.left.Next()
	if !hasNext {
		n.state = unionNodeDone
		return hasNext
	}
	if n.emit.emitLeft(n.left.Values()) {
		return true
	}
	return n.readLeft()
}

func (n *unionNode) Next() bool {
	// Because EXCEPT needs to read right first, and the other two don't care, we always
	// read right before left.
	if n.state == unionNodeRight {
		return n.readRight()
	} else if n.state == unionNodeLeft {
		return n.readLeft()
	} else {
		return false
	}
}

// unionNodeEmit represents the emitter logic for one of the six combinations of
// UNION/INTERSECT/EXCEPT and ALL/DISTINCE. As right and then left are iterated, state is kept and
// used to compute the set operation as well as distinctness.
type unionNodeEmit interface {
	emitRight(parser.DTuple) bool
	emitLeft(parser.DTuple) bool
}

type unionNodeEmitMap map[string]int

func (e unionNodeEmitMap) incEmitIfUnseen(d parser.DTuple) bool {
	var scratch []byte
	b, err := encodeDTuple(scratch, d)
	if err != nil {
		return false // TODO(dan): Log.
	}
	_, ok := e[string(b)]
	e[string(b)] = 1
	return !ok
}

func (e unionNodeEmitMap) incNoEmit(d parser.DTuple) bool {
	var scratch []byte
	b, err := encodeDTuple(scratch, d)
	if err != nil {
		return false // TODO(dan): Log.
	}
	e[string(b)] += 1
	return false
}

// TODO(dan): The implementations of the following are extremely repetitive. The unionNodeEmitMap
// abstraction helps a bit, but it's not clear if it's worth the extra complexity. This could all be
// inlined in the implementations of readLeft and readRight above, which would make the code easier
// to follow, but the logic harder to reason about. Though, a good comment could probably address
// that enough to make it the better option. Reviewers, thoughts?
type unionNodeEmitAll struct{}
type unionNodeEmitDistinct unionNodeEmitMap
type intersectNodeEmitAll unionNodeEmitMap
type intersectNodeEmitDistinct unionNodeEmitMap
type exceptNodeEmitAll unionNodeEmitMap
type exceptNodeEmitDistinct unionNodeEmitMap

func (e unionNodeEmitAll) emitRight(d parser.DTuple) bool {
	return true
}

func (e unionNodeEmitAll) emitLeft(d parser.DTuple) bool {
	return true
}

func (e unionNodeEmitDistinct) emitRight(d parser.DTuple) bool {
	return unionNodeEmitMap(e).incEmitIfUnseen(d)
}
func (e unionNodeEmitDistinct) emitLeft(d parser.DTuple) bool {
	return unionNodeEmitMap(e).incEmitIfUnseen(d)
}

func (e intersectNodeEmitAll) emitRight(d parser.DTuple) bool {
	return unionNodeEmitMap(e).incNoEmit(d)
}
func (e intersectNodeEmitAll) emitLeft(d parser.DTuple) bool {
	var scratch []byte
	b, err := encodeDTuple(scratch, d)
	if err != nil {
		return false // TODO(dan): Log.
	}
	if v, ok := e[string(b)]; ok && v > 0 {
		e[string(b)] -= 1
		return true
	}
	return false
}

func (e intersectNodeEmitDistinct) emitRight(d parser.DTuple) bool {
	return unionNodeEmitMap(e).incNoEmit(d)
}
func (e intersectNodeEmitDistinct) emitLeft(d parser.DTuple) bool {
	var scratch []byte
	b, err := encodeDTuple(scratch, d)
	if err != nil {
		return false // TODO(dan): Log.
	}
	if v, ok := e[string(b)]; ok && v > 0 {
		e[string(b)] = 0
		return true
	}
	return false
}

func (e exceptNodeEmitAll) emitRight(d parser.DTuple) bool {
	return unionNodeEmitMap(e).incNoEmit(d)
}
func (e exceptNodeEmitAll) emitLeft(d parser.DTuple) bool {
	var scratch []byte
	b, err := encodeDTuple(scratch, d)
	if err != nil {
		return false // TODO(dan): Log.
	}
	if v, ok := e[string(b)]; ok && v > 0 {
		e[string(b)] -= 1
		return false
	}
	return true
}

func (e exceptNodeEmitDistinct) emitRight(d parser.DTuple) bool {
	return unionNodeEmitMap(e).incNoEmit(d)
}
func (e exceptNodeEmitDistinct) emitLeft(d parser.DTuple) bool {
	var scratch []byte
	b, err := encodeDTuple(scratch, d)
	if err != nil {
		return false // TODO(dan): Log.
	}
	if v, ok := e[string(b)]; !ok || v != 0 {
		e[string(b)] = 0
		return true
	}
	return false
}
