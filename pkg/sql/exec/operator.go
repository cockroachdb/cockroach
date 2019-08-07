// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
)

// Operator is a column vector operator that produces a Batch as output.
type Operator interface {
	// Init initializes this operator. Will be called once at operator setup
	// time. If an operator has an input operator, it's responsible for calling
	// Init on that input operator as well.
	Init()

	// Next returns the next Batch from this operator. Once the operator is
	// finished, it will return a Batch with length 0. Subsequent calls to
	// Next at that point will always return a Batch with length 0.
	//
	// Calling Next may invalidate the contents of the last Batch returned by
	// Next.
	// Canceling the provided context results in forceful termination of
	// execution.
	Next(context.Context) coldata.Batch

	OpNode
}

// OpNode is an interface to operator-like structures with children.
type OpNode interface {
	// ChildCount returns the number of children (inputs) of the operator.
	ChildCount() int

	// Child returns the nth child (input) of the operator.
	Child(nth int) OpNode
}

// NewOneInputNode returns an OpNode with a single Operator input.
func NewOneInputNode(input Operator) OneInputNode {
	return OneInputNode{input: input}
}

// OneInputNode is an OpNode with a single Operator input.
type OneInputNode struct {
	input Operator
}

// ChildCount implements the OpNode interface.
func (OneInputNode) ChildCount() int {
	return 1
}

// Child implements the OpNode interface.
func (n OneInputNode) Child(nth int) OpNode {
	if nth == 0 {
		return n.input
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}

// Input returns the single input of this OneInputNode as an Operator.
func (n OneInputNode) Input() Operator {
	return n.input
}

// ZeroInputNode is an OpNode with no inputs.
type ZeroInputNode struct{}

// ChildCount implements the OpNode interface.
func (ZeroInputNode) ChildCount() int {
	return 0
}

// Child implements the OpNode interface.
func (ZeroInputNode) Child(nth int) OpNode {
	panic(fmt.Sprintf("invalid index %d", nth))
}

// newTwoInputNode returns an OpNode with two Operator inputs.
func newTwoInputNode(inputOne, inputTwo Operator) twoInputNode {
	return twoInputNode{inputOne: inputOne, inputTwo: inputTwo}
}

type twoInputNode struct {
	inputOne Operator
	inputTwo Operator
}

func (twoInputNode) ChildCount() int {
	return 2
}

func (n *twoInputNode) Child(nth int) OpNode {
	switch nth {
	case 0:
		return n.inputOne
	case 1:
		return n.inputTwo
	}
	panic(fmt.Sprintf("invalid idx %d", nth))
}

// StaticMemoryOperator is an interface that streaming operators can implement
// if they are able to declare their memory usage upfront.
type StaticMemoryOperator interface {
	Operator
	// EstimateStaticMemoryUsage estimates the memory usage (in bytes)
	// of an operator.
	EstimateStaticMemoryUsage() int
}

// resetter is an interface that operators can implement if they can be reset
// either for reusing (to keep the already allocated memory) or during tests.
type resetter interface {
	reset()
}

// resettableOperator is an Operator that can be reset.
type resettableOperator interface {
	Operator
	resetter
}

type noopOperator struct {
	OneInputNode
}

var _ Operator = &noopOperator{}

// NewNoop returns a new noop Operator.
func NewNoop(input Operator) Operator {
	return &noopOperator{OneInputNode: NewOneInputNode(input)}
}

func (n *noopOperator) Init() {
	n.input.Init()
}

func (n *noopOperator) Next(ctx context.Context) coldata.Batch {
	return n.input.Next(ctx)
}

func (n *noopOperator) reset() {
	if r, ok := n.input.(resetter); ok {
		r.reset()
	}
}

type zeroOperator struct {
	OneInputNode
}

var _ Operator = &zeroOperator{}

// NewZeroOp creates a new operator which just returns an empty batch.
func NewZeroOp(input Operator) Operator {
	return &zeroOperator{OneInputNode: NewOneInputNode(input)}
}

func (s *zeroOperator) Init() {
	s.input.Init()
}

func (s *zeroOperator) Next(ctx context.Context) coldata.Batch {
	// TODO(solon): Can we avoid calling Next on the input at all?
	next := s.input.Next(ctx)
	next.SetLength(0)
	return next
}

type singleTupleNoInputOperator struct {
	ZeroInputNode
	batch  coldata.Batch
	nexted bool
}

var _ Operator = &singleTupleNoInputOperator{}

// NewSingleTupleNoInputOp creates a new Operator which returns a batch of
// length 1 with no actual columns on the first call to Next() and zero-length
// batches on all consecutive calls.
func NewSingleTupleNoInputOp() Operator {
	return &singleTupleNoInputOperator{
		batch: coldata.NewMemBatchWithSize(nil, 1),
	}
}

func (s *singleTupleNoInputOperator) Init() {
}

func (s *singleTupleNoInputOperator) Next(ctx context.Context) coldata.Batch {
	s.batch.SetSelection(false)
	if s.nexted {
		s.batch.SetLength(0)
		return s.batch
	}
	s.nexted = true
	s.batch.SetLength(1)
	return s.batch
}
