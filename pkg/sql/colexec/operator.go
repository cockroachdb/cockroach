// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
)

// Operator is a column vector operator that produces a Batch as output.
type Operator interface {
	// Init initializes this operator. Will be called once at operator setup
	// time. If an operator has an input operator, it's responsible for calling
	// Init on that input operator as well.
	// TODO(yuzefovich): we might need to clarify whether it is ok to call
	// Init() multiple times before the first call to Next(). It is possible to
	// hit the memory limit during Init(), and a disk-backed operator needs to
	// make sure that the input has been initialized. We could also in case that
	// Init() doesn't succeed for bufferingInMemoryOperator - which should only
	// happen when 'workmem' setting is too low - just bail, even if we have
	// disk spilling for that operator.
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

	execinfra.OpNode
}

// NonExplainable is a marker interface which identifies an Operator that
// should be omitted from the output of EXPLAIN (VEC). Note that VERBOSE
// explain option will override the omitting behavior.
type NonExplainable interface {
	// nonExplainableMarker is just a marker method. It should never be called.
	nonExplainableMarker()
}

// NewOneInputNode returns an execinfra.OpNode with a single Operator input.
func NewOneInputNode(input Operator) OneInputNode {
	return OneInputNode{input: input}
}

// OneInputNode is an execinfra.OpNode with a single Operator input.
type OneInputNode struct {
	input Operator
}

// ChildCount implements the execinfra.OpNode interface.
func (OneInputNode) ChildCount(verbose bool) int {
	return 1
}

// Child implements the execinfra.OpNode interface.
func (n OneInputNode) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return n.input
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Input returns the single input of this OneInputNode as an Operator.
func (n OneInputNode) Input() Operator {
	return n.input
}

// ZeroInputNode is an execinfra.OpNode with no inputs.
type ZeroInputNode struct{}

// ChildCount implements the execinfra.OpNode interface.
func (ZeroInputNode) ChildCount(verbose bool) int {
	return 0
}

// Child implements the execinfra.OpNode interface.
func (ZeroInputNode) Child(nth int, verbose bool) execinfra.OpNode {
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// newTwoInputNode returns an execinfra.OpNode with two Operator inputs.
func newTwoInputNode(inputOne, inputTwo Operator) twoInputNode {
	return twoInputNode{inputOne: inputOne, inputTwo: inputTwo}
}

type twoInputNode struct {
	inputOne Operator
	inputTwo Operator
}

func (twoInputNode) ChildCount(verbose bool) int {
	return 2
}

func (n *twoInputNode) Child(nth int, verbose bool) execinfra.OpNode {
	switch nth {
	case 0:
		return n.inputOne
	case 1:
		return n.inputTwo
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// TODO(yuzefovich): audit all Operators to make sure that all internal memory
// is accounted for.

// InternalMemoryOperator is an interface that operators which use internal
// memory need to implement. "Internal memory" is defined as memory that is
// "private" to the operator and is not exposed to the outside; notably, it
// does *not* include any coldata.Batch'es and coldata.Vec's.
type InternalMemoryOperator interface {
	Operator
	// InternalMemoryUsage reports the internal memory usage (in bytes) of an
	// operator.
	InternalMemoryUsage() int
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
	NonExplainable
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
	NonExplainable
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
	NonExplainable
	batch  coldata.Batch
	nexted bool
}

var _ Operator = &singleTupleNoInputOperator{}

// NewSingleTupleNoInputOp creates a new Operator which returns a batch of
// length 1 with no actual columns on the first call to Next() and zero-length
// batches on all consecutive calls.
func NewSingleTupleNoInputOp(allocator *Allocator) Operator {
	return &singleTupleNoInputOperator{
		batch: allocator.NewMemBatchWithSize(nil /* types */, 1 /* size */),
	}
}

func (s *singleTupleNoInputOperator) Init() {
}

func (s *singleTupleNoInputOperator) Next(ctx context.Context) coldata.Batch {
	s.batch.ResetInternalBatch()
	if s.nexted {
		s.batch.SetLength(0)
		return s.batch
	}
	s.nexted = true
	s.batch.SetLength(1)
	return s.batch
}

// feedOperator is used to feed an Operator chain with input by manually
// setting the next batch.
type feedOperator struct {
	ZeroInputNode
	NonExplainable
	batch coldata.Batch
}

func (feedOperator) Init() {}

func (o *feedOperator) Next(context.Context) coldata.Batch {
	return o.batch
}

var _ Operator = &feedOperator{}
