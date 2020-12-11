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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// OperatorInitStatus indicates whether Init method has already been called on
// an Operator.
type OperatorInitStatus int

const (
	// OperatorNotInitialized indicates that Init has not been called yet.
	OperatorNotInitialized OperatorInitStatus = iota
	// OperatorInitialized indicates that Init has already been called.
	OperatorInitialized
)

// NonExplainable is a marker interface which identifies an Operator that
// should be omitted from the output of EXPLAIN (VEC). Note that VERBOSE
// explain option will override the omitting behavior.
type NonExplainable interface {
	// nonExplainableMarker is just a marker method. It should never be called.
	nonExplainableMarker()
}

// NewOneInputNode returns an execinfra.OpNode with a single Operator input.
func NewOneInputNode(input colexecbase.Operator) OneInputNode {
	return OneInputNode{input: input}
}

// OneInputNode is an execinfra.OpNode with a single Operator input.
type OneInputNode struct {
	input colexecbase.Operator
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
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// Input returns the single input of this OneInputNode as an Operator.
func (n OneInputNode) Input() colexecbase.Operator {
	return n.input
}

// newTwoInputNode returns an execinfra.OpNode with two Operator inputs.
func newTwoInputNode(inputOne, inputTwo colexecbase.Operator) twoInputNode {
	return twoInputNode{inputOne: inputOne, inputTwo: inputTwo}
}

type twoInputNode struct {
	inputOne colexecbase.Operator
	inputTwo colexecbase.Operator
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
	colexecerror.InternalError(errors.AssertionFailedf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// resetter is an interface that operators can implement if they can be reset
// either for reusing (to keep the already allocated memory) or during tests.
type resetter interface {
	reset(ctx context.Context)
}

// ResettableOperator is an Operator that can be reset.
type ResettableOperator interface {
	colexecbase.Operator
	resetter
}

// isOperatorChainResettable traverses the whole operator tree rooted at op and
// returns true if all nodes are resetters.
func isOperatorChainResettable(op execinfra.OpNode) bool {
	if _, resettable := op.(resetter); !resettable {
		return false
	}
	for i := 0; i < op.ChildCount(true /* verbose */); i++ {
		if !isOperatorChainResettable(op.Child(i, true /* verbose */)) {
			return false
		}
	}
	return true
}

// CallbackCloser is a utility struct that implements the Closer interface by
// calling a provided callback.
type CallbackCloser struct {
	CloseCb func(context.Context) error
}

var _ colexecbase.Closer = &CallbackCloser{}

// Close implements the Closer interface.
func (c *CallbackCloser) Close(ctx context.Context) error {
	return c.CloseCb(ctx)
}

// closerHelper is a simple helper that helps Operators implement
// Closer. If close returns true, resources may be released, if it
// returns false, close has already been called.
// use.
type closerHelper struct {
	closed bool
}

// close marks the closerHelper as closed. If true is returned, this is the
// first call to close.
func (c *closerHelper) close() bool {
	if c.closed {
		return false
	}
	c.closed = true
	return true
}

type closableOperator interface {
	colexecbase.Operator
	colexecbase.Closer
}

func makeOneInputCloserHelper(input colexecbase.Operator) oneInputCloserHelper {
	return oneInputCloserHelper{
		OneInputNode: NewOneInputNode(input),
	}
}

type oneInputCloserHelper struct {
	OneInputNode
	closerHelper
}

var _ colexecbase.Closer = &oneInputCloserHelper{}

func (c *oneInputCloserHelper) Close(ctx context.Context) error {
	if !c.close() {
		return nil
	}
	if closer, ok := c.input.(colexecbase.Closer); ok {
		return closer.Close(ctx)
	}
	return nil
}

type noopOperator struct {
	oneInputCloserHelper
	NonExplainable
}

var _ colexecbase.Operator = &noopOperator{}

// NewNoop returns a new noop Operator.
func NewNoop(input colexecbase.Operator) ResettableOperator {
	return &noopOperator{oneInputCloserHelper: makeOneInputCloserHelper(input)}
}

func (n *noopOperator) Init() {
	n.input.Init()
}

func (n *noopOperator) Next(ctx context.Context) coldata.Batch {
	return n.input.Next(ctx)
}

func (n *noopOperator) reset(ctx context.Context) {
	if r, ok := n.input.(resetter); ok {
		r.reset(ctx)
	}
}

type zeroOperator struct {
	OneInputNode
	NonExplainable
}

var _ colexecbase.Operator = &zeroOperator{}

// NewZeroOp creates a new operator which just returns an empty batch.
func NewZeroOp(input colexecbase.Operator) colexecbase.Operator {
	return &zeroOperator{OneInputNode: NewOneInputNode(input)}
}

func (s *zeroOperator) Init() {
	s.input.Init()
}

func (s *zeroOperator) Next(ctx context.Context) coldata.Batch {
	return coldata.ZeroBatch
}

type fixedNumTuplesNoInputOp struct {
	colexecbase.ZeroInputNode
	NonExplainable
	batch         coldata.Batch
	numTuplesLeft int
}

var _ colexecbase.Operator = &fixedNumTuplesNoInputOp{}

// NewFixedNumTuplesNoInputOp creates a new Operator which returns batches with
// no actual columns that have specified number of tuples as the sum of their
// lengths.
func NewFixedNumTuplesNoInputOp(allocator *colmem.Allocator, numTuples int) colexecbase.Operator {
	capacity := numTuples
	if capacity > coldata.BatchSize() {
		capacity = coldata.BatchSize()
	}
	return &fixedNumTuplesNoInputOp{
		batch:         allocator.NewMemBatchWithFixedCapacity(nil /* types */, capacity),
		numTuplesLeft: numTuples,
	}
}

func (s *fixedNumTuplesNoInputOp) Init() {
}

func (s *fixedNumTuplesNoInputOp) Next(context.Context) coldata.Batch {
	s.batch.ResetInternalBatch()
	if s.numTuplesLeft == 0 {
		return coldata.ZeroBatch
	}
	length := s.numTuplesLeft
	if length > coldata.BatchSize() {
		length = coldata.BatchSize()
	}
	s.numTuplesLeft -= length
	s.batch.SetLength(length)
	return s.batch
}

// FeedOperator is used to feed an Operator chain with input by manually
// setting the next batch.
type FeedOperator struct {
	colexecbase.ZeroInputNode
	NonExplainable
	batch coldata.Batch
}

// NewFeedOperator returns a new feed operator.
func NewFeedOperator() *FeedOperator {
	return &FeedOperator{}
}

// Init implements the colexecbase.Operator interface.
func (FeedOperator) Init() {}

// Next implements the colexecbase.Operator interface.
func (o *FeedOperator) Next(context.Context) coldata.Batch {
	return o.batch
}

var _ colexecbase.Operator = &FeedOperator{}

// vectorTypeEnforcer is a utility Operator that on every call to Next
// enforces that non-zero length batch from the input has a vector of the
// desired type in the desired position. If the width of the batch is less than
// the desired position, a new vector will be appended; if the batch has a
// well-typed vector of an undesired type in the desired position, an error
// will occur.
//
// This Operator is designed to be planned as a wrapper on the input to a
// "projecting" Operator (such Operator that has a single column as its output
// and does not touch other columns by simply passing them along).
//
// The intended diagram is as follows:
//
//       original input                (with schema [t1, ..., tN])
//       --------------
//             |
//             ↓
//     vectorTypeEnforcer              (will enforce that tN+1 = outputType)
//     ------------------
//             |
//             ↓
//   "projecting" operator             (projects its output of type outputType
//   ---------------------              in column at position of N+1)
//
type vectorTypeEnforcer struct {
	oneInputCloserHelper
	NonExplainable

	allocator *colmem.Allocator
	typ       *types.T
	idx       int
}

var _ ResettableOperator = &vectorTypeEnforcer{}

func newVectorTypeEnforcer(
	allocator *colmem.Allocator, input colexecbase.Operator, typ *types.T, idx int,
) colexecbase.Operator {
	return &vectorTypeEnforcer{
		oneInputCloserHelper: makeOneInputCloserHelper(input),
		allocator:            allocator,
		typ:                  typ,
		idx:                  idx,
	}
}

func (e *vectorTypeEnforcer) Init() {
	e.input.Init()
}

func (e *vectorTypeEnforcer) Next(ctx context.Context) coldata.Batch {
	b := e.input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	e.allocator.MaybeAppendColumn(b, e.typ, e.idx)
	return b
}

func (e *vectorTypeEnforcer) reset(ctx context.Context) {
	if r, ok := e.input.(resetter); ok {
		r.reset(ctx)
	}
}

// BatchSchemaSubsetEnforcer is similar to vectorTypeEnforcer in its purpose,
// but it enforces that the subset of the columns of the non-zero length batch
// satisfies the desired schema. It needs to wrap the input to a "projecting"
// operator that internally uses other "projecting" operators (for example,
// caseOp and logical projection operators). This operator supports type
// schemas with unsupported types in which case in the corresponding
// position an "unknown" vector can be appended.
//
// The word "subset" is actually more like a "range", but we chose the former
// since the latter is overloaded.
//
// NOTE: the type schema passed into BatchSchemaSubsetEnforcer *must* include
// the output type of the Operator that the enforcer will be the input to.
type BatchSchemaSubsetEnforcer struct {
	oneInputCloserHelper
	NonExplainable

	allocator                    *colmem.Allocator
	typs                         []*types.T
	subsetStartIdx, subsetEndIdx int
}

var _ colexecbase.Operator = &BatchSchemaSubsetEnforcer{}

// NewBatchSchemaSubsetEnforcer creates a new BatchSchemaSubsetEnforcer.
// - subsetStartIdx and subsetEndIdx define the boundaries of the range of
// columns that the projecting operator and its internal projecting operators
// own.
func NewBatchSchemaSubsetEnforcer(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	typs []*types.T,
	subsetStartIdx, subsetEndIdx int,
) *BatchSchemaSubsetEnforcer {
	return &BatchSchemaSubsetEnforcer{
		oneInputCloserHelper: makeOneInputCloserHelper(input),
		allocator:            allocator,
		typs:                 typs,
		subsetStartIdx:       subsetStartIdx,
		subsetEndIdx:         subsetEndIdx,
	}
}

// Init implements the colexecbase.Operator interface.
func (e *BatchSchemaSubsetEnforcer) Init() {
	e.input.Init()
	if e.subsetStartIdx >= e.subsetEndIdx {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly subsetStartIdx is not less than subsetEndIdx"))
	}
}

// Next implements the colexecbase.Operator interface.
func (e *BatchSchemaSubsetEnforcer) Next(ctx context.Context) coldata.Batch {
	b := e.input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	for i := e.subsetStartIdx; i < e.subsetEndIdx; i++ {
		e.allocator.MaybeAppendColumn(b, e.typs[i], i)
	}
	return b
}

// SetTypes sets the types of this schema subset enforcer, and sets the end
// of the range of enforced columns to the length of the input types.
func (e *BatchSchemaSubsetEnforcer) SetTypes(typs []*types.T) {
	e.typs = typs
	e.subsetEndIdx = len(typs)
}
