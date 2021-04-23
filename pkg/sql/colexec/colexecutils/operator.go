// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type zeroOperator struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable
}

var _ colexecop.Operator = &zeroOperator{}

// NewZeroOp creates a new operator which just returns an empty batch.
func NewZeroOp(input colexecop.Operator) colexecop.Operator {
	return &zeroOperator{OneInputHelper: colexecop.MakeOneInputHelper(input)}
}

func (s *zeroOperator) Next() coldata.Batch {
	return coldata.ZeroBatch
}

type fixedNumTuplesNoInputOp struct {
	colexecop.ZeroInputNode
	colexecop.NonExplainable
	batch          coldata.Batch
	numTuplesLeft  int
	opToInitialize colexecop.Operator
}

var _ colexecop.Operator = &fixedNumTuplesNoInputOp{}

// NewFixedNumTuplesNoInputOp creates a new Operator which returns batches with
// no actual columns that have specified number of tuples as the sum of their
// lengths. It takes in an optional colexecop.Operator that will be initialized
// in Init() but is otherwise ignored. This behavior is needed when the returned
// operator replaces a tree of operators which are expected to be initialized.
func NewFixedNumTuplesNoInputOp(
	allocator *colmem.Allocator, numTuples int, opToInitialize colexecop.Operator,
) colexecop.Operator {
	capacity := numTuples
	if capacity > coldata.BatchSize() {
		capacity = coldata.BatchSize()
	}
	return &fixedNumTuplesNoInputOp{
		batch:          allocator.NewMemBatchWithFixedCapacity(nil /* types */, capacity),
		numTuplesLeft:  numTuples,
		opToInitialize: opToInitialize,
	}
}

func (s *fixedNumTuplesNoInputOp) Init(ctx context.Context) {
	if s.opToInitialize != nil {
		s.opToInitialize.Init(ctx)
	}
}

func (s *fixedNumTuplesNoInputOp) Next() coldata.Batch {
	if s.numTuplesLeft == 0 {
		return coldata.ZeroBatch
	}
	s.batch.ResetInternalBatch()
	length := s.numTuplesLeft
	if length > coldata.BatchSize() {
		length = coldata.BatchSize()
	}
	s.numTuplesLeft -= length
	s.batch.SetLength(length)
	return s.batch
}

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
	colexecop.OneInputInitCloserHelper
	colexecop.NonExplainable

	allocator *colmem.Allocator
	typ       *types.T
	idx       int
}

var _ colexecop.ResettableOperator = &vectorTypeEnforcer{}

// NewVectorTypeEnforcer returns a new vectorTypeEnforcer.
func NewVectorTypeEnforcer(
	allocator *colmem.Allocator, input colexecop.Operator, typ *types.T, idx int,
) colexecop.Operator {
	return &vectorTypeEnforcer{
		OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
		allocator:                allocator,
		typ:                      typ,
		idx:                      idx,
	}
}

func (e *vectorTypeEnforcer) Next() coldata.Batch {
	b := e.Input.Next()
	if b.Length() == 0 {
		return b
	}
	e.allocator.MaybeAppendColumn(b, e.typ, e.idx)
	return b
}

func (e *vectorTypeEnforcer) Reset(ctx context.Context) {
	if r, ok := e.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
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
	colexecop.OneInputInitCloserHelper
	colexecop.NonExplainable

	allocator                    *colmem.Allocator
	typs                         []*types.T
	subsetStartIdx, subsetEndIdx int
}

var _ colexecop.Operator = &BatchSchemaSubsetEnforcer{}

// NewBatchSchemaSubsetEnforcer creates a new BatchSchemaSubsetEnforcer.
// - subsetStartIdx and subsetEndIdx define the boundaries of the range of
// columns that the projecting operator and its internal projecting operators
// own.
func NewBatchSchemaSubsetEnforcer(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	typs []*types.T,
	subsetStartIdx, subsetEndIdx int,
) *BatchSchemaSubsetEnforcer {
	return &BatchSchemaSubsetEnforcer{
		OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
		allocator:                allocator,
		typs:                     typs,
		subsetStartIdx:           subsetStartIdx,
		subsetEndIdx:             subsetEndIdx,
	}
}

// Init implements the colexecop.Operator interface.
func (e *BatchSchemaSubsetEnforcer) Init(ctx context.Context) {
	if e.subsetStartIdx >= e.subsetEndIdx {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly subsetStartIdx is not less than subsetEndIdx"))
	}
	e.OneInputInitCloserHelper.Init(ctx)
}

// Next implements the colexecop.Operator interface.
func (e *BatchSchemaSubsetEnforcer) Next() coldata.Batch {
	b := e.Input.Next()
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
