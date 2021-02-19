// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var (
	zeroBoolColumn   = make([]bool, coldata.MaxBatchSize)
	zeroIntColumn    = make([]int, coldata.MaxBatchSize)
	zeroUint64Column = make([]uint64, coldata.MaxBatchSize)
)

func newPartitionerToOperator(
	allocator *colmem.Allocator, types []*types.T, partitioner colcontainer.PartitionedQueue,
) *partitionerToOperator {
	return &partitionerToOperator{
		allocator:   allocator,
		types:       types,
		partitioner: partitioner,
	}
}

// partitionerToOperator is an Operator that Dequeue's from the corresponding
// partition on every call to Next. It is a converter from filled in
// PartitionedQueue to Operator.
type partitionerToOperator struct {
	colexecbase.ZeroInputNode
	colexecbase.NonExplainable

	allocator    *colmem.Allocator
	types        []*types.T
	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ colexecbase.Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init() {
	if p.batch == nil {
		// We will be dequeueing the batches from disk into this batch, so we
		// need to have enough capacity to support the batches of any size.
		p.batch = p.allocator.NewMemBatchWithFixedCapacity(p.types, coldata.BatchSize())
	}
}

func (p *partitionerToOperator) Next(ctx context.Context) coldata.Batch {
	var err error
	// We need to perform the memory accounting on the dequeued batch. Note that
	// such setup allows us to release the memory under the old p.batch (which
	// is no longer valid) and to retain the memory under the just dequeued one.
	p.allocator.PerformOperation(p.batch.ColVecs(), func() {
		err = p.partitioner.Dequeue(ctx, p.partitionIdx, p.batch)
	})
	if err != nil {
		colexecerror.InternalError(err)
	}
	return p.batch
}

// maybeAllocate* methods make sure that the passed in array is allocated, of
// the desired length and zeroed out.
func maybeAllocateUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroUint64Column) {
	}
	return array
}

func maybeAllocateBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	for n := 0; n < length; n += copy(array[n:], zeroBoolColumn) {
	}
	return array
}

// maybeAllocateLimitedUint64Array is an optimized version of
// maybeAllocateUint64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func maybeAllocateLimitedUint64Array(array []uint64, length int) []uint64 {
	if cap(array) < length {
		return make([]uint64, length)
	}
	array = array[:length]
	copy(array, zeroUint64Column)
	return array
}

// maybeAllocateLimitedBoolArray is an optimized version of
// maybeAllocateBool64Array that can *only* be used when length is at most
// coldata.MaxBatchSize.
func maybeAllocateLimitedBoolArray(array []bool, length int) []bool {
	if cap(array) < length {
		return make([]bool, length)
	}
	array = array[:length]
	copy(array, zeroBoolColumn)
	return array
}

func makeOrdering(cols []uint32) []execinfrapb.Ordering_Column {
	res := make([]execinfrapb.Ordering_Column, len(cols))
	for i, colIdx := range cols {
		res[i].ColIdx = colIdx
	}
	return res
}
