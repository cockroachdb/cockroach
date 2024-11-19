// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecdisk

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	colexecop.ZeroInputNode
	colexecop.InitHelper
	colexecop.NonExplainable

	allocator    *colmem.Allocator
	types        []*types.T
	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ colexecop.ResettableOperator = &partitionerToOperator{}

func (p *partitionerToOperator) Reset(ctx context.Context) {
	// When resetting we simply want to update the context - this will allow us
	// to make Init a no-op and reuse already allocated batch. If Init hasn't
	// been called yet, then Reset is a no-op.
	if p.Ctx != nil {
		p.Ctx = ctx
	}
}

func (p *partitionerToOperator) Init(ctx context.Context) {
	if !p.InitHelper.Init(ctx) {
		return
	}
	// We will be dequeueing the batches from disk into this batch, so we
	// need to have enough capacity to support the batches of any size.
	p.batch = p.allocator.NewMemBatchWithFixedCapacity(p.types, coldata.BatchSize())
}

func (p *partitionerToOperator) Next() coldata.Batch {
	var err error
	// We need to perform the memory accounting on the dequeued batch. Note that
	// such setup allows us to release the memory under the old p.batch (which
	// is no longer valid) and to retain the memory under the just dequeued one.
	p.allocator.PerformOperation(p.batch.ColVecs(), func() {
		err = p.partitioner.Dequeue(p.Ctx, p.partitionIdx, p.batch)
	})
	if err != nil {
		colexecerror.InternalError(err)
	}
	return p.batch
}

func makeOrdering(cols []uint32) []execinfrapb.Ordering_Column {
	res := make([]execinfrapb.Ordering_Column, len(cols))
	for i, colIdx := range cols {
		res[i].ColIdx = colIdx
	}
	return res
}
