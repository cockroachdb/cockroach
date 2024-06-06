// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		partitioner: partitioner,
		batch:       allocator.NewMemBatchWithFixedCapacity(types, coldata.BatchSize()),
	}
}

// partitionerToOperator is an Operator that Dequeue's from the corresponding
// partition on every call to Next. It is a converter from filled in
// PartitionedQueue to Operator.
//
// ctx field must be set explicitly by the user before this operator is being
// used.
type partitionerToOperator struct {
	colexecop.ZeroInputNode
	colexecop.NonExplainable

	ctx          context.Context
	allocator    *colmem.Allocator
	partitioner  colcontainer.PartitionedQueue
	partitionIdx int
	batch        coldata.Batch
}

var _ colexecop.Operator = &partitionerToOperator{}

func (p *partitionerToOperator) Init(context.Context) {
	// This method is deliberately a no-op since this operator is reused
	// multiple times between different partitions by the caller - the caller is
	// responsible for setting ctx correctly.
}

func (p *partitionerToOperator) Next() coldata.Batch {
	var err error
	// We need to perform the memory accounting on the dequeued batch. Note that
	// such setup allows us to release the memory under the old p.batch (which
	// is no longer valid) and to retain the memory under the just dequeued one.
	p.allocator.PerformOperation(p.batch.ColVecs(), func() {
		err = p.partitioner.Dequeue(p.ctx, p.partitionIdx, p.batch)
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
