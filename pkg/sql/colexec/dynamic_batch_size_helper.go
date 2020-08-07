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
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewDynamicBatchSizeHelper returns a new DynamicBatchSizeHelper.
func NewDynamicBatchSizeHelper(
	allocator *colmem.Allocator, typs []*types.T,
) *DynamicBatchSizeHelper {
	return &DynamicBatchSizeHelper{
		allocator: allocator,
		typs:      typs,
	}
}

// DynamicBatchSizeHelper is a utility struct that helps operators work with
// batches of dynamic size.
type DynamicBatchSizeHelper struct {
	allocator *colmem.Allocator
	typs      []*types.T
}

// ResetMaybeReallocate returns a batch that is guaranteed to be in a "reset"
// state and to have the capacity of at least minCapacity. The method will
// grow the allocated capacity of the batch exponentially, until the batch
// reaches coldata.BatchSize().
func (d *DynamicBatchSizeHelper) ResetMaybeReallocate(
	batch coldata.Batch, minCapacity int,
) (_ coldata.Batch, reallocated bool) {
	reallocated = true
	if batch == nil {
		batch = d.allocator.NewMemBatchWithFixedCapacity(d.typs, minCapacity)
	} else if batch.Capacity() < coldata.BatchSize() {
		newCapacity := batch.Capacity() * 2
		if newCapacity < minCapacity {
			newCapacity = minCapacity
		}
		if newCapacity > coldata.BatchSize() {
			newCapacity = coldata.BatchSize()
		}
		batch = d.allocator.NewMemBatchWithFixedCapacity(d.typs, newCapacity)
	} else {
		reallocated = false
		batch.ResetInternalBatch()
	}
	return batch, reallocated
}
