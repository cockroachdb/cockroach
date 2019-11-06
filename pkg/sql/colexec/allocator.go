// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// Allocator is a memory management tool for vectorized components. It provides
// new batches (and appends to existing ones) within a fixed memory budget. If
// the budget is exceeded, it will return an error.
//
// In the future this can also be used to pool coldata.Vec allocations.
//
// TODO(yuzefovich): Add memory budget logic.
type Allocator struct{}

// NewAllocator constructs a new Allocator instance.
func NewAllocator() *Allocator {
	return &Allocator{}
}

// TODO(yuzefovich): add NewMemColumn method.
// TODO(yuzefovich): it is possible for a batch obtained from an Allocator to
// be Reset in a such way that a new batch will be allocated, and it will not
// be accounted for.

// NewMemBatch allocates a new in-memory coldata.Batch.
func (a *Allocator) NewMemBatch(types []coltypes.T) (coldata.Batch, error) {
	return a.NewMemBatchWithSize(types, int(coldata.BatchSize()))
}

// NewMemBatchWithSize allocates a new in-memory coldata.Batch with the given
// column size.
func (*Allocator) NewMemBatchWithSize(
	types []coltypes.T, size int,
) (batch coldata.Batch, err error) {
	return coldata.NewMemBatchWithSize(types, size), nil
}

// Append appends elements of a source coldata.Vec into dest according to
// coldata.SliceArgs.
func (*Allocator) Append(dest coldata.Vec, args coldata.SliceArgs) error {
	dest.Append(args)
	return nil
}
