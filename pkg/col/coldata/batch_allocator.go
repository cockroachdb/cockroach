// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import "github.com/cockroachdb/cockroach/pkg/col/coltypes"

// BatchAllocator is a memory management tool for vectorized components. It
// provides new batches (and appends to existing ones) within a fixed memory
// budget. If the budget is exceeded, it will return an error.
//
// In the future this can also be used to pool Vec allocations.
//
// TODO(yuzefovich): Add memory budget logic.
type BatchAllocator struct{}

// NewBatchAllocator constructs a new BatchAllocator instance.
func NewBatchAllocator() BatchAllocator {
	return BatchAllocator{}
}

// NewMemBatchWithSize allocates a new in-memory Batch with the given column
// size.
func (*BatchAllocator) NewMemBatchWithSize(types []coltypes.T, size int) (batch Batch, err error) {
	return NewMemBatchWithSize(types, size), nil
}

// Append appends elements of a source Vec into dest according to SliceArgs.
func (*BatchAllocator) Append(dest Vec, args SliceArgs) error {
	dest.Append(args)
	return nil
}
