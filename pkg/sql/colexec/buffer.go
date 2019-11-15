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
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

// bufferOp is an operator that buffers a single batch at a time from an input,
// and makes it available to be read multiple times by downstream consumers.
type bufferOp struct {
	OneInputNode

	// read is true if someone has read the current batch already.
	read  bool
	batch *selectionBatch
}

var _ InternalMemoryOperator = &bufferOp{}

// NewBufferOp returns a new bufferOp, initialized to buffer batches from the
// supplied input.
func NewBufferOp(input Operator) Operator {
	return &bufferOp{
		OneInputNode: NewOneInputNode(input),
		batch: &selectionBatch{
			sel: make([]uint16, coldata.BatchSize()),
		},
	}
}

func (b *bufferOp) InternalMemoryUsage() int {
	// We internally use a single selection vector within the selectionBatch.
	return sizeOfBatchSizeSelVector
}

func (b *bufferOp) Init() {
	b.input.Init()
}

// rewind resets this buffer to be readable again.
func (b *bufferOp) rewind() {
	b.read = false
	b.batch.SetLength(b.batch.Batch.Length())
	b.batch.useSel = b.batch.Batch.Selection() != nil
	if b.batch.useSel {
		copy(b.batch.sel, b.batch.Batch.Selection())
	}
}

// advance reads the next batch from the input into the buffer, preparing itself
// for reads.
func (b *bufferOp) advance(ctx context.Context) {
	b.batch.Batch = b.input.Next(ctx)
	b.rewind()
}

func (b *bufferOp) Next(ctx context.Context) coldata.Batch {
	if b.read {
		b.batch.SetLength(0)
		return b.batch
	}
	b.read = true
	return b.batch
}

// selectionBatch is a smaller wrapper around coldata.Batch that adds another
// selection vector on top. This is useful for operators that might want to
// permute the selection vector for downstream operators without touching the
// original selection of the batch.
type selectionBatch struct {
	coldata.Batch
	sel    []uint16
	useSel bool
	n      uint16
}

var _ coldata.Batch = &selectionBatch{}

func (s *selectionBatch) SetSelection(b bool) {
	s.useSel = b
}

func (s *selectionBatch) Selection() []uint16 {
	if s.useSel {
		return s.sel
	}
	return nil
}

func (s *selectionBatch) Length() uint16 {
	return s.n
}

func (s *selectionBatch) SetLength(n uint16) {
	s.n = n
}
