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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// BatchBuffer exposes a buffer of coldata.Batches through an Operator
// interface. If there are no batches to return, Next will panic.
type BatchBuffer struct {
	ZeroInputNode
	buffer []coldata.Batch
}

var _ Operator = &BatchBuffer{}

// NewBatchBuffer creates a new BatchBuffer.
func NewBatchBuffer() *BatchBuffer {
	return &BatchBuffer{
		buffer: make([]coldata.Batch, 0, 2),
	}
}

// Add adds a batch to the buffer.
func (b *BatchBuffer) Add(batch coldata.Batch) {
	b.buffer = append(b.buffer, batch)
}

// Init is part of the Operator interface.
func (b *BatchBuffer) Init() {}

// Next is part of the Operator interface.
func (b *BatchBuffer) Next(context.Context) coldata.Batch {
	batch := b.buffer[0]
	b.buffer = b.buffer[1:]
	return batch
}

// RepeatableBatchSource is an Operator that returns the same batch forever.
type RepeatableBatchSource struct {
	ZeroInputNode

	colVecs  []coldata.Vec
	typs     []coltypes.T
	sel      []uint16
	batchLen uint16
	// numToCopy indicates the number of tuples that needs to be copied. It is
	// equal to batchLen when sel is nil and is equal to maxSelIdx+1 when sel is
	// non-nil.
	numToCopy uint16
	output    coldata.Batch

	batchesToReturn int
	batchesReturned int
}

var _ Operator = &RepeatableBatchSource{}

// NewRepeatableBatchSource returns a new Operator initialized to return its
// input batch forever. Note that it stores the contents of the input batch and
// copies them into a separate output batch. The output batch is allowed to be
// modified whereas the input batch is *not*.
func NewRepeatableBatchSource(allocator *Allocator, batch coldata.Batch) *RepeatableBatchSource {
	typs := make([]coltypes.T, batch.Width())
	for i, vec := range batch.ColVecs() {
		typs[i] = vec.Type()
	}
	sel := batch.Selection()
	batchLen := batch.Length()
	numToCopy := batchLen
	if sel != nil {
		maxIdx := uint16(0)
		for _, selIdx := range sel[:batchLen] {
			if selIdx > maxIdx {
				maxIdx = selIdx
			}
		}
		numToCopy = maxIdx + 1
	}
	output := allocator.NewMemBatchWithSize(typs, int(numToCopy))
	src := &RepeatableBatchSource{
		colVecs:   batch.ColVecs(),
		typs:      typs,
		sel:       sel,
		batchLen:  batchLen,
		numToCopy: numToCopy,
		output:    output,
	}
	return src
}

// Next is part of the Operator interface.
func (s *RepeatableBatchSource) Next(context.Context) coldata.Batch {
	s.batchesReturned++
	if s.batchesToReturn != 0 && s.batchesReturned > s.batchesToReturn {
		return coldata.ZeroBatch
	}
	s.output.SetSelection(s.sel != nil)
	if s.sel != nil {
		copy(s.output.Selection()[:s.batchLen], s.sel[:s.batchLen])
	}
	for i, typ := range s.typs {
		// This Copy is outside of the allocator since the RepeatableBatchSource is
		// a test utility which is often used in the benchmarks, and we want to
		// reduce the performance impact of this operator.
		s.output.ColVec(i).Copy(coldata.CopySliceArgs{
			SliceArgs: coldata.SliceArgs{
				ColType:   typ,
				Src:       s.colVecs[i],
				SrcEndIdx: uint64(s.numToCopy),
			},
		})
	}
	s.output.SetLength(s.batchLen)
	return s.output
}

// Init is part of the Operator interface.
func (s *RepeatableBatchSource) Init() {}

// ResetBatchesToReturn sets a limit on how many batches the source returns, as
// well as resetting how many batches the source has returned so far.
func (s *RepeatableBatchSource) ResetBatchesToReturn(b int) {
	s.batchesToReturn = b
	s.batchesReturned = 0
}

// CallbackOperator is a testing utility struct that delegates Next calls to a
// callback provided by the user.
type CallbackOperator struct {
	ZeroInputNode
	NextCb func(ctx context.Context) coldata.Batch
}

// Init is part of the Operator interface.
func (o *CallbackOperator) Init() {}

// Next is part of the Operator interface.
func (o *CallbackOperator) Next(ctx context.Context) coldata.Batch {
	return o.NextCb(ctx)
}
