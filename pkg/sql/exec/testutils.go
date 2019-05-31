// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
)

// BatchBuffer exposes a buffer of coldata.Batches through an Operator
// interface. If there are no batches to return, Next will panic.
type BatchBuffer struct {
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
	internalBatch coldata.Batch
	batchLen      uint16
	// sel specifies the desired selection vector for the batch.
	sel []uint16

	batchesToReturn int
	batchesReturned int
}

var _ Operator = &RepeatableBatchSource{}

// NewRepeatableBatchSource returns a new Operator initialized to return its
// input batch forever (including the selection vector if batch comes with it).
func NewRepeatableBatchSource(batch coldata.Batch) *RepeatableBatchSource {
	src := &RepeatableBatchSource{
		internalBatch: batch,
		batchLen:      batch.Length(),
	}
	if batch.Selection() != nil {
		src.sel = make([]uint16, batch.Length())
		copy(src.sel, batch.Selection())
	}
	return src
}

// Next is part of the Operator interface.
func (s *RepeatableBatchSource) Next(context.Context) coldata.Batch {
	s.internalBatch.SetSelection(s.sel != nil)
	s.batchesReturned++
	if s.batchesToReturn != 0 && s.batchesReturned > s.batchesToReturn {
		s.internalBatch.SetLength(0)
	} else {
		s.internalBatch.SetLength(s.batchLen)
	}
	if s.sel != nil {
		// Since selection vectors are mutable, to make sure that we return the
		// batch with the given selection vector, we need to reset
		// s.internalBatch.Selection() to s.sel on every iteration.
		copy(s.internalBatch.Selection(), s.sel)
	}
	return s.internalBatch
}

// Init is part of the Operator interface.
func (s *RepeatableBatchSource) Init() {}

func (s *RepeatableBatchSource) resetBatchesToReturn(b int) {
	s.batchesToReturn = b
	s.batchesReturned = 0
}
