// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecop

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
func (b *BatchBuffer) Add(batch coldata.Batch, _ []*types.T) {
	b.buffer = append(b.buffer, batch)
}

// Init is part of the Operator interface.
func (b *BatchBuffer) Init(context.Context) {}

// Next is part of the Operator interface.
func (b *BatchBuffer) Next() coldata.Batch {
	batch := b.buffer[0]
	b.buffer = b.buffer[1:]
	return batch
}

// RepeatableBatchSource is an Operator that returns the same batch forever.
type RepeatableBatchSource struct {
	ZeroInputNode

	colVecs  []coldata.Vec
	typs     []*types.T
	sel      []int
	batchLen int
	// numToCopy indicates the number of tuples that needs to be copied. It is
	// equal to batchLen when sel is nil and is equal to maxSelIdx+1 when sel is
	// non-nil.
	numToCopy int
	output    coldata.Batch

	batchesToReturn int
	batchesReturned int
}

var _ Operator = &RepeatableBatchSource{}

// NewRepeatableBatchSource returns a new Operator initialized to return its
// input batch forever. Note that it stores the contents of the input batch and
// copies them into a separate output batch. The output batch is allowed to be
// modified whereas the input batch is *not*.
func NewRepeatableBatchSource(
	allocator *colmem.Allocator, batch coldata.Batch, typs []*types.T,
) *RepeatableBatchSource {
	sel := batch.Selection()
	batchLen := batch.Length()
	numToCopy := batchLen
	if sel != nil {
		maxIdx := 0
		for _, selIdx := range sel[:batchLen] {
			if selIdx > maxIdx {
				maxIdx = selIdx
			}
		}
		numToCopy = maxIdx + 1
	}
	output := allocator.NewMemBatchWithFixedCapacity(typs, numToCopy)
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
func (s *RepeatableBatchSource) Next() coldata.Batch {
	s.batchesReturned++
	if s.batchesToReturn != 0 && s.batchesReturned > s.batchesToReturn {
		return coldata.ZeroBatch
	}
	s.output.SetSelection(s.sel != nil)
	if s.sel != nil {
		copy(s.output.Selection()[:s.batchLen], s.sel[:s.batchLen])
	}
	for i, colVec := range s.colVecs {
		// This Copy is outside of the allocator since the RepeatableBatchSource is
		// a test utility which is often used in the benchmarks, and we want to
		// reduce the performance impact of this operator.
		s.output.ColVec(i).Copy(coldata.CopySliceArgs{
			SliceArgs: coldata.SliceArgs{
				Src:       colVec,
				SrcEndIdx: s.numToCopy,
			},
		})
	}
	s.output.SetLength(s.batchLen)
	return s.output
}

// Init is part of the Operator interface.
func (s *RepeatableBatchSource) Init(context.Context) {}

// ResetBatchesToReturn sets a limit on how many batches the source returns, as
// well as resetting how many batches the source has returned so far.
func (s *RepeatableBatchSource) ResetBatchesToReturn(b int) {
	s.batchesToReturn = b
	s.batchesReturned = 0
}

// CallbackOperator is a testing utility struct that delegates calls to Init,
// Next, and Close to the callbacks provided by the user.
type CallbackOperator struct {
	ZeroInputNode
	InitCb  func(context.Context)
	NextCb  func() coldata.Batch
	CloseCb func() error
}

var _ ClosableOperator = &CallbackOperator{}

// Init is part of the Operator interface.
func (o *CallbackOperator) Init(ctx context.Context) {
	if o.InitCb == nil {
		return
	}
	o.InitCb(ctx)
}

// Next is part of the Operator interface.
func (o *CallbackOperator) Next() coldata.Batch {
	if o.NextCb == nil {
		return coldata.ZeroBatch
	}
	return o.NextCb()
}

// Close is part of the ClosableOperator interface.
func (o *CallbackOperator) Close() error {
	if o.CloseCb == nil {
		return nil
	}
	return o.CloseCb()
}

// TestingSemaphore is a semaphore.Semaphore that never blocks and is always
// successful. If the requested number of resources exceeds the given limit, an
// error is returned. If too many resources are released, the semaphore panics.
type TestingSemaphore struct {
	count int
	limit int
}

// NewTestingSemaphore initializes a new TestingSemaphore with the provided
// limit. If limit is zero, there will be no limit. Can also use
// &TestingSemaphore{} directly in this case.
func NewTestingSemaphore(limit int) *TestingSemaphore {
	return &TestingSemaphore{limit: limit}
}

// Acquire implements the semaphore.Semaphore interface.
func (s *TestingSemaphore) Acquire(_ context.Context, n int) error {
	if n < 0 {
		return errors.New("acquiring a negative amount")
	}
	if s.limit != 0 && s.count+n > s.limit {
		return errors.Errorf("testing semaphore limit exceeded: tried acquiring %d but already have a count of %d from a total limit of %d", n, s.count, s.limit)
	}
	s.count += n
	return nil
}

// TryAcquire implements the semaphore.Semaphore interface.
func (s *TestingSemaphore) TryAcquire(n int) bool {
	if s.limit != 0 && s.count+n > s.limit {
		return false
	}
	s.count += n
	return true
}

// Release implements the semaphore.Semaphore interface.
func (s *TestingSemaphore) Release(n int) int {
	if n < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("releasing a negative amount"))
	}
	if s.count-n < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("testing semaphore too many resources released, releasing %d, have %d", n, s.count))
	}
	pre := s.count
	s.count -= n
	return pre
}

// SetLimit implements the semaphore.Semaphore interface.
func (s *TestingSemaphore) SetLimit(n int) {
	s.limit = n
}

// GetLimit implements the semaphore.Semaphore interface.
func (s *TestingSemaphore) GetLimit() int {
	return s.limit
}

// GetCount implements the semaphore.Semaphore interface.
func (s *TestingSemaphore) GetCount() int {
	return s.count
}
