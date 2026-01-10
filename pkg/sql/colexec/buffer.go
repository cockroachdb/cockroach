// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// bufferOp is an operator that buffers a single batch at a time from an input,
// and makes it available to be read multiple times by downstream consumers.
type bufferOp struct {
	colexecop.OneInputHelper

	// read is true if someone has read the current batch already.
	read  bool
	batch coldata.Batch
}

var _ colexecop.Operator = &bufferOp{}

// NewBufferOp returns a new bufferOp, initialized to buffer batches from the
// supplied input.
func NewBufferOp(input colexecop.Operator) colexecop.Operator {
	return &bufferOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
	}
}

// rewind resets this buffer to be readable again.
// NOTE: it is the caller responsibility to restore the batch into the desired
// state.
func (b *bufferOp) rewind() {
	b.read = false
}

// advance reads the next batch from the input into the buffer, preparing itself
// for reads.
func (b *bufferOp) advance() *execinfrapb.ProducerMetadata {
	var meta *execinfrapb.ProducerMetadata
	b.batch, meta = b.Input.Next()
	if meta != nil {
		return meta
	}
	b.rewind()
	return nil
}

func (b *bufferOp) Next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	if b.read {
		return coldata.ZeroBatch, nil
	}
	b.read = true
	return b.batch, nil
}
