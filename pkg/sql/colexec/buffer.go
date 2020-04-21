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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
)

// bufferOp is an operator that buffers a single batch at a time from an input,
// and makes it available to be read multiple times by downstream consumers.
type bufferOp struct {
	OneInputNode
	initStatus OperatorInitStatus

	// read is true if someone has read the current batch already.
	read  bool
	batch coldata.Batch
}

var _ colexecbase.Operator = &bufferOp{}

// NewBufferOp returns a new bufferOp, initialized to buffer batches from the
// supplied input.
func NewBufferOp(input colexecbase.Operator) colexecbase.Operator {
	return &bufferOp{
		OneInputNode: NewOneInputNode(input),
	}
}

func (b *bufferOp) Init() {
	// bufferOp can be an input to multiple operator chains, so Init on it can be
	// called multiple times. However, we do not want to call Init many times on
	// the input to bufferOp, so we do this check whether Init has already been
	// performed.
	if b.initStatus == OperatorNotInitialized {
		b.input.Init()
		b.initStatus = OperatorInitialized
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
func (b *bufferOp) advance(ctx context.Context) {
	b.batch = b.input.Next(ctx)
	b.rewind()
}

func (b *bufferOp) Next(ctx context.Context) coldata.Batch {
	if b.read {
		return coldata.ZeroBatch
	}
	b.read = true
	return b.batch
}
