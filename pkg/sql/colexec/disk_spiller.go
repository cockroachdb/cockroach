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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// bufferingInMemoryOperator is an Operator that buffers up intermediate tuples
// in memory and knows how to export them once the memory limit has been
// reached.
type bufferingInMemoryOperator interface {
	Operator

	// ExportBuffered returns all the batches that have been buffered up and have
	// not yet been processed by the operator. It needs to be called once the
	// memory limit has been reached in order to "dump" the buffered tuples into
	// a disk-backed operator. It will return a zero-length batch once the buffer
	// has been emptied.
	//
	// Calling ExportBuffered may invalidate the contents of the last batch
	// returned by ExportBuffered.
	ExportBuffered(*Allocator) coldata.Batch
}

// oneInputDiskSpiller is an Operator that manages the fallback from an
// in-memory buffering operator to a disk-backed one when the former hits the
// memory limit.
//
// NOTE: if an out of memory error occurs during initialization, this operator
// simply propagates the error further.
//
// The diagram of the components involved is as follows:
//
//        -------------  input  -----------
//       |                ||                | (2nd src)
//       |                ||   (1st src)    ↓
//       |            ----||---> bufferExportingOperator
//       ↓           |    ||                |
//    inMemoryOp ----     ||                ↓
//       |                ||           diskBackedOp
//       |                ||                |
//       |                ↓↓                |
//        ---------> disk spiller <--------
//                        ||
//                        ||
//                        ↓↓
//                      output
//
// Here is the explanation:
// - the main chain of Operators is input -> disk spiller -> output
// - the dist spiller will first try running everything through the left side
//   chain of input -> inMemoryOp. If that succeeds, great! The disk spiller
//   will simply propagate the batch to the output. If that fails with an OOM
//   error, the disk spiller will then initialize the right side chain and will
//   proceed to emit from there
// - the right side chain is bufferExportingOperator -> diskBackedOp. The
//   former will first export all the buffered tuples from inMemoryOp and then
//   will proceed on emitting from input.
type oneInputDiskSpiller struct {
	allocator   *Allocator
	initialized bool
	spilled     bool

	input        Operator
	inMemoryOp   bufferingInMemoryOperator
	diskBackedOp Operator
}

var _ Operator = &oneInputDiskSpiller{}

func newOneInputDiskSpiller(
	allocator *Allocator,
	input Operator,
	inMemoryOp bufferingInMemoryOperator,
	diskBackedOpConstructor func(Operator) Operator,
) Operator {
	diskBackedOpInput := newBufferExportingOperator(allocator, inMemoryOp, input)
	return &oneInputDiskSpiller{
		allocator:    allocator,
		input:        input,
		inMemoryOp:   inMemoryOp,
		diskBackedOp: diskBackedOpConstructor(diskBackedOpInput),
	}
}

func (d *oneInputDiskSpiller) Init() {
	if d.initialized {
		return
	}
	d.initialized = true
	// It is possible that Init() calls below will hit an out of memory error,
	// but we decide to bail on this query, so we do not catch internal panics.
	d.input.Init()
	d.inMemoryOp.Init()
}

func (d *oneInputDiskSpiller) Next(ctx context.Context) coldata.Batch {
	var batch coldata.Batch
	if !d.spilled {
		if err := execerror.CatchVectorizedRuntimeError(
			func() {
				batch = d.inMemoryOp.Next(ctx)
			},
		); err != nil {
			if sqlbase.IsOutOfMemoryError(err) {
				d.spilled = true
				d.diskBackedOp.Init()
				return d.Next(ctx)
			} else {
				// Not an out of memory error, so we propagate it further.
				execerror.VectorizedInternalPanic(err)
			}
		}
	} else {
		batch = d.diskBackedOp.Next(ctx)
	}
	return batch
}

func (d *oneInputDiskSpiller) ChildCount() int {
	return 3
}

func (d *oneInputDiskSpiller) Child(nth int) execinfra.OpNode {
	switch nth {
	case 0:
		return d.input
	case 1:
		return d.inMemoryOp
	case 2:
		return d.diskBackedOp
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}

// bufferExportingOperator is an Operator that first returns all batches from
// firstSource, and once firstSource is exhausted, it proceeds on returning all
// batches from the second source.
//
// NOTE: bufferExportingOperator assumes that both sources will have been
// initialized when bufferExportingOperator.Init() is called.
type bufferExportingOperator struct {
	ZeroInputNode

	allocator       *Allocator
	firstSource     bufferingInMemoryOperator
	secondSource    Operator
	firstSourceDone bool
}

var _ Operator = &bufferExportingOperator{}

func newBufferExportingOperator(
	allocator *Allocator, firstSource bufferingInMemoryOperator, secondSource Operator,
) Operator {
	return &bufferExportingOperator{
		allocator:    allocator,
		firstSource:  firstSource,
		secondSource: secondSource,
	}
}

func (b *bufferExportingOperator) Init() {
	// Init here is a noop because the operator assumes that both sources have
	// already been initialized.
}

func (b *bufferExportingOperator) Next(ctx context.Context) coldata.Batch {
	if b.firstSourceDone {
		return b.secondSource.Next(ctx)
	}
	batch := b.firstSource.ExportBuffered(b.allocator)
	if batch.Length() == 0 {
		b.firstSourceDone = true
		return b.Next(ctx)
	}
	return batch
}
