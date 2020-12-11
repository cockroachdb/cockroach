// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Operator is a column vector operator that produces a Batch as output.
type Operator interface {
	// Init initializes this operator. Will be called once at operator setup
	// time. If an operator has an input operator, it's responsible for calling
	// Init on that input operator as well.
	//
	// It might panic with an expected error, so there must be a "root"
	// component that will catch that panic.
	// TODO(yuzefovich): we might need to clarify whether it is ok to call
	// Init() multiple times before the first call to Next(). It is possible to
	// hit the memory limit during Init(), and a disk-backed operator needs to
	// make sure that the input has been initialized. We could also in case that
	// Init() doesn't succeed for bufferingInMemoryOperator - which should only
	// happen when 'workmem' setting is too low - just bail, even if we have
	// disk spilling for that operator.
	Init()

	// Next returns the next Batch from this operator. Once the operator is
	// finished, it will return a Batch with length 0. Subsequent calls to
	// Next at that point will always return a Batch with length 0.
	//
	// Calling Next may invalidate the contents of the last Batch returned by
	// Next.
	// Canceling the provided context results in forceful termination of
	// execution.
	//
	// It might panic with an expected error, so there must be a "root"
	// component that will catch that panic.
	Next(context.Context) coldata.Batch

	execinfra.OpNode
}

// DrainableOperator is an operator that also implements DrainMeta. Next and
// DrainMeta may not be called concurrently.
type DrainableOperator interface {
	Operator
	execinfrapb.MetadataSource
}

// ZeroInputNode is an execinfra.OpNode with no inputs.
type ZeroInputNode struct{}

// ChildCount implements the execinfra.OpNode interface.
func (ZeroInputNode) ChildCount(verbose bool) int {
	return 0
}

// Child implements the execinfra.OpNode interface.
func (ZeroInputNode) Child(nth int, verbose bool) execinfra.OpNode {
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// BufferingInMemoryOperator is an Operator that buffers up intermediate tuples
// in memory and knows how to export them once the memory limit has been
// reached.
type BufferingInMemoryOperator interface {
	Operator

	// ExportBuffered returns all the batches that have been buffered up from the
	// input and have not yet been processed by the operator. It needs to be
	// called once the memory limit has been reached in order to "dump" the
	// buffered tuples into a disk-backed operator. It will return a zero-length
	// batch once the buffer has been emptied.
	//
	// Calling ExportBuffered may invalidate the contents of the last batch
	// returned by ExportBuffered.
	ExportBuffered(ctx context.Context, input Operator) coldata.Batch
}

// Closer is an object that releases resources when Close is called. Note that
// this interface must be implemented by all operators that could be planned on
// top of other operators that do actually need to release the resources (e.g.
// if we have a simple project on top of a disk-backed operator, that simple
// project needs to implement this interface so that Close() call could be
// propagated correctly).
type Closer interface {
	Close(ctx context.Context) error
}

// Closers is a slice of Closers.
type Closers []Closer

// CloseAndLogOnErr closes all Closers and logs the error if the log verbosity
// is 1 or higher. The given prefix is prepended to the log message.
// Note: this method should *only* be used when returning an error doesn't make
// sense.
func (c Closers) CloseAndLogOnErr(ctx context.Context, prefix string) {
	prefix += ":"
	for _, closer := range c {
		if err := closer.Close(ctx); err != nil && log.V(1) {
			log.Infof(ctx, "%s error closing Closer: %v", prefix, err)
		}
	}
}

// Close closes all Closers and returns the last error (if any occurs).
func (c Closers) Close(ctx context.Context) error {
	var lastErr error
	for _, closer := range c {
		if err := closer.Close(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
