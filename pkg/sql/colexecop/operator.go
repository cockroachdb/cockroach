// Copyright 2020 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Operator is a column vector operator that produces a Batch as output.
type Operator interface {
	// Init initializes this operator. It will be called once at operator setup
	// time. Second, third, etc calls should be noops. If an operator has any
	// input operators, it's responsible for calling Init on all of those input
	// operators as well.
	//
	// Canceling the provided context results in forceful termination of
	// execution. The operators are expected to hold onto the provided context
	// (and derive a new one if needed) that is then used for Next(),
	// DrainMeta(), and Close() calls (when applicable).
	//
	// It might panic with an expected error, so there must be a "root"
	// component that will catch that panic.
	Init(ctx context.Context)

	// Next returns the next Batch from this operator. Once the operator is
	// finished, it will return a Batch with length 0. Subsequent calls to
	// Next at that point will always return a Batch with length 0.
	//
	// Calling Next may invalidate the contents of the last Batch returned by
	// Next.
	//
	// It might panic with an expected error, so there must be a "root"
	// component that will catch that panic.
	Next() coldata.Batch

	execinfra.OpNode
}

// DrainableOperator is an operator that also implements DrainMeta. Next and
// DrainMeta may not be called concurrently.
type DrainableOperator interface {
	Operator
	MetadataSource
}

// KVReader is an operator that performs KV reads.
// TODO(yuzefovich): consider changing the contract to remove the mention of
// concurrency safety once stats are only retrieved from Next goroutines.
type KVReader interface {
	// GetBytesRead returns the number of bytes read from KV by this operator.
	// It must be safe for concurrent use.
	GetBytesRead() int64
	// GetRowsRead returns the number of rows read from KV by this operator.
	// It must be safe for concurrent use.
	GetRowsRead() int64
	// GetCumulativeContentionTime returns the amount of time KV reads spent
	// contending. It must be safe for concurrent use.
	GetCumulativeContentionTime() time.Duration
	// GetScanStats returns statistics about the scan that happened during the
	// KV reads. It must be safe for concurrent use.
	GetScanStats() execinfra.ScanStats
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

// NewOneInputNode returns an execinfra.OpNode with a single Operator input.
func NewOneInputNode(input Operator) OneInputNode {
	return OneInputNode{Input: input}
}

// OneInputNode is an execinfra.OpNode with a single Operator input.
type OneInputNode struct {
	Input Operator
}

// ChildCount implements the execinfra.OpNode interface.
func (OneInputNode) ChildCount(verbose bool) int {
	return 1
}

// Child implements the execinfra.OpNode interface.
func (n OneInputNode) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return n.Input
	}
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
	ExportBuffered(input Operator) coldata.Batch
}

// Closer is an object that releases resources when Close is called. Note that
// this interface must be implemented by all operators that could be planned on
// top of other operators that do actually need to release the resources (e.g.
// if we have a simple project on top of a disk-backed operator, that simple
// project needs to implement this interface so that Close() call could be
// propagated correctly).
type Closer interface {
	// Close releases the resources associated with this Closer. If this Closer
	// is an Operator, the implementation of Close must be safe to execute even
	// if Operator.Init wasn't called.
	//
	// Unless the Closer derives its own context with a separate tracing span,
	// the argument context rather than the one from Init() must be used
	// (wherever necessary) by the implementation. This is so since the span in
	// the context from Init() might be already finished when Close() is called
	// whereas the argument context will contain an unfinished span.
	//
	// If this Closer is an execinfra.Releasable, the implementation must be
	// safe to execute even after Release() was called.
	// TODO(yuzefovich): refactor this because the Release()'d objects should
	// not be used anymore.
	Close(context.Context) error
}

// Closers is a slice of Closers.
type Closers []Closer

// CloseAndLogOnErr closes all Closers and logs the error if the log verbosity
// is 1 or higher. The given prefix is prepended to the log message.
// Note: this method should *only* be used when returning an error doesn't make
// sense.
func (c Closers) CloseAndLogOnErr(ctx context.Context, prefix string) {
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		for _, closer := range c {
			if err := closer.Close(ctx); err != nil && log.V(1) {
				log.Infof(ctx, "%s: error closing Closer: %v", prefix, err)
			}
		}
	}); err != nil && log.V(1) {
		log.Infof(ctx, "%s: runtime error closing the closers: %v", prefix, err)
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

// Resetter is an interface that operators can implement if they can be reset
// either for reusing (to keep the already allocated memory) or during tests.
type Resetter interface {
	// Reset resets the operator for reuse.
	Reset(ctx context.Context)
}

// ResettableOperator is an Operator that can be reset.
type ResettableOperator interface {
	Operator
	Resetter
}

// FeedOperator is used to feed an Operator chain with input by manually
// setting the next batch.
type FeedOperator struct {
	ZeroInputNode
	NonExplainable
	batch coldata.Batch
}

// NewFeedOperator returns a new feed operator.
func NewFeedOperator() *FeedOperator {
	return &FeedOperator{}
}

// Init implements the colexecop.Operator interface.
func (FeedOperator) Init(context.Context) {}

// Next implements the colexecop.Operator interface.
func (o *FeedOperator) Next() coldata.Batch {
	return o.batch
}

// SetBatch sets the next batch to be returned on Next call.
func (o *FeedOperator) SetBatch(batch coldata.Batch) {
	o.batch = batch
}

var _ Operator = &FeedOperator{}

// NonExplainable is a marker interface which identifies an Operator that
// should be omitted from the output of EXPLAIN (VEC). Note that VERBOSE
// explain option will override the omitting behavior.
type NonExplainable interface {
	// nonExplainableMarker is just a marker method. It should never be called.
	nonExplainableMarker()
}

// InitHelper is a simple struct that helps Operators implement Init() method.
type InitHelper struct {
	// Ctx is the context passed on the first call to Init(). If it is nil, then
	// Init() hasn't been called yet.
	// NOTE: if a non-nil context is needed, use EnsureCtx() to retrieve it
	// instead of accessing this field directly.
	Ctx context.Context
}

// Init marks the InitHelper as initialized. If true is returned, this is the
// first call to Init.
func (h *InitHelper) Init(ctx context.Context) bool {
	if h.Ctx != nil {
		return false
	}
	if ctx == nil {
		colexecerror.InternalError(errors.AssertionFailedf("nil context is passed"))
	}
	h.Ctx = ctx
	return true
}

// EnsureCtx returns the context which this helper was initialized with or the
// background context if Init hasn't been called.
func (h *InitHelper) EnsureCtx() context.Context {
	if h.Ctx == nil {
		return context.Background()
	}
	return h.Ctx
}

// MakeOneInputHelper returns a new OneInputHelper.
func MakeOneInputHelper(input Operator) OneInputHelper {
	return OneInputHelper{
		OneInputNode: NewOneInputNode(input),
	}
}

// OneInputHelper is an execinfra.OpNode which only needs to initialize its
// single Operator input in Init().
type OneInputHelper struct {
	OneInputNode
	InitHelper
}

// Init implements the Operator interface.
func (h *OneInputHelper) Init(ctx context.Context) {
	if !h.InitHelper.Init(ctx) {
		return
	}
	h.Input.Init(h.Ctx)
}

// CloserHelper is a simple helper that helps Operators implement Closer. If
// close returns true, resources may be released, if it returns false, close has
// already been called.
type CloserHelper struct {
	closed bool
}

// Close marks the CloserHelper as closed. If true is returned, this is the
// first call to Close.
func (c *CloserHelper) Close() bool {
	if c.closed {
		return false
	}
	c.closed = true
	return true
}

// Reset resets the CloserHelper so that it can be closed again.
func (c *CloserHelper) Reset() {
	c.closed = false
}

// ClosableOperator is an Operator that needs to be Close()'d.
// NOTE: even if the Operator wasn't Init()'ed properly, it must still be safe
// to Close().
type ClosableOperator interface {
	Operator
	Closer
}

// MakeOneInputCloserHelper returns a new OneInputCloserHelper.
func MakeOneInputCloserHelper(input Operator) OneInputCloserHelper {
	return OneInputCloserHelper{
		OneInputNode: NewOneInputNode(input),
	}
}

// OneInputCloserHelper is an execinfra.OpNode with a single Operator input
// that might need to be Close()'d.
type OneInputCloserHelper struct {
	OneInputNode
	CloserHelper
}

var _ Closer = &OneInputCloserHelper{}

// Close implements the Closer interface.
func (c *OneInputCloserHelper) Close(ctx context.Context) error {
	if !c.CloserHelper.Close() {
		return nil
	}
	if closer, ok := c.Input.(Closer); ok {
		return closer.Close(ctx)
	}
	return nil
}

// MakeOneInputInitCloserHelper returns a new OneInputInitCloserHelper.
func MakeOneInputInitCloserHelper(input Operator) OneInputInitCloserHelper {
	return OneInputInitCloserHelper{
		OneInputCloserHelper: MakeOneInputCloserHelper(input),
	}
}

// OneInputInitCloserHelper is an execinfra.OpNode that only needs to initialize
// its single Operator input in Init() and might need to Close() it too.
type OneInputInitCloserHelper struct {
	InitHelper
	OneInputCloserHelper
}

// Init implements the Operator interface.
func (h *OneInputInitCloserHelper) Init(ctx context.Context) {
	if !h.InitHelper.Init(ctx) {
		return
	}
	h.Input.Init(h.Ctx)
}

type noopOperator struct {
	OneInputInitCloserHelper
	NonExplainable
}

var _ ResettableOperator = &noopOperator{}

// NewNoop returns a new noop Operator.
func NewNoop(input Operator) ResettableOperator {
	return &noopOperator{OneInputInitCloserHelper: MakeOneInputInitCloserHelper(input)}
}

func (n *noopOperator) Next() coldata.Batch {
	return n.Input.Next()
}

func (n *noopOperator) Reset(ctx context.Context) {
	if r, ok := n.Input.(Resetter); ok {
		r.Reset(ctx)
	}
}

// MetadataSource is an interface implemented by processors and columnar
// operators that can produce metadata.
// TODO(yuzefovich): remove this interface in favor of DrainableOperator and
// clarify that calling DrainMeta on an uninitialized operator is illegal.
type MetadataSource interface {
	// DrainMeta returns all the metadata produced by the processor or operator.
	// It will be called exactly once, usually, when the processor or operator
	// has finished doing its computations. This is a signal that the output
	// requires no more rows to be returned.
	// Implementers can choose what to do on subsequent calls (if such occur).
	// TODO(yuzefovich): modify the contract to require returning nil on all
	// calls after the first one.
	DrainMeta() []execinfrapb.ProducerMetadata
}

// MetadataSources is a slice of MetadataSource.
type MetadataSources []MetadataSource

// DrainMeta calls DrainMeta on all MetadataSources and returns a single slice
// with all the accumulated metadata. Note that this method wraps the draining
// with the panic-catcher so that the callers don't have to.
func (s MetadataSources) DrainMeta() []execinfrapb.ProducerMetadata {
	var result []execinfrapb.ProducerMetadata
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		for _, src := range s {
			result = append(result, src.DrainMeta()...)
		}
	}); err != nil {
		meta := execinfrapb.GetProducerMeta()
		meta.Err = err
		result = append(result, *meta)
	}
	return result
}

// VectorizedStatsCollector is the common interface implemented by several
// variations of the execution statistics collectors. At the moment of writing
// we have two variants: the "default" option (for all Operators) and the
// "network" option (strictly for colrpc.Inboxes).
type VectorizedStatsCollector interface {
	Operator
	// GetStats returns the execution statistics of a single Operator. It will
	// always return non-nil (but possibly empty) object.
	GetStats() *execinfrapb.ComponentStats
}
