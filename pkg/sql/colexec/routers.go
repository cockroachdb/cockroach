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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// routerOutput is an interface implemented by router outputs. It exists for
// easier test mocking of outputs.
type routerOutput interface {
	execinfra.OpNode
	// addBatch adds the elements from batch to the output. It returns whether or
	// not the output changed its state to blocked (see implementations).
	addBatch(context.Context, coldata.Batch) bool
	// cancel tells the output to stop producing batches.
	cancel(ctx context.Context)
}

// getDefaultRouterOutputBlockedThreshold returns the number of unread values
// buffered by the routerOutputOp after which the output is considered blocked.
// It is a function rather than a variable so that in tests we could modify
// coldata.BatchSize() (if it were a variable, then its value would be
// evaluated before we set the desired batch size).
func getDefaultRouterOutputBlockedThreshold() int {
	return coldata.BatchSize() * 2
}

type routerOutputOp struct {
	// input is a reference to our router.
	input execinfra.OpNode

	types []coltypes.T

	// unblockedEventsChan is signaled when a routerOutput changes state from
	// blocked to unblocked.
	unblockedEventsChan chan<- struct{}

	mu struct {
		syncutil.Mutex
		cond              *sync.Cond
		done              bool
		zeroBatchEnqueued bool
		// data is a spillingQueue, a circular buffer backed by a disk queue.
		data      *spillingQueue
		numUnread int
		blocked   bool
	}

	// These fields default to defaultRouterOutputBlockedThreshold and
	// coldata.BatchSize() but are modified by tests to test edge cases.
	// blockedThreshold is the number of buffered values above which we consider
	// a router output to be blocked.
	blockedThreshold int
	outputBatchSize  int
}

func (o *routerOutputOp) ChildCount(verbose bool) int {
	return 1
}

func (o *routerOutputOp) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return o.input
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

var _ Operator = &routerOutputOp{}

// newRouterOutputOp creates a new router output. The caller must ensure that
// unblockedEventsChan is a buffered channel, as the router output will write to
// it. The provided allocator must not have a hard limit. The passed in
// memoryLimit will act as a soft limit to allow the router output to use disk
// when it is exceeded.
func newRouterOutputOp(
	unlimitedAllocator *Allocator,
	types []coltypes.T,
	unblockedEventsChan chan<- struct{},
	memoryLimit int64,
	cfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
) *routerOutputOp {
	return newRouterOutputOpWithBlockedThresholdAndBatchSize(unlimitedAllocator, types, unblockedEventsChan, memoryLimit, cfg, fdSemaphore, getDefaultRouterOutputBlockedThreshold(), coldata.BatchSize())
}

func newRouterOutputOpWithBlockedThresholdAndBatchSize(
	unlimitedAllocator *Allocator,
	types []coltypes.T,
	unblockedEventsChan chan<- struct{},
	memoryLimit int64,
	cfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	blockedThreshold int,
	outputBatchSize int,
) *routerOutputOp {
	o := &routerOutputOp{
		types:               types,
		unblockedEventsChan: unblockedEventsChan,
		blockedThreshold:    blockedThreshold,
		outputBatchSize:     outputBatchSize,
	}
	o.mu.cond = sync.NewCond(&o.mu)
	o.mu.data = newSpillingQueue(unlimitedAllocator, types, memoryLimit, cfg, fdSemaphore, outputBatchSize)

	return o
}

func (o *routerOutputOp) Init() {}

// Next returns the next coldata.Batch from the routerOutputOp. Note that Next
// is designed for only one concurrent caller and will block until data is
// ready.
func (o *routerOutputOp) Next(ctx context.Context) coldata.Batch {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mu.done {
		return coldata.ZeroBatch
	}
	for (o.mu.data.empty() && !o.mu.zeroBatchEnqueued) && !o.mu.done {
		// Wait until there is data to read or the output is canceled.
		o.mu.cond.Wait()
	}
	if o.mu.done {
		return coldata.ZeroBatch
	}
	b, err := o.mu.data.dequeue()
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	o.mu.numUnread -= b.Length()
	if o.mu.numUnread <= o.blockedThreshold {
		o.maybeUnblockLocked()
	}
	if b.Length() == 0 {
		// This is the last batch. closeLocked will set done to protect against
		// further calls to Next since this is allowed by the interface as well as
		// cleaning up and releasing possible disk infrastructure.
		o.closeLocked(ctx)
	}
	//fmt.Printf("%p: Next batch, len=%d\n", o, b.Length())
	return b
}

func (o *routerOutputOp) closeLocked(ctx context.Context) {
	o.mu.done = true
	if err := o.mu.data.close(); err != nil {
		// This log message is Info instead of Warning because the flow will also
		// attempt to clean up the parent directory, so this failure might not have
		// any effect.
		log.Infof(ctx, "error closing vectorized hash router output, files may be left over: %s", err)
	}
}

// cancel wakes up a reader in Next if there is one and results in the output
// returning zero length batches for every Next call after cancel. Note that
// all accumulated data that hasn't been read will not be returned.
func (o *routerOutputOp) cancel(ctx context.Context) {
	o.mu.Lock()
	o.closeLocked(ctx)
	// Some goroutine might be waiting on the condition variable, so wake it up.
	// Note that read goroutines check o.mu.done, so won't wait on the condition
	// variable after we unlock the mutex.
	o.mu.cond.Signal()
	o.mu.Unlock()
}

// addBatch adds batch to the output.
// True is returned if the the output changes state to blocked (note: if the
// output is already blocked, false is returned).
// TODO(asubiotto): We should explore pipelining addBatch if disk-spilling
//  performance becomes a concern. The main router goroutine will be writing to
//  disk as the code is written, meaning that we impact the performance of
//  writing rows to a fast output if we have to write to disk for a single
//  slow output.
func (o *routerOutputOp) addBatch(ctx context.Context, batch coldata.Batch) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	//fmt.Printf("%p: add batch, len=%d\n", o, batch.Length())
	if batch.Length() == 0 {
		if err := o.mu.data.enqueue(ctx, batch); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		o.mu.zeroBatchEnqueued = true
		o.mu.cond.Signal()
		return false
	}
	o.mu.numUnread += batch.Length()
	if err := o.mu.data.enqueue(ctx, batch); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	stateChanged := false
	if o.mu.numUnread > o.blockedThreshold && !o.mu.blocked {
		// The output is now blocked.
		o.mu.blocked = true
		stateChanged = true
	}
	o.mu.cond.Signal()
	return stateChanged
}

// maybeUnblockLocked unblocks the router output if it is in a blocked state. If the
// output was previously in a blocked state, an event will be sent on
// routerOutputOp.unblockedEventsChan.
func (o *routerOutputOp) maybeUnblockLocked() {
	if o.mu.blocked {
		o.mu.blocked = false
		o.unblockedEventsChan <- struct{}{}
	}
}

// reset resets the routerOutputOp for a benchmark run.
func (o *routerOutputOp) reset() {
	o.mu.Lock()
	o.mu.done = false
	o.mu.data.reset()
	o.mu.numUnread = 0
	o.mu.blocked = false
	o.mu.zeroBatchEnqueued = false
	o.mu.Unlock()
}

// HashRouter hashes values according to provided hash columns and computes a
// destination for each row. These destinations are exposed as Operators
// returned by the constructor.
type HashRouter struct {
	OneInputNode
	// types are the input coltypes.
	types []coltypes.T
	// hashCols is a slice of indices of the columns used for hashing.
	hashCols []uint32

	// One output for each stream.
	outputs []routerOutput

	// unblockedEventsChan is a channel shared between the HashRouter and its
	// outputs. outputs send events on this channel when they are unblocked by a
	// read.
	unblockedEventsChan <-chan struct{}
	numBlockedOutputs   int

	mu struct {
		syncutil.Mutex
		bufferedMeta []execinfrapb.ProducerMetadata
	}

	scratchSel []int
	// tupleDistributor is used to decide to which output a particular tuple
	// should be routed.
	tupleDistributor *tupleHashDistributor
}

// NewHashRouter creates a new hash router that consumes coldata.Batches from
// input and hashes each row according to hashCols to one of the outputs
// returned as Operators.
// The number of allocators provided will determine the number of outputs
// returned. Note that each allocator must be unlimited, memory will be limited
// by comparing memory use in the allocator with the memoryLimit argument. Each
// Operator must have an independent allocator (this means that each allocator
// should be linked to an independent mem account) as Operator.Next will usually
// be called concurrently between different outputs.
func NewHashRouter(
	unlimitedAllocators []*Allocator,
	input Operator,
	types []coltypes.T,
	hashCols []uint32,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
) (*HashRouter, []Operator) {
	if diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeDefault {
		execerror.VectorizedInternalPanic(errors.Errorf("hash router instantiated with incompatible disk queue cache mode: %d", diskQueueCfg.CacheMode))
	}
	outputs := make([]routerOutput, len(unlimitedAllocators))
	outputsAsOps := make([]Operator, len(unlimitedAllocators))
	// unblockEventsChan is buffered to 2*numOutputs as we don't want the outputs
	// writing to it to block.
	// Unblock events only happen after a corresponding block event. Since these
	// are state changes and are done under lock (including the output sending
	// on the channel, which is why we want the channel to be buffered in the
	// first place), every time the HashRouter blocks an output, it *must* read
	// all unblock events preceding it since these *must* be on the channel.
	unblockEventsChan := make(chan struct{}, 2*len(unlimitedAllocators))
	memoryLimitPerOutput := memoryLimit / int64(len(unlimitedAllocators))
	for i := range unlimitedAllocators {
		op := newRouterOutputOp(unlimitedAllocators[i], types, unblockEventsChan, memoryLimitPerOutput, diskQueueCfg, fdSemaphore)
		outputs[i] = op
		outputsAsOps[i] = op
	}
	router := newHashRouterWithOutputs(input, types, hashCols, unblockEventsChan, outputs)
	for i := range outputs {
		outputs[i].(*routerOutputOp).input = router
	}
	return router, outputsAsOps
}

func newHashRouterWithOutputs(
	input Operator,
	types []coltypes.T,
	hashCols []uint32,
	unblockEventsChan <-chan struct{},
	outputs []routerOutput,
) *HashRouter {
	r := &HashRouter{
		OneInputNode:        NewOneInputNode(input),
		types:               types,
		hashCols:            hashCols,
		outputs:             outputs,
		unblockedEventsChan: unblockEventsChan,
		scratchSel:          make([]int, coldata.BatchSize()),
		tupleDistributor:    newTupleHashDistributor(defaultInitHashValue, len(outputs)),
	}
	return r
}

// Run runs the HashRouter. Batches are read from the input and pushed to an
// output calculated by hashing columns. Cancel the given context to terminate
// early.
func (r *HashRouter) Run(ctx context.Context) {
	r.input.Init()
	cancelOutputs := func(err error) {
		if err != nil {
			r.mu.Lock()
			r.mu.bufferedMeta = append(r.mu.bufferedMeta, execinfrapb.ProducerMetadata{Err: err})
			r.mu.Unlock()
		}
		for _, o := range r.outputs {
			o.cancel(ctx)
		}
	}
	var done bool
	processNextBatch := func() {
		done = r.processNextBatch(ctx)
	}
	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			cancelOutputs(ctx.Err())
			return
		default:
		}

		// Read all the routerOutput state changes that have happened since the
		// last iteration.
		for moreToRead := true; moreToRead; {
			select {
			case <-r.unblockedEventsChan:
				r.numBlockedOutputs--
			default:
				// No more routerOutput state changes to read without blocking.
				moreToRead = false
			}
		}

		if r.numBlockedOutputs == len(r.outputs) {
			// All outputs are blocked, wait until at least one output is unblocked.
			select {
			case <-r.unblockedEventsChan:
				r.numBlockedOutputs--
			case <-ctx.Done():
				cancelOutputs(ctx.Err())
				return
			}
		}

		if err := execerror.CatchVectorizedRuntimeError(processNextBatch); err != nil {
			cancelOutputs(err)
			return
		}
		if done {
			// The input was done and we have notified the routerOutputs that there
			// is no more data.
			return
		}
	}
}

// processNextBatch reads the next batch from its input, hashes it and adds
// each column to its corresponding output, returning whether the input is
// done.
func (r *HashRouter) processNextBatch(ctx context.Context) bool {
	b := r.input.Next(ctx)
	n := b.Length()
	if n == 0 {
		// Done. Push an empty batch to outputs to tell them the data is done as
		// well.
		for _, o := range r.outputs {
			o.addBatch(ctx, b)
		}
		return true
	}

	selections := r.tupleDistributor.distribute(ctx, b, r.types, r.hashCols)
	// We will be modifying the selection vector on the batch, so we need to
	// store its original state.
	//usesSel := b.Selection() != nil
	//var origSel []int
	//if usesSel {
	//	copy(r.scratchSel[:n], b.Selection()[:n])
	//	origSel = r.scratchSel
	//}
	for i, o := range r.outputs {
		b.SetSelection(true)
		batchSel := b.Selection()
		outputSel := selections[i]
		if len(outputSel) > 0 {
			//if usesSel {
			// We need to remap the output selection using the original selection
			// vector on the batch.
			//batchSel = batchSel[:len(outputSel)]
			//for i, selIdx := range outputSel {
			//	batchSel[i] = origSel[selIdx]
			//}
			//} else {
			// There is no selection vector on the batch itself, so we can copy the
			// "distributed" selection directly.
			copy(batchSel[:len(outputSel)], outputSel)
			//}
			b.SetLength(len(outputSel))
			if o.addBatch(ctx, b) {
				// This batch blocked the output.
				r.numBlockedOutputs++
			}
		}
	}
	//b.SetSelection(usesSel)
	//if usesSel {
	//	copy(b.Selection()[:n], origSel[:n])
	//}
	//b.SetLength(n)
	return false
}

// reset resets the HashRouter for a benchmark run.
func (r *HashRouter) reset() {
	if i, ok := r.input.(resetter); ok {
		i.reset()
	}
	r.numBlockedOutputs = 0
	for moreToRead := true; moreToRead; {
		select {
		case <-r.unblockedEventsChan:
		default:
			moreToRead = false
		}
	}
	for _, o := range r.outputs {
		o.(resetter).reset()
	}
}

// DrainMeta is part of the MetadataGenerator interface.
func (r *HashRouter) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta := r.mu.bufferedMeta
	r.mu.bufferedMeta = r.mu.bufferedMeta[:0]
	return meta
}
