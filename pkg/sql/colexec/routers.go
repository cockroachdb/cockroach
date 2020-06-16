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
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// routerOutput is an interface implemented by router outputs. It exists for
// easier test mocking of outputs.
type routerOutput interface {
	execinfra.OpNode
	// initWithHashRouter passes a reference to the HashRouter that will be
	// pushing batches to this output.
	initWithHashRouter(*HashRouter)
	// addBatch adds the elements specified by the selection vector from batch to
	// the output. It returns whether or not the output changed its state to
	// blocked (see implementations).
	addBatch(context.Context, coldata.Batch, []int) bool
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

type routerOutputOpState int

const (
	// routerOutputOpRunning is the state in which routerOutputOp operates
	// normally. The router output transitions into the draining state when
	// either it is finished (when a zero-length batch was added or when it was
	// canceled) or it encounters an error.
	routerOutputOpRunning routerOutputOpState = iota
	// routerOutputOpDraining is the state in which routerOutputOp always
	// returns zero-length batches on calls to Next.
	routerOutputOpDraining
)

// drainCoordinator is an interface that the HashRouter implements to coordinate
// cancellation of all of its outputs in the case of an error and draining in
// the case of graceful termination.
// WARNING: No locks should be held when calling these methods, as the
// HashRouter might call routerOutput methods (e.g. cancel) that attempt to
// reacquire locks.
type drainCoordinator interface {
	// encounteredError should be called when a routerOutput encounters an error.
	// This terminates execution. No locks should be held when calling this
	// method, since cancellation could occur.
	encounteredError(context.Context, error)
	// drainMeta should be called exactly once when the routerOutput moves to
	// draining.
	drainMeta() []execinfrapb.ProducerMetadata
}

type routerOutputOp struct {
	// input is a reference to our router.
	input execinfra.OpNode
	// drainCoordinator is a reference to the HashRouter to be able to notify it
	// if the output encounters an error or transitions to a draining state.
	drainCoordinator drainCoordinator

	types []*types.T

	// unblockedEventsChan is signaled when a routerOutput changes state from
	// blocked to unblocked.
	unblockedEventsChan chan<- struct{}

	mu struct {
		syncutil.Mutex
		state routerOutputOpState
		// unlimitedAllocator tracks the memory usage of this router output,
		// providing a signal for when it should spill to disk.
		// The memory lifecycle is as follows:
		//
		// o.mu.pendingBatch is allocated as a "staging" area. Tuples are copied
		// into it in addBatch.
		// A read may come in in this state, in which case pendingBatch is returned
		// and references to it are removed. Since batches are unsafe for reuse,
		// the batch is also manually released from the allocator.
		// If a read does not come in and the batch becomes full of tuples, that
		// batch is stored in o.mu.data, which is a queue with an in-memory circular
		// buffer backed by disk. If the batch fits in memory, a reference to it
		// is retained and a new pendingBatch is allocated.
		//
		// If a read comes in at this point, the batch is dequeued from o.mu.data
		// and returned, but the memory is still accounted for. In fact, memory use
		// increases up to when o.mu.data is full and must spill to disk.
		// Once it spills to disk, the spillingQueue (o.mu.data), will release
		// batches it spills to disk to stop accounting for them.
		// The tricky part comes when o.mu.data is dequeued from. In this case, the
		// reference for a previously-returned batch is overwritten with an on-disk
		// batch, so the memory for the overwritten batch is released, while the
		// new batch's memory is retained. Note that if batches are being dequeued
		// from disk, it must be the case that the circular buffer is now empty,
		// holding references to batches that have been previously returned.
		//
		// In short, batches whose references are retained are also retained in the
		// allocator, but if any references are overwritten or lost, those batches
		// are released.
		unlimitedAllocator *colmem.Allocator
		cond               *sync.Cond
		// pendingBatch is a partially-filled batch with data added through
		// addBatch. Once this batch reaches capacity, it is flushed to data. The
		// main use of pendingBatch is coalescing various fragmented batches into
		// one.
		pendingBatch coldata.Batch
		// data is a spillingQueue, a circular buffer backed by a disk queue.
		data      *spillingQueue
		numUnread int
		blocked   bool
	}

	testingKnobs routerOutputOpTestingKnobs
}

func (o *routerOutputOp) ChildCount(verbose bool) int {
	return 1
}

func (o *routerOutputOp) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return o.input
	}
	colexecerror.InternalError(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

var _ colexecbase.Operator = &routerOutputOp{}

type routerOutputOpTestingKnobs struct {
	// blockedThreshold is the number of buffered values above which we consider
	// a router output to be blocked. It defaults to
	// defaultRouterOutputBlockedThreshold but can be modified by tests to test
	// edge cases.
	blockedThreshold int
	// outputBatchSize defaults to coldata.BatchSize() but can be modified by
	// tests to test edge cases.
	outputBatchSize int
	// alwaysFlush, if set to true, will always flush o.mu.pendingBatch to
	// o.mu.data.
	alwaysFlush bool
	// addBatchTestInducedErrorCb is called after any function call that could
	// produce an error if that error is nil. If the callback returns an error,
	// the router output overwrites the nil error with the returned error.
	// It is guaranteed that this callback will be called at least once during
	// normal execution.
	addBatchTestInducedErrorCb func() error
	// nextTestInducedErrorCb is called after any function call that could
	// produce an error if that error is nil. If the callback returns an error,
	// the router output overwrites the nil error with the returned error.
	// It is guaranteed that this callback will be called at least once during
	// normal execution.
	nextTestInducedErrorCb func() error
}

// routerOutputOpArgs are the arguments to newRouterOutputOp. All fields apart
// from the testing knobs are optional.
type routerOutputOpArgs struct {
	// All fields are required unless marked optional.
	types []*types.T

	// unlimitedAllocator should not have a memory limit. Pass in a soft
	// memoryLimit that will be respected instead.
	unlimitedAllocator *colmem.Allocator
	// memoryLimit acts as a soft limit to allow the router output to use disk
	// when it is exceeded.
	memoryLimit int64
	diskAcc     *mon.BoundAccount
	cfg         colcontainer.DiskQueueCfg
	fdSemaphore semaphore.Semaphore

	// unblockedEventsChan must be a buffered channel.
	unblockedEventsChan chan<- struct{}

	testingKnobs routerOutputOpTestingKnobs
}

// newRouterOutputOp creates a new router output.
func newRouterOutputOp(args routerOutputOpArgs) *routerOutputOp {
	if args.testingKnobs.blockedThreshold == 0 {
		args.testingKnobs.blockedThreshold = getDefaultRouterOutputBlockedThreshold()
	}
	if args.testingKnobs.outputBatchSize == 0 {
		args.testingKnobs.outputBatchSize = coldata.BatchSize()
	}

	o := &routerOutputOp{
		types:               args.types,
		unblockedEventsChan: args.unblockedEventsChan,
		testingKnobs:        args.testingKnobs,
	}
	o.mu.unlimitedAllocator = args.unlimitedAllocator
	o.mu.cond = sync.NewCond(&o.mu)
	o.mu.data = newSpillingQueue(
		args.unlimitedAllocator,
		args.types,
		args.memoryLimit,
		args.cfg,
		args.fdSemaphore,
		args.testingKnobs.outputBatchSize,
		args.diskAcc,
	)

	return o
}

func (o *routerOutputOp) Init() {}

// nextErrorLocked is a helper method that handles an error encountered in Next.
func (o *routerOutputOp) nextErrorLocked(ctx context.Context, err error) {
	o.mu.state = routerOutputOpDraining
	o.maybeUnblockLocked()
	// Unlock the mutex, since the HashRouter will cancel all outputs.
	o.mu.Unlock()
	o.drainCoordinator.encounteredError(ctx, err)
	o.mu.Lock()
	colexecerror.InternalError(err)
}

// Next returns the next coldata.Batch from the routerOutputOp. Note that Next
// is designed for only one concurrent caller and will block until data is
// ready.
func (o *routerOutputOp) Next(ctx context.Context) coldata.Batch {
	o.mu.Lock()
	defer o.mu.Unlock()
	for o.mu.state == routerOutputOpRunning && o.mu.pendingBatch == nil && o.mu.data.empty() {
		// Wait until there is data to read or the output is canceled.
		o.mu.cond.Wait()
	}
	if o.mu.state == routerOutputOpDraining {
		return coldata.ZeroBatch
	}
	var b coldata.Batch
	if o.mu.pendingBatch != nil && o.mu.data.empty() {
		// o.mu.data is empty (i.e. nothing has been flushed to the spillingQueue),
		// but there is a o.mu.pendingBatch that has not been flushed yet. Return
		// this batch directly.
		b = o.mu.pendingBatch
		o.mu.unlimitedAllocator.ReleaseBatch(b)
		o.mu.pendingBatch = nil
	} else {
		var err error
		b, err = o.mu.data.dequeue(ctx)
		if err == nil && o.testingKnobs.nextTestInducedErrorCb != nil {
			err = o.testingKnobs.nextTestInducedErrorCb()
		}
		if err != nil {
			o.nextErrorLocked(ctx, err)
		}
	}
	o.mu.numUnread -= b.Length()
	if o.mu.numUnread <= o.testingKnobs.blockedThreshold {
		o.maybeUnblockLocked()
	}
	if b.Length() == 0 {
		if o.testingKnobs.nextTestInducedErrorCb != nil {
			if err := o.testingKnobs.nextTestInducedErrorCb(); err != nil {
				o.nextErrorLocked(ctx, err)
			}
		}
		// This is the last batch. closeLocked will set done to protect against
		// further calls to Next since this is allowed by the interface as well as
		// cleaning up and releasing possible disk infrastructure.
		o.closeLocked(ctx)
	}
	return b
}

func (o *routerOutputOp) DrainMeta(_ context.Context) []execinfrapb.ProducerMetadata {
	o.mu.Lock()
	o.mu.state = routerOutputOpDraining
	o.maybeUnblockLocked()
	o.mu.Unlock()
	return o.drainCoordinator.drainMeta()
}

func (o *routerOutputOp) initWithHashRouter(r *HashRouter) {
	o.input = r
	o.drainCoordinator = r
}

func (o *routerOutputOp) closeLocked(ctx context.Context) {
	o.mu.state = routerOutputOpDraining
	if err := o.mu.data.close(ctx); err != nil {
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
	defer o.mu.Unlock()
	o.closeLocked(ctx)
	// Some goroutine might be waiting on the condition variable, so wake it up.
	// Note that read goroutines check o.mu.done, so won't wait on the condition
	// variable after we unlock the mutex.
	o.mu.cond.Signal()
}

// addBatch copies the columns in batch according to selection into an internal
// buffer.
// The routerOutputOp only adds the elements specified by selection. Therefore,
// an empty selection slice will add no elements. Note that the selection vector
// on the batch is ignored. This is so that callers of addBatch can push the
// same batch with different selection vectors to many different outputs.
// True is returned if the the output changes state to blocked (note: if the
// output is already blocked, false is returned).
// TODO(asubiotto): We should explore pipelining addBatch if disk-spilling
//  performance becomes a concern. The main router goroutine will be writing to
//  disk as the code is written, meaning that we impact the performance of
//  writing rows to a fast output if we have to write to disk for a single
//  slow output.
func (o *routerOutputOp) addBatch(ctx context.Context, batch coldata.Batch, selection []int) bool {
	if len(selection) > batch.Length() {
		selection = selection[:batch.Length()]
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mu.state == routerOutputOpDraining {
		// This output is draining, discard any data.
		return false
	}

	if batch.Length() == 0 {
		if o.mu.pendingBatch != nil {
			err := o.mu.data.enqueue(ctx, o.mu.pendingBatch)
			if err == nil && o.testingKnobs.addBatchTestInducedErrorCb != nil {
				err = o.testingKnobs.addBatchTestInducedErrorCb()
			}
			if err != nil {
				colexecerror.InternalError(err)
			}
		} else if o.testingKnobs.addBatchTestInducedErrorCb != nil {
			// This is the last chance to run addBatchTestInducedErorCb if it has
			// been set.
			if err := o.testingKnobs.addBatchTestInducedErrorCb(); err != nil {
				colexecerror.InternalError(err)
			}
		}
		o.mu.pendingBatch = coldata.ZeroBatch
		o.mu.cond.Signal()
		return false
	}

	if len(selection) == 0 {
		// Non-zero batch with no selection vector. Nothing to do.
		return false
	}

	// Increment o.mu.numUnread before going into the loop, as we will consume
	// selection.
	o.mu.numUnread += len(selection)

	for toAppend := len(selection); toAppend > 0; {
		if o.mu.pendingBatch == nil {
			o.mu.pendingBatch = o.mu.unlimitedAllocator.NewMemBatchWithSize(o.types, o.testingKnobs.outputBatchSize)
		}
		available := o.testingKnobs.outputBatchSize - o.mu.pendingBatch.Length()
		numAppended := toAppend
		if toAppend > available {
			numAppended = available
		}
		o.mu.unlimitedAllocator.PerformOperation(o.mu.pendingBatch.ColVecs(), func() {
			for i := range o.types {
				o.mu.pendingBatch.ColVec(i).Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Src:       batch.ColVec(i),
							Sel:       selection[:numAppended],
							DestIdx:   o.mu.pendingBatch.Length(),
							SrcEndIdx: numAppended,
						},
					},
				)
			}
		})
		newLength := o.mu.pendingBatch.Length() + numAppended
		o.mu.pendingBatch.SetLength(newLength)
		if o.testingKnobs.alwaysFlush || newLength >= o.testingKnobs.outputBatchSize {
			// The capacity in o.mu.pendingBatch has been filled.
			err := o.mu.data.enqueue(ctx, o.mu.pendingBatch)
			if err == nil && o.testingKnobs.addBatchTestInducedErrorCb != nil {
				err = o.testingKnobs.addBatchTestInducedErrorCb()
			}
			if err != nil {
				colexecerror.InternalError(err)
			}
			o.mu.pendingBatch = nil
		}
		toAppend -= numAppended
		selection = selection[numAppended:]
	}

	stateChanged := false
	if o.mu.numUnread > o.testingKnobs.blockedThreshold && !o.mu.blocked {
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

// resetForBenchmarks resets the routerOutputOp for a benchmark run.
func (o *routerOutputOp) resetForBenchmarks(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.mu.state = routerOutputOpRunning
	o.mu.data.reset(ctx)
	o.mu.numUnread = 0
	o.mu.blocked = false
}

// hashRouterDrainState is a state that specifically describes the hashRouter's
// state in the draining process. This differs from its "general" state. For
// example, a hash router can have drained and exited the Run method but still
// be in hashRouterDrainStateRunning until somebody calls drainMeta.
type hashRouterDrainState int

const (
	// hashRouterDrainStateRunning is the state that a hashRouter is in when
	// running normally (i.e. pulling and pushing batches).
	hashRouterDrainStateRunning = iota
	// hashRouterDrainStateRequested is the state that a hashRouter is in when
	// either all outputs have called drainMeta or an error was encountered by one
	// of the outputs.
	hashRouterDrainStateRequested
	// hashRouterDrainStateCompleted is the state that a hashRouter is in when
	// draining has completed.
	hashRouterDrainStateCompleted
)

// HashRouter hashes values according to provided hash columns and computes a
// destination for each row. These destinations are exposed as Operators
// returned by the constructor.
type HashRouter struct {
	OneInputNode
	// types are the input types.
	types []*types.T
	// hashCols is a slice of indices of the columns used for hashing.
	hashCols []uint32

	// One output for each stream.
	outputs []routerOutput
	// metadataSources is a slice of execinfrapb.MetadataSources that need to be
	// drained when the HashRouter terminates.
	metadataSources execinfrapb.MetadataSources
	// closers is a slice of IdempotentClosers that need to be closed when the
	// hash router terminates.
	closers []IdempotentCloser

	// unblockedEventsChan is a channel shared between the HashRouter and its
	// outputs. outputs send events on this channel when they are unblocked by a
	// read.
	unblockedEventsChan <-chan struct{}
	numBlockedOutputs   int

	bufferedMeta []execinfrapb.ProducerMetadata

	// atomics is shared state between the Run goroutine and any routerOutput
	// goroutines that call drainMeta.
	atomics struct {
		// drainState is the state the hashRouter is in. The Run goroutine should
		// only ever read these states, never set them.
		drainState        int32
		numDrainedOutputs int32
	}

	// waitForMetadata is a channel that the last output to drain will read from
	// to pass on any metadata buffered through the Run goroutine.
	waitForMetadata chan []execinfrapb.ProducerMetadata

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
// be called concurrently between different outputs. Similarly, each output
// needs to have a separate disk account.
func NewHashRouter(
	unlimitedAllocators []*colmem.Allocator,
	input colexecbase.Operator,
	types []*types.T,
	hashCols []uint32,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAccounts []*mon.BoundAccount,
	toDrain []execinfrapb.MetadataSource,
	toClose []IdempotentCloser,
) (*HashRouter, []colexecbase.DrainableOperator) {
	if diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeDefault {
		colexecerror.InternalError(errors.Errorf("hash router instantiated with incompatible disk queue cache mode: %d", diskQueueCfg.CacheMode))
	}
	outputs := make([]routerOutput, len(unlimitedAllocators))
	outputsAsOps := make([]colexecbase.DrainableOperator, len(unlimitedAllocators))
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
		op := newRouterOutputOp(
			routerOutputOpArgs{
				types:               types,
				unlimitedAllocator:  unlimitedAllocators[i],
				memoryLimit:         memoryLimitPerOutput,
				diskAcc:             diskAccounts[i],
				cfg:                 diskQueueCfg,
				fdSemaphore:         fdSemaphore,
				unblockedEventsChan: unblockEventsChan,
			},
		)
		outputs[i] = op
		outputsAsOps[i] = op
	}
	return newHashRouterWithOutputs(input, types, hashCols, unblockEventsChan, outputs, toDrain, toClose), outputsAsOps
}

func newHashRouterWithOutputs(
	input colexecbase.Operator,
	types []*types.T,
	hashCols []uint32,
	unblockEventsChan <-chan struct{},
	outputs []routerOutput,
	toDrain []execinfrapb.MetadataSource,
	toClose []IdempotentCloser,
) *HashRouter {
	r := &HashRouter{
		OneInputNode:        NewOneInputNode(input),
		types:               types,
		hashCols:            hashCols,
		outputs:             outputs,
		closers:             toClose,
		metadataSources:     toDrain,
		unblockedEventsChan: unblockEventsChan,
		// waitForMetadata is a buffered channel to avoid blocking if nobody will
		// read the metadata.
		waitForMetadata:  make(chan []execinfrapb.ProducerMetadata, 1),
		tupleDistributor: newTupleHashDistributor(defaultInitHashValue, len(outputs)),
	}
	for i := range outputs {
		outputs[i].initWithHashRouter(r)
	}
	return r
}

// bufferErr buffers the given error to be returned by one of the router outputs.
func (r *HashRouter) bufferErr(err error) {
	if err == nil {
		return
	}
	r.bufferedMeta = append(r.bufferedMeta, execinfrapb.ProducerMetadata{Err: err})
}

func (r *HashRouter) cancelOutputs(ctx context.Context) {
	for _, o := range r.outputs {
		if err := colexecerror.CatchVectorizedRuntimeError(func() {
			o.cancel(ctx)
		}); err != nil {
			r.bufferErr(err)
		}
	}
}

func (r *HashRouter) setDrainState(drainState hashRouterDrainState) {
	atomic.StoreInt32(&r.atomics.drainState, int32(drainState))
}

func (r *HashRouter) getDrainState() hashRouterDrainState {
	return hashRouterDrainState(atomic.LoadInt32(&r.atomics.drainState))
}

// Run runs the HashRouter. Batches are read from the input and pushed to an
// output calculated by hashing columns. Cancel the given context to terminate
// early.
func (r *HashRouter) Run(ctx context.Context) {
	// Since HashRouter runs in a separate goroutine, we want to be safe and
	// make sure that we catch errors in all code paths, so we wrap the whole
	// method with a catcher. Note that we also have "internal" catchers as
	// well for more fine-grained control of error propagation.
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		r.input.Init()
		// bufferErrAndCancelOutputs buffers non-nil error as metadata, cancels all
		// of the outputs additionally buffering any error if such occurs during the
		// outputs' cancellation as metadata as well. Note that it attempts to
		// cancel every output regardless of whether "previous" output's
		// cancellation succeeds.
		bufferErrAndCancelOutputs := func(err error) {
			r.bufferErr(err)
			r.cancelOutputs(ctx)
		}
		var done bool
		processNextBatch := func() {
			done = r.processNextBatch(ctx)
		}
		for {
			if r.getDrainState() != hashRouterDrainStateRunning {
				break
			}

			// Check for cancellation.
			select {
			case <-ctx.Done():
				bufferErrAndCancelOutputs(ctx.Err())
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
					bufferErrAndCancelOutputs(ctx.Err())
					return
				}
			}

			if err := colexecerror.CatchVectorizedRuntimeError(processNextBatch); err != nil {
				bufferErrAndCancelOutputs(err)
				return
			}
			if done {
				// The input was done and we have notified the routerOutputs that there
				// is no more data.
				return
			}
		}
	}); err != nil {
		r.bufferErr(err)
	}

	// Non-blocking send of metadata so that one of the outputs can return it
	// in DrainMeta.
	r.bufferedMeta = append(r.bufferedMeta, r.metadataSources.DrainMeta(ctx)...)
	r.waitForMetadata <- r.bufferedMeta
	close(r.waitForMetadata)

	for _, closer := range r.closers {
		if err := closer.IdempotentClose(ctx); err != nil {
			if log.V(1) {
				log.Infof(ctx, "error closing IdempotentCloser: %v", err)
			}
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
			o.addBatch(ctx, b, nil)
		}
		return true
	}

	selections := r.tupleDistributor.distribute(ctx, b, r.types, r.hashCols)
	for i, o := range r.outputs {
		if o.addBatch(ctx, b, selections[i]) {
			// This batch blocked the output.
			r.numBlockedOutputs++
		}
	}
	return false
}

// resetForBenchmarks resets the HashRouter for a benchmark run.
func (r *HashRouter) resetForBenchmarks(ctx context.Context) {
	if i, ok := r.input.(resetter); ok {
		i.reset(ctx)
	}
	r.setDrainState(hashRouterDrainStateRunning)
	r.waitForMetadata = make(chan []execinfrapb.ProducerMetadata, 1)
	r.atomics.numDrainedOutputs = 0
	r.bufferedMeta = nil
	r.numBlockedOutputs = 0
	for moreToRead := true; moreToRead; {
		select {
		case <-r.unblockedEventsChan:
		default:
			moreToRead = false
		}
	}
	for _, o := range r.outputs {
		if op, ok := o.(*routerOutputOp); ok {
			op.resetForBenchmarks(ctx)
		}
	}
}

func (r *HashRouter) encounteredError(ctx context.Context, err error) {
	// Once one output returns an error the hash router needs to stop running
	// and drain its input.
	r.setDrainState(hashRouterDrainStateRequested)
	// cancel all outputs. The Run goroutine will eventually realize that the
	// HashRouter is done and exit without draining.
	r.cancelOutputs(ctx)
}

func (r *HashRouter) drainMeta() []execinfrapb.ProducerMetadata {
	if int(atomic.AddInt32(&r.atomics.numDrainedOutputs, 1)) != len(r.outputs) {
		return nil
	}
	// All outputs have been drained, return any buffered metadata to the last
	// output to call drainMeta.
	r.setDrainState(hashRouterDrainStateRequested)
	meta := <-r.waitForMetadata
	r.setDrainState(hashRouterDrainStateCompleted)
	return meta
}
