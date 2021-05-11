// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexechash"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	// addBatch adds the elements specified by the selection vector from batch
	// to the output. It returns whether or not the output changed its state to
	// blocked (see implementations).
	addBatch(context.Context, coldata.Batch) bool
	// cancel tells the output to stop producing batches. Optionally forwards an
	// error if not nil.
	cancel(context.Context, error)
	// forwardErr forwards an error to the output. The output should call
	// colexecerror.ExpectedError with this error on the next call to Next.
	// Calling forwardErr multiple times will result in the most recent error
	// overwriting the previous error.
	forwardErr(error)
	// resetForTests resets the routerOutput for a benchmark or test run.
	resetForTests(context.Context)
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
	// normally. The router output transitions into routerOutputDoneAdding when
	// a zero-length batch was added or routerOutputOpDraining when it
	// encounters an error or the drain is requested.
	routerOutputOpRunning routerOutputOpState = iota
	// routerOutputDoneAdding is the state in which a zero-length was batch was
	// added to routerOutputOp and no more batches will be added. The router
	// output transitions to routerOutputOpDraining when the output is canceled
	// (either closed or the drain is requested).
	routerOutputDoneAdding
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
	encounteredError(context.Context)
	// drainMeta should be called exactly once when the routerOutput moves to
	// draining.
	drainMeta() []execinfrapb.ProducerMetadata
}

type routerOutputOp struct {
	colexecop.InitHelper
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
		// forwardedErr is an error that was forwarded by the HashRouter. If set,
		// any subsequent calls to Next will return this error.
		forwardedErr error
		cond         *sync.Cond
		// data is a SpillingQueue, a circular buffer backed by a disk queue.
		data      *colexecutils.SpillingQueue
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
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

var _ colexecop.Operator = &routerOutputOp{}

type routerOutputOpTestingKnobs struct {
	// blockedThreshold is the number of buffered values above which we consider
	// a router output to be blocked. It defaults to
	// defaultRouterOutputBlockedThreshold but can be modified by tests to test
	// edge cases.
	blockedThreshold int
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

	o := &routerOutputOp{
		types:               args.types,
		unblockedEventsChan: args.unblockedEventsChan,
		testingKnobs:        args.testingKnobs,
	}
	o.mu.cond = sync.NewCond(&o.mu)
	o.mu.data = colexecutils.NewSpillingQueue(
		&colexecutils.NewSpillingQueueArgs{
			UnlimitedAllocator: args.unlimitedAllocator,
			Types:              args.types,
			MemoryLimit:        args.memoryLimit,
			DiskQueueCfg:       args.cfg,
			FDSemaphore:        args.fdSemaphore,
			DiskAcc:            args.diskAcc,
		},
	)

	return o
}

func (o *routerOutputOp) Init(ctx context.Context) {
	o.InitHelper.Init(ctx)
}

// nextErrorLocked is a helper method that handles an error encountered in Next.
func (o *routerOutputOp) nextErrorLocked(err error) {
	o.mu.state = routerOutputOpDraining
	o.maybeUnblockLocked()
	// Unlock the mutex, since the HashRouter will cancel all outputs.
	o.mu.Unlock()
	o.drainCoordinator.encounteredError(o.Ctx)
	o.mu.Lock()
	colexecerror.InternalError(err)
}

// Next returns the next coldata.Batch from the routerOutputOp. Note that Next
// is designed for only one concurrent caller and will block until data is
// ready.
func (o *routerOutputOp) Next() coldata.Batch {
	o.mu.Lock()
	defer o.mu.Unlock()
	for o.mu.forwardedErr == nil && o.mu.state == routerOutputOpRunning && o.mu.data.Empty() {
		// Wait until there is data to read or the output is canceled.
		o.mu.cond.Wait()
	}
	if o.mu.forwardedErr != nil {
		colexecerror.ExpectedError(o.mu.forwardedErr)
	}
	if o.mu.state == routerOutputOpDraining {
		return coldata.ZeroBatch
	}
	b, err := o.mu.data.Dequeue(o.Ctx)
	if err == nil && o.testingKnobs.nextTestInducedErrorCb != nil {
		err = o.testingKnobs.nextTestInducedErrorCb()
	}
	if err != nil {
		o.nextErrorLocked(err)
	}
	o.mu.numUnread -= b.Length()
	if o.mu.numUnread <= o.testingKnobs.blockedThreshold {
		o.maybeUnblockLocked()
	}
	if b.Length() == 0 {
		if o.testingKnobs.nextTestInducedErrorCb != nil {
			if err := o.testingKnobs.nextTestInducedErrorCb(); err != nil {
				o.nextErrorLocked(err)
			}
		}
		// This is the last batch. closeLocked will set done to protect against
		// further calls to Next since this is allowed by the interface as well as
		// cleaning up and releasing possible disk infrastructure.
		o.closeLocked(o.Ctx)
	}
	return b
}

func (o *routerOutputOp) DrainMeta() []execinfrapb.ProducerMetadata {
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
	if err := o.mu.data.Close(ctx); err != nil {
		// This log message is Info instead of Warning because the flow will also
		// attempt to clean up the parent directory, so this failure might not have
		// any effect.
		log.Infof(ctx, "error closing vectorized hash router output, files may be left over: %s", err)
	}
}

// cancel wakes up a reader in Next if there is one and results in the output
// returning zero length batches for every Next call after cancel. Note that
// all accumulated data that hasn't been read will not be returned.
func (o *routerOutputOp) cancel(ctx context.Context, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closeLocked(ctx)
	o.forwardErrLocked(err)
	// Some goroutine might be waiting on the condition variable, so wake it up.
	// Note that read goroutines check o.mu.done, so won't wait on the condition
	// variable after we unlock the mutex.
	o.mu.cond.Signal()
}

func (o *routerOutputOp) forwardErrLocked(err error) {
	if err != nil {
		o.mu.forwardedErr = err
	}
}

func (o *routerOutputOp) forwardErr(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.forwardErrLocked(err)
	o.mu.cond.Signal()
}

// addBatch copies the batch (according to its selection vector) into an
// internal buffer. Zero-length batch should be passed-in to indicate that no
// more batches will be added.
// TODO(asubiotto): We should explore pipelining addBatch if disk-spilling
//  performance becomes a concern. The main router goroutine will be writing to
//  disk as the code is written, meaning that we impact the performance of
//  writing rows to a fast output if we have to write to disk for a single
//  slow output.
func (o *routerOutputOp) addBatch(ctx context.Context, batch coldata.Batch) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch o.mu.state {
	case routerOutputDoneAdding:
		colexecerror.InternalError(errors.AssertionFailedf("a batch was added to routerOutput in DoneAdding state"))
	case routerOutputOpDraining:
		// This output is draining, discard any data.
		return false
	}

	o.mu.numUnread += batch.Length()
	o.mu.data.Enqueue(ctx, batch)
	if o.testingKnobs.addBatchTestInducedErrorCb != nil {
		if err := o.testingKnobs.addBatchTestInducedErrorCb(); err != nil {
			colexecerror.InternalError(err)
		}
	}

	if batch.Length() == 0 {
		o.mu.state = routerOutputDoneAdding
		o.mu.cond.Signal()
		return false
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

// resetForTests resets the routerOutputOp for a test or benchmark run.
func (o *routerOutputOp) resetForTests(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.mu.state = routerOutputOpRunning
	o.mu.forwardedErr = nil
	o.mu.data.Reset(ctx)
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
	colexecop.OneInputNode
	// inputMetaInfo contains all of the meta components that the hash router
	// is responsible for. Root field is exactly the same as OneInputNode.Input.
	inputMetaInfo colexecargs.OpWithMetaInfo
	// hashCols is a slice of indices of the columns used for hashing.
	hashCols []uint32

	// One output for each stream.
	outputs []routerOutput

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
	tupleDistributor *colexechash.TupleHashDistributor
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
	input colexecargs.OpWithMetaInfo,
	types []*types.T,
	hashCols []uint32,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAccounts []*mon.BoundAccount,
) (*HashRouter, []colexecop.DrainableOperator) {
	if diskQueueCfg.CacheMode != colcontainer.DiskQueueCacheModeDefault {
		colexecerror.InternalError(errors.Errorf("hash router instantiated with incompatible disk queue cache mode: %d", diskQueueCfg.CacheMode))
	}
	outputs := make([]routerOutput, len(unlimitedAllocators))
	outputsAsOps := make([]colexecop.DrainableOperator, len(unlimitedAllocators))
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
	return newHashRouterWithOutputs(input, hashCols, unblockEventsChan, outputs), outputsAsOps
}

func newHashRouterWithOutputs(
	input colexecargs.OpWithMetaInfo,
	hashCols []uint32,
	unblockEventsChan <-chan struct{},
	outputs []routerOutput,
) *HashRouter {
	r := &HashRouter{
		OneInputNode:        colexecop.NewOneInputNode(input.Root),
		inputMetaInfo:       input,
		hashCols:            hashCols,
		outputs:             outputs,
		unblockedEventsChan: unblockEventsChan,
		// waitForMetadata is a buffered channel to avoid blocking if nobody will
		// read the metadata.
		waitForMetadata:  make(chan []execinfrapb.ProducerMetadata, 1),
		tupleDistributor: colexechash.NewTupleHashDistributor(colexechash.DefaultInitHashValue, len(outputs)),
	}
	for i := range outputs {
		outputs[i].initWithHashRouter(r)
	}
	return r
}

// cancelOutputs cancels all outputs and forwards the given error to all of
// them if non-nil. The only case where the error is not forwarded is if no
// output could be canceled due to an error. In this case each output will
// forward the error returned during cancellation.
func (r *HashRouter) cancelOutputs(ctx context.Context, errToForward error) {
	for _, o := range r.outputs {
		if err := colexecerror.CatchVectorizedRuntimeError(func() {
			o.cancel(ctx, errToForward)
		}); err != nil {
			// If there was an error canceling this output, this error can be
			// forwarded to whoever is calling Next.
			o.forwardErr(err)
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
	var span *tracing.Span
	ctx, span = execinfra.ProcessorSpan(ctx, "hash router")
	if span != nil {
		defer span.Finish()
	}
	var inputInitialized bool
	// Since HashRouter runs in a separate goroutine, we want to be safe and
	// make sure that we catch errors in all code paths, so we wrap the whole
	// method with a catcher. Note that we also have "internal" catchers as
	// well for more fine-grained control of error propagation.
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		r.Input.Init(ctx)
		inputInitialized = true
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
				r.cancelOutputs(ctx, ctx.Err())
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
					r.cancelOutputs(ctx, ctx.Err())
					return
				}
			}

			if err := colexecerror.CatchVectorizedRuntimeError(processNextBatch); err != nil {
				r.cancelOutputs(ctx, err)
				return
			}
			if done {
				// The input was done and we have notified the routerOutputs that there
				// is no more data.
				return
			}
		}
	}); err != nil {
		r.cancelOutputs(ctx, err)
	}
	if inputInitialized {
		// Retrieving stats and draining the metadata is only safe if the input
		// to the hash router was properly initialized.
		if span != nil {
			for _, s := range r.inputMetaInfo.StatsCollectors {
				span.RecordStructured(s.GetStats())
			}
			if meta := execinfra.GetTraceDataAsMetadata(span); meta != nil {
				r.bufferedMeta = append(r.bufferedMeta, *meta)
			}
		}
		r.bufferedMeta = append(r.bufferedMeta, r.inputMetaInfo.MetadataSources.DrainMeta()...)
	}
	// Non-blocking send of metadata so that one of the outputs can return it
	// in DrainMeta.
	r.waitForMetadata <- r.bufferedMeta
	close(r.waitForMetadata)

	r.inputMetaInfo.ToClose.CloseAndLogOnErr(ctx, "hash router")
}

// processNextBatch reads the next batch from its input, hashes it and adds
// each column to its corresponding output, returning whether the input is
// done.
func (r *HashRouter) processNextBatch(ctx context.Context) bool {
	b := r.Input.Next()
	n := b.Length()
	if n == 0 {
		// Done. Push an empty batch to outputs to tell them the data is done as
		// well.
		for _, o := range r.outputs {
			o.addBatch(ctx, b)
		}
		return true
	}

	// It is ok that we call Init() on every batch since all calls except for
	// the first one are noops.
	r.tupleDistributor.Init(ctx)
	selections := r.tupleDistributor.Distribute(b, r.hashCols)
	for i, o := range r.outputs {
		if len(selections[i]) > 0 {
			b.SetSelection(true)
			copy(b.Selection(), selections[i])
			b.SetLength(len(selections[i]))
			if o.addBatch(ctx, b) {
				// This batch blocked the output.
				r.numBlockedOutputs++
			}
		}
	}
	return false
}

// resetForTests resets the HashRouter for a test or benchmark run.
func (r *HashRouter) resetForTests(ctx context.Context) {
	if i, ok := r.Input.(colexecop.Resetter); ok {
		i.Reset(ctx)
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
		o.resetForTests(ctx)
	}
}

func (r *HashRouter) encounteredError(ctx context.Context) {
	// Once one output returns an error the hash router needs to stop running
	// and drain its input.
	r.setDrainState(hashRouterDrainStateRequested)
	// cancel all outputs. The Run goroutine will eventually realize that the
	// HashRouter is done and exit without draining.
	r.cancelOutputs(ctx, nil /* errToForward */)
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
