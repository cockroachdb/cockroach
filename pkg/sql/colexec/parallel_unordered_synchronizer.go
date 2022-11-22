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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// unorderedSynchronizerMsg is a light wrapper over a coldata.Batch or metadata
// sent over a channel so that the main goroutine can know which input this
// message originated from.
// Note that either a batch or metadata must be sent, but not both.
type unorderedSynchronizerMsg struct {
	b        coldata.Batch
	meta     []execinfrapb.ProducerMetadata
	inputIdx int
}

var _ colexecop.Operator = &ParallelUnorderedSynchronizer{}
var _ execopnode.OpNode = &ParallelUnorderedSynchronizer{}

type parallelUnorderedSynchronizerState int

const (
	// parallelUnorderedSynchronizerStateUninitialized is the state the
	// ParallelUnorderedSynchronizer is in when not yet initialized.
	parallelUnorderedSynchronizerStateUninitialized = iota
	// parallelUnorderedSynchronizerStateRunning is the state the
	// ParallelUnorderedSynchronizer is in when all input goroutines have been
	// spawned and are returning batches.
	parallelUnorderedSynchronizerStateRunning
	// parallelUnorderedSynchronizerStateDraining is the state the
	// ParallelUnorderedSynchronizer is in when a drain has been requested through
	// DrainMeta. All input goroutines will call DrainMeta on its input and exit.
	parallelUnorderedSynchronizerStateDraining
	// parallelUnorderedSyncrhonizerStateDone is the state the
	// ParallelUnorderedSynchronizer is in when draining has completed.
	parallelUnorderedSynchronizerStateDone
)

// ParallelUnorderedSynchronizer is an Operator that combines multiple Operator streams
// into one.
type ParallelUnorderedSynchronizer struct {
	colexecop.InitHelper

	allocator *colmem.Allocator
	inputs    []colexecargs.OpWithMetaInfo
	inputCtxs []context.Context
	// cancelLocalInput stores context cancellation functions for each of the
	// inputs. The functions are populated only if LocalPlan is true.
	cancelLocalInput []context.CancelFunc
	// LocalPlan indicates whether this synchronizer is a part of the fully
	// local plan.
	LocalPlan    bool
	tracingSpans []*tracing.Span
	// readNextBatch is a slice of channels, where each channel corresponds to the
	// input at the same index in inputs. It is used as a barrier for input
	// goroutines to wait on until the Next goroutine signals that it is safe to
	// retrieve the next batch. This is done so that inputs that are running
	// asynchronously do not overwrite batches returned previously, given that
	// batches must be safe for reuse until the next call to Next.
	readNextBatch []chan struct{}
	// numFinishedInputs is incremented atomically whenever one of the provided
	// inputs exits from a goroutine (gracefully or otherwise).
	numFinishedInputs uint32
	// lastReadInputIdx is the index of the input whose batch we last returned.
	// Used so that on the next call to Next, we can resume the input.
	lastReadInputIdx int
	// batches are the last batches read from the corresponding input.
	batches []coldata.Batch
	// nextBatch is a slice of functions each of which obtains a next batch from
	// the corresponding to it input.
	nextBatch []func()

	state int32
	// externalWaitGroup refers to the WaitGroup passed in externally. Since the
	// ParallelUnorderedSynchronizer spawns goroutines, this allows callers to
	// wait for the completion of these goroutines.
	externalWaitGroup *sync.WaitGroup
	// internalWaitGroup refers to the WaitGroup internally managed by the
	// ParallelUnorderedSynchronizer. This will only ever be incremented by the
	// ParallelUnorderedSynchronizer and decremented by the input goroutines. This
	// allows the ParallelUnorderedSynchronizer to wait only on internal
	// goroutines.
	internalWaitGroup *sync.WaitGroup
	batchCh           chan *unorderedSynchronizerMsg
	errCh             chan error

	// bufferedMeta is the metadata buffered during a
	// ParallelUnorderedSynchronizer run.
	bufferedMeta []execinfrapb.ProducerMetadata
}

var _ colexecop.DrainableClosableOperator = &ParallelUnorderedSynchronizer{}

// ChildCount implements the execopnode.OpNode interface.
func (s *ParallelUnorderedSynchronizer) ChildCount(verbose bool) int {
	return len(s.inputs)
}

// Child implements the execopnode.OpNode interface.
func (s *ParallelUnorderedSynchronizer) Child(nth int, verbose bool) execopnode.OpNode {
	return s.inputs[nth].Root
}

// NewParallelUnorderedSynchronizer creates a new ParallelUnorderedSynchronizer.
// On the first call to Next, len(inputs) goroutines are spawned to read each
// input asynchronously (to not be limited by a slow input). These will
// increment the passed-in WaitGroup and decrement when done. It is also
// guaranteed that these spawned goroutines will have completed on any error or
// zero-length batch received from Next.
// - allocator must use a memory account that is not shared with any other user.
func NewParallelUnorderedSynchronizer(
	allocator *colmem.Allocator, inputs []colexecargs.OpWithMetaInfo, wg *sync.WaitGroup,
) *ParallelUnorderedSynchronizer {
	readNextBatch := make([]chan struct{}, len(inputs))
	for i := range readNextBatch {
		// Buffer readNextBatch chans to allow for non-blocking writes. There will
		// only be one message on the channel at a time.
		readNextBatch[i] = make(chan struct{}, 1)
	}
	return &ParallelUnorderedSynchronizer{
		allocator:         allocator,
		inputs:            inputs,
		inputCtxs:         make([]context.Context, len(inputs)),
		cancelLocalInput:  make([]context.CancelFunc, len(inputs)),
		tracingSpans:      make([]*tracing.Span, len(inputs)),
		readNextBatch:     readNextBatch,
		batches:           make([]coldata.Batch, len(inputs)),
		nextBatch:         make([]func(), len(inputs)),
		externalWaitGroup: wg,
		internalWaitGroup: &sync.WaitGroup{},
		// batchCh is a buffered channel in order to offer non-blocking writes to
		// input goroutines. During normal operation, this channel will have at most
		// len(inputs) messages. However, during DrainMeta, inputs might need to
		// push an extra metadata message without blocking, hence the need to double
		// the size of this channel.
		batchCh: make(chan *unorderedSynchronizerMsg, len(inputs)*2),
		// errCh is buffered so that writers do not block. If errCh is full, the
		// input goroutines will not push an error and exit immediately, given that
		// the Next goroutine will read an error and panic anyway.
		errCh: make(chan error, 1),
	}
}

// Init is part of the colexecop.Operator interface.
func (s *ParallelUnorderedSynchronizer) Init(ctx context.Context) {
	if !s.InitHelper.Init(ctx) {
		return
	}
	for i, input := range s.inputs {
		s.inputCtxs[i], s.tracingSpans[i] = execinfra.ProcessorSpan(s.Ctx, fmt.Sprintf("parallel unordered sync input %d", i))
		if s.LocalPlan {
			// If there plan is local, there are no colrpc.Inboxes in this input
			// tree, and the synchronizer can cancel the current work eagerly
			// when transitioning into draining.
			//
			// If there plan is distributed, there might be an inbox in the
			// input tree, and the synchronizer cannot cancel the work eagerly
			// because canceling the context would break the gRPC stream and
			// make it impossible to fetch the remote metadata. Furthermore, it
			// will result in the remote flow cancellation.
			s.inputCtxs[i], s.cancelLocalInput[i] = context.WithCancel(s.inputCtxs[i])
		}
		input.Root.Init(s.inputCtxs[i])
		s.nextBatch[i] = func(inputOp colexecop.Operator, inputIdx int) func() {
			return func() {
				s.batches[inputIdx] = inputOp.Next()
			}
		}(input.Root, i)
	}
}

func (s *ParallelUnorderedSynchronizer) getState() parallelUnorderedSynchronizerState {
	return parallelUnorderedSynchronizerState(atomic.LoadInt32(&s.state))
}

func (s *ParallelUnorderedSynchronizer) setState(state parallelUnorderedSynchronizerState) {
	atomic.SwapInt32(&s.state, int32(state))
}

// init starts one goroutine per input to read from each input asynchronously
// and push to batchCh. Canceling the context (passed in Init() above) results
// in all goroutines terminating, otherwise they keep on pushing batches until a
// zero-length batch is encountered. Once all inputs terminate, s.batchCh is
// closed. If an error occurs, the goroutines will make a non-blocking best
// effort to push that error on s.errCh, resulting in the first error pushed to
// be observed by the Next goroutine. Inputs are asynchronous so that the
// synchronizer is minimally affected by slow inputs.
func (s *ParallelUnorderedSynchronizer) init() {
	for i, input := range s.inputs {
		s.externalWaitGroup.Add(1)
		s.internalWaitGroup.Add(1)
		// TODO(asubiotto): Most inputs are Inboxes, and these have handler
		// goroutines just sitting around waiting for cancellation. I wonder if we
		// could reuse those goroutines to push batches to batchCh directly.
		go func(input colexecargs.OpWithMetaInfo, inputIdx int) {
			span := s.tracingSpans[inputIdx]
			defer func() {
				if span != nil {
					defer span.Finish()
				}
				if int(atomic.AddUint32(&s.numFinishedInputs, 1)) == len(s.inputs) {
					close(s.batchCh)
				}
				s.internalWaitGroup.Done()
				s.externalWaitGroup.Done()
			}()
			sendErr := func(err error) {
				select {
				// Non-blocking write to errCh, if an error is present the main
				// goroutine will use that and cancel all inputs.
				case s.errCh <- err:
				default:
				}
			}
			if s.nextBatch[inputIdx] == nil {
				// The initialization of this input wasn't successful, so it is
				// invalid to call Next or DrainMeta on it. Exit early.
				return
			}
			msg := &unorderedSynchronizerMsg{
				inputIdx: inputIdx,
			}
			for {
				state := s.getState()
				switch state {
				case parallelUnorderedSynchronizerStateRunning:
					if err := colexecerror.CatchVectorizedRuntimeError(s.nextBatch[inputIdx]); err != nil {
						if s.getState() == parallelUnorderedSynchronizerStateDraining && s.Ctx.Err() == nil && s.cancelLocalInput[inputIdx] != nil {
							// The synchronizer has just transitioned into the
							// draining state and eagerly canceled work of this
							// input. That cancellation is likely to manifest
							// itself as the context.Canceled error, but it
							// could be another error too; in any case, we will
							// swallow the error because the user of the
							// synchronizer is only interested in the metadata
							// at this point.
							continue
						}
						sendErr(err)
						// After we encounter an error, we proceed to draining.
						// If this is a context cancellation, we'll realize that
						// in the select below, so the drained meta will be
						// ignored, for all other errors the drained meta will
						// be sent to the coordinator goroutine.
						s.setState(parallelUnorderedSynchronizerStateDraining)
						continue
					}
					msg.b = s.batches[inputIdx]
					if s.batches[inputIdx].Length() != 0 {
						// Send the batch.
						break
					}
					// In case of a zero-length batch, proceed to drain the input.
					fallthrough
				case parallelUnorderedSynchronizerStateDraining:
					// Create a new message for metadata. The previous message cannot be
					// overwritten since it might still be in the channel.
					msg = &unorderedSynchronizerMsg{
						inputIdx: inputIdx,
					}
					if span != nil {
						for _, s := range input.StatsCollectors {
							span.RecordStructured(s.GetStats())
						}
						if meta := execinfra.GetTraceDataAsMetadata(span); meta != nil {
							msg.meta = append(msg.meta, *meta)
						}
					}
					if input.MetadataSources != nil {
						msg.meta = append(msg.meta, input.MetadataSources.DrainMeta()...)
					}
					if msg.meta == nil {
						// Initialize msg.meta to be non-nil, which is a signal that
						// metadata has been drained.
						msg.meta = make([]execinfrapb.ProducerMetadata, 0)
					}
				default:
					sendErr(errors.AssertionFailedf("unhandled state in ParallelUnorderedSynchronizer input goroutine: %d", state))
					return
				}
				// Check msg.meta before sending over the channel since the channel is
				// the synchronization primitive of meta.
				sentMeta := false
				if msg.meta != nil {
					sentMeta = true
				}
				select {
				case <-s.Ctx.Done():
					sendErr(s.Ctx.Err())
					return
				case s.batchCh <- msg:
				}

				if sentMeta {
					// The input has been drained and this input has pushed the metadata
					// over the channel, exit.
					return
				}

				// Wait until Next goroutine tells us we are good to go.
				select {
				case <-s.readNextBatch[inputIdx]:
				case <-s.Ctx.Done():
					sendErr(s.Ctx.Err())
					return
				}
			}
		}(input, i)
	}
}

// Next is part of the colexecop.Operator interface.
func (s *ParallelUnorderedSynchronizer) Next() coldata.Batch {
	for {
		state := s.getState()
		switch state {
		case parallelUnorderedSynchronizerStateDone:
			return coldata.ZeroBatch
		case parallelUnorderedSynchronizerStateUninitialized:
			s.setState(parallelUnorderedSynchronizerStateRunning)
			s.init()
		case parallelUnorderedSynchronizerStateRunning:
			// Signal the input whose batch we returned in the last call to Next that it
			// is safe to retrieve the next batch. Since Next has been called, we can
			// reuse memory instead of making safe copies of batches returned.
			s.notifyInputToReadNextBatch(s.lastReadInputIdx)
		case parallelUnorderedSynchronizerStateDraining:
			// One of the inputs has just encountered an error. We do nothing
			// here and will read that error from the errCh below.
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled state in ParallelUnorderedSynchronizer Next goroutine: %d", state))
		}

		select {
		case err := <-s.errCh:
			if err != nil {
				// If we got an error from one of our inputs, propagate this error
				// through a panic. The caller should then proceed to call DrainMeta,
				// which will take care of closing any inputs.
				colexecerror.InternalError(err)
			}
		case msg := <-s.batchCh:
			if msg == nil {
				// All inputs have exited, double check that this is indeed the case.
				s.internalWaitGroup.Wait()
				// Check if this was a graceful termination or not.
				select {
				case err := <-s.errCh:
					if err != nil {
						colexecerror.InternalError(err)
					}
				default:
				}
				s.setState(parallelUnorderedSynchronizerStateDone)
				return coldata.ZeroBatch
			}
			s.lastReadInputIdx = msg.inputIdx
			if msg.meta != nil {
				colexecutils.AccountForMetadata(s.allocator, msg.meta)
				s.bufferedMeta = append(s.bufferedMeta, msg.meta...)
				continue
			}
			return msg.b
		}
	}
}

// notifyInputToReadNextBatch is a non-blocking send to notify the given input
// that it may proceed to read the next batch from the input. Refer to the
// comment of the readNextBatch field in ParallelUnorderedSynchronizer for more
// information.
func (s *ParallelUnorderedSynchronizer) notifyInputToReadNextBatch(inputIdx int) {
	select {
	// This write is non-blocking because if the channel is full, it must be the
	// case that there is a pending message for the input to proceed.
	case s.readNextBatch[inputIdx] <- struct{}{}:
	default:
	}
}

// DrainMeta is part of the colexecop.MetadataSource interface.
func (s *ParallelUnorderedSynchronizer) DrainMeta() []execinfrapb.ProducerMetadata {
	prevState := s.getState()
	s.setState(parallelUnorderedSynchronizerStateDraining)
	if prevState == parallelUnorderedSynchronizerStateUninitialized {
		s.init()
	}
	// Cancel all local inputs (we will still wait for all remote ones to
	// return the next batch).
	for _, cancelFunc := range s.cancelLocalInput {
		if cancelFunc != nil {
			cancelFunc()
		}
	}

	// Non-blocking drain of batchCh. This is important mostly because of the
	// following edge case: all n inputs have pushed batches to the batchCh, so
	// there are currently n messages. Next notifies the last read input to
	// retrieve the next batch but encounters an error. There are now n+1 messages
	// in batchCh. Notifying all these inputs to read the next batch would result
	// in 2n+1 messages on batchCh, which would cause a deadlock since this
	// goroutine blocks on the wait group, but an input will block on writing to
	// batchCh. This is a best effort, but note that for this scenario to occur,
	// there *must* be at least one message in batchCh (the message belonging to
	// the input that was notified).
	for batchChDrained := false; !batchChDrained; {
		select {
		case msg := <-s.batchCh:
			if msg == nil {
				batchChDrained = true
			} else if msg.meta != nil {
				s.bufferedMeta = append(s.bufferedMeta, msg.meta...)
			}
		default:
			batchChDrained = true
		}
	}

	// Unblock any goroutines currently waiting to be told to read the next batch.
	// This will force all inputs to observe the new draining state.
	for _, ch := range s.readNextBatch {
		close(ch)
	}

	// Wait for all inputs to exit.
	s.internalWaitGroup.Wait()

	// Drain the batchCh, this reads the metadata that was pushed.
	for msg := <-s.batchCh; msg != nil; msg = <-s.batchCh {
		if msg.meta != nil {
			s.bufferedMeta = append(s.bufferedMeta, msg.meta...)
		}
	}

	// Buffer any errors that may have happened without blocking on the channel.
	for exitLoop := false; !exitLoop; {
		select {
		case err := <-s.errCh:
			s.bufferedMeta = append(s.bufferedMeta, execinfrapb.ProducerMetadata{Err: err})
		default:
			exitLoop = true
		}
	}

	// Done.
	s.setState(parallelUnorderedSynchronizerStateDone)
	bufferedMeta := s.bufferedMeta
	// Eagerly lose the reference to the metadata since it might be of
	// non-trivial footprint.
	s.bufferedMeta = nil
	// The caller takes ownership of the metadata, so we can release all of the
	// allocations.
	s.allocator.ReleaseAll()
	return bufferedMeta
}

// Close is part of the colexecop.ClosableOperator interface.
func (s *ParallelUnorderedSynchronizer) Close(ctx context.Context) error {
	if state := s.getState(); state != parallelUnorderedSynchronizerStateUninitialized {
		// Input goroutines have been started and will take care of finishing
		// the tracing spans.
		return nil
	}
	// If the synchronizer is in "uninitialized" state, it means that the
	// goroutines for each input haven't been started, so they won't be able to
	// finish their tracing spans. In such a scenario the synchronizer must do
	// that on its own.
	for i, span := range s.tracingSpans {
		if span != nil {
			span.Finish()
			s.tracingSpans[i] = nil
		}
	}
	return nil
}
