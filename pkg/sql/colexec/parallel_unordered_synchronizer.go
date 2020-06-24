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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/errors"
)

// unorderedSynchronizerMsg is a light wrapper over a coldata.Batch sent over a
// channel so that the main goroutine can know which input this message
// originated from.
type unorderedSynchronizerMsg struct {
	inputIdx int
	b        coldata.Batch
	meta     []execinfrapb.ProducerMetadata
}

var _ colexecbase.Operator = &ParallelUnorderedSynchronizer{}
var _ execinfra.OpNode = &ParallelUnorderedSynchronizer{}

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
	inputs []SynchronizerInput
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
	cancelFn          context.CancelFunc
	batchCh           chan *unorderedSynchronizerMsg
	errCh             chan error

	// bufferedMeta is the metadata buffered during a
	// ParallelUnorderedSynchronizer run.
	bufferedMeta []execinfrapb.ProducerMetadata
}

// ChildCount implements the execinfra.OpNode interface.
func (s *ParallelUnorderedSynchronizer) ChildCount(verbose bool) int {
	return len(s.inputs)
}

// Child implements the execinfra.OpNode interface.
func (s *ParallelUnorderedSynchronizer) Child(nth int, verbose bool) execinfra.OpNode {
	return s.inputs[nth].Op
}

// SynchronizerInput is a wrapper over a colexecbase.Operator that a
// synchronizer goroutine will be calling Next on. An accompanying
// []execinfrapb.MetadataSource may also be specified, in which case
// DrainMeta will be called from the same goroutine.
type SynchronizerInput struct {
	// Op is the input Operator.
	Op colexecbase.Operator
	// MetadataSources are metadata sources in the input tree that should be
	// drained in the same goroutine as Op.
	MetadataSources execinfrapb.MetadataSources
}

func operatorsToSynchronizerInputs(ops []colexecbase.Operator) []SynchronizerInput {
	result := make([]SynchronizerInput, len(ops))
	for i := range result {
		result[i].Op = ops[i]
	}
	return result
}

// NewParallelUnorderedSynchronizer creates a new ParallelUnorderedSynchronizer.
// On the first call to Next, len(inputs) goroutines are spawned to read each
// input asynchronously (to not be limited by a slow input). These will
// increment the passed-in WaitGroup and decrement when done. It is also
// guaranteed that these spawned goroutines will have completed on any error or
// zero-length batch received from Next.
func NewParallelUnorderedSynchronizer(
	inputs []SynchronizerInput, wg *sync.WaitGroup,
) *ParallelUnorderedSynchronizer {
	readNextBatch := make([]chan struct{}, len(inputs))
	for i := range readNextBatch {
		// Buffer readNextBatch chans to allow for non-blocking writes. There will
		// only be one message on the channel at a time.
		readNextBatch[i] = make(chan struct{}, 1)
	}
	return &ParallelUnorderedSynchronizer{
		inputs:            inputs,
		readNextBatch:     readNextBatch,
		batches:           make([]coldata.Batch, len(inputs)),
		nextBatch:         make([]func(), len(inputs)),
		externalWaitGroup: wg,
		internalWaitGroup: &sync.WaitGroup{},
		batchCh:           make(chan *unorderedSynchronizerMsg, len(inputs)),
		// errCh is buffered so that writers do not block. If errCh is full, the
		// input goroutines will not push an error and exit immediately, given that
		// the Next goroutine will read an error and panic anyway.
		errCh: make(chan error, 1),
	}
}

// Init is part of the Operator interface.
func (s *ParallelUnorderedSynchronizer) Init() {
	for _, input := range s.inputs {
		input.Op.Init()
	}
}

func (s *ParallelUnorderedSynchronizer) getState() parallelUnorderedSynchronizerState {
	return parallelUnorderedSynchronizerState(atomic.LoadInt32(&s.state))
}

func (s *ParallelUnorderedSynchronizer) setState(state parallelUnorderedSynchronizerState) {
	atomic.SwapInt32(&s.state, int32(state))
}

// init starts one goroutine per input to read from each input asynchronously
// and push to batchCh. Canceling the context results in all goroutines
// terminating, otherwise they keep on pushing batches until a zero-length batch
// is encountered. Once all inputs terminate, s.batchCh is closed. If an error
// occurs, the goroutines will make a non-blocking best effort to push that
// error on s.errCh, resulting in the first error pushed to be observed by the
// Next goroutine. Inputs are asynchronous so that the synchronizer is minimally
// affected by slow inputs.
func (s *ParallelUnorderedSynchronizer) init(ctx context.Context) {
	s.setState(parallelUnorderedSynchronizerStateRunning)
	var (
		cancelFn context.CancelFunc
		// internalCancellation is an atomic that will be set to 1 if cancelFn is
		// called (i.e. this is an internal cancellation), so that input goroutines
		// know not propagate this cancellation.
		internalCancellation int32
	)
	ctx, cancelFn = contextutil.WithCancel(ctx)
	s.cancelFn = func() {
		atomic.StoreInt32(&internalCancellation, 1)
		cancelFn()
	}
	for i, input := range s.inputs {
		s.nextBatch[i] = func(input SynchronizerInput, inputIdx int) func() {
			return func() {
				s.batches[inputIdx] = input.Op.Next(ctx)
			}
		}(input, i)
		s.externalWaitGroup.Add(1)
		s.internalWaitGroup.Add(1)
		// TODO(asubiotto): Most inputs are Inboxes, and these have handler
		// goroutines just sitting around waiting for cancellation. I wonder if we
		// could reuse those goroutines to push batches to batchCh directly.
		go func(input SynchronizerInput, inputIdx int) {
			defer func() {
				if int(atomic.AddUint32(&s.numFinishedInputs, 1)) == len(s.inputs) {
					close(s.batchCh)
				}
				s.internalWaitGroup.Done()
				s.externalWaitGroup.Done()
			}()
			sendErr := func(err error) {
				if strings.Contains(err.Error(), context.Canceled.Error()) && atomic.LoadInt32(&internalCancellation) == 1 {
					// Don't propagate an internal cancellation error.
					return
				}
				select {
				// Non-blocking write to errCh, if an error is present the main
				// goroutine will use that and cancel all inputs.
				case s.errCh <- err:
				default:
				}
			}
			msg := &unorderedSynchronizerMsg{
				inputIdx: inputIdx,
			}
			for {
				state := s.getState()
				switch state {
				case parallelUnorderedSynchronizerStateRunning:
					if err := colexecerror.CatchVectorizedRuntimeError(s.nextBatch[inputIdx]); err != nil {
						sendErr(err)
						return
					}
					msg.b = s.batches[inputIdx]
					if s.batches[inputIdx].Length() != 0 {
						// Send the batch.
						break
					}
					// In case of a zero-length batch, proceed to drain the input.
					fallthrough
				case parallelUnorderedSynchronizerStateDraining:
					if input.MetadataSources != nil {
						msg.meta = input.MetadataSources.DrainMeta(ctx)
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
				case <-ctx.Done():
					sendErr(ctx.Err())
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
				case <-ctx.Done():
					sendErr(ctx.Err())
					return
				}
			}
		}(input, i)
	}
}

// Next is part of the Operator interface.
func (s *ParallelUnorderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	for {
		state := s.getState()
		switch state {
		case parallelUnorderedSynchronizerStateDone:
			return coldata.ZeroBatch
		case parallelUnorderedSynchronizerStateUninitialized:
			s.init(ctx)
		case parallelUnorderedSynchronizerStateRunning:
			// Signal the input whose batch we returned in the last call to Next that it
			// is safe to retrieve the next batch. Since Next has been called, we can
			// reuse memory instead of making safe copies of batches returned.
			s.notifyInputToReadNextBatch(s.lastReadInputIdx)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unhandled state in ParallelUnorderedSynchronizer Next goroutine: %d", state))
		}

		select {
		case err := <-s.errCh:
			if err != nil {
				// If we got an error from one of our inputs, cancel all inputs and
				// propagate this error through a panic.
				s.cancelFn()
				s.internalWaitGroup.Wait()
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

// DrainMeta is part of the MetadataSource interface.
func (s *ParallelUnorderedSynchronizer) DrainMeta(
	ctx context.Context,
) []execinfrapb.ProducerMetadata {
	prevState := s.getState()
	s.setState(parallelUnorderedSynchronizerStateDraining)
	if prevState == parallelUnorderedSynchronizerStateUninitialized {
		s.init(ctx)
	} else {
		// The inputs were initialized in Next. This means that the inputs will
		// either read the new draining state and exit, or have yet to read the
		// state, in which case they are either pushing a batch or waiting for a
		// signal to get the next batch. These latter inputs are preempted below by
		// draining batchCh and sending a signal to the inputs whose batches have
		// been read.
		// However, there is also a case where a batch was read in Next and that
		// input is waiting for preemption, so do that here.
		// Note that if this is not the case (i.e. inputs were initialized but the
		// synchronizer immediately transitioned to draining) this preemption won't
		// hurt.
		s.notifyInputToReadNextBatch(s.lastReadInputIdx)
	}
	for msg := <-s.batchCh; msg != nil; msg = <-s.batchCh {
		if msg.meta == nil {
			// This input goroutine pushed a batch to the batchCh and is waiting to be
			// told to proceed. Notify it that it is ok to do so.
			s.notifyInputToReadNextBatch(msg.inputIdx)
			continue
		}
		s.bufferedMeta = append(s.bufferedMeta, msg.meta...)
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
	s.setState(parallelUnorderedSynchronizerStateDone)
	return s.bufferedMeta
}
