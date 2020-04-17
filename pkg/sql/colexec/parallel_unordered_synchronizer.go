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
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colbase/vecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
)

// unorderedSynchronizerMsg is a light wrapper over a coldata.Batch sent over a
// channel so that the main goroutine can know which input this message
// originated from.
type unorderedSynchronizerMsg struct {
	inputIdx int
	b        coldata.Batch
}

var _ colbase.Operator = &ParallelUnorderedSynchronizer{}
var _ execinfra.OpNode = &ParallelUnorderedSynchronizer{}

// ParallelUnorderedSynchronizer is an Operator that combines multiple Operator streams
// into one.
type ParallelUnorderedSynchronizer struct {
	inputs []colbase.Operator
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

	initialized bool
	done        bool
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
}

// ChildCount implements the execinfra.OpNode interface.
func (s *ParallelUnorderedSynchronizer) ChildCount(verbose bool) int {
	return len(s.inputs)
}

// Child implements the execinfra.OpNode interface.
func (s *ParallelUnorderedSynchronizer) Child(nth int, verbose bool) execinfra.OpNode {
	return s.inputs[nth]
}

// NewParallelUnorderedSynchronizer creates a new ParallelUnorderedSynchronizer.
// On the first call to Next, len(inputs) goroutines are spawned to read each
// input asynchronously (to not be limited by a slow input). These will
// increment the passed-in WaitGroup and decrement when done. It is also
// guaranteed that these spawned goroutines will have completed on any error or
// zero-length batch received from Next.
func NewParallelUnorderedSynchronizer(
	inputs []colbase.Operator, typs []coltypes.T, wg *sync.WaitGroup,
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
		input.Init()
	}
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
	ctx, s.cancelFn = contextutil.WithCancel(ctx)
	for i, input := range s.inputs {
		s.nextBatch[i] = func(input colbase.Operator, inputIdx int) func() {
			return func() {
				s.batches[inputIdx] = input.Next(ctx)
			}
		}(input, i)
		s.externalWaitGroup.Add(1)
		s.internalWaitGroup.Add(1)
		// TODO(asubiotto): Most inputs are Inboxes, and these have handler
		// goroutines just sitting around waiting for cancellation. I wonder if we
		// could reuse those goroutines to push batches to batchCh directly.
		go func(input colbase.Operator, inputIdx int) {
			defer func() {
				if int(atomic.AddUint32(&s.numFinishedInputs, 1)) == len(s.inputs) {
					close(s.batchCh)
				}
				s.internalWaitGroup.Done()
				s.externalWaitGroup.Done()
			}()
			msg := &unorderedSynchronizerMsg{
				inputIdx: inputIdx,
			}
			for {
				if err := vecerror.CatchVectorizedRuntimeError(s.nextBatch[inputIdx]); err != nil {
					select {
					// Non-blocking write to errCh, if an error is present the main
					// goroutine will use that and cancel all inputs.
					case s.errCh <- err:
					default:
					}
					return
				}
				if s.batches[inputIdx].Length() == 0 {
					return
				}
				msg.b = s.batches[inputIdx]
				select {
				case <-ctx.Done():
					select {
					// Non-blocking write to errCh, if an error is present the main
					// goroutine will use that and cancel all inputs.
					case s.errCh <- ctx.Err():
					default:
					}
					return
				case s.batchCh <- msg:
				}

				// Wait until Next goroutine tells us we are good to go.
				select {
				case <-s.readNextBatch[inputIdx]:
				case <-ctx.Done():
					select {
					// Non-blocking write to errCh, if an error is present the main
					// goroutine will use that and cancel all inputs.
					case s.errCh <- ctx.Err():
					default:
					}
					return
				}
			}
		}(input, i)
	}
	s.initialized = true
}

// Next is part of the Operator interface.
func (s *ParallelUnorderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	if s.done {
		return coldata.ZeroBatch
	}
	if !s.initialized {
		s.init(ctx)
	} else {
		// Signal the input whose batch we returned in the last call to Next that it
		// is safe to retrieve the next batch. Since Next has been called, we can
		// reuse memory instead of making safe copies of batches returned.
		s.readNextBatch[s.lastReadInputIdx] <- struct{}{}
	}
	select {
	case err := <-s.errCh:
		if err != nil {
			// If we got an error from one of our inputs, cancel all inputs and
			// propagate this error through a panic.
			s.cancelFn()
			s.internalWaitGroup.Wait()
			vecerror.InternalError(err)
		}
	case msg := <-s.batchCh:
		if msg == nil {
			// All inputs have exited, double check that this is indeed the case.
			s.internalWaitGroup.Wait()
			// Check if this was a graceful termination or not.
			select {
			case err := <-s.errCh:
				if err != nil {
					vecerror.InternalError(err)
				}
			default:
			}
			s.done = true
			return coldata.ZeroBatch
		}
		s.lastReadInputIdx = msg.inputIdx
		return msg.b
	}
	return nil
}
