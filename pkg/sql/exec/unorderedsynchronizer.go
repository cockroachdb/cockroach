// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
)

// unorderedSynchronizerMsg is a light wrapper over a coldata.Batch sent over a
// channel so that the main goroutine can know which input this message
// originated from.
type unorderedSynchronizerMsg struct {
	inputIdx int
	b        coldata.Batch
}

// UnorderedSynchronizer is an Operator that combines multiple Operator streams
// into one.
type UnorderedSynchronizer struct {
	inputs []Operator
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

	initialized bool
	done        bool
	zeroBatch   coldata.Batch
	// externalWaitGroup refers to the WaitGroup passed in externally. Since the
	// UnorderedSynchronizer spawns goroutines, this allows callers to wait for
	// the completion of these goroutines.
	externalWaitGroup *sync.WaitGroup
	// internalWaitGroup refers to the WaitGroup internally managed by the
	// UnorderedSynchronizer. This will only ever be incremented by the
	// UnorderedSynchronizer and decremented by the input goroutines. This allows
	// the UnorderedSynchronizer to wait only on internal goroutines.
	internalWaitGroup *sync.WaitGroup
	cancelFn          context.CancelFunc
	batchCh           chan *unorderedSynchronizerMsg
	errCh             chan error
}

// NewUnorderedSynchronizer creates a new UnorderedSynchronizer. On the first
// call to Next, len(inputs) goroutines are spawned to read each input
// asynchronously (to not be limited by a slow input). These will increment
// the passed-in WaitGroup and decrement when done. It is also guaranteed that
// these spawned goroutines will have completed on any error or zero-length
// batch received from Next.
func NewUnorderedSynchronizer(
	inputs []Operator, typs []types.T, wg *sync.WaitGroup,
) *UnorderedSynchronizer {
	readNextBatch := make([]chan struct{}, len(inputs))
	for i := range readNextBatch {
		// Buffer readNextBatch chans to allow for non-blocking writes. There will
		// only be one message on the channel at a time.
		readNextBatch[i] = make(chan struct{}, 1)
	}
	zeroBatch := coldata.NewMemBatchWithSize(typs, 0)
	zeroBatch.SetLength(0)
	return &UnorderedSynchronizer{
		inputs:            inputs,
		readNextBatch:     readNextBatch,
		zeroBatch:         zeroBatch,
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
func (s *UnorderedSynchronizer) Init() {
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
func (s *UnorderedSynchronizer) init(ctx context.Context) {
	ctx, s.cancelFn = contextutil.WithCancel(ctx)
	for i, input := range s.inputs {
		s.externalWaitGroup.Add(1)
		s.internalWaitGroup.Add(1)
		// TODO(asubiotto): Most inputs are Inboxes, and these have handler
		// goroutines just sitting around waiting for cancellation. I wonder if we
		// could reuse those goroutines to push batches to batchCh directly.
		go func(input Operator, inputIdx int) {
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
				var b coldata.Batch
				if err := CatchVectorizedRuntimeError(func() { b = input.Next(ctx) }); err != nil {
					select {
					// Non-blocking write to errCh, if an error is present the main
					// goroutine will use that and cancel all inputs.
					case s.errCh <- err:
					default:
					}
					return
				}
				if b.Length() == 0 {
					return
				}
				msg.b = b
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
func (s *UnorderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	if s.done {
		return s.zeroBatch
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
			panic(err)
		}
	case msg := <-s.batchCh:
		if msg == nil {
			// All inputs have exited, double check that this is indeed the case.
			s.internalWaitGroup.Wait()
			// Check if this was a graceful termination or not.
			select {
			case err := <-s.errCh:
				if err != nil {
					panic(err)
				}
			default:
			}
			s.done = true
			return s.zeroBatch
		}
		s.lastReadInputIdx = msg.inputIdx
		return msg.b
	}
	return nil
}
