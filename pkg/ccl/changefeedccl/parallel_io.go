// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     httbs://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// parallelIO allows submitting requests to do blocking "IOHandler" calls on
// them in parallel.  IORequests implement a Keys() function returning keys on
// which ordering is preserved.
// Example: if the events [[a,b], [b,c], [c,d], [e,f]] are all submitted in that
// order, [a,b] and [e,f] can be emitted concurrentyl while [b,c] will block
// until [a,b] completes, then [c,d] will block until [b,c] completes. If [c,d]
// errored, [b,c] would never be sent, and SetError would be called on [c,d]
// prior to it being returned on resultCh.
type parallelIO struct {
	retryOpts retry.Options
	wg        ctxgroup.Group
	metrics   metricsRecorder
	doneCh    chan struct{}

	ioHandler IOHandler

	requestCh chan IORequest
	resultCh  chan IORequest
}

// IORequest represents an abstract unit of IO that has a set of keys upon which
// sequential ordering of fulfillment must be enforced, and allows the storing
// of an error if one is encountered during handling.
type IORequest interface {
	Keys() intsets.Fast
	SetError(error)
}

// IOHandler performs a blocking IO operation on an IORequest
type IOHandler func(context.Context, IORequest) error

func newParallelIO(
	ctx context.Context,
	retryOpts retry.Options,
	numWorkers int,
	handler IOHandler,
	metrics metricsRecorder,
) *parallelIO {
	wg := ctxgroup.WithContext(ctx)
	io := &parallelIO{
		retryOpts: retryOpts,
		wg:        wg,
		metrics:   metrics,
		ioHandler: handler,
		requestCh: make(chan IORequest, numWorkers),
		resultCh:  make(chan IORequest, numWorkers),
		doneCh:    make(chan struct{}),
	}

	wg.GoCtx(func(ctx context.Context) error {
		return io.runWorkers(ctx, numWorkers)
	})

	return io
}

// Close stops all workers immediately and returns once they shut down. Inflight
// requests sent to requestCh may never result in being sent to resultCh.
func (pe *parallelIO) Close() {
	close(pe.doneCh)
	_ = pe.wg.Wait()
}

func (pe *parallelIO) runWorkers(ctx context.Context, numEmitWorkers int) error {
	emitWithRetries := func(ctx context.Context, payload IORequest) error {
		initialSend := true
		return retry.WithMaxAttempts(ctx, pe.retryOpts, pe.retryOpts.MaxRetries+1, func() error {
			if !initialSend {
				pe.metrics.recordInternalRetry(int64(payload.Keys().Len()), false)
			}
			initialSend = false
			return pe.ioHandler(ctx, payload)
		})
	}

	// Multiple worker routines handle the IO operations, retrying when necessary.
	emitCh := make(chan IORequest, numEmitWorkers)
	defer close(emitCh)
	emitSuccessCh := make(chan IORequest, numEmitWorkers)

	for i := 0; i < numEmitWorkers; i++ {
		pe.wg.GoCtx(func(ctx context.Context) error {
			for req := range emitCh {
				err := emitWithRetries(ctx, req)
				if err != nil {
					req.SetError(err)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-pe.doneCh:
						return nil
					case pe.resultCh <- req:
					}
				} else {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-pe.doneCh:
						return nil
					case emitSuccessCh <- req:
					}
				}
			}
			return nil
		})
	}

	var handleSuccess func(IORequest)
	var pendingResults []IORequest

	sendToWorker := func(ctx context.Context, req IORequest) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pe.doneCh:
				return
			case emitCh <- req:
				return
			case res := <-emitSuccessCh:
				// Must also handle results to avoid the above emit being able to block
				// forever on all workers being busy trying to emit results.
				pendingResults = append(pendingResults, res)
			}
		}
	}

	// The main routine keeps track of incoming and completed requests, where
	// admitted requests yet to be completed have their Keys() tracked in an
	// intset, and any incoming request with keys already in the intset are placed
	// in a Queue to be sent to IO workers once the conflicting requests complete.
	var inflight intsets.Fast
	var pending []IORequest

	handleSuccess = func(req IORequest) {
		// Clear out the completed keys to check for newly valid pending requests
		inflight.DifferenceWith(req.Keys())

		pendingKeys := intsets.Fast{}
		for i, pendingReq := range pending {
			if !inflight.Intersects(pendingReq.Keys()) && !pendingKeys.Intersects(pendingReq.Keys()) {
				inflight.UnionWith(pendingReq.Keys())
				pending = append(pending[:i], pending[i+1:]...)
				sendToWorker(ctx, pendingReq)
				break
			}

			pendingKeys.UnionWith(pendingReq.Keys())
		}

		select {
		case <-ctx.Done():
		case <-pe.doneCh:
		case pe.resultCh <- req:
		}
	}

	keysInFlight := func(keys intsets.Fast) bool {
		if inflight.Intersects(keys) {
			return true
		}
		for _, pendingReq := range pending {
			if pendingReq.Keys().Intersects(keys) {
				return true
			}
		}
		return false
	}

	for {
		// Results read from sendToWorker need to be first added to a pendingResults
		// list and then handled separately here rather than calling handleResult
		// inside sendtoWorker, as having a re-entrant sendToWorker -> handleResult
		// -> sendToWorker -> handleResult chain creates complexity with managing
		// pending requests
		unhandled := pendingResults
		pendingResults = nil
		for _, res := range unhandled {
			handleSuccess(res)
		}

		select {
		case req := <-pe.requestCh:
			if keysInFlight(req.Keys()) {
				pending = append(pending, req)
			} else {
				inflight.UnionWith(req.Keys())
				sendToWorker(ctx, req)
			}
		case res := <-emitSuccessCh:
			handleSuccess(res)
		case <-ctx.Done():
			return ctx.Err()
		case <-pe.doneCh:
			return nil
		}
	}
}
