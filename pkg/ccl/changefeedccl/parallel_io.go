// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// parallelIO allows performing blocking "IOHandler" calls on in parallel.
// IORequests implement a Keys() function returning keys on which ordering is
// preserved.
// Example: if the events [[a,b], [b,c], [c,d], [e,f]] are all submitted in that
// order, [a,b] and [e,f] can be emitted concurrentyl while [b,c] will block
// until [a,b] completes, then [c,d] will block until [b,c] completes. If [c,d]
// errored, [b,c] would never be sent, and an error would be returned with [c,d]
// in an ioResult struct sent to resultCh.  After sending an error to resultCh
// all workers are torn down and no further requests are received or handled.
type parallelIO struct {
	retryOpts retry.Options
	wg        ctxgroup.Group
	metrics   metricsRecorder
	doneCh    chan struct{}

	ioHandler IOHandler

	requestCh chan IORequest
	resultCh  chan *ioResult // Readers should freeIOResult after handling result events
}

// IORequest represents an abstract unit of IO that has a set of keys upon which
// sequential ordering of fulfillment must be enforced.
type IORequest interface {
	Keys() intsets.Fast
}

// ioResult stores the full request that was sent as well as an error if even
// after retries the IOHanlder was unable to succeed.
type ioResult struct {
	request IORequest
	err     error
}

var resultPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return new(ioResult)
	},
}

func newIOResult(req IORequest, err error) *ioResult {
	res := resultPool.Get().(*ioResult)
	res.request = req
	res.err = err
	return res
}
func freeIOResult(e *ioResult) {
	*e = ioResult{}
	resultPool.Put(e)
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
		resultCh:  make(chan *ioResult, numWorkers),
		doneCh:    make(chan struct{}),
	}

	wg.GoCtx(func(ctx context.Context) error {
		return io.runWorkers(ctx, numWorkers)
	})

	return io
}

// Close stops all workers immediately and returns once they shut down. Inflight
// requests sent to requestCh may never result in being sent to resultCh.
func (p *parallelIO) Close() {
	close(p.doneCh)
	_ = p.wg.Wait()
}

func (p *parallelIO) runWorkers(ctx context.Context, numEmitWorkers int) error {
	emitWithRetries := func(ctx context.Context, payload IORequest) error {
		initialSend := true
		return retry.WithMaxAttempts(ctx, p.retryOpts, p.retryOpts.MaxRetries+1, func() error {
			if !initialSend {
				p.metrics.recordInternalRetry(int64(payload.Keys().Len()), false)
			}
			initialSend = false
			return p.ioHandler(ctx, payload)
		})
	}

	// Multiple worker routines handle the IO operations, retrying when necessary.
	emitCh := make(chan IORequest, numEmitWorkers)
	defer close(emitCh)
	workerResultCh := make(chan *ioResult, numEmitWorkers)

	for i := 0; i < numEmitWorkers; i++ {
		p.wg.GoCtx(func(ctx context.Context) error {
			for req := range emitCh {
				result := newIOResult(req, emitWithRetries(ctx, req))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-p.doneCh:
					return nil
				case workerResultCh <- result:
				}
			}
			return nil
		})
	}

	var pendingResults []*ioResult

	submitIO := func(req IORequest) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.doneCh:
				return nil
			case emitCh <- req:
				return nil
			case res := <-workerResultCh:
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

	handleResult := func(res *ioResult) error {
		if res.err == nil {
			// Clear out the completed keys to check for newly valid pending requests.
			inflight.DifferenceWith(res.request.Keys())

			// Check for a pending request that is now able to be sent i.e. is not
			// conflicting with any inflight requests or any requests that arrived
			// earlier than itself in the pending queue.
			pendingKeys := intsets.Fast{}
			for i, pendingReq := range pending {
				if !inflight.Intersects(pendingReq.Keys()) && !pendingKeys.Intersects(pendingReq.Keys()) {
					inflight.UnionWith(pendingReq.Keys())
					pending = append(pending[:i], pending[i+1:]...)
					if err := submitIO(pendingReq); err != nil {
						return err
					}
					break
				}

				pendingKeys.UnionWith(pendingReq.Keys())
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.doneCh:
			return nil
		case p.resultCh <- res:
			return nil
		}
	}

	// A set of keys can be sent immediately if no yet-to-be-handled request
	// observed so far shares any of those keys.
	canSendKeys := func(keys intsets.Fast) bool {
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
		// inside sendToWorker, as having a re-entrant sendToWorker -> handleResult
		// -> sendToWorker -> handleResult chain creates complexity with managing
		// pending requests.
		unhandled := pendingResults
		pendingResults = nil
		for _, res := range unhandled {
			if err := handleResult(res); err != nil {
				return err
			}
		}

		select {
		case req := <-p.requestCh:
			if canSendKeys(req.Keys()) {
				// If a request conflicts with any currently unhandled requests, add it
				// to the pending queue to be rechecked for validity later.
				pending = append(pending, req)
			} else {
				inflight.UnionWith(req.Keys())
				if err := submitIO(req); err != nil {
					return err
				}
			}
		case res := <-workerResultCh:
			if err := handleResult(res); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-p.doneCh:
			return nil
		}
	}
}
