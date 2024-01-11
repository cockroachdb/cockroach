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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

	requestQ *requestQueue
	resultCh chan *ioResult // readers should freeIOResult after handling result events
}

// IORequest represents an abstract unit of IO that has a set of keys upon which
// sequential ordering of fulfillment must be enforced.
type IORequest interface {
	Keys() intsets.Fast
	NumMessages() int
}

// ioResult stores the full request that was sent as well as an error if even
// after retries the IOHanlder was unable to succeed.
type ioResult struct {
	request IORequest
	err     error
	// Time representing when this result was received from the sink.
	arrivalTime time.Time
}

var resultPool = sync.Pool{
	New: func() interface{} {
		return new(ioResult)
	},
}

func newIOResult(req IORequest, err error) *ioResult {
	res := resultPool.Get().(*ioResult)
	res.request = req
	res.err = err
	res.arrivalTime = timeutil.Now()
	return res
}
func freeIOResult(e *ioResult) {
	*e = ioResult{}
	resultPool.Put(e)
}

type queuedRequest struct {
	req       IORequest
	admitTime time.Time
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
		requestQ:  newRequestQueue(numWorkers),
		resultCh:  make(chan *ioResult, numWorkers),
		doneCh:    make(chan struct{}),
	}

	wg.GoCtx(func(ctx context.Context) error {
		return io.processIO(ctx, numWorkers)
	})

	return io
}

// Close stops all workers immediately and returns once they shut down. Inflight
// requests sent to requestCh may never result in being sent to resultCh.
func (p *parallelIO) Close() {
	close(p.doneCh)
	_ = p.wg.Wait()
}

var testQueuingDelay = 0 * time.Second

var testingEnableQueuingDelay = func() func() {
	testQueuingDelay = 250 * time.Millisecond
	return func() {
		testQueuingDelay = 0 * time.Second
	}
}

// requestQueue is a single consumer multi-producer queue.
type requestQueue struct {
	mu struct {
		syncutil.Mutex
		q                ring.Buffer[requestWithAlloc]
		waitBecauseEmpty chan struct{}
	}
	p *quotapool.IntPool
}

type requestWithAlloc struct {
	req IORequest
	a   *quotapool.IntAlloc
}

func newRequestQueue(sz int) *requestQueue {
	rq := &requestQueue{}
	rq.mu.q = ring.MakeBuffer([]requestWithAlloc{})
	rq.p = quotapool.NewIntPool("changefeed-parallel-io", uint64(sz))
	return rq
}

// tryPush pushes the request to the queue if it is not full. If the request was
// added to the queue, this method returns true.
func (rq *requestQueue) tryPush(ctx context.Context, req IORequest) (bool, error) {
	// Check if the queue is full.
	a, err := rq.p.TryAcquire(ctx, 1)
	if errors.Is(err, quotapool.ErrNotEnoughQuota) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	rq.mu.Lock()
	defer rq.mu.Unlock()

	// Add the request to the queue.
	rq.mu.q.AddLast(requestWithAlloc{req: req, a: a})

	// Unblock any waiters.
	if rq.mu.waitBecauseEmpty != nil {
		close(rq.mu.waitBecauseEmpty)
		rq.mu.waitBecauseEmpty = nil
	}

	return true, nil
}

// maybePop pops a request from the queue if the queue is non-empty.
// If the queue is empty, a channel is returned which will be closed
// when requests are available to read.
func (rq *requestQueue) maybePop() (IORequest, bool, chan struct{}) {
	rq.mu.Lock()
	defer rq.mu.Unlock()

	if rq.mu.q.Len() > 0 {
		reqWithAlloc := rq.mu.q.GetFirst()
		rq.mu.q.RemoveFirst()
		reqWithAlloc.a.Release()
		return reqWithAlloc.req, true, nil
	}

	wait := rq.mu.waitBecauseEmpty
	if wait == nil {
		rq.mu.waitBecauseEmpty = make(chan struct{})
		wait = rq.mu.waitBecauseEmpty
	}
	return nil, false, wait
}

// This is the maximum number of requests which can be queued/pending due to
// conflicting in-flight keys.
var maxPendingSize = 128

// processIO starts numEmitWorkers worker threads to run the IOHandler on
// non-conflicting IORequests each retrying according to the retryOpts, then:
// - Reads incoming messages from requestCh, sending them to any worker if there
// aren't any conflicting messages and queing them up to be sent later
// otherwise.
// - Reads results from the workers and forwards the information to resultCh, at
// this point also sending the first pending request that would now be sendable.
//
//	                       ┌───────────┐
//	                     ┌►│io worker 1├──┐
//	                     │ └───────────┘  │
//	<-requestCh──────────┤►      ...      ├─┬─► resultCh<-
//	     │            ▲  │ ┌───────────┐  │ │
//	     │conflict    │  └►│io worker n├──┘ │
//	     │            │    └───────────┘    │
//	     ▼            │                     │check pending
//	┌─────────┐       │no conflict          │
//	│ pending ├───────┘                     │
//	└─────────┘                             │
//	     ▲                                  │
//	     └──────────────────────────────────┘
//
// The conflict checking is done via an intset.Fast storing the union of all
// keys currently being sent, followed by checking each pending batch's intset.
func (p *parallelIO) processIO(ctx context.Context, numEmitWorkers int) error {
	emitWithRetries := func(ctx context.Context, payload IORequest) error {
		if testQueuingDelay > 0*time.Second {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(testQueuingDelay):
			}
		}

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
	workerEmitCh := make(chan IORequest, numEmitWorkers)
	defer close(workerEmitCh)
	workerResultCh := make(chan *ioResult, numEmitWorkers)

	for i := 0; i < numEmitWorkers; i++ {
		p.wg.GoCtx(func(ctx context.Context) error {
			for req := range workerEmitCh {
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

	// submitIO, which sends requests to the workers, has to select on both
	// workerEmitCh<- and <-workerResultCh, since without the <-workerResultCh
	// there could be a deadlock where all workers are blocked on emitting results
	// and thereby unable to accept new work. If a result is received, it also
	// cannot immediately call handleResult on it as that could cause a re-entrant
	// submitIO -> handleResult -> submitIO -> handleResult chain which is complex
	// to manage. To avoid this, results are added to a pending list to be handled
	// separately.
	var pendingResults []*ioResult
	submitIO := func(req IORequest) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.doneCh:
				return nil
			case workerEmitCh <- req:
				return nil
			case res := <-workerResultCh:
				pendingResults = append(pendingResults, res)
			}
		}
	}

	// The main routine keeps track of incoming and completed requests, where
	// admitted requests yet to be completed have their Keys() tracked in an
	// intset, and any incoming request with keys already in the intset are placed
	// in a Queue to be sent to IO workers once the conflicting requests complete.
	var inflight intsets.Fast
	var pending []queuedRequest
	metricsRec := p.metrics.newParallelIOMetricsRecorder()

	handleResult := func(res *ioResult) error {
		if res.err == nil {
			// Clear out the completed keys to check for newly valid pending requests.
			requestKeys := res.request.Keys()
			inflight.DifferenceWith(requestKeys)
			metricsRec.setInFlightKeys(int64(inflight.Len()))
			// Check for a pending request that is now able to be sent i.e. is not
			// conflicting with any inflight requests or any requests that arrived
			// earlier than itself in the pending queue.
			pendingKeys := intsets.Fast{}
			for i, pendingReq := range pending {
				if !inflight.Intersects(pendingReq.req.Keys()) && !pendingKeys.Intersects(pendingReq.req.Keys()) {
					inflight.UnionWith(pendingReq.req.Keys())
					metricsRec.setInFlightKeys(int64(inflight.Len()))
					pending = append(pending[:i], pending[i+1:]...)
					metricsRec.recordPendingQueuePop(int64(pendingReq.req.NumMessages()), timeutil.Since(pendingReq.admitTime))
					if err := submitIO(pendingReq.req); err != nil {
						return err
					}
					break
				}

				pendingKeys.UnionWith(pendingReq.req.Keys())
			}
		}

		// Copy the arrival time for the metrics recorder below.
		// Otherwise, it would be possible for res to be admitted to the
		// resultCh and freed before we read rec.arrivalTime.
		arrivalTime := res.arrivalTime
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.doneCh:
			return nil
		case p.resultCh <- res:
			metricsRec.recordResultQueueLatency(timeutil.Since(arrivalTime))
			return nil
		}
	}

	// If any yet-to-be-handled request observed so far shares any keys with an
	// incoming request, the incoming request cannot be sent out and risk arriving
	// earlier.
	hasConflictingKeys := func(keys intsets.Fast) bool {
		if inflight.Intersects(keys) {
			return true
		}
		for _, pendingReq := range pending {
			if pendingReq.req.Keys().Intersects(keys) {
				return true
			}
		}
		return false
	}

	for {
		// Handle any results that arrived during any submitIO attempts
		unhandled := pendingResults
		pendingResults = nil
		for _, res := range unhandled {
			if err := handleResult(res); err != nil {
				return err
			}
		}

		// Check for incoming requests and ush back on the producer if we have too many pending requests.
		waitForRequest := make(chan struct{})
		close(waitForRequest)
		if len(pending) < maxPendingSize {
			req, ok, maybeWaitForRequest := p.requestQ.maybePop()
			if ok {
				if hasConflictingKeys(req.Keys()) {
					// If a request conflicts with any currently unhandled requests, add it
					// to the pending queue to be rechecked for validity later.
					pending = append(pending, queuedRequest{req: req, admitTime: timeutil.Now()})
					metricsRec.recordPendingQueuePush(int64(req.NumMessages()))
				} else {
					// Otherwise, put the request in flight.
					newInFlightKeys := req.Keys()
					inflight.UnionWith(newInFlightKeys)
					metricsRec.setInFlightKeys(int64(inflight.Len()))
					if err := submitIO(req); err != nil {
						return err
					}
				}
			} else {
				// If there is nothing to read, we should wait until there is
				// something to read.
				waitForRequest = maybeWaitForRequest
			}
		}

		select {
		case <-waitForRequest:
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
