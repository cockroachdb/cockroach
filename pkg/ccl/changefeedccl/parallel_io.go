// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	resultCh  chan *ioResult // readers should freeIOResult after handling result events
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
		requestCh: make(chan IORequest, numWorkers),
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

		select {
		case req := <-p.requestCh:
			if hasConflictingKeys(req.Keys()) {
				// If a request conflicts with any currently unhandled requests, add it
				// to the pending queue to be rechecked for validity later.
				pending = append(pending, queuedRequest{req: req, admitTime: timeutil.Now()})
				metricsRec.recordPendingQueuePush(int64(req.NumMessages()))
			} else {
				newInFlightKeys := req.Keys()
				inflight.UnionWith(newInFlightKeys)
				metricsRec.setInFlightKeys(int64(inflight.Len()))
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
