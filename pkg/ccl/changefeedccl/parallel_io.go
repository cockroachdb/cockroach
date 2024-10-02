// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ParallelIO allows performing blocking "IOHandler" calls on in parallel.
// IORequests implement a Keys() function returning keys on which ordering is
// preserved.
// Example: if the events [[a,b], [b,c], [c,d], [e,f]] are all submitted in that
// order, [a,b] and [e,f] can be emitted concurrentyl while [b,c] will block
// until [a,b] completes, then [c,d] will block until [b,c] completes. If [c,d]
// errored, [b,c] would never be sent, and an error would be returned with [c,d]
// in an ioResult struct sent to resultCh.  After sending an error to resultCh
// all workers are torn down and no further requests are received or handled.
type ParallelIO struct {
	retryOpts retry.Options
	wg        ctxgroup.Group
	metrics   metricsRecorder
	doneCh    chan struct{}

	ioHandler IOHandler

	quota     *quotapool.IntPool
	requestCh chan AdmittedIORequest
	resultCh  chan IOResult
}

// IORequest represents an abstract unit of IO that has a set of keys upon which
// sequential ordering of fulfillment must be enforced.
type IORequest interface {
	Keys() intsets.Fast
	NumMessages() int
}

var resultPool = sync.Pool{
	New: func() interface{} {
		return new(ioRequest)
	},
}

// newIORequest is used to allocate *ioRequest structs using a pool.
func newIORequest(req IORequest, a *quotapool.IntAlloc) *ioRequest {
	res := resultPool.Get().(*ioRequest)
	res.r = req
	res.a = a
	return res
}

// freeIORequest frees the ioRequest and returns it to the pool.
func freeIORequest(e *ioRequest) {
	*e = ioRequest{}
	resultPool.Put(e)
}

// IOHandler performs a blocking IO operation on an IORequest
type IOHandler func(context.Context, IORequest) error

// NewParallelIO creates a new ParallelIO.
func NewParallelIO(
	ctx context.Context,
	retryOpts retry.Options,
	numWorkers int,
	handler IOHandler,
	metrics metricsRecorder,
	settings *cluster.Settings,
) *ParallelIO {
	quota := uint64(requestQuota.Get(&settings.SV))
	wg := ctxgroup.WithContext(ctx)
	io := &ParallelIO{
		retryOpts: retryOpts,
		wg:        wg,
		metrics:   metrics,
		ioHandler: handler,
		quota:     quotapool.NewIntPool("changefeed-parallel-io", quota),
		// NB: The size of these channels should not be less than the quota. This prevents the producer from
		// blocking on sending requests which have been admitted.
		requestCh: make(chan AdmittedIORequest, quota),
		resultCh:  make(chan IOResult, quota),
		doneCh:    make(chan struct{}),
	}

	wg.GoCtx(func(ctx context.Context) error {
		return io.processIO(ctx, numWorkers)
	})

	return io
}

// Close stops all workers immediately and returns once they shut down. Inflight
// requests sent to requestCh may never result in being sent to resultCh.
func (p *ParallelIO) Close() {
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

// ioRequest is a wrapper around the IORequest that handles quota to limit the
// number of in-flight requests.
type ioRequest struct {
	// Set upon initialization.
	r IORequest
	a *quotapool.IntAlloc

	// If the request conflicts with in-flight requests, this is the time at
	// which the request is placed in the pending queue.
	pendingQueueAdmitTime time.Time

	// err is the result of this request. resultTime is when it was obtained.
	err        error
	resultTime time.Time
}

// Consume implements IOResult.
func (r *ioRequest) Consume() (IORequest, error) {
	result := r.r
	err := r.err

	r.a.Release()
	freeIORequest(r)

	return result, err
}

// IOResult contains the original IORequest and its resultant error.
type IOResult interface {
	// Consume returns the original request and result error. It removes the
	// request from ParallelIO, freeing its resources and budget in the request
	// quota.
	Consume() (IORequest, error)
}

// requestQuota is the number of requests which can be admitted into the
// parallelio system before blocking the producer.
var requestQuota = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"changefeed.parallel_io.request_quota",
	"the number of requests which can be admitted into the parallelio"+
		" system before blocking the producer",
	metamorphic.ConstantWithTestChoice[int64](
		"changefeed.parallel_io.request_quota",
		128, 16, 32, 64, 256),
	settings.IntInRange(1, 256),
	settings.WithVisibility(settings.Reserved),
)

// ErrNotEnoughQuota indicates that a request was not emitted due to a lack of
// quota.
var ErrNotEnoughQuota = quotapool.ErrNotEnoughQuota

// AdmitRequest returns a AdmittedIORequest and a channel to send it on
// if there is quota available for the request. Otherwise, it returns an
// ErrNotEnoughQuota.
//
// Quota can be freed by calling GetResult() and Consume()ing the IOResult.
//
// TODO(jayants): This should use an `Acquire` instead of a `TryAcquire`, and
// the API should not use channels. The reasons things are done this way are:
//
// 1.The callers (batching sink) use one goroutine to both produce requests and
// consume results. If it blocks on producing, it will deadlock because that
// goroutine cannot free up quota by consuming. This is why the `TryAcquire` is
// used. The caller should use separate goroutines. One for consuming and one for producing.
//
// 2. Both the batching sink and ParallelIO use a `done` channel to close. They
// should use context cancellation. The `done` channel is problematic because `Acquire`
// cannot select on that channel, and ParallelIO cannot detect if the batching
// sink's channel has been closed. Using contexts and cancelling them fixes
// this problem.
func (p *ParallelIO) AdmitRequest(
	ctx context.Context, r IORequest,
) (req AdmittedIORequest, send chan AdmittedIORequest, err error) {
	a, err := p.quota.TryAcquire(ctx, 1)

	if errors.Is(err, quotapool.ErrNotEnoughQuota) {
		return nil, nil, ErrNotEnoughQuota
	} else if err != nil {
		return nil, nil, err
	}

	ra := newIORequest(r, a)

	return ra, p.requestCh, nil
}

// AdmittedIORequest is an admitted IORequest.
type AdmittedIORequest interface{}

var _ AdmittedIORequest = (*ioRequest)(nil)

// GetResult returns a channel which can be waited upon to read the next
// IOResult.
func (p *ParallelIO) GetResult() chan IOResult {
	return p.resultCh
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
func (p *ParallelIO) processIO(ctx context.Context, numEmitWorkers int) error {
	emitWithRetries := func(ctx context.Context, r IORequest) error {
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
				p.metrics.recordInternalRetry(int64(r.Keys().Len()), false)
			}
			initialSend = false
			return p.ioHandler(ctx, r)
		})
	}

	// Multiple worker routines handle the IO operations, retrying when necessary.
	workerEmitCh := make(chan *ioRequest, numEmitWorkers)
	defer close(workerEmitCh)
	workerResultCh := make(chan *ioRequest, numEmitWorkers)

	for i := 0; i < numEmitWorkers; i++ {
		p.wg.GoCtx(func(ctx context.Context) error {
			for req := range workerEmitCh {
				req.err = emitWithRetries(ctx, req.r)
				req.resultTime = timeutil.Now()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-p.doneCh:
					return nil
				case workerResultCh <- req:
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
	var pendingResults []*ioRequest
	submitIO := func(req *ioRequest) error {
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
	// intset, and any incoming ioRequest with keys already in the intset are placed
	// in a Queue to be sent to IO workers once the conflicting requests complete.
	var inflight intsets.Fast
	var pending []*ioRequest
	metricsRec := p.metrics.newParallelIOMetricsRecorder()

	handleResult := func(res *ioRequest) error {
		if res.err == nil {
			// Clear out the completed keys to check for newly valid pending requests.
			requestKeys := res.r.Keys()
			inflight.DifferenceWith(requestKeys)
			metricsRec.setInFlightKeys(int64(inflight.Len()))
			// Check for a pending request that is now able to be sent i.e. is not
			// conflicting with any inflight requests or any requests that arrived
			// earlier than itself in the pending queue.
			pendingKeys := intsets.Fast{}
			for i, req := range pending {
				if !inflight.Intersects(req.r.Keys()) && !pendingKeys.Intersects(req.r.Keys()) {
					inflight.UnionWith(req.r.Keys())
					metricsRec.setInFlightKeys(int64(inflight.Len()))
					pending = append(pending[:i], pending[i+1:]...)
					metricsRec.recordPendingQueuePop(int64(req.r.NumMessages()), timeutil.Since(req.pendingQueueAdmitTime))
					if err := submitIO(req); err != nil {
						return err
					}
					break
				}

				pendingKeys.UnionWith(req.r.Keys())
			}
		}

		// Copy the arrival time for the metrics recorder below.
		// Otherwise, it would be possible for res to be admitted to the
		// resultCh and freed before we read res.resultTime.
		arrivalTime := res.resultTime
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
		for _, req := range pending {
			if req.r.Keys().Intersects(keys) {
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
		case admittedReq := <-p.requestCh:
			req := admittedReq.(*ioRequest)
			if hasConflictingKeys(req.r.Keys()) {
				// If a request conflicts with any currently unhandled requests, add it
				// to the pending queue to be rechecked for validity later.
				req.pendingQueueAdmitTime = timeutil.Now()
				pending = append(pending, req)
				metricsRec.recordPendingQueuePush(int64(req.r.NumMessages()))
			} else {
				newInFlightKeys := req.r.Keys()
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
