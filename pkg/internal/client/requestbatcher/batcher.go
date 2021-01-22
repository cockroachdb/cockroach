// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package requestbatcher is a library to enable easy batching of roachpb
// requests.
//
// Batching in general represents a tradeoff between throughput and latency. The
// underlying assumption being that batched operations are cheaper than an
// individual operation. If this is not the case for your workload, don't use
// this library.
//
// Batching assumes that data with the same key can be sent in a single batch.
// The initial implementation uses rangeID as the key explicitly to avoid
// creating an overly general solution without motivation but interested readers
// should recognize that it would be easy to extend this package to accept an
// arbitrary comparable key.
package requestbatcher

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// The motivating use case for this package are opportunities to perform cleanup
// operations in a single raft transaction rather than several. Three main
// opportunities are known:
//
//   1) Intent resolution
//   2) Txn heartbeating
//   3) Txn record garbage collection
//
// The first two have a relatively tight time bound expectations. In other words
// it would be surprising and negative for a client if operations were not sent
// soon after they were queued. The transaction record GC workload can be rather
// asynchronous. This motivates the need for some knobs to control the maximum
// acceptable amount of time to buffer operations before sending.
// Another wrinkle is dealing with the different ways in which sending a batch
// may fail. A batch may fail in an ambiguous way (RPC/network errors), it may
// fail completely (which is likely indistinguishable from the ambiguous
// failure) and lastly it may fail partially. Today's Sender contract is fairly
// ambiguous about the contract between BatchResponse inner responses and errors
// returned from a batch request.

// TODO(ajwerner): Do we need to consider ordering dependencies between
// operations? For the initial motivating use cases for this library there are
// no data dependencies between operations and the only key will be the guess
// for where an operation should go.

// TODO(ajwerner): Consider a more sophisticated mechanism to limit on maximum
// number of requests in flight at a time. This may ultimately lead to a need
// for queuing. Furthermore consider using batch time to dynamically tune the
// amount of time we wait.

// TODO(ajwerner): Consider filtering requests which might have been canceled
// before sending a batch.

// TODO(ajwerner): Consider more dynamic policies with regards to deadlines.
// Perhaps we want to wait no more than some percentile of the duration of
// historical operations and stay idle only some other percentile. For example
// imagine if the max delay was the 50th and the max idle was the 10th? This
// has a problem when much of the prior workload was say local operations and
// happened very rapidly. Perhaps we need to provide some bounding envelope?

// TODO(ajwerner): Consider a more general purpose interface for this package.
// While several interface-oriented interfaces have been explored they all felt
// heavy and allocation intensive.

// TODO(ajwerner): Consider providing an interface which enables a single
// goroutine to dispatch a number of requests destined for different ranges to
// the RequestBatcher which may then wait for completion of all of the requests.
// What is the right contract for error handling? Imagine a situation where a
// client has dispatched N requests and one has been sent and returns with an
// error while others are queued. What should happen? Should the client receive
// the error rapidly? Should the other requests be sent at all? Should they be
// filtered before sending?

// Config contains the dependencies and configuration for a Batcher.
type Config struct {

	// Name of the batcher, used for logging, timeout errors, and the stopper.
	Name string

	// Sender can round-trip a batch. Sender must not be nil.
	Sender kv.Sender

	// Stopper controls the lifecycle of the Batcher. Stopper must not be nil.
	Stopper *stop.Stopper

	// MaxSizePerBatch is the maximum number of bytes in individual requests in a
	// batch. If MaxSizePerBatch <= 0 then no limit is enforced.
	MaxSizePerBatch int

	// MaxMsgsPerBatch is the maximum number of messages.
	// If MaxMsgsPerBatch <= 0 then no limit is enforced.
	MaxMsgsPerBatch int

	// MaxKeysPerBatchReq is the maximum number of keys that each batch is
	// allowed to touch during one of its requests. If the limit is exceeded,
	// the batch is paginated over a series of individual requests. This limit
	// corresponds to the MaxSpanRequestKeys assigned to the Header of each
	// request. If MaxKeysPerBatchReq <= 0 then no limit is enforced.
	MaxKeysPerBatchReq int

	// MaxWait is the maximum amount of time a message should wait in a batch
	// before being sent. If MaxWait is <= 0 then no wait timeout is enforced.
	// It is inadvisable to disable both MaxIdle and MaxWait.
	MaxWait time.Duration

	// MaxIdle is the amount of time a batch should wait between message additions
	// before being sent. The idle timer allows clients to observe low latencies
	// when throughput is low. If MaxWait is <= 0 then no wait timeout is
	// enforced. It is inadvisable to disable both MaxIdle and MaxWait.
	MaxIdle time.Duration

	// InFlightBackpressureLimit is the number of batches in flight above which
	// sending clients should experience backpressure. If the batcher has more
	// requests than this in flight it will not accept new requests until the
	// number of in flight batches is again below this threshold. This value does
	// not limit the number of batches which may ultimately be in flight as
	// batches which are queued to send but not yet in flight will still send.
	// Note that values	less than or equal to zero will result in the use of
	// DefaultInFlightBackpressureLimit.
	InFlightBackpressureLimit int

	// NowFunc is used to determine the current time. It defaults to timeutil.Now.
	NowFunc func() time.Time
}

const (
	// DefaultInFlightBackpressureLimit is the InFlightBackpressureLimit used if
	// a zero value for that setting is passed in a Config to New.
	// TODO(ajwerner): Justify this number.
	DefaultInFlightBackpressureLimit = 1000

	// BackpressureRecoveryFraction is the fraction of InFlightBackpressureLimit
	// used to detect when enough in flight requests have completed such that more
	// requests should now be accepted. A value less than 1 is chosen in order to
	// avoid thrashing on backpressure which might ultimately defeat the purpose
	// of the RequestBatcher.
	backpressureRecoveryFraction = .8
)

func backpressureRecoveryThreshold(limit int) int {
	if l := int(float64(limit) * backpressureRecoveryFraction); l > 0 {
		return l
	}
	return 1 // don't allow the recovery threshold to be 0
}

// RequestBatcher batches requests destined for a single range based on
// a configured batching policy.
type RequestBatcher struct {
	pool pool
	cfg  Config

	// sendBatchOpName is the string passed to contextutil.RunWithTimeout when
	// sending a batch.
	sendBatchOpName string

	batches batchQueue

	requestChan  chan *request
	sendDoneChan chan struct{}
}

// Response is exported for use with the channel-oriented SendWithChan method.
// At least one of Resp or Err will be populated for every sent Response.
type Response struct {
	Resp roachpb.Response
	Err  error
}

// New creates a new RequestBatcher.
func New(cfg Config) *RequestBatcher {
	validateConfig(&cfg)
	b := &RequestBatcher{
		cfg:          cfg,
		pool:         makePool(),
		batches:      makeBatchQueue(),
		requestChan:  make(chan *request),
		sendDoneChan: make(chan struct{}),
	}
	b.sendBatchOpName = b.cfg.Name + ".sendBatch"
	if err := cfg.Stopper.RunAsyncTask(context.Background(), b.cfg.Name, b.run); err != nil {
		panic(err)
	}
	return b
}

func validateConfig(cfg *Config) {
	if cfg.Stopper == nil {
		panic("cannot construct a Batcher with a nil Stopper")
	} else if cfg.Sender == nil {
		panic("cannot construct a Batcher with a nil Sender")
	}
	if cfg.InFlightBackpressureLimit <= 0 {
		cfg.InFlightBackpressureLimit = DefaultInFlightBackpressureLimit
	}
	if cfg.NowFunc == nil {
		cfg.NowFunc = timeutil.Now
	}
}

// SendWithChan sends a request with a client provided response channel. The
// client is responsible for ensuring that the passed respChan has a buffer at
// least as large as the number of responses it expects to receive. Using an
// insufficiently buffered channel can lead to deadlocks and unintended delays
// processing requests inside the RequestBatcher.
func (b *RequestBatcher) SendWithChan(
	ctx context.Context, respChan chan<- Response, rangeID roachpb.RangeID, req roachpb.Request,
) error {
	select {
	case b.requestChan <- b.pool.newRequest(ctx, rangeID, req, respChan):
		return nil
	case <-b.cfg.Stopper.ShouldQuiesce():
		return stop.ErrUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Send sends req as a part of a batch. An error is returned if the context
// is canceled before the sending of the request completes. The context with
// the latest deadline for a batch is used to send the underlying batch request.
func (b *RequestBatcher) Send(
	ctx context.Context, rangeID roachpb.RangeID, req roachpb.Request,
) (roachpb.Response, error) {
	responseChan := b.pool.getResponseChan()
	if err := b.SendWithChan(ctx, responseChan, rangeID, req); err != nil {
		return nil, err
	}
	select {
	case resp := <-responseChan:
		// It's only safe to put responseChan back in the pool if it has been
		// received from.
		b.pool.putResponseChan(responseChan)
		return resp.Resp, resp.Err
	case <-b.cfg.Stopper.ShouldQuiesce():
		return nil, stop.ErrUnavailable
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *RequestBatcher) sendDone(ba *batch) {
	b.pool.putBatch(ba)
	select {
	case b.sendDoneChan <- struct{}{}:
	case <-b.cfg.Stopper.ShouldQuiesce():
	}
}

func (b *RequestBatcher) sendBatch(ctx context.Context, ba *batch) {
	if err := b.cfg.Stopper.RunAsyncTask(ctx, "send-batch", func(ctx context.Context) {
		defer b.sendDone(ba)
		var br *roachpb.BatchResponse
		send := func(ctx context.Context) error {
			var pErr *roachpb.Error
			if br, pErr = b.cfg.Sender.Send(ctx, ba.batchRequest(&b.cfg)); pErr != nil {
				return pErr.GoError()
			}
			return nil
		}
		if !ba.sendDeadline.IsZero() {
			actualSend := send
			send = func(context.Context) error {
				return contextutil.RunWithTimeout(
					ctx, b.sendBatchOpName, timeutil.Until(ba.sendDeadline), actualSend)
			}
		}
		// Send requests in a loop to support pagination, which may be necessary
		// if MaxKeysPerBatchReq is set. If so, partial responses with resume
		// spans may be returned for requests, indicating that the limit was hit
		// before they could complete and that they should be resumed over the
		// specified key span. Requests in the batch are neither guaranteed to
		// be ordered nor guaranteed to be non-overlapping, so we can make no
		// assumptions about the requests that will result in full responses
		// (with no resume spans) vs. partial responses vs. empty responses (see
		// the comment on roachpb.Header.MaxSpanRequestKeys).
		//
		// To accommodate this, we keep track of all partial responses from
		// previous iterations. After receiving a batch of responses during an
		// iteration, the responses are each combined with the previous response
		// for their corresponding requests. From there, responses that have no
		// resume spans are removed. Responses that have resume spans are
		// updated appropriately and sent again in the next iteration. The loop
		// proceeds until all requests have been run to completion.
		var prevResps []roachpb.Response
		for len(ba.reqs) > 0 {
			err := send(ctx)
			nextReqs, nextPrevResps := ba.reqs[:0], prevResps[:0]
			for i, r := range ba.reqs {
				var res Response
				if br != nil {
					resp := br.Responses[i].GetInner()
					if prevResps != nil {
						prevResp := prevResps[i]
						if cErr := roachpb.CombineResponses(prevResp, resp); cErr != nil {
							log.Fatalf(ctx, "%v", cErr)
						}
						resp = prevResp
					}
					if resume := resp.Header().ResumeSpan; resume != nil {
						// Add a trimmed request to the next batch.
						h := r.req.Header()
						h.SetSpan(*resume)
						r.req = r.req.ShallowCopy()
						r.req.SetHeader(h)
						nextReqs = append(nextReqs, r)
						// Strip resume span from previous response and record.
						prevH := resp.Header()
						prevH.ResumeSpan = nil
						prevResp := resp
						prevResp.SetHeader(prevH)
						nextPrevResps = append(nextPrevResps, prevResp)
						continue
					}
					res.Resp = resp
				}
				if err != nil {
					res.Err = err
				}
				b.sendResponse(r, res)
			}
			ba.reqs, prevResps = nextReqs, nextPrevResps
		}
	}); err != nil {
		b.sendDone(ba)
	}
}

func (b *RequestBatcher) sendResponse(req *request, resp Response) {
	// This send should never block because responseChan is buffered.
	req.responseChan <- resp
	b.pool.putRequest(req)
}

func addRequestToBatch(cfg *Config, now time.Time, ba *batch, r *request) (shouldSend bool) {
	// Update the deadline for the batch if this requests's deadline is later
	// than the current latest.
	rDeadline, rHasDeadline := r.ctx.Deadline()
	// If this is the first request or
	if len(ba.reqs) == 0 ||
		// there are already requests and there is a deadline and
		(len(ba.reqs) > 0 && !ba.sendDeadline.IsZero() &&
			// this request either doesn't have a deadline or has a later deadline,
			(!rHasDeadline || rDeadline.After(ba.sendDeadline))) {
		// set the deadline to this request's deadline.
		ba.sendDeadline = rDeadline
	}

	ba.reqs = append(ba.reqs, r)
	ba.size += r.req.Size()
	ba.lastUpdated = now

	if cfg.MaxIdle > 0 {
		ba.deadline = ba.lastUpdated.Add(cfg.MaxIdle)
	}
	if cfg.MaxWait > 0 {
		waitDeadline := ba.startTime.Add(cfg.MaxWait)
		if cfg.MaxIdle <= 0 || waitDeadline.Before(ba.deadline) {
			ba.deadline = waitDeadline
		}
	}
	return (cfg.MaxMsgsPerBatch > 0 && len(ba.reqs) >= cfg.MaxMsgsPerBatch) ||
		(cfg.MaxSizePerBatch > 0 && ba.size >= cfg.MaxSizePerBatch)
}

func (b *RequestBatcher) cleanup(err error) {
	for ba := b.batches.popFront(); ba != nil; ba = b.batches.popFront() {
		for _, r := range ba.reqs {
			b.sendResponse(r, Response{Err: err})
		}
	}
}

func (b *RequestBatcher) run(ctx context.Context) {
	// Create a context to be used in sendBatch to cancel in-flight batches when
	// this function exits. If we did not cancel in-flight requests then the
	// Stopper might get stuck waiting for those requests to complete.
	sendCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		// inFlight tracks the number of batches currently being sent.
		// true.
		inFlight = 0
		// inBackPressure indicates whether the reqChan is enabled.
		// It becomes true when inFlight exceeds b.cfg.InFlightBackpressureLimit.
		inBackPressure = false
		// recoveryThreshold is the number of in flight requests below which the
		// the inBackPressure state should exit.
		recoveryThreshold = backpressureRecoveryThreshold(b.cfg.InFlightBackpressureLimit)
		// reqChan consults inBackPressure to determine whether the goroutine is
		// accepting new requests.
		reqChan = func() <-chan *request {
			if inBackPressure {
				return nil
			}
			return b.requestChan
		}
		sendBatch = func(ba *batch) {
			inFlight++
			if inFlight >= b.cfg.InFlightBackpressureLimit {
				inBackPressure = true
			}
			b.sendBatch(sendCtx, ba)
		}
		handleSendDone = func() {
			inFlight--
			if inFlight < recoveryThreshold {
				inBackPressure = false
			}
		}
		handleRequest = func(req *request) {
			now := b.cfg.NowFunc()
			ba, existsInQueue := b.batches.get(req.rangeID)
			if !existsInQueue {
				ba = b.pool.newBatch(now)
			}
			if shouldSend := addRequestToBatch(&b.cfg, now, ba, req); shouldSend {
				if existsInQueue {
					b.batches.remove(ba)
				}
				sendBatch(ba)
			} else {
				b.batches.upsert(ba)
			}
		}
		deadline      time.Time
		timer         = timeutil.NewTimer()
		maybeSetTimer = func() {
			var nextDeadline time.Time
			if next := b.batches.peekFront(); next != nil {
				nextDeadline = next.deadline
			}
			if !deadline.Equal(nextDeadline) || timer.Read {
				deadline = nextDeadline
				if !deadline.IsZero() {
					timer.Reset(timeutil.Until(deadline))
				} else {
					// Clear the current timer due to a sole batch already sent before
					// the timer fired.
					timer.Stop()
					timer = timeutil.NewTimer()
				}
			}
		}
	)
	for {
		select {
		case req := <-reqChan():
			handleRequest(req)
			maybeSetTimer()
		case <-timer.C:
			timer.Read = true
			sendBatch(b.batches.popFront())
			maybeSetTimer()
		case <-b.sendDoneChan:
			handleSendDone()
		case <-b.cfg.Stopper.ShouldQuiesce():
			b.cleanup(stop.ErrUnavailable)
			return
		case <-ctx.Done():
			b.cleanup(ctx.Err())
			return
		}
	}
}

type request struct {
	ctx          context.Context
	req          roachpb.Request
	rangeID      roachpb.RangeID
	responseChan chan<- Response
}

type batch struct {
	reqs []*request
	size int // bytes

	// sendDeadline is the latest deadline reported by a request's context.
	// It will be zero valued if any request does not contain a deadline.
	sendDeadline time.Time

	// idx is the batch's index in the batchQueue.
	idx int

	// deadline is the time at which this batch should be sent according to the
	// Batcher's configuration.
	deadline time.Time
	// startTime is the time at which the first request was added to the batch.
	startTime time.Time
	// lastUpdated is the latest time when a request was added to the batch.
	lastUpdated time.Time
}

func (b *batch) rangeID() roachpb.RangeID {
	if len(b.reqs) == 0 {
		panic("rangeID cannot be called on an empty batch")
	}
	return b.reqs[0].rangeID
}

func (b *batch) batchRequest(cfg *Config) roachpb.BatchRequest {
	req := roachpb.BatchRequest{
		// Preallocate the Requests slice.
		Requests: make([]roachpb.RequestUnion, 0, len(b.reqs)),
	}
	for _, r := range b.reqs {
		req.Add(r.req)
	}
	if cfg.MaxKeysPerBatchReq > 0 {
		req.MaxSpanRequestKeys = int64(cfg.MaxKeysPerBatchReq)
	}
	return req
}

// pool stores object pools for the various commonly reused objects of the
// batcher
type pool struct {
	responseChanPool sync.Pool
	batchPool        sync.Pool
	requestPool      sync.Pool
}

func makePool() pool {
	return pool{
		responseChanPool: sync.Pool{
			New: func() interface{} { return make(chan Response, 1) },
		},
		batchPool: sync.Pool{
			New: func() interface{} { return &batch{} },
		},
		requestPool: sync.Pool{
			New: func() interface{} { return &request{} },
		},
	}
}

func (p *pool) getResponseChan() chan Response {
	return p.responseChanPool.Get().(chan Response)
}

func (p *pool) putResponseChan(r chan Response) {
	p.responseChanPool.Put(r)
}

func (p *pool) newRequest(
	ctx context.Context, rangeID roachpb.RangeID, req roachpb.Request, responseChan chan<- Response,
) *request {
	r := p.requestPool.Get().(*request)
	*r = request{
		ctx:          ctx,
		rangeID:      rangeID,
		req:          req,
		responseChan: responseChan,
	}
	return r
}

func (p *pool) putRequest(r *request) {
	*r = request{}
	p.requestPool.Put(r)
}

func (p *pool) newBatch(now time.Time) *batch {
	ba := p.batchPool.Get().(*batch)
	*ba = batch{
		startTime: now,
		idx:       -1,
	}
	return ba
}

func (p *pool) putBatch(b *batch) {
	*b = batch{}
	p.batchPool.Put(b)
}

// batchQueue is a container for batch objects which offers O(1) get based on
// rangeID and peekFront as well as O(log(n)) upsert, removal, popFront.
// Batch structs are heap ordered inside of the batches slice based on their
// deadline with the earliest deadline at the front.
//
// Note that the batch struct stores its index in the batches slice and is -1
// when not part of the queue. The heap methods update the batch indices when
// updating the heap. Take care not to ever put a batch in to multiple
// batchQueues. At time of writing this package only ever used one batchQueue
// per RequestBatcher.
type batchQueue struct {
	batches []*batch
	byRange map[roachpb.RangeID]*batch
}

var _ heap.Interface = (*batchQueue)(nil)

func makeBatchQueue() batchQueue {
	return batchQueue{
		byRange: map[roachpb.RangeID]*batch{},
	}
}

func (q *batchQueue) peekFront() *batch {
	if q.Len() == 0 {
		return nil
	}
	return q.batches[0]
}

func (q *batchQueue) popFront() *batch {
	if q.Len() == 0 {
		return nil
	}
	return heap.Pop(q).(*batch)
}

func (q *batchQueue) get(id roachpb.RangeID) (*batch, bool) {
	b, exists := q.byRange[id]
	return b, exists
}

func (q *batchQueue) remove(ba *batch) {
	delete(q.byRange, ba.rangeID())
	heap.Remove(q, ba.idx)
}

func (q *batchQueue) upsert(ba *batch) {
	if ba.idx >= 0 {
		heap.Fix(q, ba.idx)
	} else {
		heap.Push(q, ba)
	}
}

func (q *batchQueue) Len() int {
	return len(q.batches)
}

func (q *batchQueue) Swap(i, j int) {
	q.batches[i], q.batches[j] = q.batches[j], q.batches[i]
	q.batches[i].idx = i
	q.batches[j].idx = j
}

func (q *batchQueue) Less(i, j int) bool {
	idl, jdl := q.batches[i].deadline, q.batches[j].deadline
	if before := idl.Before(jdl); before || !idl.Equal(jdl) {
		return before
	}
	return q.batches[i].rangeID() < q.batches[j].rangeID()
}

func (q *batchQueue) Push(v interface{}) {
	ba := v.(*batch)
	ba.idx = len(q.batches)
	q.byRange[ba.rangeID()] = ba
	q.batches = append(q.batches, ba)
}

func (q *batchQueue) Pop() interface{} {
	ba := q.batches[len(q.batches)-1]
	q.batches = q.batches[:len(q.batches)-1]
	delete(q.byRange, ba.rangeID())
	ba.idx = -1
	return ba
}
