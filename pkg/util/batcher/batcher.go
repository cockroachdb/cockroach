// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package batcher is a library to enable easy batching of operations.
//
// Batching in general represents a tradeoff between throughput and latency. The
// underlying assumption being that batched operations are cheaper than an
// individual operation. If this is not the case for your workload, don't use
// this library.
//
// Batching assumes that data with the same key can be sent in a single batch.
package batcher

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
// asynchronous. This implies that we need knobs to control the maximum
// acceptable amount of time to buffer operations before sending.
// Another wrinkle is dealing with the different ways in which sending a batch
// may fail. A batch may fail in an ambiguous way (RPC/network errors), it may
// fail completely (which is likely indistinguishable from the ambiguous
// failure) and lastly it may fail partially.

// TODO(ajwerner): Consider the difference between a batch key
// (node/destination) and an order key which might imply a data dependency
// between operations. For the initial motivating use cases for this library
// there are no data dependencies between operations and the only key will be
// the guess for where an operation should go.

// TODO(ajwerner): Consider providing a limit on maximum number of requests in
// flight at a time. This may ultimately lead to a need for queuing. Furthermore
// consider using batch time to dynamically tune the amount of time we wait.

// TODO(ajwerner): Consider filtering requests which might have been canceled
// before sending a batch.

// TODO(ajwerner): Consider more dynamic policies with regards to deadlines.
// Perhaps we want to wait no more than some percentile of the duration of
// historical operations and stay idle only some other percentile. For example
// imagine if the max delay was the 50th and the max idle was the 10th? This
// has a problem when much of the prior workload was say local operations and
// happened very rapidly. Perhaps we need to provide some envelope?

// Config contains the dependencies and configuration for a Batcher.
type Config struct {

	// Name of the batcher, used for logging and stopper.
	Name string

	// Sender can round-trip a batch.
	Sender client.Sender

	// Stopper controls the lifecycle of the Batcher.
	Stopper *stop.Stopper

	// MaxSizePerBatch is the maximum number of bytes in individual requests in a
	// batch.
	MaxSizePerBatch int

	// MaxMsgsPerBatch is the maximum number of messages
	MaxMsgsPerBatch int

	// MaxWait is the maximum amount of time a message should wait in a batch
	// before being sent.
	MaxWait time.Duration

	// MaxIdle is the amount of time a batch should wait between message additions
	// before being sent. The idle timer allows clients to observe low latencies
	// when throughput is low.
	MaxIdle time.Duration
}

type Batcher struct {
	pool pool
	cfg  Config

	batches batchQueue

	requestChan chan *request
}

// New creates a new Batcher.
func New(cfg Config) *Batcher {
	if cfg.Stopper == nil {
		panic("cannot pass nil stopper!")
	}
	b := &Batcher{
		cfg:         cfg,
		pool:        makePool(),
		batches:     makeBatchQueue(),
		requestChan: make(chan *request),
	}
	cfg.Stopper.RunAsyncTask(context.Background(), b.cfg.Name, b.run)
	return b
}

// Send sends req as a part of a batch. An error is returned if the context
// is canceled before the sending of the request completes.
func (b *Batcher) Send(
	ctx context.Context, rangeID roachpb.RangeID, req roachpb.Request,
) (roachpb.Response, *roachpb.Error, error) {
	responseChan := b.pool.getResponseChan()
	select {
	case b.requestChan <- b.pool.newRequest(ctx, rangeID, req, responseChan):
	case <-b.cfg.Stopper.ShouldQuiesce():
		return nil, nil, stop.ErrUnavailable
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	select {
	case resp := <-responseChan:
		// It's only safe to put responseChan back in the pool if it has been
		// received from.
		b.pool.putResponseChan(responseChan)
		return resp.resp, resp.pErr, resp.err
	case <-b.cfg.Stopper.ShouldQuiesce():
		return nil, nil, stop.ErrUnavailable
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (b *Batcher) sendBatch(ctx context.Context, ba *batch) {
	b.cfg.Stopper.RunWorker(ctx, func(ctx context.Context) {
		resp, pErr := b.cfg.Sender.Send(ctx, ba.batchRequest())
		for i, r := range ba.reqs {
			res := response{pErr: pErr}
			if resp != nil && i < len(resp.Responses) {
				res.resp = resp.Responses[i].GetInner()
			}
			b.sendResponse(r, res)
		}
	})
}

func (b *Batcher) sendResponse(req *request, resp response) {
	// This send should never block because responseChan is buffered.
	req.responseChan <- resp
	b.pool.putRequest(req)
}

func addRequestToBatch(cfg *Config, now time.Time, ba *batch, r *request) (shouldSend bool) {
	ba.reqs = append(ba.reqs, r)
	ba.size += r.req.Size()
	ba.lastUpdated = now
	ba.deadline = ba.lastUpdated.Add(cfg.MaxIdle)
	if waitDeadline := ba.startTime.Add(cfg.MaxWait); waitDeadline.Before(ba.deadline) {
		ba.deadline = waitDeadline
	}
	return (cfg.MaxMsgsPerBatch > 0 && len(ba.reqs) >= cfg.MaxMsgsPerBatch) ||
		(cfg.MaxSizePerBatch > 0 && ba.size >= cfg.MaxSizePerBatch)
}

func (b *Batcher) cleanup(err error) {
	for ba := b.batches.popFront(); ba != nil; ba = b.batches.popFront() {
		for _, r := range ba.reqs {
			b.sendResponse(r, response{err: err})
		}
	}
}

func (b *Batcher) run(ctx context.Context) {
	var deadline time.Time
	var timer timeutil.Timer
	maybeSetTimer := func() {
		var nextDeadline time.Time
		if next := b.batches.peekFront(); next != nil {
			nextDeadline = next.deadline
		}
		if !deadline.Equal(nextDeadline) {
			deadline = nextDeadline
			if !deadline.IsZero() {
				timer.Reset(time.Until(deadline))
			}
		}
	}
	for {
		select {
		case req := <-b.requestChan:
			now := timeutil.Now()
			ba, existsInQueue := b.batches.get(req.rangeID)
			if !existsInQueue {
				ba = b.pool.newBatch(now)
			}
			if shouldSend := addRequestToBatch(&b.cfg, now, ba, req); shouldSend {
				if existsInQueue {
					b.batches.remove(ba)
				}
				b.sendBatch(ctx, ba)
			} else {
				b.batches.upsert(ba)
			}
			maybeSetTimer()
		case <-timer.C:
			timer.Read = true
			b.sendBatch(ctx, b.batches.popFront())
			maybeSetTimer()
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
	responseChan chan<- response
}

type response struct {
	resp roachpb.Response
	pErr *roachpb.Error
	err  error
}

type batch struct {
	reqs []*request
	size int // bytes

	// idx is the batch's index in the batchQueue.
	idx int

	deadline    time.Time
	startTime   time.Time
	lastUpdated time.Time
}

func (b *batch) rangeID() roachpb.RangeID {
	if len(b.reqs) == 0 {
		panic("rangeID cannot be called on an empty batch")
	}
	return b.reqs[0].rangeID
}

func (b *batch) batchRequest() roachpb.BatchRequest {
	req := roachpb.BatchRequest{
		// Preallocate the Requests slice.
		Requests: make([]roachpb.RequestUnion, 0, len(b.reqs)),
	}
	for _, r := range b.reqs {
		req.Add(r.req)
	}
	return req
}

////////////////////////////////////////////////////////////////////////////////
// pool
////////////////////////////////////////////////////////////////////////////////

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
			New: func() interface{} { return make(chan response, 1) },
		},
		batchPool: sync.Pool{
			New: func() interface{} { return &batch{} },
		},
		requestPool: sync.Pool{
			New: func() interface{} { return &request{} },
		},
	}
}

func (p *pool) getResponseChan() chan response {
	return p.responseChanPool.Get().(chan response)
}

func (p *pool) putResponseChan(r chan response) {
	p.responseChanPool.Put(r)
}

func (p *pool) newRequest(
	ctx context.Context, rangeID roachpb.RangeID, req roachpb.Request, responseChan chan<- response,
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

// batchQueue is a container for batch objects which offers O(1) get based on
// rangeID and peekFront as well as O(log(n)) upsert, removal, popFront.
// Batch structs are heap ordered inside of the batches slice.
// Note that the batch struct stores its index in the batches slice and is -1
// when not part of the queue. The heap methods update the batch indices when
// updating the heap. Take care not to ever put a batch in to multiple
// batchQueues. At time of writing this package only ever used one batchQueue
// per Batcher.
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
	return q.batches[i].deadline.Before(q.batches[j].deadline)
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
