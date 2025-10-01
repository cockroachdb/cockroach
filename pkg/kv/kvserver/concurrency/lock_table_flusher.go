// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"container/heap"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// LockTableFlusher is responsible for sending FlushLockTableRequests for ranges
// whose lock table has grown too large. Ranges are enqueued by their respective
// lock tables and the lock table flusher maintains a set of workers.
type LockTableFlusher struct {
	db      *kv.DB
	clock   *hlc.Clock
	stopper *stop.Stopper

	mu struct {
		syncutil.Mutex

		queue       flushQueue
		workerCount int
	}
	workReady chan struct{}
}

const (
	maxWorkerCount      = 16
	maxFlushBytes       = 16 << 20 // 16 MiB
	workerQuiescePeriod = 30 * time.Second
)

func NewLockTableFlusher(db *kv.DB, clock *hlc.Clock, stopper *stop.Stopper) *LockTableFlusher {
	ltf := &LockTableFlusher{
		db:        db,
		clock:     clock,
		stopper:   stopper,
		workReady: make(chan struct{}),
	}
	ltf.mu.queue = makeFlushQueue()
	return ltf
}

func (f *LockTableFlusher) sendFlush(ctx context.Context, r *lockFlushRequest) error {
	ctx, span := tracing.ChildSpan(ctx, "LockTableFlusher.sendFlush")

	defer span.Finish()

	header := kvpb.Header{
		TargetBytes: maxFlushBytes,
	}
	admissionHeader := kvpb.AdmissionHeader{
		Priority:   int32(admissionpb.LockingNormalPri),
		CreateTime: f.clock.PhysicalNow(),
		Source:     kvpb.AdmissionHeader_ROOT_KV,
	}
	req := kvpb.FlushLockTableRequest{
		RequestHeader: kvpb.RequestHeaderFromSpan(r.span),
	}

	log.KvExec.Infof(ctx, "flushing locks for range %d (%s) (expectedNumToFlush: %d)", r.rangeID, r.span, r.numToFlush)
	resp, pErr := kv.SendWrappedWithAdmission(
		ctx, f.db.NonTransactionalSender(), header, admissionHeader, &req)
	if pErr != nil {
		return pErr.GoError()
	}
	flushResp := resp.(*kvpb.FlushLockTableResponse)
	log.KvExec.Infof(ctx, "flushed %d unreplicated locks as replicated (resume reason: %s)", flushResp.LocksWritten, flushResp.ResumeReason)

	return nil
}

// MaybeEnqueueFlush add a request to flush the given range to the queue. If the
// range is already in the queue, it adjusts the priority of the existing
// request. If a request for the range is already in flight, this call is a
// no-op.
//
// NB: numKeys is only used for request ordering. An in-flight flush request
// will always attempt to flush up to maxFlushByte.
func (f *LockTableFlusher) MaybeEnqueueFlush(id roachpb.RangeID, span roachpb.Span, numKeys int64) {
	added := f.enqueue(&lockFlushRequest{
		span:       span,
		rangeID:    id,
		numToFlush: numKeys,
	})
	if !added {
		return
	}
	// Notify worker that we've added something to the queue.
	select {
	case f.workReady <- struct{}{}:
	default:
	}
}

func (f *LockTableFlusher) enqueue(req *lockFlushRequest) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	added := f.mu.queue.maybeEnqueue(req)
	if f.mu.workerCount == 0 {
		f.maybeStartWorkerLocked()
	}
	return added
}

func (f *LockTableFlusher) maybeStartWorker() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.maybeStartWorkerLocked()
}

func (f *LockTableFlusher) maybeStartWorkerLocked() {
	if f.mu.workerCount >= maxWorkerCount {
		return
	}
	f.mu.workerCount++

	ctx := f.db.AmbientContext.AnnotateCtx(context.Background())
	if err := f.stopper.RunAsyncTask(ctx, "lock-table-flusher", func(ctx context.Context) {
		ctx, cancel := f.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		f.worker(ctx)
	}); err != nil {
		log.KvExec.Errorf(ctx, "could not start lock table flush worker: %s", err.Error())
	}
}

func (f *LockTableFlusher) worker(ctx context.Context) {
	log.KvExec.Infof(ctx, "starting lock table flush worker")
	defer func() { log.KvExec.Infof(ctx, "stopping lock table flush worker") }()

	var lastWorkItem *lockFlushRequest
	var currentWorkItem *lockFlushRequest

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lastWorkItem = currentWorkItem
		var remaining int
		currentWorkItem, remaining = f.nextWorkItem(lastWorkItem)
		if currentWorkItem != nil {
			if remaining > 0 {
				// If the queue has work remaining, try to start another worker.
				f.maybeStartWorker()
			}
			if err := f.sendFlush(ctx, currentWorkItem); err != nil {
				log.KvExec.Errorf(ctx, "lock table flush failed: %s", err.Error())
			}

			// If we had work this time, look for work again.
			continue
		}

		// No work in the queue twice in a row, exit if able.
		if lastWorkItem == nil && f.workerShouldExit() {
			return
		}

		// No work in the queue, let's wait a bit for work. If we don't get
		select {
		case <-f.workReady:
		case <-ctx.Done():
			return
		case <-time.After(workerQuiescePeriod):
			if f.workerShouldExit() {
				return
			}
		}
	}
}

// workerShouldExit returns true if the given worker should exit. When true is
// returned, the worker must exit as the worker count has been decreased.
//
// We never allow the last worker to exit once it is started.
func (f *LockTableFlusher) workerShouldExit() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.mu.workerCount > 1 {
		f.mu.workerCount--
		return true
	}
	return false
}

func (f *LockTableFlusher) nextWorkItem(lastWorkItem *lockFlushRequest) (*lockFlushRequest, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.queue.dequeue(lastWorkItem), f.mu.queue.Len()
}

type lockFlushRequest struct {
	idx int

	span       roachpb.Span
	rangeID    roachpb.RangeID
	numToFlush int64
}

func (l *lockFlushRequest) Combine(o *lockFlushRequest) {
	l.numToFlush = max(l.numToFlush, o.numToFlush)
	if o.span.Key.Less(l.span.Key) {
		l.span.Key = o.span.Key
	}
	if l.span.EndKey.Less(o.span.EndKey) {
		l.span.EndKey = o.span.EndKey
	}
}

func (l *lockFlushRequest) String() string {
	return fmt.Sprintf("lock flush request for %s (%s)", l.rangeID, l.span)
}

// flushQueue maintains a heap ordered by the number of keys we need to flush
// for the given range. It keeps track of dequeued ranges until the worker
// indicates it has finished that work by dequeue-ing another item.
//
// This implementation is not thread-safe and should be wrapped in a Mutex if
// called from more than 1 thread.
//
// This implementation was based on the batchQueue in the request batcher.
type flushQueue struct {
	reqs    []*lockFlushRequest
	byRange map[roachpb.RangeID]*lockFlushRequest

	// inflight ranges currently in-flight by users of the queue.
	inflight map[roachpb.RangeID]struct{}
}

var _ heap.Interface = (*flushQueue)(nil)

func makeFlushQueue() flushQueue {
	return flushQueue{
		byRange:  make(map[roachpb.RangeID]*lockFlushRequest),
		inflight: make(map[roachpb.RangeID]struct{}),
	}
}

// maybeEnqueue adds a request to the queue if it is not already in-flight. If a
// request for this range is already in the queue, its priority is updated based
// on the new request.
func (q *flushQueue) maybeEnqueue(req *lockFlushRequest) bool {
	queuedReq, inFlight, queued := q.get(req.rangeID)
	if inFlight {
		return false
	}

	if queued {
		queuedReq.Combine(req)
		heap.Fix(q, req.idx)
		return false
	}

	heap.Push(q, req)
	return true
}

func (q *flushQueue) dequeue(lastRequest *lockFlushRequest) *lockFlushRequest {
	if lastRequest != nil {
		delete(q.inflight, lastRequest.rangeID)
	}
	item := q.popFront()
	if item == nil {
		return nil
	}
	q.inflight[item.rangeID] = struct{}{}
	return item
}

func (q *flushQueue) popFront() *lockFlushRequest {
	if q.Len() == 0 {
		return nil
	}
	return heap.Pop(q).(*lockFlushRequest)
}

func (q *flushQueue) get(id roachpb.RangeID) (_ *lockFlushRequest, inflight bool, queued bool) {
	if _, ok := q.inflight[id]; ok {
		return nil, true, false
	}
	b, exists := q.byRange[id]
	return b, false, exists
}

func (q *flushQueue) Len() int {
	return len(q.reqs)
}

func (q *flushQueue) Swap(i, j int) {
	q.reqs[i], q.reqs[j] = q.reqs[j], q.reqs[i]
	q.reqs[i].idx = i
	q.reqs[j].idx = j
}

func (q *flushQueue) Less(i, j int) bool {
	iNum, jNum := q.reqs[i].numToFlush, q.reqs[j].numToFlush
	if iNum != jNum {
		// The heap library provides a min heap, so we reverse this conditional so
		// that the largest number of keys to flush is the first to be popped.
		return iNum > jNum
	}
	return q.reqs[i].rangeID < q.reqs[j].rangeID
}

func (q *flushQueue) Push(v interface{}) {
	r := v.(*lockFlushRequest)
	r.idx = len(q.reqs)
	q.byRange[r.rangeID] = r
	q.reqs = append(q.reqs, r)
}

func (q *flushQueue) Pop() interface{} {
	r := q.reqs[len(q.reqs)-1]
	q.reqs[len(q.reqs)-1] = nil
	q.reqs = q.reqs[:len(q.reqs)-1]
	delete(q.byRange, r.rangeID)
	r.idx = -1
	return r
}
