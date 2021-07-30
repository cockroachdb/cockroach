// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// blockingBuffer is an implementation of Buffer which allocates memory
// from a mon.BoundAccount and blocks if no resources are available.
type blockingBuffer struct {
	sv       *settings.Values
	metrics  *Metrics
	qp       *quotapool.AbstractPool // Pool for memory allocations.
	signalCh chan struct{}           // Signal when new events are available.

	mu struct {
		syncutil.Mutex
		closed bool
		queue  bufferEntryQueue
	}
}

// NewMemBuffer returns a new in-memory buffer which will store events.
// It will grow the bound account to buffer more messages but will block if it
// runs out of space. If ever any entry exceeds the allocatable size of the
// account, an error will be returned when attempting to buffer it.
func NewMemBuffer(
	acc mon.BoundAccount, sv *settings.Values, metrics *Metrics, opts ...quotapool.Option,
) Buffer {
	opts = append(opts,
		quotapool.OnWaitFinish(
			func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
				metrics.BufferPushbackNanos.Inc(timeutil.Since(start).Nanoseconds())
			}))

	return &blockingBuffer{
		signalCh: make(chan struct{}, 1),
		metrics:  metrics,
		sv:       sv,
		qp:       quotapool.New("changefeed", &memQuota{acc: acc}, opts...),
	}
}

var _ Buffer = (*blockingBuffer)(nil)

func (b *blockingBuffer) pop() (e *bufferEntry, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.closed {
		return nil, io.EOF
	}
	return b.mu.queue.dequeue(), nil
}

// Get implements kvevent.Reader interface.
func (b *blockingBuffer) Get(ctx context.Context) (ev Event, r Resource, err error) {
	for {
		got, err := b.pop()
		if err != nil {
			return Event{}, NoResource, err
		}

		if got != nil {
			b.metrics.BufferEntriesOut.Inc(1)
			e := got.e
			e.bufferGetTimestamp = timeutil.Now()
			bufferEntryPool.Put(got)
			r = allocatedResource{alloc: got.alloc, qp: b.qp}
			return e, r, nil
		}

		select {
		case <-ctx.Done():
			return Event{}, NoResource, ctx.Err()
		case <-b.signalCh:
		}
	}
}

// Add implements Writer interface.
func (b *blockingBuffer) Add(ctx context.Context, e Event) error {
	ensureOpened := func(acquireLock bool) error {
		if acquireLock {
			b.mu.Lock()
			defer b.mu.Unlock()
		}

		return nil
	}

	const acquireLock = true
	const doNotLock = false
	if err := ensureOpened(acquireLock); err != nil {
		return err
	}

	// Acquire the quota first.
	alloc := int64(changefeedbase.EventMemoryMultiplier.Get(b.sv) * float64(e.approxSize))
	if err := b.qp.Acquire(ctx, memRequest(alloc)); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ensureOpened(doNotLock); err != nil {
		return err
	}

	b.metrics.BufferEntriesIn.Inc(1)
	be := newBufferEntry(e, alloc)
	b.mu.queue.enqueue(be)

	select {
	case b.signalCh <- struct{}{}:
	default:
	}
	return nil
}

func (b *blockingBuffer) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.mu.closed {
		msg := "close called multiple times"
		logcrash.ReportOrPanic(ctx, b.sv, msg)
		return errors.AssertionFailedf(msg)
	}

	b.mu.closed = true
	close(b.signalCh)

	// Close quota pool -- any requests waiting to acquire will receive an error.
	// It would be nice if we can logcrash if anybody was waiting.
	b.qp.Close("blocking buffer closing")

	// Release all resources we have queued up.
	var alloc int64
	for be := b.mu.queue.dequeue(); be != nil; be = b.mu.queue.dequeue() {
		alloc += be.alloc
		bufferEntryPool.Put(be)
	}
	if alloc > 0 {
		allocatedResource{qp: b.qp, alloc: alloc}.Release()
	}

	// Mark memory quota closed, and close the underlying bound account,
	// releasing all of its allocated resources at once.
	//
	// Note: we might be releasing memory prematurely, and that there could still
	// be some resources batched up in the sink.  We could try to wait for all
	// the resources to be releases (e.g. memQuota.alllocated reaching 0); however
	// it is unlikely to work correctly, and can block Close from ever completing.
	// That's because the shutdown is often accomplished via context cancellation,
	// and in those cases we may not even get a notification that a resource
	// is no longer in use (e.g. asynchronous kafka flush may not deliver notification at all).
	// So, instead, we just mark memQuota closed; there is a short period of time
	// before shutdown completes when we are under counting resources
	// (if we run out of memory here, it probably means we're way too tight on memory anyway.
	// After all it is not much different from memory counting against program memory usage
	// until GC loop runs).
	b.qp.Update(func(r quotapool.Resource) (shouldNotify bool) {
		quota := r.(*memQuota)
		quota.closed = true
		quota.acc.Close(ctx)
		return false
	})

	return nil
}

// memQuota represents memory quota resource.
type memQuota struct {
	// Below fields accessed underneath the quotapool lock.

	// closed indicates this memory quota is closed.
	// Attempts to release against this quota should be ignored.
	closed bool

	// allocated is the number of bytes currently allocated.
	allocated int64

	// Errors indicating a failure to allocate are relatively expensive.
	// We don't want to see them often. If we see one, avoid allocating
	// again until the allocated budget drops to below half that level.
	canAllocateBelow int64

	acc mon.BoundAccount
}

var _ quotapool.Resource = (*memQuota)(nil)

// bufferEntry forms a linked list of elements in the buffer.
// These entries are pooled to eliminate allocations.
type bufferEntry struct {
	e     Event
	next  *bufferEntry // linked-list element
	alloc int64        // bytes allocated from the memQuota
}

var bufferEntryPool = sync.Pool{
	New: func() interface{} {
		return new(bufferEntry)
	},
}

func newBufferEntry(e Event, alloc int64) *bufferEntry {
	be := bufferEntryPool.Get().(*bufferEntry)
	be.e = e
	be.alloc = alloc
	be.next = nil
	return be
}

// memRequest is a quotapool memory request.
type memRequest int64

var _ quotapool.Request = memRequest(0)

// Acquire implements quotapool.Request interface.
func (r memRequest) Acquire(
	ctx context.Context, resource quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	quota := resource.(*memQuota)
	if quota.canAllocateBelow > 0 {
		if quota.allocated > quota.canAllocateBelow {
			return false, 0
		}
		quota.canAllocateBelow = 0
	}

	if err := quota.acc.Grow(ctx, int64(r)); err != nil {
		if quota.allocated == 0 {
			// We've failed but there's nothing outstanding.  It seems that this request
			// is doomed to fail forever. However, that's not the case since our memory
			// quota is tied to a larger memory pool.  We failed to allocate memory for this
			// single request, but we may succeed if we try again later since some other
			// process may release it into the pool.
			// TODO(yevgeniy): Consider making retry configurable; possibly with backoff.
			return true, time.Second
		}

		// Back off on allocating until we've cleared up half of our usage.
		quota.canAllocateBelow = quota.allocated/2 + 1
		return false, 0
	}

	quota.allocated += int64(r)
	quota.canAllocateBelow = 0
	return true, 0
}

// ShouldWait implements quotapool.Request interface.
func (r memRequest) ShouldWait() bool {
	return true
}

// bufferEntryQueue is a queue implemented as a linked-list of bufferEntry.
type bufferEntryQueue struct {
	head, tail *bufferEntry
}

func (l *bufferEntryQueue) enqueue(be *bufferEntry) {
	if l.tail == nil {
		l.head, l.tail = be, be
	} else {
		l.tail.next = be
		l.tail = be
	}
}

func (l *bufferEntryQueue) dequeue() *bufferEntry {
	if l.head == nil {
		return nil
	}
	ret := l.head
	if l.head = l.head.next; l.head == nil {
		l.tail = nil
	}
	return ret
}

// allocatedResource is a Resource associated with allocated memory that should
// be released.
type allocatedResource struct {
	alloc int64
	qp    *quotapool.AbstractPool
}

func (a allocatedResource) Release() {
	a.qp.Update(func(r quotapool.Resource) (shouldNotify bool) {
		quota := r.(*memQuota)
		if quota.closed {
			return false
		}
		quota.acc.Shrink(context.TODO(), a.alloc)
		quota.allocated -= a.alloc
		return true
	})
}

var _ Resource = allocatedResource{}
