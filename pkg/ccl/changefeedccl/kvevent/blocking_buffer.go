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
	qp       allocPool     // Pool for memory allocations.
	signalCh chan struct{} // Signal when new events are available.

	mu struct {
		syncutil.Mutex
		closed  bool
		drainCh chan struct{}
		queue   bufferEntryQueue
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
		quotapool.OnSlowAcquisition(5*time.Second, quotapool.LogSlowAcquisition),
		quotapool.OnWaitFinish(
			func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
				metrics.BufferPushbackNanos.Inc(timeutil.Since(start).Nanoseconds())
			}))

	return &blockingBuffer{
		signalCh: make(chan struct{}, 1),
		metrics:  metrics,
		sv:       sv,
		qp: allocPool{
			AbstractPool: quotapool.New("changefeed", &memQuota{acc: acc}, opts...),
			metrics:      metrics,
		},
	}
}

var _ Buffer = (*blockingBuffer)(nil)

func (b *blockingBuffer) pop() (e *bufferEntry, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.closed {
		return nil, ErrBufferClosed
	}
	e = b.mu.queue.dequeue()
	if b.mu.drainCh != nil && b.mu.queue.empty() {
		close(b.mu.drainCh)
		b.mu.drainCh = nil
	}
	return e, nil
}

// Get implements kvevent.Reader interface.
func (b *blockingBuffer) Get(ctx context.Context) (ev Event, err error) {
	for {
		got, err := b.pop()
		if err != nil {
			return Event{}, err
		}

		if got != nil {
			b.metrics.BufferEntriesOut.Inc(1)
			e := got.e
			e.bufferGetTimestamp = timeutil.Now()
			bufferEntryPool.Put(got)
			return e, nil
		}

		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-b.signalCh:
		}
	}
}

func (b *blockingBuffer) ensureOpened(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ensureOpenedLocked(ctx)
}

func (b *blockingBuffer) ensureOpenedLocked(ctx context.Context) error {
	if b.mu.closed {
		logcrash.ReportOrPanic(ctx, b.sv, "buffer unexpectedly closed")
		return errors.AssertionFailedf("buffer unexpectedly closed")
	}

	return nil
}

// Add implements Writer interface.
func (b *blockingBuffer) Add(ctx context.Context, e Event) error {
	if err := b.ensureOpened(ctx); err != nil {
		return err
	}

	var be *bufferEntry
	if e.alloc.ap == nil {
		// Acquire the quota first.
		alloc := int64(changefeedbase.EventMemoryMultiplier.Get(b.sv) * float64(e.approxSize))
		if l := changefeedbase.PerChangefeedMemLimit.Get(b.sv); alloc > l {
			return errors.Newf("event size %d exceeds per changefeed limit %d", alloc, l)
		}
		e.alloc = Alloc{
			bytes:   alloc,
			entries: 1,
			ap:      &b.qp,
		}
		be = newBufferEntry(e)

		if err := b.qp.Acquire(ctx, be); err != nil {
			bufferEntryPool.Put(be)
			return err
		}
		b.metrics.BufferEntriesMemAcquired.Inc(alloc)
	} else {
		// Use allocation associated with the event itself.
		be = newBufferEntry(e)
	}

	// Enqueue message, and signal if anybody is waiting.
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenedLocked(ctx); err != nil {
		return err
	}
	b.metrics.BufferEntriesIn.Inc(1)
	b.mu.queue.enqueue(be)

	select {
	case b.signalCh <- struct{}{}:
	default:
	}
	return nil
}

// tryDrain attempts to see if the buffer already empty.
// If so, returns nil.  If not, returns a channel that will be closed once the buffer is empty.
func (b *blockingBuffer) tryDrain() chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.queue.empty() {
		return nil
	}

	b.mu.drainCh = make(chan struct{})
	return b.mu.drainCh
}

// Drain implements Writer interface.
func (b *blockingBuffer) Drain(ctx context.Context) error {
	if drained := b.tryDrain(); drained != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-drained:
			return nil
		}
	}

	return nil
}

// Close implements Writer interface.
func (b *blockingBuffer) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.mu.closed {
		logcrash.ReportOrPanic(ctx, b.sv, "close called multiple times")
		return errors.AssertionFailedf("close called multiple times")
	}

	b.mu.closed = true
	close(b.signalCh)

	// Close quota pool -- any requests waiting to acquire will receive an error.
	// It would be nice if we can logcrash if anybody was waiting.
	b.qp.Close("blocking buffer closing")

	// Release all resources we have queued up.
	var alloc Alloc
	for be := b.mu.queue.dequeue(); be != nil; be = b.mu.queue.dequeue() {
		alloc.Merge(&be.e.alloc)
	}
	alloc.Release(ctx)

	// Mark memory quota closed, and close the underlying bound account,
	// releasing all of its allocated resources at once.
	//
	// Note: we might be releasing memory prematurely, and that there could still
	// be some resources batched up in the sink.  We could try to wait for all
	// the resources to be releases (e.g. memQuota.alllocated reaching 0); however
	// it is unlikely to work correctly, and can block Close from ever completing.
	// That's because the shutdown is often accomplished via context cancellation,
	// and in those cases we may not even get a notification that a alloc
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

// memQuota represents memory quota alloc.
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
// bufferEntry also implements quotapool.Request interface for resource acquisition.
type bufferEntry struct {
	e    Event
	next *bufferEntry // linked-list element
}

var bufferEntryPool = sync.Pool{
	New: func() interface{} {
		return new(bufferEntry)
	},
}

func newBufferEntry(e Event) *bufferEntry {
	be := bufferEntryPool.Get().(*bufferEntry)
	be.e = e
	be.next = nil
	return be
}

var _ quotapool.Request = (*bufferEntry)(nil)

// Acquire implements quotapool.Request interface.
func (be *bufferEntry) Acquire(
	ctx context.Context, resource quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	quota := resource.(*memQuota)
	if quota.canAllocateBelow > 0 {
		if quota.allocated > quota.canAllocateBelow {
			return false, 0
		}
		quota.canAllocateBelow = 0
	}

	if err := quota.acc.Grow(ctx, be.e.alloc.bytes); err != nil {
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

	quota.allocated += be.e.alloc.bytes
	quota.canAllocateBelow = 0
	return true, 0
}

// ShouldWait implements quotapool.Request interface.
func (be *bufferEntry) ShouldWait() bool {
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

func (l *bufferEntryQueue) empty() bool {
	return l.head == nil
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

type allocPool struct {
	*quotapool.AbstractPool
	metrics *Metrics
}

func (ap allocPool) Release(ctx context.Context, bytes, entries int64) {
	ap.AbstractPool.Update(func(r quotapool.Resource) (shouldNotify bool) {
		quota := r.(*memQuota)
		if quota.closed {
			return false
		}
		quota.acc.Shrink(ctx, bytes)
		quota.allocated -= bytes
		ap.metrics.BufferEntriesMemReleased.Inc(bytes)
		ap.metrics.BufferEntriesReleased.Inc(entries)
		return true
	})
}
