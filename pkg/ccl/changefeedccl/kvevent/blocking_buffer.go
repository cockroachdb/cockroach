// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvevent

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// blockingBuffer is an implementation of Buffer which allocates memory
// from a mon.BoundAccount and blocks if no resources are available.
type blockingBuffer struct {
	sv       *settings.Values
	metrics  *PerBufferMetricsWithCompat
	qp       allocPool     // Pool for memory allocations.
	signalCh chan struct{} // Signal when new events are available.

	req struct {
		syncutil.Mutex
		memRequest
	}

	mu struct {
		syncutil.Mutex
		closed     bool          // True when buffer closed.
		reason     error         // Reason buffer is closed.
		drainCh    chan struct{} // Set when Drain request issued.
		numBlocked int           // Number of waitors blocked to acquire quota.
		canFlush   bool
		queue      *bufferEventChunkQueue // Queue of added events.
	}
}

// NewMemBuffer returns a new in-memory buffer which will store events.
// It will grow the bound account to buffer more messages but will block if it
// runs out of space. If ever any entry exceeds the allocatable size of the
// account, an error will be returned when attempting to buffer it.
func NewMemBuffer(
	acc mon.BoundAccount, sv *settings.Values, metrics *PerBufferMetricsWithCompat,
) Buffer {
	return newMemBuffer(acc, sv, metrics, nil)
}

// TestingNewMemBuffer allows test to construct buffer which will invoked
// specified notification function when blocked, waiting for memory.
func TestingNewMemBuffer(
	acc mon.BoundAccount,
	sv *settings.Values,
	metrics *PerBufferMetricsWithCompat,
	onWaitStart quotapool.OnWaitStartFunc,
) Buffer {
	return newMemBuffer(acc, sv, metrics, onWaitStart)
}

func newMemBuffer(
	acc mon.BoundAccount,
	sv *settings.Values,
	metrics *PerBufferMetricsWithCompat,
	onWaitStart quotapool.OnWaitStartFunc,
) Buffer {
	const slowAcquisitionThreshold = 5 * time.Second

	b := &blockingBuffer{
		signalCh: make(chan struct{}, 1),
		metrics:  metrics,
		sv:       sv,
	}
	b.mu.queue = &bufferEventChunkQueue{}

	// Quota pool notifies out of quota events through notifyOutOfQuota
	quota := &memQuota{acc: acc, notifyOutOfQuota: b.notifyOutOfQuota}

	opts := []quotapool.Option{
		quotapool.OnSlowAcquisition(slowAcquisitionThreshold, logSlowAcquisition(slowAcquisitionThreshold, metrics.BufferType)),
		// OnWaitStart invoked once by quota pool when request cannot acquire quota.
		quotapool.OnWaitStart(func(ctx context.Context, poolName string, r quotapool.Request) {
			if onWaitStart != nil {
				onWaitStart(ctx, poolName, r)
			}
			b.producerBlocked()
		}),
		// Similarly, this function is invoked by quota pool once, when the quota
		// have been obtained *after* OnWaitStart
		quotapool.OnWaitFinish(
			func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) {
				metrics.BufferPushbackNanos.Inc(timeutil.Since(start).Nanoseconds())
				b.quotaAcquiredAfterWait()
			},
		),
	}

	b.qp = allocPool{
		AbstractPool: quotapool.New("changefeed", quota, opts...),
		sv:           sv,
		metrics:      metrics,
	}

	return b
}

var _ Buffer = (*blockingBuffer)(nil)

func (b *blockingBuffer) pop() (e Event, ok bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.closed {
		return Event{}, false, ErrBufferClosed{reason: b.mu.reason}
	}

	e, ok = b.mu.queue.dequeue()
	if !ok && b.mu.canFlush {
		// Here, we know that we are blocked, waiting for memory; yet we have nothing queued up
		// (and thus, no resources that could be released by draining the queue).
		// This means that all the previously added entries have been read by the consumer,
		// but their resources have not been yet released.
		// The delayed release could happen when multiple events, along with their allocs,
		// are batched prior to being released (e.g. a sink producing files).
		// If the batching event consumer does not have periodic flush configured,
		// we may never be able to make forward progress.
		// So, we issue the flush request to the consumer to ensure that we release some memory.
		e = Event{et: TypeFlush}
		ok = true
		// Ensure we notify only once.  If we're still out of quota,
		// subsequent notifyOutOfQuota will reset this field.
		b.mu.canFlush = false
	}

	if b.mu.drainCh != nil && b.mu.queue.empty() {
		close(b.mu.drainCh)
		b.mu.drainCh = nil
	}
	return e, ok, nil
}

// notifyOutOfQuota is invoked by memQuota to notify blocking buffer that
// event is blocked, waiting for more resources.
func (b *blockingBuffer) notifyOutOfQuota(canFlush bool) {
	b.mu.Lock()
	b.mu.canFlush = canFlush
	b.mu.Unlock()

	if canFlush {
		select {
		case b.signalCh <- struct{}{}:
		default:
		}
	}
}

// producerBlocked in invoked by quota pool to notify blocking buffer
// that producer is blocked.
func (b *blockingBuffer) producerBlocked() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.numBlocked++
}

// quotaAcquiredAfterWait is invoked by quota pool to notify blocking buffer
// that quota has been acquired after being blocked.
// NB: always called after producerBlocked
func (b *blockingBuffer) quotaAcquiredAfterWait() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.numBlocked > 0 {
		b.mu.numBlocked--
	} else {
		logcrash.ReportOrPanic(context.Background(), b.sv,
			"quotaAcquiredAfterWait called with 0 blocked consumers")
	}
	if b.mu.numBlocked == 0 {
		// Clear out canFlush since we know that producers no longer blocked.
		b.mu.canFlush = false
	}
}

// Get implements kvevent.Reader interface.
func (b *blockingBuffer) Get(ctx context.Context) (ev Event, err error) {
	for {
		got, ok, err := b.pop()
		if err != nil {
			return Event{}, err
		}

		if ok {
			b.metrics.BufferEntriesOut.Inc(1)
			return got, nil
		}

		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-b.signalCh:
		}
	}
}

func (b *blockingBuffer) enqueue(ctx context.Context, e Event) (err error) {
	// Enqueue message, and signal if anybody is waiting.
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.mu.closed {
		logcrash.ReportOrPanic(ctx, b.sv, "buffer unexpectedly closed")
		return errors.New("buffer unexpectedly closed")
	}

	b.metrics.BufferEntriesIn.Inc(1)
	b.metrics.BufferEntriesByType[e.et.Index()].Inc(1)
	b.mu.queue.enqueue(e)

	select {
	case b.signalCh <- struct{}{}:
	default:
	}
	return nil
}

// AcquireMemory acquires specified number of bytes form the memory monitor,
// blocking acquisition if needed.
func (b *blockingBuffer) AcquireMemory(ctx context.Context, n int64) (alloc Alloc, _ error) {
	if l := changefeedbase.PerChangefeedMemLimit.Get(b.sv); n > l {
		return alloc, errors.Newf("event size %d exceeds per changefeed limit %d", alloc, l)
	}
	alloc.init(n, &b.qp)
	if err := func() error {
		b.req.Lock()
		defer b.req.Unlock()
		b.req.memRequest = memRequest(n)
		if err := b.qp.Acquire(ctx, &b.req); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return alloc, err
	}
	b.metrics.BufferEntriesMemAcquired.Inc(n)
	b.metrics.AllocatedMem.Inc(n)
	return alloc, nil
}

// Add implements Writer interface.
func (b *blockingBuffer) Add(ctx context.Context, e Event) error {
	if log.V(2) {
		log.Infof(ctx, "Add event: %s", e.String())
	}

	// Immediately enqueue event if it already has allocation,
	// or if it's a Flush request -- which has no allocations.
	// Such events happen when we switch from backfill to rangefeed mode.
	if e.alloc.ap != nil || e.et == TypeFlush {
		return b.enqueue(ctx, e)
	}

	// Acquire the quota first.
	n := int64(changefeedbase.EventMemoryMultiplier.Get(b.sv) * float64(e.ApproximateSize()))
	e.bufferAddTimestamp = timeutil.Now()
	alloc, err := b.AcquireMemory(ctx, n)
	if err != nil {
		return err
	}
	e.alloc = alloc
	return b.enqueue(ctx, e)
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

// CloseWithReason implements Writer interface.
func (b *blockingBuffer) CloseWithReason(ctx context.Context, reason error) error {
	// Close quota pool -- any requests waiting to acquire will receive an error.
	b.qp.Close("blocking buffer closing")

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
		b.metrics.AllocatedMem.Dec(quota.allocated)
		return false
	})

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.mu.closed {
		logcrash.ReportOrPanic(ctx, b.sv, "close called multiple times")
		return errors.AssertionFailedf("close called multiple times")
	}

	b.mu.closed = true
	b.mu.reason = reason
	close(b.signalCh)

	// Return all queued up entries to the buffer pool.
	b.mu.queue.purge()

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

	// When memQuota blocks waiting for resources, invoke the callback
	// to notify about this. The notification maybe invoked multiple
	// times for a single request that's blocked.
	notifyOutOfQuota func(canFlush bool)

	acc mon.BoundAccount
}

var _ quotapool.Resource = (*memQuota)(nil)

type memRequest int64

// Acquire implements quotapool.Request interface.
func (r *memRequest) Acquire(
	ctx context.Context, resource quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	quota := resource.(*memQuota)
	fulfilled, tryAgainAfter = r.acquireQuota(ctx, quota)
	if !fulfilled {
		// canFlush indicates to the consumer (Get() caller) that it may issue flush
		// request if necessary.
		//
		// Consider the case when we have 2 producers that are blocked (Pa, Pb).
		// Consumer will issue flush request if no events are buffered in this
		// blocking buffer, and we have blocked producers (i.e. Pa and Pb). As soon
		// as flush completes and releases lots of resources (actually, the entirety
		// of mem buffer limit worth of resources are released), Pa manages to put
		// in the event into the queue. If the consumer consumes that message, plus
		// attempts to consume the next message, before Pb had a chance to unblock
		// itself, the consumer will mistakenly think that it must flush to release
		// resources (i.e. just after 1 message).
		//
		// canFlush is set to true if we are *really* blocked -- i.e. we
		// have non-zero canAllocateBelow threshold; OR in the corner case when
		// nothing is allocated (and  we are still blocked -- see comment in
		// acquireQuota)
		canFlush := quota.allocated == 0 || quota.canAllocateBelow > 0
		quota.notifyOutOfQuota(canFlush)
	}
	return fulfilled, tryAgainAfter
}

func (r *memRequest) acquireQuota(
	ctx context.Context, quota *memQuota,
) (fulfilled bool, tryAgainAfter time.Duration) {
	if quota.canAllocateBelow > 0 {
		if quota.allocated > quota.canAllocateBelow {
			return false, 0
		}
		quota.canAllocateBelow = 0
	}

	if err := quota.acc.Grow(ctx, int64(*r)); err != nil {
		if quota.allocated == 0 {
			// We've failed but there's nothing outstanding.  It seems that this request
			// is doomed to fail forever. However, that's not the case since our memory
			// quota is tied to a larger memory pool.  We failed to allocate memory for this
			// single request, but we may succeed if we try again later since some other
			// process may release it into the pool.
			// TODO(yevgeniy): Consider making retry configurable; possibly with backoff.
			return false, time.Second
		}

		// Back off on allocating until we've cleared up half of our usage.
		quota.canAllocateBelow = quota.allocated/2 + 1
		return false, 0
	}

	quota.allocated += int64(*r)
	quota.canAllocateBelow = 0
	return true, 0
}

// ShouldWait implements quotapool.Request interface.
func (r *memRequest) ShouldWait() bool {
	return true
}

type allocPool struct {
	*quotapool.AbstractPool
	metrics *PerBufferMetricsWithCompat
	sv      *settings.Values
}

func (ap allocPool) Release(ctx context.Context, bytes, entries int64) {
	if bytes < 0 {
		logcrash.ReportOrPanic(ctx, ap.sv, "attempt to release negative bytes (%d) into pool", bytes)
	}

	ap.AbstractPool.Update(func(r quotapool.Resource) (shouldNotify bool) {
		quota := r.(*memQuota)
		if quota.closed {
			return false
		}
		quota.acc.Shrink(ctx, bytes)
		quota.allocated -= bytes
		ap.metrics.AllocatedMem.Dec(bytes)
		ap.metrics.BufferEntriesMemReleased.Inc(bytes)
		ap.metrics.BufferEntriesReleased.Inc(entries)
		return true
	})
}

// logSlowAcquisition is a function returning a quotapool.SlowAcquisitionFunction.
// It differs from the quotapool.LogSlowAcquisition in that only some of slow acquisition
// events are logged to reduce log spam.
func logSlowAcquisition(
	slowAcquisitionThreshold time.Duration, bufType bufferType,
) quotapool.SlowAcquisitionFunc {
	logSlowAcquire := log.Every(slowAcquisitionThreshold)

	return func(ctx context.Context, poolName string, r quotapool.Request, start time.Time) func() {
		shouldLog := logSlowAcquire.ShouldLog()
		if shouldLog {
			log.Warningf(ctx, "have been waiting %s attempting to acquire changefeed quota (buffer=%s)", redact.SafeString(bufType),
				timeutil.Since(start))
		}

		return func() {
			if shouldLog {
				log.Infof(ctx, "acquired changefeed quota after %s (buffer=%s)", timeutil.Since(start), redact.SafeString(bufType))
			}
		}
	}
}
