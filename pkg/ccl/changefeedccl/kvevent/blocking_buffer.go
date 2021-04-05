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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// MemBufferDefaultCapacity is the default capacity for a memBuffer for a single
// changefeed.
//
// TODO(dan): It would be better if all changefeeds shared a single capacity
// that was given by the operater at startup, like we do for RocksDB and SQL.
var MemBufferDefaultCapacity = envutil.EnvOrDefaultBytes(
	"COCKROACH_CHANGEFEED_BUFFER_CAPACITY", 1<<30) // 1GB

// blockingBuffer is an implementation of Buffer which allocates memory
// from a mon.BoundAccount and blocks if no resources are available.
type blockingBuffer struct {
	blockingBufferQuotaPool
	signalCh chan struct{}
	mu       struct {
		syncutil.Mutex
		closed bool
		queue  bufferEntryQueue
	}
}

// NewMemBuffer returns a new in-memory buffer which will store events.
// It will grow the bound account to buffer more messages but will block if it
// runs out of space. If ever any entry exceeds the allocatable size of the
// account, an error will be returned when attempting to buffer it.
func NewMemBuffer(acc mon.BoundAccount, metrics *Metrics) Buffer {
	bb := &blockingBuffer{
		signalCh: make(chan struct{}),
	}
	bb.acc = acc
	bb.metrics = metrics
	bb.qp = quotapool.New("changefeed", &bb.blockingBufferQuotaPool)
	return bb
}

var _ Buffer = (*blockingBuffer)(nil)

func (b *blockingBuffer) Get(ctx context.Context) (ev Event, err error) {
	for {
		b.mu.Lock()
		closed := b.mu.closed
		var got *bufferEntry
		if !closed {
			got = b.mu.queue.dequeue()
		}
		b.mu.Unlock()
		if closed {
			return Event{}, nil
		}
		if got != nil {
			e := got.e
			e.bufferGetTimestamp = timeutil.Now()
			b.qp.Add(got)
			return e, nil
		}

		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-b.signalCh:
		}
	}
}

func (b *blockingBuffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	size := kv.Size() + prevVal.Size() + backfillTimestamp.Size() + int(unsafe.Sizeof(bufferEntry{}))
	e := makeKVEvent(kv, prevVal, backfillTimestamp)
	return b.addEvent(ctx, e, size)
}

func (b *blockingBuffer) AddResolved(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) error {
	size := span.Size() + ts.Size() + 4 + int(unsafe.Sizeof(bufferEntry{}))
	e := makeResolvedEvent(span, ts, boundaryType)
	return b.addEvent(ctx, e, size)
}

func (b *blockingBuffer) addEvent(ctx context.Context, e Event, size int) error {
	be := bufferEntryPool.Get().(*bufferEntry)
	be.e = e
	be.alloc = int64(size)

	// Acquire the quota first.
	err := b.qp.Acquire(ctx, be)
	if err != nil {
		return err
	}
	if be.err != nil {
		return be.err
	}

	b.mu.Lock()
	closed := b.mu.closed
	if !closed {
		b.mu.queue.enqueue(be)
	}
	b.mu.Unlock()

	if closed {
		b.qp.Add(be)
		return nil
	}
	select {
	case b.signalCh <- struct{}{}:
	default:
	}
	return nil
}

func (b *blockingBuffer) Close(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.mu.closed {
		b.mu.closed = true
		b.qp.Close("")
		for be := b.mu.queue.dequeue(); be != nil; be = b.mu.queue.dequeue() {
			b.qp.Add(be)
		}
		b.acc.Close(ctx)
		close(b.signalCh)
	}
}

type blockingBufferQuotaPool struct {
	qp      *quotapool.QuotaPool
	metrics *Metrics

	// Below fields accessed underneath the quotapool.

	// allocated is the number of bytes currently allocated.
	allocated int64

	// Errors indicating a failure to allocate are relatively expensive.
	// We don't want to see them often. If we see one, avoid allocating
	// again until the allocated budget drops to below half that level.
	canAllocateBelow int64

	acc mon.BoundAccount
}

var _ quotapool.Resource = (*blockingBufferQuotaPool)(nil)

func (b *blockingBufferQuotaPool) Merge(val interface{}) (shouldNotify bool) {
	switch val := val.(type) {
	case *bufferEntry:
		b.acc.Shrink(context.TODO(), val.alloc)
		b.allocated -= val.alloc
		*val = bufferEntry{}
		bufferEntryPool.Put(val)
		b.metrics.BufferEntriesOut.Inc(1)
		return true
	default:
		panic(errors.AssertionFailedf("unknown resource type %T", val))
	}
}

// bufferEntry forms a linked list of elements in the buffer.
// It also implements quotapool.Request and is used to acquire quota.
// These entries are pooled to eliminate allocations.
type bufferEntry struct {
	e Event

	alloc int64 // bytes allocated from the quotapool
	err   error // error populated from under the quotapool

	next *bufferEntry // linked-list element
}

var bufferEntryPool = sync.Pool{
	New: func() interface{} {
		return new(bufferEntry)
	},
}

var _ quotapool.Request = (*bufferEntry)(nil)

func (r *bufferEntry) Acquire(
	ctx context.Context, resource quotapool.Resource,
) (fulfilled bool, tryAgainAfter time.Duration) {
	res := resource.(*blockingBufferQuotaPool)
	if res.canAllocateBelow > 0 {
		if res.allocated > res.canAllocateBelow {
			return false, 0
		}
		res.canAllocateBelow = 0
	}
	if err := res.acc.Grow(ctx, r.alloc); err != nil {
		if res.allocated == 0 {
			// We've failed but there's nothing outstanding, that means we're doomed
			// to fail forever and should propagate the error.
			r.err = err
			return true, 0
		}

		// Back off on allocating until we've cleared up half of our usage.
		res.canAllocateBelow = res.allocated/2 + 1
		return false, 0
	}
	res.metrics.BufferEntriesIn.Inc(1)
	res.allocated += r.alloc
	res.canAllocateBelow = 0
	return true, 0
}

func (r *bufferEntry) ShouldWait() bool {
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
