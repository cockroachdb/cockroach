// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ErrBufferClosed is returned by Readers when no more values will be
// returned from the buffer.
var ErrBufferClosed = errors.New("buffer closed")

// EventBuffer is an interface for communicating kvfeed entries between processors.
type EventBuffer interface {
	EventBufferReader
	EventBufferWriter
}

// EventBufferReader is the read portion of the EventBuffer interface.
type EventBufferReader interface {
	// Get retrieves an entry from the buffer.
	Get(ctx context.Context) (Event, error)
}

// EventBufferWriter is the write portion of the EventBuffer interface.
type EventBufferWriter interface {
	AddKV(ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp) error
	AddResolved(ctx context.Context, span roachpb.Span, ts hlc.Timestamp, boundaryType jobspb.ResolvedSpan_BoundaryType) error
	Close(ctx context.Context)
}

// EventType indicates the type of the event.
// Different types indicate which methods will be meaningful.
// Events are implemented this way rather than as an interface to remove the
// need to box the events and allow for events to be used in slices directly.
type EventType int

const (
	// KVEvent indicates that the KV, PrevValue, and BackfillTimestamp methods
	// on the Event meaningful.
	KVEvent EventType = iota

	// ResolvedEvent indicates that the Resolved method on the Event will be
	// meaningful.
	ResolvedEvent

	// TypeUnknown indicates the event could not be parsed. Will fail the feed.
	TypeUnknown
)

// Event represents an event emitted by a kvfeed. It is either a KV
// or a resolved timestamp.
type Event struct {
	kv                 roachpb.KeyValue
	prevVal            roachpb.Value
	resolved           *jobspb.ResolvedSpan
	backfillTimestamp  hlc.Timestamp
	bufferGetTimestamp time.Time
}

// Type returns the event's EventType.
func (b *Event) Type() EventType {
	if b.kv.Key != nil {
		return KVEvent
	}
	if b.resolved != nil {
		return ResolvedEvent
	}
	return TypeUnknown
}

// ApproximateSize returns events approximate size in bytes.
func (b *Event) ApproximateSize() int {
	if b.kv.Key != nil {
		return b.kv.Size() + b.prevVal.Size()
	}
	return b.resolved.Size()
}

// KV is populated if this event returns true for IsKV().
func (b *Event) KV() roachpb.KeyValue {
	return b.kv
}

// PrevValue returns the previous value for this event. PrevValue is non-zero
// if this is a KV event and the key had a non-tombstone value before the change
// and the before value of each change was requested (optDiff).
func (b *Event) PrevValue() roachpb.Value {
	return b.prevVal
}

// Resolved will be non-nil if this is a resolved timestamp event (i.e. IsKV()
// returns false).
func (b *Event) Resolved() *jobspb.ResolvedSpan {
	return b.resolved
}

// BackfillTimestamp overrides the timestamp of the schema that should be
// used to interpret this KV. If set and prevVal is provided, the previous
// timestamp will be used to interpret the previous value.
//
// If unset (zero-valued), the KV's timestamp will be used to interpret both
// of the current and previous values instead.
func (b *Event) BackfillTimestamp() hlc.Timestamp {
	return b.backfillTimestamp
}

// BufferGetTimestamp is the time this event came out of the buffer.
func (b *Event) BufferGetTimestamp() time.Time {
	return b.bufferGetTimestamp
}

// Timestamp returns the timestamp of the write if this is a KV event.
// If there is a non-zero BackfillTimestamp, that is returned.
// If this is a resolved timestamp event, the timestamp is the resolved
// timestamp.
func (b *Event) Timestamp() hlc.Timestamp {
	switch b.Type() {
	case ResolvedEvent:
		return b.resolved.Timestamp
	case KVEvent:
		if !b.backfillTimestamp.IsEmpty() {
			return b.backfillTimestamp
		}
		return b.kv.Value.Timestamp
	default:
		log.Warningf(context.TODO(),
			"setting empty timestamp for unknown event type")
		return hlc.Timestamp{}
	}
}

// chanBuffer mediates between the changed data KVFeed and the rest of the
// changefeed pipeline (which is backpressured all the way to the sink).
type chanBuffer struct {
	entriesCh chan Event
}

// MakeChanBuffer returns an EventBuffer backed by an unbuffered channel.
//
// TODO(ajwerner): Consider adding a buffer here. We know performance of the
// backfill is terrible. Probably some of that is due to every KV being sent
// on a channel. This should all get benchmarked and tuned.
func MakeChanBuffer() EventBuffer {
	return &chanBuffer{entriesCh: make(chan Event)}
}

// AddKV inserts a changed KV into the buffer. Individual keys must be added in
// increasing mvcc order.
func (b *chanBuffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	return b.addEvent(ctx, Event{
		kv:                kv,
		prevVal:           prevVal,
		backfillTimestamp: backfillTimestamp,
	})
}

// AddResolved inserts a Resolved timestamp notification in the buffer.
func (b *chanBuffer) AddResolved(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) error {
	return b.addEvent(ctx, Event{resolved: &jobspb.ResolvedSpan{
		Span:                      span,
		Timestamp:                 ts,
		DeprecatedBoundaryReached: boundaryType != jobspb.ResolvedSpan_NONE,
		BoundaryType:              boundaryType,
	}})
}

func (b *chanBuffer) Close(_ context.Context) {
	close(b.entriesCh)
}

func (b *chanBuffer) addEvent(ctx context.Context, e Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.entriesCh <- e:
		return nil
	}
}

// Get returns an entry from the buffer. They are handed out in an order that
// (if it is maintained all the way to the sink) meets our external guarantees.
func (b *chanBuffer) Get(ctx context.Context) (Event, error) {
	select {
	case <-ctx.Done():
		return Event{}, ctx.Err()
	case e, ok := <-b.entriesCh:
		if !ok {
			// Our channel has been closed by the
			// Writer. No more events will be returned.
			return e, ErrBufferClosed
		}
		e.bufferGetTimestamp = timeutil.Now()
		return e, nil
	}
}

var memBufferColTypes = []*types.T{
	types.Bytes, // KV.Key
	types.Bytes, // KV.Value
	types.Bytes, // KV.PrevValue
	types.Bytes, // span.Key
	types.Bytes, // span.EndKey
	types.Int,   // ts.WallTime
	types.Int,   // ts.Logical
}

// memBuffer is an in-memory buffer for changed KV and Resolved timestamp
// events. It's size is limited only by the BoundAccount passed to the
// constructor. memBuffer is only for use with single-producer single-consumer.
type memBuffer struct {
	metrics *Metrics

	mu struct {
		syncutil.Mutex
		entries rowcontainer.RowContainer
	}
	// signalCh can be selected on to learn when an entry is written to
	// mu.entries.
	signalCh chan struct{}

	allocMu struct {
		syncutil.Mutex
		a rowenc.DatumAlloc
	}
}

// MakeMemBuffer returns an EventBuffer backed by memory, limited
// as specified by bound account.
func MakeMemBuffer(acc mon.BoundAccount, metrics *Metrics) EventBuffer {
	b := &memBuffer{
		metrics:  metrics,
		signalCh: make(chan struct{}, 1),
	}
	b.mu.entries.Init(acc, colinfo.ColTypeInfoFromColTypes(memBufferColTypes), 0 /* rowCapacity */)
	return b
}

func (b *memBuffer) Close(ctx context.Context) {
	b.mu.Lock()
	b.mu.entries.Close(ctx)
	b.mu.Unlock()
}

// AddKV inserts a changed KV into the buffer. Individual keys must be added in
// increasing mvcc order.
func (b *memBuffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	b.allocMu.Lock()
	prevValDatum := tree.DNull
	if prevVal.IsPresent() {
		prevValDatum = b.allocMu.a.NewDBytes(tree.DBytes(prevVal.RawBytes))
	}
	row := tree.Datums{
		b.allocMu.a.NewDBytes(tree.DBytes(kv.Key)),
		b.allocMu.a.NewDBytes(tree.DBytes(kv.Value.RawBytes)),
		prevValDatum,
		tree.DNull,
		tree.DNull,
		b.allocMu.a.NewDInt(tree.DInt(kv.Value.Timestamp.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(kv.Value.Timestamp.Logical)),
	}
	b.allocMu.Unlock()
	return b.addRow(ctx, row)
}

// AddResolved inserts a Resolved timestamp notification in the buffer.
func (b *memBuffer) AddResolved(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) error {
	b.allocMu.Lock()
	row := tree.Datums{
		tree.DNull,
		tree.DNull,
		tree.DNull,
		b.allocMu.a.NewDBytes(tree.DBytes(span.Key)),
		b.allocMu.a.NewDBytes(tree.DBytes(span.EndKey)),
		b.allocMu.a.NewDInt(tree.DInt(ts.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(ts.Logical)),
	}
	b.allocMu.Unlock()
	return b.addRow(ctx, row)
}

// Get returns an entry from the buffer. They are handed out in an order that
// (if it is maintained all the way to the sink) meets our external guarantees.
func (b *memBuffer) Get(ctx context.Context) (Event, error) {
	row, err := b.getRow(ctx)
	if err != nil {
		return Event{}, err
	}
	e := Event{bufferGetTimestamp: timeutil.Now()}
	ts := hlc.Timestamp{
		WallTime: int64(*row[5].(*tree.DInt)),
		Logical:  int32(*row[6].(*tree.DInt)),
	}
	if row[2] != tree.DNull {
		e.prevVal = roachpb.Value{
			RawBytes: []byte(*row[2].(*tree.DBytes)),
		}
	}
	if row[0] != tree.DNull {
		e.kv = roachpb.KeyValue{
			Key: []byte(*row[0].(*tree.DBytes)),
			Value: roachpb.Value{
				RawBytes:  []byte(*row[1].(*tree.DBytes)),
				Timestamp: ts,
			},
		}
		return e, nil
	}
	e.resolved = &jobspb.ResolvedSpan{
		Span: roachpb.Span{
			Key:    []byte(*row[3].(*tree.DBytes)),
			EndKey: []byte(*row[4].(*tree.DBytes)),
		},
		Timestamp: ts,
	}
	return e, nil
}

func (b *memBuffer) addRow(ctx context.Context, row tree.Datums) error {
	b.mu.Lock()
	_, err := b.mu.entries.AddRow(ctx, row)
	b.mu.Unlock()
	b.metrics.BufferEntriesIn.Inc(1)
	select {
	case b.signalCh <- struct{}{}:
	default:
		// Already signaled, don't need to signal again.
	}
	return err
}

func (b *memBuffer) getRow(ctx context.Context) (tree.Datums, error) {
	for {
		var row tree.Datums
		b.mu.Lock()
		if b.mu.entries.Len() > 0 {
			row = b.mu.entries.At(0)
			b.mu.entries.PopFirst(ctx)
		}
		b.mu.Unlock()
		if row != nil {
			b.metrics.BufferEntriesOut.Inc(1)
			return row, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-b.signalCh:
		}
	}
}

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

// NewBlockingBuffer returns a new in-memory buffer which will store events.
// It will grow the bound account to buffer more messages but will block if it
// runs out of space. If ever any entry exceeds the allocatable size of the
// account, an error will be returned when attempting to buffer it.
func NewBlockingBuffer(acc mon.BoundAccount, metrics *Metrics) EventBuffer {
	bb := &blockingBuffer{
		signalCh: make(chan struct{}, 1),
	}
	bb.acc = acc
	bb.metrics = metrics
	bb.qp = quotapool.New("changefeed", &bb.blockingBufferQuotaPool)
	return bb
}

var _ EventBuffer = (*blockingBuffer)(nil)

func (b *blockingBuffer) pop() (e *bufferEntry, closed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mu.closed {
		return nil, true
	}
	return b.mu.queue.dequeue(), false
}

// Get implements kvevent.Reader interface.
func (b *blockingBuffer) Get(ctx context.Context) (ev Event, err error) {
	for {
		got, closed := b.pop()
		if closed {
			return Event{}, nil
		}

		if got != nil {
			e := got.e
			e.bufferGetTimestamp = timeutil.Now()
			b.qp.Update(func(r quotapool.Resource) (shouldNotify bool) {
				res := r.(*blockingBufferQuotaPool)
				res.release(got)
				return true
			})
			return e, nil
		}

		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-b.signalCh:
		}
	}
}

// AddKV implements kvevent.Writer interface.
func (b *blockingBuffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	size := kv.Size() + prevVal.Size() + backfillTimestamp.Size() + int(unsafe.Sizeof(bufferEntry{}))
	e := Event{
		kv:                kv,
		prevVal:           prevVal,
		backfillTimestamp: backfillTimestamp,
	}
	return b.addEvent(ctx, e, size)
}

// AddResolved implements kvevent.Writer interface.
func (b *blockingBuffer) AddResolved(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) error {
	size := span.Size() + ts.Size() + 4 + int(unsafe.Sizeof(bufferEntry{}))
	e := Event{resolved: &jobspb.ResolvedSpan{
		Span:                      span,
		Timestamp:                 ts,
		DeprecatedBoundaryReached: boundaryType != jobspb.ResolvedSpan_NONE,
		BoundaryType:              boundaryType,
	}}
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
		b.qp.Update(func(r quotapool.Resource) (shouldNotify bool) {
			r.(*blockingBufferQuotaPool).release(be)
			return false
		})
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
			b.release(be)
		}
		b.acc.Close(ctx)
		close(b.signalCh)
	}
}

type blockingBufferQuotaPool struct {
	qp      *quotapool.AbstractPool
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

// release releases resources allocated for buffer entry, and puts this entry
// back into the entry pool.
func (b *blockingBufferQuotaPool) release(e *bufferEntry) {
	b.acc.Shrink(context.TODO(), e.alloc)
	b.allocated -= e.alloc
	*e = bufferEntry{}
	bufferEntryPool.Put(e)
	b.metrics.BufferEntriesOut.Inc(1)
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

// Acquire implements quotapool.Request interface.
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

// ShouldWait implements quotapool.Request interface.
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
