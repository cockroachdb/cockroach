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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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
	AddResolved(ctx context.Context, span roachpb.Span, ts hlc.Timestamp, boundaryReached bool) error
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
	log.Fatalf(context.TODO(), "found event with unknown type: %+v", *b)
	return 0 // unreachable
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
		if b.backfillTimestamp != (hlc.Timestamp{}) {
			return b.backfillTimestamp
		}
		return b.kv.Value.Timestamp
	default:
		log.Fatalf(context.TODO(), "unknown event type")
		return hlc.Timestamp{} // unreachable
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
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, boundaryReached bool,
) error {
	return b.addEvent(ctx, Event{resolved: &jobspb.ResolvedSpan{Span: span, Timestamp: ts, BoundaryReached: boundaryReached}})
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
	case e := <-b.entriesCh:
		e.bufferGetTimestamp = timeutil.Now()
		return e, nil
	}
}

// MemBufferDefaultCapacity is the default capacity for a memBuffer for a single
// changefeed.
//
// TODO(dan): It would be better if all changefeeds shared a single capacity
// that was given by the operater at startup, like we do for RocksDB and SQL.
var MemBufferDefaultCapacity = envutil.EnvOrDefaultBytes(
	"COCKROACH_CHANGEFEED_BUFFER_CAPACITY", 1<<30) // 1GB

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
		a sqlbase.DatumAlloc
	}
}

func makeMemBuffer(acc mon.BoundAccount, metrics *Metrics) *memBuffer {
	b := &memBuffer{
		metrics:  metrics,
		signalCh: make(chan struct{}, 1),
	}
	b.mu.entries.Init(acc, sqlbase.ColTypeInfoFromColTypes(memBufferColTypes), 0 /* rowCapacity */)
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
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, boundaryReached bool,
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
			b.mu.entries.PopFirst()
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
