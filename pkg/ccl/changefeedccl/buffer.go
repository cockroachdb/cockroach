// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

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
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type bufferEntry struct {
	kv roachpb.KeyValue
	// prevVal is set if the key had a non-tombstone value before the change
	// and the before value of each change was requested (optDiff).
	prevVal  roachpb.Value
	resolved *jobspb.ResolvedSpan
	// backfillTimestamp overrides the timestamp of the schema that should be
	// used to interpret this KV. If set and prevVal is provided, the previous
	// timestamp will be used to interpret the previous value.
	//
	// If unset (zero-valued), the KV's timestamp will be used to interpret both
	// of the current and previous values instead.
	backfillTimestamp hlc.Timestamp
	// bufferGetTimestamp is the time this entry came out of the buffer.
	bufferGetTimestamp time.Time
}

// buffer mediates between the changed data poller and the rest of the
// changefeed pipeline (which is backpressured all the way to the sink).
type buffer struct {
	entriesCh chan bufferEntry
}

func makeBuffer() *buffer {
	return &buffer{entriesCh: make(chan bufferEntry)}
}

// AddKV inserts a changed kv into the buffer. Individual keys must be added in
// increasing mvcc order.
func (b *buffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	return b.addEntry(ctx, bufferEntry{
		kv:                kv,
		prevVal:           prevVal,
		backfillTimestamp: backfillTimestamp,
	})
}

// AddResolved inserts a resolved timestamp notification in the buffer.
func (b *buffer) AddResolved(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) error {
	return b.addEntry(ctx, bufferEntry{resolved: &jobspb.ResolvedSpan{Span: span, Timestamp: ts}})
}

func (b *buffer) addEntry(ctx context.Context, e bufferEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.entriesCh <- e:
		return nil
	}
}

// Get returns an entry from the buffer. They are handed out in an order that
// (if it is maintained all the way to the sink) meets our external guarantees.
func (b *buffer) Get(ctx context.Context) (bufferEntry, error) {
	select {
	case <-ctx.Done():
		return bufferEntry{}, ctx.Err()
	case e := <-b.entriesCh:
		e.bufferGetTimestamp = timeutil.Now()
		return e, nil
	}
}

// memBufferDefaultCapacity is the default capacity for a memBuffer for a single
// changefeed.
//
// TODO(dan): It would be better if all changefeeds shared a single capacity
// that was given by the operater at startup, like we do for RocksDB and SQL.
var memBufferDefaultCapacity = envutil.EnvOrDefaultBytes(
	"COCKROACH_CHANGEFEED_BUFFER_CAPACITY", 1<<30) // 1GB

var memBufferColTypes = []types.T{
	*types.Bytes, // kv.Key
	*types.Bytes, // kv.Value
	*types.Bytes, // kv.PrevValue
	*types.Bytes, // span.Key
	*types.Bytes, // span.EndKey
	*types.Int,   // ts.WallTime
	*types.Int,   // ts.Logical
}

// memBuffer is an in-memory buffer for changed KV and resolved timestamp
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

// AddKV inserts a changed kv into the buffer. Individual keys must be added in
// increasing mvcc order.
func (b *memBuffer) AddKV(ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value) error {
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

// AddResolved inserts a resolved timestamp notification in the buffer.
func (b *memBuffer) AddResolved(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) error {
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
func (b *memBuffer) Get(ctx context.Context) (bufferEntry, error) {
	row, err := b.getRow(ctx)
	if err != nil {
		return bufferEntry{}, err
	}
	e := bufferEntry{bufferGetTimestamp: timeutil.Now()}
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
