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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type bufferEntry struct {
	kv       roachpb.KeyValue
	resolved *jobspb.ResolvedSpan
	// Timestamp of the schema that should be used to read this KV.
	// If unset (zero-valued), the value's timestamp will be used instead.
	schemaTimestamp hlc.Timestamp
	// bufferGetTimestamp is the time this entry came out of the buffer.
	bufferGetTimestamp time.Time
}

// buffer mediates between the changed data poller and the rest of the
// changefeed pipeline (which is backpressured all the way to the sink).
//
// TODO(dan): Monitor memory usage and spill to disk when necessary.
type buffer struct {
	entriesCh chan bufferEntry
}

func makeBuffer() *buffer {
	return &buffer{entriesCh: make(chan bufferEntry)}
}

// AddKV inserts a changed kv into the buffer.
//
// TODO(dan): AddKV currently requires that each key is added in increasing mvcc
// timestamp order. This will have to change when we add support for RangeFeed,
// which starts out in a catchup state without this guarantee.
func (b *buffer) AddKV(ctx context.Context, kv roachpb.KeyValue, minTimestamp hlc.Timestamp) error {
	return b.addEntry(ctx, bufferEntry{kv: kv, schemaTimestamp: minTimestamp})
}

// AddResolved inserts a resolved timestamp notification in the buffer.
func (b *buffer) AddResolved(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) error {
	return b.addEntry(ctx, bufferEntry{resolved: &jobspb.ResolvedSpan{Span: span, Timestamp: ts}})
}

func (b *buffer) addEntry(ctx context.Context, e bufferEntry) error {
	// TODO(dan): Spill to a temp rocksdb if entriesCh would block.
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

var memBufferColTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES}, // kv.Key
	{SemanticType: sqlbase.ColumnType_BYTES}, // kv.Value
	{SemanticType: sqlbase.ColumnType_BYTES}, // span.Key
	{SemanticType: sqlbase.ColumnType_BYTES}, // span.EndKey
	{SemanticType: sqlbase.ColumnType_INT},   // ts.WallTime
	{SemanticType: sqlbase.ColumnType_INT},   // ts.Logical
	{SemanticType: sqlbase.ColumnType_INT},   // schemaTimestamp.WallTime
	{SemanticType: sqlbase.ColumnType_INT},   // schemaTimestamp.Logical
}

// memBuffer is an in-memory buffer for changed KV and resolved timestamp
// events. It's size is limited only by the BoundAccount passed to the
// constructor.
type memBuffer struct {
	mu struct {
		syncutil.Mutex
		entries rowcontainer.RowContainer
	}

	allocMu struct {
		syncutil.Mutex
		a sqlbase.DatumAlloc
	}
}

func makeMemBuffer(acc mon.BoundAccount) *memBuffer {
	b := &memBuffer{}
	b.mu.entries.Init(acc, sqlbase.ColTypeInfoFromColTypes(memBufferColTypes), 0 /* rowCapacity */)
	return b
}

func (b *memBuffer) Close(ctx context.Context) {
	b.mu.Lock()
	b.mu.entries.Close(ctx)
	b.mu.Unlock()
}

// AddKV inserts a changed kv into the buffer.
func (b *memBuffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, schemaTimestamp hlc.Timestamp,
) error {
	b.allocMu.Lock()
	row := tree.Datums{
		b.allocMu.a.NewDBytes(tree.DBytes(kv.Key)),
		b.allocMu.a.NewDBytes(tree.DBytes(kv.Value.RawBytes)),
		tree.DNull,
		tree.DNull,
		b.allocMu.a.NewDInt(tree.DInt(kv.Value.Timestamp.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(kv.Value.Timestamp.Logical)),
		b.allocMu.a.NewDInt(tree.DInt(schemaTimestamp.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(schemaTimestamp.Logical)),
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
		b.allocMu.a.NewDBytes(tree.DBytes(span.Key)),
		b.allocMu.a.NewDBytes(tree.DBytes(span.EndKey)),
		b.allocMu.a.NewDInt(tree.DInt(ts.WallTime)),
		b.allocMu.a.NewDInt(tree.DInt(ts.Logical)),
		tree.DNull,
		tree.DNull,
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
		WallTime: int64(*row[4].(*tree.DInt)),
		Logical:  int32(*row[5].(*tree.DInt)),
	}
	if row[0] != tree.DNull {
		e.kv = roachpb.KeyValue{
			Key: []byte(*row[0].(*tree.DBytes)),
			Value: roachpb.Value{
				RawBytes:  []byte(*row[1].(*tree.DBytes)),
				Timestamp: ts,
			},
		}
		e.schemaTimestamp = hlc.Timestamp{
			WallTime: int64(*row[6].(*tree.DInt)),
			Logical:  int32(*row[7].(*tree.DInt)),
		}
		return e, nil
	}
	e.resolved = &jobspb.ResolvedSpan{
		Span: roachpb.Span{
			Key:    []byte(*row[2].(*tree.DBytes)),
			EndKey: []byte(*row[3].(*tree.DBytes)),
		},
		Timestamp: ts,
	}
	return e, nil
}

func (b *memBuffer) addRow(ctx context.Context, row tree.Datums) error {
	b.mu.Lock()
	_, err := b.mu.entries.AddRow(ctx, row)
	b.mu.Unlock()
	return err
}

func (b *memBuffer) getRow(ctx context.Context) (tree.Datums, error) {
	retryOpts := retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Second,
	}

	var row tree.Datums
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		b.mu.Lock()
		if b.mu.entries.Len() > 0 {
			row = b.mu.entries.At(0)
			b.mu.entries.PopFirst()
		}
		b.mu.Unlock()
		if row != nil {
			return row, nil
		}
	}
	return nil, ctx.Err()
}
