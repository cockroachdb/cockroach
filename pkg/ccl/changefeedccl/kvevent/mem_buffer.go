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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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

func NewMemBuffer(acc mon.BoundAccount, metrics *Metrics) *memBuffer {
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
