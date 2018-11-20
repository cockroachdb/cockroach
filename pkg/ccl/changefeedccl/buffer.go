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

	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var bufferColTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES}, // kv.Key
	{SemanticType: sqlbase.ColumnType_BYTES}, // kv.Value
	{SemanticType: sqlbase.ColumnType_BYTES}, // span.Key
	{SemanticType: sqlbase.ColumnType_BYTES}, // span.EndKey
	{SemanticType: sqlbase.ColumnType_INT},   // ts.WallTime
	{SemanticType: sqlbase.ColumnType_INT},   // ts.Logical
}

type bufferEntry struct {
	kv       roachpb.KeyValue
	resolved *jobspb.ResolvedSpan
	// Timestamp of the schema that should be used to read this KV.
	// If unset (zero-valued), the value's timestamp will be used instead.
	schemaTimestamp hlc.Timestamp
}

// buffer mediates between the changed data poller and the rest of the
// changefeed pipeline (which is backpressured all the way to the sink).
type buffer struct {
	mu struct {
		syncutil.Mutex
		entries sqlbase.RowContainer
	}

	a sqlbase.DatumAlloc
}

func makeBuffer(acc mon.BoundAccount) *buffer {
	b := &buffer{}
	b.mu.entries.Init(acc, sqlbase.ColTypeInfoFromColTypes(bufferColTypes), 0 /* rowCapacity */)
	return b
}

func (b *buffer) Close(ctx context.Context) {
	b.mu.Lock()
	// b.mu.entries.Close(ctx)
	b.mu.Unlock()
}

// AddKV inserts a changed kv into the buffer.
//
// TODO(dan): AddKV currently requires that each key is added in increasing mvcc
// timestamp order. This will have to change when we add support for RangeFeed,
// which starts out in a catchup state without this guarantee.
func (b *buffer) AddKV(ctx context.Context, kv roachpb.KeyValue, minTimestamp hlc.Timestamp) error {
	log.Infof(ctx, "AddKV %v", kv)
	// WIP minTimestamp
	return b.addRow(ctx, tree.Datums{
		b.a.NewDBytes(tree.DBytes(kv.Key)),
		b.a.NewDBytes(tree.DBytes(kv.Value.RawBytes)),
		tree.DNull,
		tree.DNull,
		b.a.NewDInt(tree.DInt(kv.Value.Timestamp.WallTime)),
		b.a.NewDInt(tree.DInt(kv.Value.Timestamp.Logical)),
	})
}

// AddResolved inserts a resolved timestamp notification in the buffer.
func (b *buffer) AddResolved(ctx context.Context, span roachpb.Span, ts hlc.Timestamp) error {
	log.Infof(ctx, "AddResolved %v %s", span, ts)
	return b.addRow(ctx, tree.Datums{
		tree.DNull,
		tree.DNull,
		b.a.NewDBytes(tree.DBytes(span.Key)),
		b.a.NewDBytes(tree.DBytes(span.EndKey)),
		b.a.NewDInt(tree.DInt(ts.WallTime)),
		b.a.NewDInt(tree.DInt(ts.Logical)),
	})
}

// Get returns an entry from the buffer. They are handed out in an order that
// (if it is maintained all the way to the sink) meets our external guarantees.
func (b *buffer) Get(ctx context.Context) (bufferEntry, error) {
	row, err := b.getRow(ctx)
	if err != nil {
		return bufferEntry{}, err
	}
	ts := hlc.Timestamp{
		WallTime: int64(*row[4].(*tree.DInt)),
		Logical:  int32(*row[5].(*tree.DInt)),
	}
	if row[0] != tree.DNull {
		kv := roachpb.KeyValue{
			Key: []byte(*row[0].(*tree.DBytes)),
			Value: roachpb.Value{
				RawBytes:  []byte(*row[1].(*tree.DBytes)),
				Timestamp: ts,
			},
		}
		// WIP minTimestamp
		log.Infof(ctx, "Get KV %s %s -> %s", kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
		return bufferEntry{kv: kv}, nil
	}
	span := roachpb.Span{
		Key:    []byte(*row[2].(*tree.DBytes)),
		EndKey: []byte(*row[3].(*tree.DBytes)),
	}
	log.Infof(ctx, "Get Resolved %s %s", span, ts)
	return bufferEntry{resolved: &jobspb.ResolvedSpan{Span: span, Timestamp: ts}}, nil
}

func (b *buffer) addRow(ctx context.Context, row tree.Datums) error {
	b.mu.Lock()
	_, err := b.mu.entries.AddRow(ctx, row)
	b.mu.Unlock()
	if err != nil {
		// WIP check err in case we need to shut down poller.
		return err
	}
	// WIP if b.entries is larger than some size, shut down the poller.
	return nil
}

func (b *buffer) getRow(ctx context.Context) (tree.Datums, error) {
	var row tree.Datums
	for {
		b.mu.Lock()
		if b.mu.entries.Len() > 0 {
			row = b.mu.entries.At(0)
			b.mu.entries.PopFirst()
		}
		b.mu.Unlock()
		if row != nil {
			return row, nil
		}

		// WIP use a sync.Cond or something
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
