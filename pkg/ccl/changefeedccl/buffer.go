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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type bufferEntry struct {
	kv       roachpb.KeyValue
	resolved *jobspb.ResolvedSpan
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
func (b *buffer) AddKV(ctx context.Context, kv roachpb.KeyValue) error {
	return b.addEntry(ctx, bufferEntry{kv: kv})
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
		return e, nil
	}
}
