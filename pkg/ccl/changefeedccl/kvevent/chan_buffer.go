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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// chanBuffer mediates between the changed data KVFeed and the rest of the
// changefeed pipeline (which is backpressured all the way to the sink).
type chanBuffer struct {
	entriesCh chan Event
}

// MakeChanBuffer returns an Buffer backed by an unbuffered channel.
//
// TODO(ajwerner): Consider adding a buffer here. We know performance of the
// backfill is terrible. Probably some of that is due to every KV being sent
// on a channel. This should all get benchmarked and tuned.
func MakeChanBuffer() Buffer {
	return &chanBuffer{entriesCh: make(chan Event)}
}

// AddKV inserts a changed KV into the buffer. Individual keys must be added in
// increasing mvcc order.
func (b *chanBuffer) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	return b.addEvent(ctx, makeKVEvent(kv, prevVal, backfillTimestamp))
}

// AddResolved inserts a Resolved timestamp notification in the buffer.
func (b *chanBuffer) AddResolved(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) error {
	return b.addEvent(ctx, makeResolvedEvent(span, ts, boundaryType))
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
