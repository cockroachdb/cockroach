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

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type eventResourceTuple struct {
	e Event
	r Resource
}

// chanBuffer mediates between the changed data KVFeed and the rest of the
// changefeed pipeline (which is backpressured all the way to the sink).
type chanBuffer struct {
	entriesCh chan eventResourceTuple
}

// MakeChanBuffer returns an Buffer backed by an unbuffered channel.
//
// TODO(ajwerner): Consider adding a buffer here. We know performance of the
// backfill is terrible. Probably some of that is due to every KV being sent
// on a channel. This should all get benchmarked and tuned.
func MakeChanBuffer() Buffer {
	return &chanBuffer{entriesCh: make(chan eventResourceTuple)}
}

// Add implements Writer interface.
func (b *chanBuffer) Add(ctx context.Context, event Event, resource Resource) error {
	return b.addEvent(ctx, event, resource)
}

func (b *chanBuffer) Close(_ context.Context) {
	close(b.entriesCh)
}

func (b *chanBuffer) addEvent(ctx context.Context, e Event, r Resource) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.entriesCh <- eventResourceTuple{e, r}:
		return nil
	}
}

// Get returns an entry from the buffer. They are handed out in an order that
// (if it is maintained all the way to the sink) meets our external guarantees.
func (b *chanBuffer) Get(ctx context.Context) (Event, Resource, error) {
	select {
	case <-ctx.Done():
		return Event{}, NoResource, ctx.Err()
	case er := <-b.entriesCh:
		er.e.bufferGetTimestamp = timeutil.Now()
		return er.e, er.r, nil
	}
}
