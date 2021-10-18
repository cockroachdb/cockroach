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

// Add implements Writer interface.
func (b *chanBuffer) Add(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.entriesCh <- event:
		return nil
	}
}

// Drain implements Writer interface.
func (b *chanBuffer) Drain(ctx context.Context) error {
	// channel buffer is unbuffered.
	return nil
}

func (b *chanBuffer) Close(_ context.Context) error {
	close(b.entriesCh)
	return nil
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
