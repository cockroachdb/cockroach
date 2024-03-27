// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/queue"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type GenericStream[T RangefeedEvent] interface {
	Context() context.Context
	Send(T) error
}

type RangefeedEvent interface {
	*kvpb.RangeFeedEvent | *kvpb.MuxRangeFeedEvent
}

type EventWithAlloc[T RangefeedEvent] interface {
	Detatch() (event T, alloc *SharedBudgetAllocation, onErr func(error))
}

var _ EventWithAlloc[*kvpb.RangeFeedEvent] = (*REventWithAlloc)(nil)
var _ EventWithAlloc[*kvpb.MuxRangeFeedEvent] = (*MuxEventWithAlloc)(nil)

type MuxEventWithAlloc struct {
	event    *kvpb.MuxRangeFeedEvent
	alloc    *SharedBudgetAllocation
	callback func(error)
}

func (e *MuxEventWithAlloc) Detatch() (
	event *kvpb.MuxRangeFeedEvent,
	alloc *SharedBudgetAllocation,
	onErr func(error),
) {
	event = e.event
	alloc = e.alloc
	onErr = e.callback

	e.event = nil
	e.alloc = nil
	e.callback = nil
	return
}

func NewMuxEventWithAlloc(
	event *kvpb.MuxRangeFeedEvent, alloc *SharedBudgetAllocation, callback func(error),
) *MuxEventWithAlloc {
	return &MuxEventWithAlloc{
		event:    event,
		alloc:    alloc,
		callback: callback,
	}
}

type BufferedSender[T RangefeedEvent] struct {
	ctx       context.Context
	outStream GenericStream[T]

	g              ctxgroup.Group
	cancelInnerCtx func()

	mu struct {
		syncutil.Mutex
		buf *queue.Queue[EventWithAlloc[T]]
		// Created by the output loop if no events are present. Closed when the first
		// event arrives BufferedSend.
		notify chan struct{}
	}
}

func (bs *BufferedSender[T]) outputLoop() error {
	for {
		// Check for an event and set the notify channel if empty.
		var e EventWithAlloc[T]
		var notify chan struct{}
		var ok bool
		func() {
			bs.mu.Lock()
			defer bs.mu.Unlock()
			e, ok = bs.mu.buf.Dequeue()
			if !ok {
				bs.mu.notify = make(chan struct{})
				notify = bs.mu.notify
			}
		}()

		if !ok {
			select {
			// Child context for this output loop.
			case <-bs.ctx.Done():
				return bs.ctx.Err()
			case <-bs.outStream.Context().Done():
				return bs.outStream.Context().Err()
			case <-notify:
			}
			continue
		}

		ev, alloc, callback := e.Detatch()
		err := bs.outStream.Send(ev)
		alloc.Release(bs.ctx)
		callback(err)
	}
}

func (bs *BufferedSender[T]) BufferedSend(e EventWithAlloc[T]) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	bs.mu.buf.Enqueue(e)
	if bs.mu.notify != nil {
		close(bs.mu.notify)
		bs.mu.notify = nil
	}
}

func (bs *BufferedSender[T]) Send(e T) error {
	return bs.outStream.Send(e)
}

func (bs *BufferedSender[T]) Context() context.Context {
	return bs.ctx
}

func (bs *BufferedSender[T]) Close() {
	bs.cancelInnerCtx()
	_ = bs.g.Wait()

	// NB: Don't need to acquire the mutex because the output goroutine was cancelled.
	for e, ok := bs.mu.buf.Dequeue(); ok; e, ok = bs.mu.buf.Dequeue() {
		_, alloc, _ := e.Detatch()
		alloc.Release(bs.ctx)
	}
}

func NewBufferedSender[T RangefeedEvent](
	ctx context.Context, outStream GenericStream[T],
) (*BufferedSender[T], error) {
	innerCtx, cancel := context.WithCancel(ctx)
	bs := &BufferedSender[T]{
		ctx:       innerCtx,
		outStream: outStream,
	}
	buf, err := queue.NewQueue[EventWithAlloc[T]]()
	if err != nil {
		cancel()
		return nil, err
	}
	bs.mu.buf = buf
	bs.g = ctxgroup.WithContext(innerCtx)
	bs.g.Go(func() error {
		return bs.outputLoop()
	})
	bs.cancelInnerCtx = cancel
	return bs, nil
}
