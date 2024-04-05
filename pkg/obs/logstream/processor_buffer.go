// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstream

import (
	"container/list"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// processorBuffer buffers the execution of its underlying Processor to be asynchronous.
// It wraps a child Processor and triggers "flushes" to the underlying flush
// goroutine based on various flush triggers. It's designed to never block
// callers.
type processorBuffer struct {
	// child is the wrapped Processor.
	child Processor
	// maxStaleness is the duration after which a flush is triggered.
	// 0 disables this trigger.
	maxStaleness time.Duration
	// triggerLen is the number of buffered events that will trigger a flush to the
	// underlying child Processor.
	// TODO(abarganier): Ideally we would trigger based on size, not len.
	triggerLen int
	// maxLen is the max number of buffered events allowed at a given time. If an event comes
	// in and maxLen has been reached, the oldest buffered event will be dropped in favor of
	// the new.
	// TODO(abarganier): Ideally we would limit based on size, not len.
	maxLen int
	// flushC is a channel on which requests to flush the buffer are sent to the
	// runFlusher goroutine.
	flushC chan struct{}

	mu struct {
		syncutil.Mutex
		// buf buffers the events that have yet to be flushed.
		buf *eventBuffer
		// timer is set when a flushAsyncLocked() call is scheduled to happen in the
		// future.
		timer *time.Timer
	}
}

var _ Processor = (*processorBuffer)(nil)

func newProcessorBuffer(
	child Processor, maxStaleness time.Duration, triggerLen int, maxLen int,
) *processorBuffer {
	if triggerLen != 0 && maxLen != 0 {
		// Validate triggerSize in relation to maxLen. We actually want some gap between
		// these two, but the minimum acceptable gap is left to the caller.
		if triggerLen >= maxLen {
			panic(errors.AssertionFailedf("expected triggerLen (%d) < maxLen (%d)",
				triggerLen, maxLen))
		}
	}
	sink := &processorBuffer{
		child: child,
		// flushC is a buffered channel, so that an async flush triggered while
		// another flush is in progress doesn't block.
		flushC:       make(chan struct{}, 1),
		maxStaleness: maxStaleness,
		triggerLen:   triggerLen,
		maxLen:       maxLen,
	}
	sink.mu.buf = newEventBuffer(maxLen)
	return sink
}

// Start starts an internal goroutine that will run until the provided context is
// cancelled. This goroutine is used to perform flushes of buffered events.
func (bs *processorBuffer) Start(ctx context.Context) {
	// TODO(abarganier): Use a stopper.
	go func() {
		bs.runFlusher(ctx, ctx.Done())
	}()
}

// Process buffers the provided event, eventually flushing it to an
// underlying Processor for processing.
func (bs *processorBuffer) Process(ctx context.Context, e any) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	// Append the event to the buffer.
	err := bs.mu.buf.appendEvent(e)
	if err != nil {
		return err
	}

	flush := bs.triggerLen > 0 && bs.mu.buf.len() >= bs.triggerLen
	if flush {
		// Trigger a flush. The flush will take effect asynchronously (and can be
		// arbitrarily delayed if there's another flush in progress). In the
		// meantime, the current buffer continues accumulating events until it
		// hits its limit.
		bs.flushAsyncLocked(ctx)
	} else {
		// Schedule a flush for the future based on maxStaleness, unless
		// one is scheduled already.
		if bs.mu.timer == nil && bs.maxStaleness > 0 {
			bs.mu.timer = time.AfterFunc(bs.maxStaleness, func() {
				bs.mu.Lock()
				defer bs.mu.Unlock()
				bs.flushAsyncLocked(ctx)
			})
		}
	}
	return nil
}

// flushAsyncLocked signals the flusher goroutine to flush.
func (bs *processorBuffer) flushAsyncLocked(ctx context.Context) {
	// Make a best-effort attempt at de-scheduling any timed flushes.
	if bs.mu.timer != nil {
		bs.mu.timer.Stop()
		bs.mu.timer = nil
	}
	// Signal the flushC to flush, unless it's already been signaled.
	// If it's full, the event will be picked up in the next upcoming flush.
	select {
	case bs.flushC <- struct{}{}:
	default:
		log.Warning(ctx, "processorBuffer's flushC full, aborting flush")
	}
}

func (bs *processorBuffer) getAndResetLogBufferLocked() *eventBuffer {
	ret := bs.mu.buf
	bs.mu.buf = newEventBuffer(bs.maxLen)
	return ret
}

// runFlusher waits for flush signals in a loop and, when it gets one, flushes
// the current buffer to the wrapped Processor. The function returns when
// the provided stopC is signaled.
func (bs *processorBuffer) runFlusher(ctx context.Context, stopC <-chan struct{}) {
	for {
		done := false
		select {
		case <-bs.flushC:
		case <-stopC:
			// We'll return after flushing everything.
			done = true
		}
		payload := func() *eventBuffer {
			bs.mu.Lock()
			defer bs.mu.Unlock()
			return bs.getAndResetLogBufferLocked()
		}()
		if payload != nil {
			bufList := payload.events
			for bufList.Len() > 0 {
				e := bufList.Front()
				if err := bs.child.Process(ctx, e.Value); err != nil {
					// TODO(abarganier): Protect against log spam.
					log.Errorf(ctx, "processing structured event: %v", err)
				}
				bufList.Remove(e)
			}
		}
		if done {
			return
		}
	}
}

type eventBuffer struct {
	// maxLen is the buffer length limit. Trying to appendEvent() an event that would
	// cause the buffer to exceed this limit causes the buffer to drop the oldest
	// message to make room for the new.
	// TODO(abarganier): maxSize would be better.
	maxLen int

	// The events that have been appended to the buffer.
	events *list.List
}

func newEventBuffer(maxLen int) *eventBuffer {
	return &eventBuffer{
		maxLen: maxLen,
		events: list.New(),
	}
}

func (b *eventBuffer) len() int {
	return b.events.Len()
}

// appendEvent appends event to the buffer.
//
// If the buffer is full, then we drop the oldest event in the buffer
// to make way for the new event.
func (b *eventBuffer) appendEvent(event any) error {
	// Make room for the new message, potentially by dropping the oldest events
	// in the buffer.
	if b.maxLen > 0 && b.len() >= b.maxLen {
		b.dropFirstEvent()
	}

	b.events.PushBack(event)
	return nil
}

func (b *eventBuffer) dropFirstEvent() {
	// TODO(abarganier): Counter metric to track dropped events.
	toRemove := b.events.Front()
	if toRemove == nil {
		return
	}
	b.events.Remove(toRemove)
}
