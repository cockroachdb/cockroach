// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstream

import (
	"container/list"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// asyncProcessorRouter buffers the execution of its underlying Processor(s) to be
// asynchronous, and routes the consumed events based on their log.EventType.
//
// It accepts and buffers a stream of typedEvent objects and "flushes" to
// the underlying flush goroutine based on various flush triggers, which
// in turn invokes the registered Processor(s) for each event based on event type.
//
// It's designed to never block callers, even in the face of an unavailable consumer.
//
// NB: asyncProcessorRouter will drop old events to make room for new ones if the configured
// maxLen is reached. Generally, this would only happen if an underlying Processor became
// unavailable.
type asyncProcessorRouter struct {
	// maxStaleness is the duration after which a flush is triggered.
	// 0 disables this trigger.
	maxStaleness time.Duration
	// triggerLen is the number of buffered events that will trigger a flush to the
	// underlying Processors.
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

	rwmu struct {
		syncutil.RWMutex
		// routes maps each log.EventType to the Processors registered for that type.
		routes map[log.EventType][]Processor
	}
}

// typedEvent is a tuple containing an EventType and event.
// It's the type that asyncProcessorBuffer operates on. The
// type information is used for processor routing.
type typedEvent struct {
	eventType log.EventType
	event     any
}

func newAsyncProcessorRouter(
	maxStaleness time.Duration, triggerLen int, maxLen int,
) *asyncProcessorRouter {
	if triggerLen != 0 && maxLen != 0 {
		// Validate triggerSize in relation to maxLen. We actually want some gap between
		// these two, but the minimum acceptable gap is left to the caller.
		if triggerLen >= maxLen {
			panic(errors.AssertionFailedf("expected triggerLen (%d) < maxLen (%d)",
				triggerLen, maxLen))
		}
	}
	apr := &asyncProcessorRouter{
		// flushC is a buffered channel, so that an async flush triggered while
		// another flush is in progress doesn't block.
		flushC:       make(chan struct{}, 1),
		maxStaleness: maxStaleness,
		triggerLen:   triggerLen,
		maxLen:       maxLen,
	}
	apr.mu.buf = newEventBuffer(maxLen)
	apr.rwmu.routes = make(map[log.EventType][]Processor)
	return apr
}

// Register registers a new processor for events of the given type.
func (bs *asyncProcessorRouter) register(eventType log.EventType, p Processor) {
	bs.rwmu.Lock()
	defer bs.rwmu.Unlock()
	container, ok := bs.rwmu.routes[eventType]
	if !ok {
		container = make([]Processor, 0)
	}
	bs.rwmu.routes[eventType] = append(container, p)
}

// Start starts an internal goroutine that will run until the provided stopper is
// cancelled. This goroutine is used to perform flushes of buffered events.
func (bs *asyncProcessorRouter) Start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "logstream_processor_buffer", func(ctx context.Context) {
		bs.runFlusher(ctx, stopper.ShouldQuiesce())
	})
}

// Process buffers the provided event, eventually flushing it to any
// underlying register Processor instances for processing, based on its event type.
func (bs *asyncProcessorRouter) Process(ctx context.Context, e *typedEvent) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	err := bs.mu.buf.appendEvent(e)
	if err != nil {
		return err
	}

	bs.maybeFlushAsyncLocked(ctx)
	return nil
}

// maybeFlushAsyncLocked either signals the flusher goroutine to flush if one of the
// flush triggers have been met, or schedules a timed flush if one hasn't already been
// scheduled.
func (bs *asyncProcessorRouter) maybeFlushAsyncLocked(ctx context.Context) {
	flushLocked := func() {
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
			log.Warning(ctx, "asyncProcessorRouter's flushC full, aborting flush")
		}
	}

	flush := bs.triggerLen > 0 && bs.mu.buf.len() >= bs.triggerLen
	if flush {
		// Trigger a flush. The flush will take effect asynchronously (and can be
		// arbitrarily delayed if there's another flush in progress). In the
		// meantime, the current buffer continues accumulating events until it
		// hits its limit.
		flushLocked()
	} else {
		// Schedule a flush for the future based on maxStaleness, unless
		// one is scheduled already.
		if bs.mu.timer == nil && bs.maxStaleness > 0 {
			bs.mu.timer = time.AfterFunc(bs.maxStaleness, func() {
				bs.mu.Lock()
				defer bs.mu.Unlock()
				flushLocked()
			})
		}
	}
}

func (bs *asyncProcessorRouter) getAndResetLogBufferLocked() *eventBuffer {
	ret := bs.mu.buf
	bs.mu.buf = newEventBuffer(bs.maxLen)
	return ret
}

// runFlusher waits for flush signals in a loop and, when it gets one, flushes
// the current buffer to the wrapped processor. The function returns when
// the provided stopC is signaled.
func (bs *asyncProcessorRouter) runFlusher(ctx context.Context, stopC <-chan struct{}) {
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
			bs.routeAndProcess(ctx, payload.events)
		}
		if done {
			return
		}
	}
}

func (bs *asyncProcessorRouter) routeAndProcess(ctx context.Context, events *list.List) {
	bs.rwmu.RLock()
	defer bs.rwmu.RUnlock()
	for events.Len() > 0 {
		e := events.Front()
		events.Remove(e)
		typed, ok := e.Value.(*typedEvent)
		if !ok {
			panic(errors.AssertionFailedf("unexpected type buffered within asyncProcessorRouter"))
		}
		routes, ok := bs.rwmu.routes[typed.eventType]
		if !ok {
			continue
		}
		for _, r := range routes {
			if err := r.Process(ctx, typed.event); err != nil {
				// TODO(abarganier): Protect against log spam.
				log.Errorf(ctx, "processing structured event: %v", err)
			}
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
func (b *eventBuffer) appendEvent(event *typedEvent) error {
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
