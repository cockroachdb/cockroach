// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// stream is a object capable of transmitting RangeFeedEvents.
type Stream interface {
	// Context returns the context for this stream.
	Context() context.Context
	// Send blocks until it sends m, the stream is done, or the stream breaks.
	// Send must be safe to call on the same stream in different goroutines.
	Send(*kvpb.RangeFeedEvent) error
	// Make sure this does nothing if called repatedly for the same disconnect.
	// Possible since r.disconnect no longer checks for r.disconnected. This is
	// necessary to avoid deadlocks.
	Disconnect(err *kvpb.Error)
	RegisterRangefeedCleanUp(func())
}

// Shared event is an entry stored in registration channel. Each entry is
// specific to registration but allocation is shared between all registrations
// to track memory budgets. event itself could either be shared or not in case
// we optimized unused fields in it based on registration options.
type sharedEvent struct {
	event *kvpb.RangeFeedEvent
	alloc *SharedBudgetAllocation
}

var sharedEventSyncPool = sync.Pool{
	New: func() interface{} {
		return new(sharedEvent)
	},
}

func getPooledSharedEvent(e sharedEvent) *sharedEvent {
	ev := sharedEventSyncPool.Get().(*sharedEvent)
	*ev = e
	return ev
}

func putPooledSharedEvent(e *sharedEvent) {
	*e = sharedEvent{}
	sharedEventSyncPool.Put(e)
}

// registration is an instance of a rangefeed subscriber who has
// registered to receive updates for a specific range of keys.
// Updates are delivered to its stream until one of the following
// conditions is met:
// 1. a Send to the Stream returns an error
// 2. the Stream's context is canceled
// 3. the registration is manually unregistered
//
// In all cases, when a registration is unregistered its error
// channel is sent an error to inform it that the registration
// has finished.
type registration struct {
	// Input.
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	withFiltering    bool
	metrics          *Metrics

	// Output.
	stream Stream
	unreg  func()
	// Internal.
	id            int64
	keys          interval.Range
	buf           chan *sharedEvent
	blockWhenFull bool // if true, block when buf is full (for tests)

	mu struct {
		sync.Locker
		// True if this registration buffer has overflowed, dropping a live event.
		// This will cause the registration to exit with an error once the buffer
		// has been emptied.
		overflowed bool
		// Boolean indicating if all events have been output to stream. Used only
		// for testing.
		caughtUp bool
		// Management of the output loop goroutine, used to ensure proper teardown.
		outputLoopCancelFn func()
		disconnected       bool

		// catchUpIter is created by replcia under raftMu lock when registration is
		// created. It is detached by output loop for processing and closed.
		// If output loop was not started and catchUpIter is non-nil at the time
		// that disconnect is called, it is closed by disconnect.
		catchUpIter *CatchUpIterator
	}
}

func newRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	bufferSz int,
	blockWhenFull bool,
	metrics *Metrics,
	stream Stream,
	unregisterFn func(),
) *registration {
	r := registration{
		span:             span,
		catchUpTimestamp: startTS,
		withDiff:         withDiff,
		withFiltering:    withFiltering,
		metrics:          metrics,
		stream:           stream,
		unreg:            unregisterFn,
		buf:              make(chan *sharedEvent, bufferSz),
		blockWhenFull:    blockWhenFull,
	}
	r.mu.Locker = &syncutil.Mutex{}
	r.mu.caughtUp = true
	r.mu.catchUpIter = catchUpIter

	// Check why r returned was not a pointer.
	return &r
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catch-up scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
func (r *registration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	assertEvent(ctx, event)
	e := getPooledSharedEvent(sharedEvent{event: r.maybeStripEvent(ctx, event), alloc: alloc})

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.overflowed {
		return
	}
	alloc.Use(ctx)
	select {
	case r.buf <- e:
		r.mu.caughtUp = false
	default:
		// If we're asked to block (in tests), do a blocking send after releasing
		// the mutex -- otherwise, the output loop won't be able to consume from the
		// channel. We optimistically attempt the non-blocking send above first,
		// since we're already holding the mutex.
		if r.blockWhenFull {
			r.mu.Unlock()
			select {
			case r.buf <- e:
				r.mu.Lock()
				r.mu.caughtUp = false
			case <-ctx.Done():
				r.mu.Lock()
				alloc.Release(ctx)
			}
			return
		}
		// Buffer exceeded and we are dropping this event. Registration will need
		// a catch-up scan.
		r.mu.overflowed = true
		alloc.Release(ctx)
	}
}

// assertEvent asserts that the event contains the necessary data.
func assertEvent(ctx context.Context, event *kvpb.RangeFeedEvent) {
	switch t := event.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.Key == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Key: %v", t)
		}
		if t.Value.RawBytes == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Value.RawBytes: %v", t)
		}
		if t.Value.Timestamp.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedValue.Value.Timestamp: %v", t)
		}
	case *kvpb.RangeFeedCheckpoint:
		if t.Span.Key == nil {
			log.Fatalf(ctx, "unexpected empty RangeFeedCheckpoint.Span.Key: %v", t)
		}
	case *kvpb.RangeFeedSSTable:
		if len(t.Data) == 0 {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Data: %v", t)
		}
		if len(t.Span.Key) == 0 {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Span: %v", t)
		}
		if t.WriteTS.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedSSTable.Timestamp: %v", t)
		}
	case *kvpb.RangeFeedDeleteRange:
		if len(t.Span.Key) == 0 || len(t.Span.EndKey) == 0 {
			log.Fatalf(ctx, "unexpected empty key in RangeFeedDeleteRange.Span: %v", t)
		}
		if t.Timestamp.IsEmpty() {
			log.Fatalf(ctx, "unexpected empty RangeFeedDeleteRange.Timestamp: %v", t)
		}
	default:
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}
}

// maybeStripEvent determines whether the event contains excess information not
// applicable to the current registration. If so, it makes a copy of the event
// and strips the incompatible information to match only what the registration
// requested.
func (r *registration) maybeStripEvent(
	ctx context.Context, event *kvpb.RangeFeedEvent,
) *kvpb.RangeFeedEvent {
	ret := event
	copyOnWrite := func() interface{} {
		if ret == event {
			ret = event.ShallowCopy()
		}
		return ret.GetValue()
	}

	switch t := ret.GetValue().(type) {
	case *kvpb.RangeFeedValue:
		if t.PrevValue.IsPresent() && !r.withDiff {
			// If no registrations for the current Range are requesting previous
			// values, then we won't even retrieve them on the Raft goroutine.
			// However, if any are and they overlap with an update then the
			// previous value on the corresponding events will be populated.
			// If we're in this case and any other registrations don't want
			// previous values then we'll need to strip them.
			t = copyOnWrite().(*kvpb.RangeFeedValue)
			t.PrevValue = roachpb.Value{}
		}
	case *kvpb.RangeFeedCheckpoint:
		if !t.Span.EqualValue(r.span) {
			// Checkpoint events are always created spanning the entire Range.
			// However, a registration might not be listening on updates over
			// the entire Range. If this is the case then we need to constrain
			// the checkpoint events published to that registration to just the
			// span that it's listening on. This is more than just a convenience
			// to consumers - it would be incorrect to say that a rangefeed has
			// observed all values up to the checkpoint timestamp over a given
			// key span if any updates to that span have been filtered out.
			if !t.Span.Contains(r.span) {
				log.Fatalf(ctx, "registration span %v larger than checkpoint span %v", r.span, t.Span)
			}
			t = copyOnWrite().(*kvpb.RangeFeedCheckpoint)
			t.Span = r.span
		}
	case *kvpb.RangeFeedDeleteRange:
		// Truncate the range tombstone to the registration bounds.
		if i := t.Span.Intersect(r.span); !i.Equal(t.Span) {
			t = copyOnWrite().(*kvpb.RangeFeedDeleteRange)
			t.Span = i.Clone()
		}
	case *kvpb.RangeFeedSSTable:
		// SSTs are always sent in their entirety, it is up to the caller to
		// filter out irrelevant entries.
	default:
		log.Fatalf(ctx, "unexpected RangeFeedEvent variant: %v", t)
	}
	return ret
}

// safe to csll multiple times, but subsequent calls would be ignored.
func (r *registration) setDisconnected() (needCleanUp bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.disconnected {
		return false
	}

	if r.mu.catchUpIter != nil {
		r.mu.catchUpIter.Close()
		r.mu.catchUpIter = nil
	}
	if r.mu.outputLoopCancelFn != nil {
		r.mu.outputLoopCancelFn()
	}
	r.mu.disconnected = true
	return true
}

// disconnect cancels the output loop context for the registration and passes an
// error to the output error stream for the registration.
// Safe to run multiple times, but subsequent errors would be discarded.
func (r *registration) disconnect(pErr *kvpb.Error) {
	needCleanUp := r.setDisconnected()
	if !needCleanUp {
		r.stream.Disconnect(pErr)
	}
}

// outputLoop is the operational loop for a single registration. The behavior
// is as thus:
//
// 1. If a catch-up scan is indicated, run one before beginning the proper
// output loop.
// 2. After catch-up is complete, begin reading from the registration buffer
// channel and writing to the output stream until the buffer is empty *and*
// the overflow flag has been set.
//
// The loop exits with any error encountered, if the provided context is
// canceled, or when the buffer has overflowed and all pre-overflow entries
// have been emitted.
func (r *registration) outputLoop(ctx context.Context) error {
	// If the registration has a catch-up scan, run it.
	if err := r.maybeRunCatchUpScan(ctx); err != nil {
		err = errors.Wrap(err, "catch-up scan failed")
		log.Errorf(ctx, "%v", err)
		return err
	}

	firstIteration := true
	// Normal buffered output loop.
	for {
		overflowed := false
		r.mu.Lock()
		if len(r.buf) == 0 {
			overflowed = r.mu.overflowed
			r.mu.caughtUp = true
		}
		r.mu.Unlock()
		if overflowed {
			if firstIteration {
				log.Warningf(ctx, "rangefeed on %s was already overflowed by the time that first iteration (after catch up scan from %s) ran", r.span, r.catchUpTimestamp)
			}
			return newErrBufferCapacityExceeded().GoError()
		}
		firstIteration = false
		select {
		case nextEvent := <-r.buf:
			err := r.stream.Send(nextEvent.event)
			nextEvent.alloc.Release(ctx)
			putPooledSharedEvent(nextEvent)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stream.Context().Done():
			return r.stream.Context().Err()
		}
	}
}

func (r *registration) runOutputLoop(ctx context.Context, _forStacks roachpb.RangeID) {
	r.mu.Lock()
	if r.mu.disconnected {
		// The registration has already been disconnected.
		r.mu.Unlock()
		return
	}
	ctx, r.mu.outputLoopCancelFn = context.WithCancel(ctx)
	r.mu.Unlock()
	err := r.outputLoop(ctx)
	r.disconnect(kvpb.NewError(err))
}

// drainAllocations should be done after registration is disconnected from
// processor to release all memory budget that its pending events hold.
func (r *registration) drainAllocations(ctx context.Context) {
	for {
		select {
		case e, ok := <-r.buf:
			if !ok {
				return
			}
			e.alloc.Release(ctx)
			putPooledSharedEvent(e)
		default:
			return
		}
	}
}

// maybeRunCatchUpScan starts a catch-up scan which will output entries for all
// recorded changes in the replica that are newer than the catchUpTimestamp.
// This uses the iterator provided when the registration was originally created;
// after the scan completes, the iterator will be closed.
//
// If the registration does not have a catchUpIteratorConstructor, this method
// is a no-op.
func (r *registration) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpIter := r.detachCatchUpIter()
	if catchUpIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		catchUpIter.Close()
		r.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	return catchUpIter.CatchUpScan(ctx, r.stream.Send, r.withDiff, r.withFiltering)
}

// ID implements interval.Interface.
func (r *registration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *registration) Range() interval.Range {
	return r.keys
}

func (r registration) String() string {
	return fmt.Sprintf("[%s @ %s+]", r.span, r.catchUpTimestamp)
}

// Wait for this registration to completely process its internal buffer.
func (r *registration) waitForCaughtUp(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		r.mu.Lock()
		caughtUp := len(r.buf) == 0 && r.mu.caughtUp
		r.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Errorf("registration %v failed to empty in time", r.Range())
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (r *registration) detachCatchUpIter() *CatchUpIterator {
	r.mu.Lock()
	defer r.mu.Unlock()
	catchUpIter := r.mu.catchUpIter
	r.mu.catchUpIter = nil
	return catchUpIter
}

func (r *registration) setID(id int64) {
	r.id = id
}

func (r *registration) setSpanAsKeys() {
	r.keys = r.span.AsRange()
}

func (r *registration) getSpan() roachpb.Span {
	return r.span
}

func (r *registration) getCatchUpTimestamp() hlc.Timestamp {
	return r.catchUpTimestamp
}

func (r *registration) getWithFiltering() bool {
	return r.withFiltering
}

func (r *registration) getUnreg() func() {
	return r.unreg
}

func (r *registration) registerRangefeedCleanUp(cleanUp func()) {
	r.stream.RegisterRangefeedCleanUp(cleanUp)
}
