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

// bufferedRegistration is an instance of a rangefeed subscriber who has
// registered to receive updates for a specific range of keys. It buffers live
// raft updates in buf and has a dedicated goroutine runOutputLoop responsible
// for volleying events to underlying Stream after catch up scan is done. Events
// from catch up scans are directly sent to underlying Stream.
//
// Updates are delivered to its stream until one of the following conditions is
// met:
// 1. a Send to the Stream returns an error
// 2. the Stream's context is canceled
// 3. the registration is manually unregistered
//
// In all cases, when a registration is unregistered its error
// channel is sent an error to inform it that the registration
// has finished.
type bufferedRegistration struct {
	// Input.
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	withFiltering    bool
	withOmitRemote   bool
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

func newBufferedRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	bufferSz int,
	blockWhenFull bool,
	metrics *Metrics,
	stream Stream,
	unregisterFn func(),
) *bufferedRegistration {
	br := &bufferedRegistration{
		span:             span,
		catchUpTimestamp: startTS,
		withDiff:         withDiff,
		withFiltering:    withFiltering,
		withOmitRemote:   withOmitRemote,
		metrics:          metrics,
		stream:           stream,
		unreg:            unregisterFn,
		buf:              make(chan *sharedEvent, bufferSz),
		blockWhenFull:    blockWhenFull,
	}
	br.mu.Locker = &syncutil.Mutex{}
	br.mu.caughtUp = true
	br.mu.catchUpIter = catchUpIter
	return br
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catch-up scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
func (br *bufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	br.assertEvent(ctx, event)
	e := getPooledSharedEvent(sharedEvent{event: br.maybeStripEvent(ctx, event), alloc: alloc})

	br.mu.Lock()
	defer br.mu.Unlock()
	if br.mu.overflowed {
		return
	}
	alloc.Use(ctx)
	select {
	case br.buf <- e:
		br.mu.caughtUp = false
	default:
		// If we're asked to block (in tests), do a blocking send after releasing
		// the mutex -- otherwise, the output loop won't be able to consume from the
		// channel. We optimistically attempt the non-blocking send above first,
		// since we're already holding the mutex.
		if br.blockWhenFull {
			br.mu.Unlock()
			select {
			case br.buf <- e:
				br.mu.Lock()
				br.mu.caughtUp = false
			case <-ctx.Done():
				br.mu.Lock()
				alloc.Release(ctx)
			}
			return
		}
		// Buffer exceeded and we are dropping this event. Registration will need
		// a catch-up scan.
		br.mu.overflowed = true
		alloc.Release(ctx)
	}
}

// assertEvent asserts that the event contains the necessary data.
func (br *bufferedRegistration) assertEvent(ctx context.Context, event *kvpb.RangeFeedEvent) {
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
func (br *bufferedRegistration) maybeStripEvent(
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
		if t.PrevValue.IsPresent() && !br.withDiff {
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
		if !t.Span.EqualValue(br.span) {
			// Checkpoint events are always created spanning the entire Range.
			// However, a registration might not be listening on updates over
			// the entire Range. If this is the case then we need to constrain
			// the checkpoint events published to that registration to just the
			// span that it's listening on. This is more than just a convenience
			// to consumers - it would be incorrect to say that a rangefeed has
			// observed all values up to the checkpoint timestamp over a given
			// key span if any updates to that span have been filtered out.
			if !t.Span.Contains(br.span) {
				log.Fatalf(ctx, "bufferedRegistration span %v larger than checkpoint span %v", br.span, t.Span)
			}
			t = copyOnWrite().(*kvpb.RangeFeedCheckpoint)
			t.Span = br.span
		}
	case *kvpb.RangeFeedDeleteRange:
		// Truncate the range tombstone to the registration bounds.
		if i := t.Span.Intersect(br.span); !i.Equal(t.Span) {
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

// disconnect cancels the output loop context for the registration and passes an
// error to the output error stream for the registration.
// Safe to run multiple times, but subsequent errors would be discarded.
func (br *bufferedRegistration) disconnect(pErr *kvpb.Error) {
	br.mu.Lock()
	defer br.mu.Unlock()
	if !br.mu.disconnected {
		if br.mu.catchUpIter != nil {
			br.mu.catchUpIter.Close()
			br.mu.catchUpIter = nil
		}
		if br.mu.outputLoopCancelFn != nil {
			br.mu.outputLoopCancelFn()
		}
		br.mu.disconnected = true
		br.stream.Disconnect(pErr)
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
func (br *bufferedRegistration) outputLoop(ctx context.Context) error {
	// If the registration has a catch-up scan, run it.
	if err := br.maybeRunCatchUpScan(ctx); err != nil {
		err = errors.Wrap(err, "catch-up scan failed")
		log.Errorf(ctx, "%v", err)
		return err
	}

	firstIteration := true
	// Normal buffered output loop.
	for {
		overflowed := false
		br.mu.Lock()
		if len(br.buf) == 0 {
			overflowed = br.mu.overflowed
			br.mu.caughtUp = true
		}
		br.mu.Unlock()
		if overflowed {
			if firstIteration {
				log.Warningf(ctx, "rangefeed on %s was already overflowed by the time that first iteration (after catch up scan from %s) ran", br.span, br.catchUpTimestamp)
			}
			return newErrBufferCapacityExceeded().GoError()
		}
		firstIteration = false
		select {
		case nextEvent := <-br.buf:
			err := br.stream.Send(nextEvent.event)
			nextEvent.alloc.Release(ctx)
			putPooledSharedEvent(nextEvent)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-br.stream.Context().Done():
			return br.stream.Context().Err()
		}
	}
}

func (br *bufferedRegistration) runOutputLoop(ctx context.Context, _forStacks roachpb.RangeID) {
	br.mu.Lock()
	if br.mu.disconnected {
		// The registration has already been disconnected.
		br.mu.Unlock()
		return
	}
	ctx, br.mu.outputLoopCancelFn = context.WithCancel(ctx)
	br.mu.Unlock()
	err := br.outputLoop(ctx)
	br.disconnect(kvpb.NewError(err))
}

// drainAllocations should be done after registration is disconnected from
// processor to release all memory budget that its pending events hold.
func (br *bufferedRegistration) drainAllocations(ctx context.Context) {
	for {
		select {
		case e, ok := <-br.buf:
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
func (br *bufferedRegistration) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpIter := br.detachCatchUpIter()
	if catchUpIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		catchUpIter.Close()
		br.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	return catchUpIter.CatchUpScan(ctx, br.stream.Send, br.withDiff, br.withFiltering, br.withOmitRemote)
}

// ID implements interval.Interface.
func (br *bufferedRegistration) ID() uintptr {
	return uintptr(br.id)
}

// Range implements interval.Interface.
func (br *bufferedRegistration) Range() interval.Range {
	return br.keys
}

func (br *bufferedRegistration) String() string {
	return fmt.Sprintf("[%s @ %s+]", br.span, br.catchUpTimestamp)
}

// Wait for this registration to completely process its internal buffer.
func (br *bufferedRegistration) waitForCaughtUp(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		br.mu.Lock()
		caughtUp := len(br.buf) == 0 && br.mu.caughtUp
		br.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Errorf("bufferedRegistration %v failed to empty in time", br.Range())
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (br *bufferedRegistration) detachCatchUpIter() *CatchUpIterator {
	br.mu.Lock()
	defer br.mu.Unlock()
	catchUpIter := br.mu.catchUpIter
	br.mu.catchUpIter = nil
	return catchUpIter
}
