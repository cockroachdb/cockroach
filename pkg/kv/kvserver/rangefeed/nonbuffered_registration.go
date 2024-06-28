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

type nonBufferedRegistration struct {
	span             roachpb.Span
	catchUpTimestamp hlc.Timestamp // exclusive
	withDiff         bool
	withFiltering    bool
	metrics          *Metrics

	stream Stream
	unreg  func() // not used

	id            int64
	keys          interval.Range
	catchUpBuf    chan *sharedEvent
	blockWhenFull bool // if true, block when buf is full (for tests)

	mu struct {
		sync.Locker
		// True if this registration buffer has overflowed, dropping a live event.
		// This will cause the registration to exit with an error once the buffer
		// has been emptied.
		catchUpOverflow bool
		// Boolean indicating if all events have been output to stream. Used only
		// for testing.
		caughtUp bool
		// Management of the output loop goroutine, used to ensure proper teardown.
		catchUpScanCancelFn func()
		disconnected        bool

		// catchUpIter is created by replcia under raftMu lock when registration is
		// created. It is detached by output loop for processing and closed.
		// If output loop was not started and catchUpIter is non-nil at the time
		// that disconnect is called, it is closed by disconnect.
		catchUpIter *CatchUpIterator
	}
}

type registrationI interface {
	publish(ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation)
	disconnect(pErr *kvpb.Error)
	runOutputLoop(ctx context.Context, forStacks roachpb.RangeID)
	drainAllocations(ctx context.Context)
	waitForCaughtUp(ctx context.Context) error
	setID(int64)
	setSpanAsKeys()
	getSpan() roachpb.Span
	getCatchUpTimestamp() hlc.Timestamp
	getWithFiltering() bool
	Range() interval.Range
	ID() uintptr
	setDisconnected() (needCleanUp bool)
	getUnreg() func()
	registerRangefeedCleanUp(cleanUp func())
}

func (nr *nonBufferedRegistration) registerRangefeedCleanUp(cleanUp func()) {
	nr.stream.RegisterRangefeedCleanUp(cleanUp)
}

func (nr *nonBufferedRegistration) getUnreg() func() {
	return nr.unreg
}

func (nr *nonBufferedRegistration) setID(id int64) {
	nr.id = id
}

func (nr *nonBufferedRegistration) setSpanAsKeys() {
	nr.keys = nr.span.AsRange()
}

func (nr *nonBufferedRegistration) getSpan() roachpb.Span {
	return nr.span
}

func (nr *nonBufferedRegistration) getCatchUpTimestamp() hlc.Timestamp {
	return nr.catchUpTimestamp
}

func (nr *nonBufferedRegistration) getWithFiltering() bool {
	return nr.withFiltering
}

func newNonBufferedRegistration(
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
) *nonBufferedRegistration {
	r := nonBufferedRegistration{
		span:             span,
		catchUpTimestamp: startTS,
		withDiff:         withDiff,
		withFiltering:    withFiltering,
		metrics:          metrics,
		stream:           stream,
		unreg:            unregisterFn,
		catchUpBuf:       make(chan *sharedEvent, bufferSz),
		blockWhenFull:    blockWhenFull,
	}
	r.mu.Locker = &syncutil.Mutex{}
	r.mu.caughtUp = true
	r.mu.catchUpIter = catchUpIter
	return &r
}

// Put in catch up buffer if it is not nil -> which is when we are currently
// doing catch up scan and live events come. Catch up buffer gets set to nil
// when we drain it after catch up scan is done. It's possible that we send a
// disconnected event to the stream but stream will coordinate that to reject
// disconnected.
func (nr *nonBufferedRegistration) maybePutInCatchUpBuffer(e *sharedEvent) (success bool) {
	// should we check for disconnected here I think we should and right now r
	// doesn't. It still gets sent to r.buf and we may be still publishing some
	// events after disconnect. It eventually stops when the clean up happens and
	// p.unreg
	nr.mu.Lock()
	defer nr.mu.Unlock()

	// Is this thread safe??? MOve up if it is.
	if nr.catchUpBuf == nil {
		return false
	}

	// Treat these as true so that we don't send to buffered stream.
	if nr.mu.disconnected || nr.mu.catchUpOverflow {
		// Don't send to catch up buffer. We will drain after catch up scan is done.
		return true
	}

	// Figure out what to do for memory accounting later.
	e.alloc.Use(context.Background())
	select {
	case nr.catchUpBuf <- e:
	default:
		e.alloc.Release(context.Background())
		putPooledSharedEvent(e)
		nr.mu.catchUpOverflow = true
	}
	return true
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catch-up scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
func (nr *nonBufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	assertEvent(ctx, event)
	e := getPooledSharedEvent(sharedEvent{event: nr.maybeStripEvent(ctx, event), alloc: alloc})
	success := nr.maybePutInCatchUpBuffer(e)
	if success {
		return
	}
	if err := nr.stream.Send(e.event); err != nil {
		// This should be impossible. But just in case. We shouldn't send anything
		// if disconnected already.
		nr.disconnect(kvpb.NewError(err))
	}
}

// maybeStripEvent determines whether the event contains excess information not
// applicable to the current registration. If so, it makes a copy of the event
// and strips the incompatible information to match only what the registration
// requested.
func (nr *nonBufferedRegistration) maybeStripEvent(
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
		if t.PrevValue.IsPresent() && !nr.withDiff {
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
		if !t.Span.EqualValue(nr.span) {
			// Checkpoint events are always created spanning the entire Range.
			// However, a registration might not be listening on updates over
			// the entire Range. If this is the case then we need to constrain
			// the checkpoint events published to that registration to just the
			// span that it's listening on. This is more than just a convenience
			// to consumers - it would be incorrect to say that a rangefeed has
			// observed all values up to the checkpoint timestamp over a given
			// key span if any updates to that span have been filtered out.
			if !t.Span.Contains(nr.span) {
				log.Fatalf(ctx, "registration span %v larger than checkpoint span %v", nr.span, t.Span)
			}
			t = copyOnWrite().(*kvpb.RangeFeedCheckpoint)
			t.Span = nr.span
		}
	case *kvpb.RangeFeedDeleteRange:
		// Truncate the range tombstone to the registration bounds.
		if i := t.Span.Intersect(nr.span); !i.Equal(t.Span) {
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
func (nr *nonBufferedRegistration) setDisconnected() (needCleanUp bool) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	if nr.mu.disconnected {
		return false
	}

	if nr.mu.catchUpIter != nil {
		nr.mu.catchUpIter.Close()
		nr.mu.catchUpIter = nil
	}
	if nr.mu.catchUpScanCancelFn != nil {
		nr.mu.catchUpScanCancelFn()
	}
	nr.mu.disconnected = true
	// We don't drain catch up buffer here. p.Unregister does it. But we reject
	// any live updates after this.
	return true
}

// disconnect cancels the output loop context for the registration and passes an
// error to the output error stream for the registration.
// Safe to run multiple times, but subsequent errors would be discarded.
func (nr *nonBufferedRegistration) disconnect(pErr *kvpb.Error) {
	needCleanUp := nr.setDisconnected()
	if !needCleanUp {
		nr.stream.Disconnect(pErr)
	}
}

func (nr *nonBufferedRegistration) runOutputLoop(ctx context.Context, _forStacks roachpb.RangeID) {
	nr.mu.Lock()
	if nr.mu.disconnected {
		// The registration has already been disconnected.
		nr.mu.Unlock()
		return
	}
	ctx, nr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	nr.mu.Unlock()

	if err := nr.maybeRunCatchUpScan(ctx); err != nil {
		nr.disconnect(kvpb.NewError(err))
	}
}

// drainAllocations should be done after registration is disconnected from
// processor to release all memory budget that its pending events hold.
func (nr *nonBufferedRegistration) drainAllocations(ctx context.Context) {
	if len(nr.catchUpBuf) == 0 {
		// note this is thread safe
		return
	}
	func() {
		for {
			select {
			// Check if we should check for e, ok here.
			case e := <-nr.catchUpBuf:
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
			default:
				return
			}
		}
	}()

	// we unregister before we drain. so it is impossible to receive updates inbetween.
	nr.mu.Lock()
	defer nr.mu.Unlock()
	nr.catchUpBuf = nil
}

func (nr *nonBufferedRegistration) drainAndPublishCatchUpBufferLocked(ctx context.Context) error {
	for {
		select {
		// Check if we should check for e, ok here.
		case e := <-nr.catchUpBuf:
			if err := nr.stream.Send(e.event); err != nil {
				// nr already disconnect. Possible if we disconnect while draining. We
				// will drain everything as part of p.unregister in the end.
				return err
			}
			e.alloc.Release(ctx)
			putPooledSharedEvent(e)
		default:
			return nil
		}
	}
}

func (nr *nonBufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	// Do it without locked first to avoid holding it for too long.
	if err := nr.drainAndPublishCatchUpBufferLocked(ctx); err != nil {
		return err
	}
	nr.mu.Lock()
	defer nr.mu.Unlock()
	if err := nr.drainAndPublishCatchUpBufferLocked(ctx); err != nil {
		return err
	}

	if nr.mu.catchUpOverflow {
		return newErrBufferCapacityExceeded().GoError()
	}

	// success: any future events will be sent directly to the stream since catchUpBuf is nil.
	nr.catchUpBuf = nil
	nr.mu.catchUpScanCancelFn = nil
	return nil
}

func (nr *nonBufferedRegistration) detachCatchUpIter() *CatchUpIterator {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	catchUpIter := nr.mu.catchUpIter
	nr.mu.catchUpIter = nil
	return catchUpIter
}

// maybeRunCatchUpScan starts a catch-up scan which will output entries for all
// recorded changes in the replica that are newer than the catchUpTimestamp.
// This uses the iterator provided when the registration was originally created;
// after the scan completes, the iterator will be closed.
//
// If the registration does not have a catchUpIteratorConstructor, this method
// is a no-op.
func (nr *nonBufferedRegistration) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpIter := nr.detachCatchUpIter()
	if catchUpIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		catchUpIter.Close()
		nr.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	err := catchUpIter.CatchUpScan(ctx, nr.stream.Send, nr.withDiff, nr.withFiltering)

	if err != nil || ctx.Err() != nil {
		err = errors.Wrap(errors.CombineErrors(err, ctx.Err()), "catch-up scan failed")
		log.Errorf(ctx, "%v", err)
		return err
	}
	return nr.publishCatchUpBuffer(ctx)
}

// ID implements interval.Interface.
func (nr *nonBufferedRegistration) ID() uintptr {
	return uintptr(nr.id)
}

// Range implements interval.Interface.
func (nr *nonBufferedRegistration) Range() interval.Range {
	return nr.keys
}

func (nr *nonBufferedRegistration) String() string {
	return fmt.Sprintf("[%s @ %s+]", nr.span, nr.catchUpTimestamp)
}

func (nr *nonBufferedRegistration) waitForCaughtUp(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		nr.mu.Lock()
		caughtUp := len(nr.catchUpBuf) == 0 && nr.mu.caughtUp
		// TODO(wenyihu6): check how we should do this for the new thing
		nr.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Errorf("registration %v failed to empty in time", nr.Range())
}
