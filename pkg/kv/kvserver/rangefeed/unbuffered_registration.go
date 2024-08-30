// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// unbufferedRegistration is similar to bufferedRegistration but does not buffer
// events in publish directly. Rather, when caught up, it send events directly
// to the sender, depending on the BufferedStream to be non-blocking. As a
// result, it does not require a long-lived goroutine to volley event when
// reading from raft.
//
// Note that UnbufferedRegistration needs to ensure that events sent to
// BufferedStream from the processor during a running catchup scan are sent to
// the client in order. To achieve this, events from catch-up scans are send
// using UnbufferedSend. While the catch-up scan is ongoing, live updates
// delivered via publish() are temporarily buffered in catchUpBuf to hold them
// until catch-up scan is complete. After the catch-up scan is done, we know
// that catch-up scan events have been sent to grpc stream successfully. The
// catch-up buffer is then drained by our hopefully short lived output loop
// goroutine. Once the catchup buffer is fully drained publish begins sending to
// the buffered sender directly.
//
// All errors are delivered to this registration via Disonnect() which is
// non-blocking. Disconnect sends an error to the stream and invokes any
// necessary cleanup.
//
// unbufferedRegistration is responsible for allocating and draining memory
// catch-up buffer.
type unbufferedRegistration struct {
	// Input.
	baseRegistration
	metrics *Metrics

	// Output.
	stream BufferedStream

	// Internal.
	mu struct {
		sync.Locker

		// True if this registration catchUpBuf has overflowed, live raft events are
		// dropped. This will cause the registration to disconnect with an error
		// once catch-up scan is done and catchUpBuf is drained and published. Once
		// set to true, cannot unset.
		catchUpOverflowed bool

		// It is set to nil if catchUpBuf has been drained (either catch-up scan
		// succeeded or failed). Safe to send to BufferedStream if nil and
		// disconnected is false. In the case of error, disconnected flag is set.
		// Once set to nil, cannot unset.
		//
		// Note that it needs to be protected under mutex since it is shared between
		// goroutines and set to nil after it's done.
		//
		// If no catch-up iter is provided, catchUpBuf will be nil since the
		// initialization.
		catchUpBuf chan *sharedEvent

		// Fine to repeatedly cancel context. Management of the catch-up runOutput
		// context goroutine.
		catchUpScanCancelFn func()

		// Once disconnected is set, it cannot be unset. This flag indicates that
		// the registration is marked for disconnection, but it may still receive
		// raft updates from publish until the actual p.reg.Unregister happens.
		// Unregistration only happens when r.cleanup is called. Since this could
		// happen very late, we check the disconnected flag during publish to
		// prevent further raft events from being sent to BufferedStream.
		disconnected bool

		// catchUpIter is created by replcia under raftMu lock when registration is
		// created. It is detached by output loop for processing and closed. If
		// runOutputLoop was not started and catchUpIter is non-nil at the time that
		// disconnect is called, it is closed by disconnect. Otherwise,
		// maybeRunCatchUpScan is responsible for detaching and closing it.
		catchUpIter *CatchUpIterator

		// Used for testing only. Boolean indicating if all events in catchUpBuf
		// have been output to BufferedStream. Note that this does not mean that
		// BufferedStream has been sent to underlying grpc Stream yet.
		caughtUp bool
	}
}

var _ registration = (*unbufferedRegistration)(nil)

func newUnbufferedRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	bufferSz int,
	metrics *Metrics,
	stream BufferedStream,
	unregisterFn func(),
	cleanup func(registration),
) *unbufferedRegistration {
	br := &unbufferedRegistration{
		baseRegistration: baseRegistration{
			span:             span,
			catchUpTimestamp: startTS,
			withDiff:         withDiff,
			withFiltering:    withFiltering,
			withOmitRemote:   withOmitRemote,
			unreg:            unregisterFn,
			cleanup:          cleanup,
		},
		metrics: metrics,
		stream:  stream,
	}
	br.mu.Locker = &syncutil.Mutex{}
	br.mu.catchUpIter = catchUpIter
	br.mu.caughtUp = true
	if br.mu.catchUpIter != nil {
		// Send to underlying stream directly if catch-up scan is not needed.
		br.mu.catchUpBuf = make(chan *sharedEvent, bufferSz)
	}
	return br
}

// publish attempts to send a single event to catch-up buffer or directly to
// BufferedStream. If registration is disconnected or catchUpOverflowed is set,
// live events are dropped. Rangefeed clients will need a catchup-scan after
// restarting rangefeeds. Note that publish is responsible for calling alloc.Use
// and alloc.Release. If event is sent to catch-up buffer,
// runOutputLoop is responsible for releasing it. If event is sent to
// BufferedStream, the ownership of alloc is transferred to BufferedStream.
// BufferedStream is responsible for allocating and releasing it.
func (ubr *unbufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if ubr.mu.disconnected || ubr.mu.catchUpOverflowed {
		return
	}

	ubr.assertEvent(ctx, event)
	strippedEvent := ubr.maybeStripEvent(ctx, event)

	// Disconnected or catchUpOverflowed is not set and catchUpBuf
	// is nil. Safe to send to underlying stream.
	if ubr.mu.catchUpBuf == nil {
		if err := ubr.stream.SendBuffered(strippedEvent, alloc); err != nil {
			// BufferedSender is full or has been stopped. A node level shutdown is
			// happening, so BufferedSender should disconnect all streams soon. There
			// is not anything else we can do here. Disconnect again just in case.
			// TODO(ssd) 2024-10-23: maybe we have a disconnectLocked() or we just log?
			// ubr.Disconnect(kvpb.NewError(err))
		}
	} else {
		// catchUpBuf is set, put event into the catchUpBuf if
		// there is room.
		e := getPooledSharedEvent(sharedEvent{event: event, alloc: alloc})
		alloc.Use(ctx)
		select {
		case ubr.mu.catchUpBuf <- e:
			ubr.mu.caughtUp = false
		default:
			// catchUpBuf exceeded and we are dropping
			// this event.
			ubr.mu.catchUpOverflowed = true
			e.alloc.Release(ctx)
			putPooledSharedEvent(e)
		}
	}
}

// Disconnect is called to shut down the registration. It is safe to run
// multiple times, but subsequent errors would be discarded. It may be invoked
// to shut down rangefeed from the rangefeed level or from the node level
// StreamManager.
//
// When a registration disconnects, the following clean-up must happen:
// a ) Registration level clean-up:
//   - catch-up iter needs to be closed if not detached already
//   - catchUpBuf needs to be drained. runOutputLoop goroutine is responsible for
//     draining catch-up buffer.
//
// b) Processor level clean-up: p.reg.Unregister
//
// c) BufferedSender level clean-up: stream.SendError
//   - metrics updates, send a disconnect error back to rangefeed client
func (ubr *unbufferedRegistration) Disconnect(pErr *kvpb.Error) {
	// TODO(wenyihu6 during code reviews or later on): think about if we should
	// just drain catchUpBuf here during reviews. We never publish anything in
	// catch-up buf if disconnected. But this might take a long time and you are
	// on a hot path.ubr.mu.Lock()
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if ubr.mu.disconnected {
		return
	}
	if ubr.mu.catchUpIter != nil {
		// catch-up scan hasn't started yet.
		ubr.mu.catchUpIter.Close()
		ubr.mu.catchUpIter = nil
	}
	if ubr.mu.catchUpScanCancelFn != nil {
		ubr.mu.catchUpScanCancelFn()
	}
	ubr.mu.disconnected = true
	ubr.stream.SendError(pErr)
	// Note that clean up unregisters registration from processor async.
	ubr.cleanup(ubr)
}

// IsDisconnected returns true if the registration is disconnected.
func (ubr *unbufferedRegistration) IsDisconnected() bool {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	return ubr.mu.disconnected
}

// runOutputLoop is run in a goroutine. It is short-lived and responsible for
// 1. running catch-up scan
// 2. publishing/discarding catch-up buffer after catch-up scan is done.
//
// The contract is that it will empty catch-up buffer and set it to nil when
// this goroutine ends. Once set to nil, no more events should be put in
// catch-up buffer.
//
// TODO(wenyihu6): check if it is ever possible for disconnect happens and
// runOutputLoop to not start at all and no one is draining catch-up buffer
// during reviews
func (ubr *unbufferedRegistration) runOutputLoop(ctx context.Context, forStacks roachpb.RangeID) {
	ubr.mu.Lock()
	defer ubr.discardCatchUpBuffer(ctx)
	if ubr.mu.disconnected {
		// already disconnected
		ubr.mu.Unlock()
		return
	}

	ctx, ubr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	ubr.mu.Unlock()

	if err := ubr.maybeRunCatchUpScan(ctx); err != nil {
		// Important to disconnect before draining to avoid upstream to interpret
		// nil catch-up buf as sending to underlying stream directly.
		ubr.Disconnect(kvpb.NewError(errors.Wrap(err, "catch-up scan failed")))
		return
	}

	if err := ubr.publishCatchUpBuffer(ctx); err != nil {
		// Important to disconnect before draining to avoid upstream to interpret
		// nil catch-up buf as sending to underlying stream directly.
		ubr.Disconnect(kvpb.NewError(err))
		return
	}
	// Success: publishCatchUpBuffer should have drained and set catchUpBuf to nil
	// when it succeeds.
}

// Noop for unbuffered registration since events are not buffered in buf.
// runOutputLoop is responsible for draining catchUpBuf when being disconnected.
func (ubr *unbufferedRegistration) drainAllocations(ctx context.Context) {}

// Wait for this registration to completely drain its catchUpBuf. Note that this
// does not mean that BufferedStream has been sent to underlying grpc Stream
// yet. Only used in testing.
func (ubr *unbufferedRegistration) waitForCaughtUp(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		ubr.mu.Lock()
		caughtUp := len(ubr.mu.catchUpBuf) == 0 && ubr.mu.caughtUp
		ubr.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Errorf("unbufferedRegistration %v failed to empty in time", ubr.Range())
}

// discardCatchUpBufferWithLock drains catchUpBuf and release all memory held by
// the events in catch-up buffer without publishing them. It is safe to assume
// that catch-up buffer is empty and nil after call.
func (ubr *unbufferedRegistration) discardCatchUpBufferWithLock(ctx context.Context) {
	if ubr.mu.catchUpBuf == nil {
		// Already drained.
		return
	}
	func() {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
			default:
				// Done.
				return
			}
		}
	}()

	ubr.mu.catchUpBuf = nil
	ubr.mu.caughtUp = true
}

// publishCatchUpBuffer drains catchUpBuf and release all memory held by the
// events in catch-up buffer while publishing them. Note that
// discardCatchUpBuffer should never be called concurrently with this function.
//
// Caller is responsible for draining it again if error is returned. If no
// error, it is safe to assume that catch-up buffer is empty and nil after this
// call.
func (ubr *unbufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	// TODO(wenyihu6): check if we can just drain without holding the lock first
	// during reviews We shouldn't be reading from the buffer at the same time
	publish := func() error {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				err := ubr.stream.SendBuffered(e.event, e.alloc)
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Done.
				return nil
			}
		}
	}

	if err := publish(); err != nil {
		return err
	}

	ubr.mu.Lock()
	defer ubr.mu.Unlock()

	if err := publish(); err != nil {
		return err
	}

	// Even if the catch-up buffer has overflowed, all events in it should still
	// be published. But we should return an error here before setting catchUpBuf
	// to nil. Doing so might be misinterpreted by publish as a successful
	// catch-up scan, causing it to start sending to the underlying stream. Caller
	// will be responsible for disconnecting first. Caller will drain catchUpBuf
	// again after disconnect, but it will be no-op.
	if ubr.mu.catchUpOverflowed {
		return newRetryErrBufferCapacityExceeded()
	}

	// Success.
	ubr.mu.catchUpBuf = nil
	ubr.mu.caughtUp = true
	return nil
}

// discardCatchUpBuffer discards all events in catch-up buffer without
// publishing them. It is safe to assume that catch-up buffer is empty and nil
// after this call. In case of error, caller should make sure to set
// disconnected to true before this call to make sure publish doesn't treat nil
// catchUpBuf as a successful catch-up scan.
func (ubr *unbufferedRegistration) discardCatchUpBuffer(ctx context.Context) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	// TODO(wenyihu6): Check if we can just discard without holding the lock first
	// ? We shouldn't be reading from the buffer at the same time
	ubr.discardCatchUpBufferWithLock(ctx)
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (ubr *unbufferedRegistration) detachCatchUpIter() *CatchUpIterator {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	catchUpIter := ubr.mu.catchUpIter
	ubr.mu.catchUpIter = nil
	return catchUpIter
}

// maybeRunCatchUpScan runs the catch-up scan if catchUpIter is not nil. It
// promises to close catchUpIter once detached. It returns an error if catch-up
// scan fails. The caller should drain catchUpBuf and disconnects. Note that
// catch up scan bypasses BufferedStream and are sent to underlying stream
// directly.
func (ubr *unbufferedRegistration) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpIter := ubr.detachCatchUpIter()
	if catchUpIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		catchUpIter.Close()
		ubr.metrics.RangeFeedCatchUpScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	// In the future, we might want a separate queue for catch up scans.
	return catchUpIter.CatchUpScan(ctx, ubr.stream.SendUnbuffered, ubr.withDiff, ubr.withFiltering,
		ubr.withOmitRemote)
}
