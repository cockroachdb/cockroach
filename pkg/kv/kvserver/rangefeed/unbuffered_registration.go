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

// unbufferedRegistration is similar to bufferedRangefeed but uses
// BufferedStream to buffer live raft updates instead of a using buf channel and
// having a dedicated per-range per-registration goroutine to volley events to
// underlying grpc stream. Instead, there is only one BufferedStream for each
// incoming node.MuxRangefeed rpc call. BufferedStream is responsible for
// buffering and sending its updates to the underlying grpc stream in a
// dedicated goroutine O(node).
//
// Note that UnbufferedRegistration needs to ensure that events sent to
// BufferedStream are in order and can be sent to the underlying stream
// directly. To achieve this, events from catch-up scans bypass BufferedStream
// buffers and are sent to underlying Stream directly. While catch-up scan is
// ongoing, live updates are temporarily buffered in buf to hold them until
// catch-up scan is complete. After catch-up scan is done, we know that catch-up
// scan events have been sent to grpc stream successfully. Raft updates buffered
// in the buf will then be sent to BufferedStream first and live updates will be
// sent to BufferedStream directly from then on.
//
// Note that there will still be a short-lived dedicated goroutine for catch up
// scan.
//
// Updates are delivered to the BufferedStream until
// 1. unbufferedRegistration.disconnect is called which can happen when: a) A
// SendBuffered to the BufferedStream returns an error (BufferedStream is
// stopped or is full) b) catch-up scan fails or catch-up buffer overflows. c)
// Registration is manually unregistered from processor d) client requests to
// close rangefeed with a specific streamID.
// 2. BufferedSender can disconnect all registrations when a node-level shutdown
// is needed.
//
// unbufferedRegistration is responsible for allocating and draining memory
// catch-up buffer while BufferedStream is responsible for allocating and
// draining memory buffered in BufferedStream.
type unbufferedRegistration struct {
	// Input.
	baseRegistration
	metrics *Metrics

	// Output.
	// unbufferedRegistration chooses to bypass buffer and blocking send to the
	// underlying stream directly for catch-up scans by calling
	// stream.SendUnbuffered.
	stream BufferedStream

	// Internal.
	mu struct {
		// TODO(wenyihu6 during code reviews): ask why we are using Locker for
		// registrations insted of Mutex
		sync.Locker

		// True if this registration catchUpBuf has overflowed, live raft events are
		// dropped. This will cause the registration to disconnect with an error
		// once catch-up scan is done and catchUpBuf is drained and published. Once
		// set to true, cannot unset.
		catchUpOverflowed bool

		// It is set to nil if catchUpBuf has been drained (either catch-up scan
		// succeeded or failed). Safe to send to BufferedStream if nil and
		// disconnected is false. the case of error, disconnected flag is set. Once
		// set, cannot unset.
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
		// Unregistration only happens when the callback registered via
		// RegisterRangefeedCleanUp is invoked from BufferedStream. Since this could
		// happen very late (when even is popped and ready to be sent to grpc
		// stream), we check the disconnected flag during publish to prevent further
		// raft events from being sent to BufferedStream. However, this doesn’t
		// guarantee that no Raft updates will be sent after this flag is set.
		// Catch-up scans and buffer may still send more updates, but this is fine.
		// Rangefeed might still send a few events after signaling completion errors
		// to the client, which should be handled by the client.
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
) *unbufferedRegistration {
	br := &unbufferedRegistration{
		baseRegistration: baseRegistration{
			span:             span,
			catchUpTimestamp: startTS,
			withDiff:         withDiff,
			withFiltering:    withFiltering,
			withOmitRemote:   withOmitRemote,
			unreg:            unregisterFn,
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
	ubr.assertEvent(ctx, event)
	strippedEvent := ubr.maybeStripEvent(ctx, event)
	if shouldSendToStream := ubr.maybePutInCatchUpBuffer(ctx, strippedEvent, alloc); shouldSendToStream {
		// We are caught up and can send to underlying stream directly.
		if err := ubr.stream.SendBuffered(strippedEvent, alloc); err != nil {
			// BufferedSender is full or has been stopped. A node level shutdown is
			// happening, so BufferedSender should disconnect all streams soon. There
			// is not anything else we can do here. Disconnect again just in case.
			ubr.disconnect(kvpb.NewError(err))
		}
	}
}

// When a registration disconnects, the following clean-up must happen:
// a ) Registration level clean-up: setDisconnectedIfNotWithLock
// - catch-up iter needs to be closed if not detached already
// - catchUpBuf needs to be drained. runOutputLoop goroutine is responsible for
// watching the stream context cancellation and drain catch-up buffer.
// b) Processor level clean-up: p.reg.Unregister
// c) BufferedSender level clean-up: stream.Disconnect
// - cancel stream context, metrics updates, send a disconnect error back to
// rangefeed client
//
// There are two ways to disconnect a registration:
// 1. ubr.disconnect is called
// - a) happens here. b), c) happen during stream.Disconnect.
// 2. BufferedSender invokes the callback registered via
// RegisterRangefeedCleanUp.
// - b), c) happen in BufferedSender and a happens via the callback
//
// Safe to run multiple times, but subsequent errors would be discarded.
func (ubr *unbufferedRegistration) disconnect(pErr *kvpb.Error) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if alreadyDisconnected := ubr.setDisconnectedIfNotWithLock(); !alreadyDisconnected {
		ubr.stream.Disconnect(pErr)
	}
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
	if ubr.mu.disconnected {
		// already disconnected
		ubr.discardCatchUpBufferWithLock(ctx)
		ubr.mu.Unlock()
		return
	}

	ctx, ubr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	ubr.mu.Unlock()

	if err := ubr.maybeRunCatchUpScan(ctx); err != nil {
		ubr.disconnect(kvpb.NewError(errors.Wrap(err, "catch-up scan failed")))
		// Important to disconnect before draining to avoid upstream to interpret
		// nil catch-up buf as sending to underlying stream directly.
		ubr.discardCatchUpBuffer(ctx)
		return
	}

	if err := ubr.publishCatchUpBuffer(ctx); err != nil {
		ubr.disconnect(kvpb.NewError(err))
		// Important to disconnect before draining to avoid upstream to interpret
		// nil catch-up buf as sending to underlying stream directly.
		ubr.discardCatchUpBuffer(ctx)
		return
	}
	// Success: publishCatchUpBuffer should have drained and set catchUpBuf to nil
	// when it succeeds.
}

// Noop for unbuffered registration since events are not buffered in buf.
// runOutputLoop is responsible for draining catchUpBuf.
func (ubr *unbufferedRegistration) drainAllocations(ctx context.Context) {}

// maybePutInCatchUpBuffer tries to put event in catch-up buffer and returns a
// boolean indicating whether the caller should still try sending the event to
// stream instead. If disconnected or catchUpOverflowed is already set, events
// are simply ignored. If catchUpBuf is nil, the function return true,
// signalling the event should be sent to stream instead. Otherwise, it
// allocates for memory and sending to catch-up buffer. Caller is responsible
// for draining buffer.
//
// Note that the caller is responsible for calling alloc.Use and alloc.Release if
// shouldSendToStream is true. And caller does not need to call disconnect if
// shouldSendToStream is false.
func (ubr *unbufferedRegistration) maybePutInCatchUpBuffer(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) (shouldSendToStream bool) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if ubr.mu.disconnected || ubr.mu.catchUpOverflowed {
		// No memory is allocated yet. BufferedRegistration does not need to check
		// for disconnected since it has a dedicated goroutine to watch for stream
		// context cancellation and unregister from processor happens relatively
		// fast. However, unbufferedRegistration does not have such luxury. It only
		// unregisters from processor when callback registered via
		// RegisterRangefeedCleanUp is called (which can happen late). Thus, we
		// check for disconnected to sync here.
		return false
	}
	// Disconnected or catchUpOverflowed is not set and catchUpBuf is nil -> Safe
	// to send to underlying stream.
	if ubr.mu.catchUpBuf == nil {
		return true
	}

	e := getPooledSharedEvent(sharedEvent{event: event, alloc: alloc})
	alloc.Use(ctx)
	select {
	case ubr.mu.catchUpBuf <- e:
		ubr.mu.caughtUp = false
		// Success.
	default:
		// catchUpBuf exceeded and we are dropping this event. Registration will
		// need a catch-up scan after being disconnected.
		ubr.mu.catchUpOverflowed = true
		putPooledSharedEvent(e)
		e.alloc.Release(ctx)
	}
	return false
}

// Wait for this registration to completely drain its catchUpBuf. Note that this
// does not mean that BufferedStream has been sent to underlying grpc Stream
// yet.
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

func (ubr *unbufferedRegistration) setDisconnectedIfNot() {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	ubr.setDisconnectedIfNotWithLock()
}

// setDisconnectedIfNotWithLock sets disconnected to true if it is not already
// set. It handles registration level cleanup. It is called from BufferedSender
// to disconnect registrations. It is no-op if ubr.disconnect already happens.
//
// Note it is called while holding ubr.mu.
func (ubr *unbufferedRegistration) setDisconnectedIfNotWithLock() (alreadyDisconnected bool) {
	// TODO(wenyihu6 during code reviews or later on): think about if we should
	// just drain catchUpBuf here during reviews. We never publish anything in
	// catch-up buf if disconnected. But this might take a long time and you are
	// on a hot path.
	if ubr.mu.disconnected {
		return true
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
	return false
}

// discardCatchUpBufferWithLock drains catchUpBuf and release all memory held by
// the events in catch-up buffer without publishing them. It is safe to assume
// that catch-up buffer is empty and nil after call.
func (ubr *unbufferedRegistration) discardCatchUpBufferWithLock(ctx context.Context) {
	func() {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				// TODO(wenyihu6): check if release with context.Background is fine
				// during reviews
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

// publishCatchUpBuffer drains catchUpBuf and release all memory held by
// the events in catch-up buffer while publishing them.
//
// Caller is responsible for draining it again if error is returned. If no
// error, it is safe to assume that catch-up buffer is empty and nil after this
// call.
func (ubr *unbufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()

	// TODO(wenyihu6): check if we can just drain without holding the lock first
	// during reviews We shouldn't be reading from the buffer at the same time
	publish := func() error {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				if err := ubr.stream.SendBuffered(e.event, e.alloc); err != nil {
					return err
				}
				e.alloc.Release(ctx)
				putPooledSharedEvent(e)
			case <-ctx.Done():
				return ctx.Err()
			case <-ubr.stream.Context().Done():
				return ubr.stream.Context().Err()
			default:
				// Done.
				return nil
			}
		}
	}

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
	// TODO(wenyihu6): check if we should sent to BufferedStream instead during
	// code reviews.
	return catchUpIter.CatchUpScan(ctx, ubr.stream.SendUnbuffered, ubr.withDiff, ubr.withFiltering,
		ubr.withOmitRemote)
}
