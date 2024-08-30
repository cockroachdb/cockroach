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
// to the BufferedStream, depending on the BufferedStream to be non-blocking. As
// a result, it does not require a long-lived goroutine to volley event when
// reading from raft.
//
// Note that UnbufferedRegistration needs to ensure that events sent to
// BufferedStream from the processor during a running catchup scan are sent to
// the client in order. To achieve this, events from catch-up scans are send
// using UnbufferedSend. While the catch-up scan is ongoing, live updates
// delivered via publish() are temporarily buffered in catchUpBuf to hold them
// until catch-up scan is complete. After the catch-up scan is done, we know
// that catch-up scan events have been sent to grpc stream successfully.
// Catch-up buffer is then drained by our hopefully short lived output loop
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

		// True if this catchUpBuf has overflowed, live raft events are dropped.
		// This will cause the registration to disconnect with an error once
		// catch-up scan is done and catchUpBuf is drained and published. Once set
		// to true, cannot unset.
		catchUpOverflowed bool

		// It is set to nil if catchUpBuf has been drained (either catch-up scan
		// succeeded or failed). If no catch-up iter is provided, catchUpBuf will be
		// nil since the initialization.
		catchUpBuf chan *sharedEvent

		// Fine to repeatedly cancel context. Management of the catch-up
		// runOutputLoop context goroutine.
		catchUpScanCancelFn func()

		// disconnection indicates that the registration is marked for
		// disconnection and disconnection is taking place.
		disconnected bool

		// catchUpIter is created by replcia under raftMu lock when registration is
		// created. It is detached by output loop for processing and closed when
		// done.
		catchUpIter *CatchUpIterator

		// Used for testing only. Indicates that all events in catchUpBuf have been
		// sent to BufferedStream.
		caughtUp bool
	}
}

var _ registration = (*unbufferedRegistration)(nil)

func newUnbufferedRegistration(
	streamCtx context.Context,
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	bufferSz int,
	metrics *Metrics,
	stream BufferedStream,
	removeRegFromProcessor func(registration),
) *unbufferedRegistration {
	br := &unbufferedRegistration{
		baseRegistration: baseRegistration{
			streamCtx:              streamCtx,
			span:                   span,
			catchUpTimestamp:       startTS,
			withDiff:               withDiff,
			withFiltering:          withFiltering,
			withOmitRemote:         withOmitRemote,
			removeRegFromProcessor: removeRegFromProcessor,
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

// publish sends a single event either to catch up buffer or to BufferedStream
// directly depending on whether catch up scan has done. publish is responsible
// for using and releasing alloc.
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
			// Disconnect here for testing purposes only: there are test stream
			// implementations that inject errors without calling disconnect. For
			// production code, we expect buffered sender to shut down all
			// registrations.
			ubr.disconnectLocked(kvpb.NewError(err))
		}
	} else {
		// catchUpBuf is set, put event into the catchUpBuf if
		// there is room.
		e := getPooledSharedEvent(sharedEvent{event: strippedEvent, alloc: alloc})
		alloc.Use(ctx)
		select {
		case ubr.mu.catchUpBuf <- e:
			ubr.mu.caughtUp = false
		default:
			// catchUpBuf exceeded and we are dropping this event. A catch up scan is
			// needed later.
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
func (ubr *unbufferedRegistration) Disconnect(pErr *kvpb.Error) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	ubr.disconnectLocked(pErr)
}

func (ubr *unbufferedRegistration) disconnectLocked(pErr *kvpb.Error) {
	if ubr.mu.disconnected {
		return
	}
	if ubr.mu.catchUpIter != nil {
		// Catch-up scan hasn't started yet.
		ubr.mu.catchUpIter.Close()
		ubr.mu.catchUpIter = nil
	}
	if ubr.mu.catchUpScanCancelFn != nil {
		ubr.mu.catchUpScanCancelFn()
	}
	ubr.mu.disconnected = true

	// SendError cleans up metrics and sends error back to client without
	// blocking.
	ubr.stream.SendError(pErr)
	// Clean up unregisters registration from processor async.
	ubr.removeRegFromProcessor(ubr)
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
func (ubr *unbufferedRegistration) runOutputLoop(ctx context.Context, forStacks roachpb.RangeID) {
	ubr.mu.Lock()
	defer ubr.drainCatchUpBuffer(ctx)
	if ubr.mu.disconnected {
		ubr.mu.Unlock()
		return
	}

	ctx, ubr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	ubr.mu.Unlock()

	if err := ubr.maybeRunCatchUpScan(ctx); err != nil {
		// Important to disconnect before draining.
		ubr.Disconnect(kvpb.NewError(errors.Wrap(err, "catch-up scan failed")))
		return
	}

	if err := ubr.publishCatchUpBuffer(ctx); err != nil {
		// Important to disconnect before draining.
		ubr.Disconnect(kvpb.NewError(err))
		return
	}
	// Success: publishCatchUpBuffer should have drained and set catchUpBuf to nil
	// when it succeeds.
}

// Noop for unbuffered registration since events are not buffered in buf.
// runOutputLoop is responsible for draining catchUpBuf when being disconnected.
func (ubr *unbufferedRegistration) drainAllocations(ctx context.Context) {}

// Used for testing only. Wait for this registration to completely drain its
// catchUpBuf.
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

// publishCatchUpBuffer drains catchUpBuf and release all memory held by the
// events in catch-up buffer while publishing them. Note that
// drainCatchUpBuffer should never be called concurrently with this function.
// Caller is responsible for draining it again if error is returned.
func (ubr *unbufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
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
	// be published. Important to disconnect before setting catchUpBuf to nil.
	if ubr.mu.catchUpOverflowed {
		return newRetryErrBufferCapacityExceeded()
	}

	// Success.
	ubr.mu.catchUpBuf = nil
	ubr.mu.caughtUp = true
	return nil
}

// drainCatchUpBuffer drains catchUpBuf and release all memory held by
// the events in catch-up buffer without publishing them. It is safe to assume
// that catch-up buffer is empty and nil after this.
func (ubr *unbufferedRegistration) drainCatchUpBuffer(ctx context.Context) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	// TODO(wenyihu6): Check if we can just discard without holding the lock first
	// ? We shouldn't be reading from the buffer at the same time
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
// scan fails. Note that catch up scan bypasses BufferedStream and are sent to
// underlying stream directly.
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

// Used for testing only.
func (ubr *unbufferedRegistration) getBuf() chan *sharedEvent {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	return ubr.mu.catchUpBuf
}

// Used for testing only.
func (ubr *unbufferedRegistration) getOverflowed() bool {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	return ubr.mu.catchUpOverflowed
}
