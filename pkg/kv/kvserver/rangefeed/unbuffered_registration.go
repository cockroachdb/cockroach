// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
)

// unbufferedRegistration is similar to bufferedRegistration but does not
// internally buffer events delivered via publish() after its initial catch-up
// scan. Rather, it sends events directly to the BufferedStream. It assumes
// that BufferedStream's SendBuffered method is non-blocking. As a result, it
// does not require a long-lived goroutine to move event between an internal
// buffer and the sender.
//
// Despite the name, the unbuffered registration does contain a buffer. The
// catchUpBuf buffers live events delivered via publish() while the initial
// catch-up scan is running. The catch-up scan sends all updates directly to the
// sender using SendUnbuffered. When the catch-up scan is done, we then publish
// all events in the catchUpBuf to the buffered sender and then send all future
// events directly to the stream.
//
// The unbufferedRegistration is responsible for correctly handling memory
// reservations for data stored in the catchUpBuf.
//
// All errors are delivered to this registration via Disconnect() which is
// non-blocking and responsible for cleanup.
type unbufferedRegistration struct {
	baseRegistration
	metrics *Metrics

	// stream is where any events or errors published to this registration are
	// sent.
	stream BufferedStream

	mu struct {
		syncutil.Mutex

		// disconnected indicates that the registration has been disconnected. Once
		// disconnected is true, events sent via publish() are ignored.
		disconnected bool

		// catchUpOverflowed is true if catchUpBuf overflows during the catchUpScan.
		// Once overflowed, live raft events are dropped. This will cause the
		// registration to disconnect with an error once catch-up scan is done. Once
		// catchUpOverflowed is true, it will always be true.
		catchUpOverflowed bool

		// catchUpSnap is created by replica under raftMu lock when registration is
		// created if a catch-up scan is required. It is set to nil by output loop
		// for processing and closed when done.
		catchUpSnap *CatchUpSnapshot

		// catchUpBuf hold events published to this registration while the catch up
		// scan is running. It is set to nil once it has been drained (because
		// either catch-up scan succeeded or failed). If no catchUpSnap is provided,
		// catchUpBuf will be nil from the point of initialization.
		//
		// NB: Readers read from this without holding mu as there are not (and
		// should never be) concurrent readers.
		catchUpBuf chan *sharedEvent

		// catchUpScanCancelFn is called to tear down the goroutine responsible for
		// the catch-up scan. Can be called more than once.
		catchUpScanCancelFn func()
	}
}

var _ registration = (*unbufferedRegistration)(nil)

func newUnbufferedRegistration(
	streamCtx context.Context,
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpSnap *CatchUpSnapshot,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	bulkDeliverySize int,
	bufferSz int,
	metrics *Metrics,
	stream BufferedStream,
	removeRegFromProcessor func(registration),
) *unbufferedRegistration {
	br := &unbufferedRegistration{
		baseRegistration: newBaseRegistration(
			streamCtx,
			span,
			startTS,
			withDiff,
			withFiltering,
			withOmitRemote,
			bulkDeliverySize,
			removeRegFromProcessor),
		metrics: metrics,
		stream:  stream,
	}
	br.mu.catchUpSnap = catchUpSnap
	if br.mu.catchUpSnap != nil {
		// A nil catchUpSnap indicates we don't need a catch-up scan. We avoid
		// initializing catchUpBuf in this case, which will result in publish()
		// sending all events to the underlying stream immediately.
		br.mu.catchUpBuf = make(chan *sharedEvent, bufferSz)
	}
	return br
}

// publish sends a single event to this registration. It is called by the
// processor if the event overlaps the span this registration is interested in.
// Events are either stored in catchUpBuf or sent to BufferedStream directly,
// depending on whether catch-up scan is done.
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
			ubr.disconnectLocked(kvpb.NewError(err))
		}
	} else {
		// catchUpBuf is set, put event into the catchUpBuf if there is room.
		e := getPooledSharedEvent(sharedEvent{event: strippedEvent, alloc: alloc})
		alloc.Use(ctx)
		select {
		case ubr.mu.catchUpBuf <- e:
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
// multiple times, but subsequent errors are discarded. It is invoked by both
// the processor in response to errors from the replica and by the StreamManager
// in response to shutdowns.
func (ubr *unbufferedRegistration) Disconnect(pErr *kvpb.Error) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	ubr.disconnectLocked(pErr)
}

func (ubr *unbufferedRegistration) disconnectLocked(pErr *kvpb.Error) {
	if ubr.mu.disconnected {
		return
	}
	if ubr.mu.catchUpSnap != nil {
		// Catch-up scan hasn't started yet.
		ubr.mu.catchUpSnap.Close()
		ubr.mu.catchUpSnap = nil
	}
	if ubr.mu.catchUpScanCancelFn != nil {
		ubr.mu.catchUpScanCancelFn()
	}
	ubr.mu.disconnected = true
	ubr.stream.SendError(pErr)
	// NB: The unbuffered registration does not unregister itself on Disconnect
	// because it still has memory in the buffered sender and we do not want to
	// free any underlying memory budgets until that has been cleared.
}

// IsDisconnected returns true if the registration is disconnected.
func (ubr *unbufferedRegistration) IsDisconnected() bool {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	return ubr.mu.disconnected
}

// Unregister implements Disconnector.
func (ubr *unbufferedRegistration) Unregister() {
	assertTrue(ubr.IsDisconnected(), "connected registration in Unregister")
	ubr.removeRegFromProcessor(ubr)
}

// runOutputLoop is run in a goroutine. It is responsible for running the
// catch-up scan, and then publishing any events buffered in catchUpBuf to the
// sender (or discarding catch-up buffer in the case of an error).
//
// It is expected to be relatively short-lived.
//
// Once the catchUpBuf is drained, it will be set to nil, indicating to publish
// that it is now safe to deliver events directly to the sender.
//
// nolint:deferunlockcheck
func (ubr *unbufferedRegistration) runOutputLoop(ctx context.Context, forStacks roachpb.RangeID) {
	start := crtime.NowMono()
	defer func() {
		// We always want to drainAllocations even if we are already disconnected.
		// It will be a no-op if the catch up scan completed successfully.
		ubr.drainAllocations(ctx)
		ubr.metrics.RangefeedOutputLoopNanosForUnbufferedReg.Inc(start.Elapsed().Nanoseconds())
	}()

	ubr.mu.Lock()
	if ubr.mu.disconnected {
		ubr.mu.Unlock()
		return
	}
	ctx, ubr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	ubr.mu.Unlock()

	if err := ubr.maybeRunCatchUpScan(ctx); err != nil {
		// It is important to disconnect before draining so that (1) the drain
		// doesn't block publish and (2) publish can't erroneously observe a nil
		// catchUpBuf and start sending events to the underlying stream.
		ubr.Disconnect(kvpb.NewError(err))
		return
	}

	if err := ubr.publishCatchUpBuffer(ctx); err != nil {
		// It is important to disconnect before draining. See above.
		ubr.Disconnect(kvpb.NewError(err))
		return
	}
}

// maybeRunCatchUpScan runs the catch-up scan if the catchUpSnap is non-nil. It
// promises to close catchUpSnap once detached. It returns an error if catch-up
// scan fails. Note that catch up scan bypasses BufferedStream and are sent to
// the underlying stream directly.
func (ubr *unbufferedRegistration) maybeRunCatchUpScan(ctx context.Context) error {
	catchUpSnap := ubr.detachCatchUpSnap()
	if catchUpSnap == nil {
		return nil
	}

	start := crtime.NowMono()
	defer func() {
		catchUpSnap.Close()
		ubr.metrics.RangeFeedCatchUpScanNanos.Inc(start.Elapsed().Nanoseconds())
	}()
	return catchUpSnap.CatchUpScan(ctx, ubr.stream.SendUnbuffered, ubr.withDiff, ubr.withFiltering,
		ubr.withOmitRemote, ubr.bulkDelivery)
}

// publishCatchUpBuffer sends all items from catchUpBuf to the sender.
//
// drainAllocations should never be called concurrently with this function. The
// caller is responsible for draining the catchUpBuf if an error is returned.
func (ubr *unbufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	// We use the unbuffered sender until we have sent a non-empty checkpoint or
	// until maxUnbufferedSends is reached. The goal is to inform the stream of
	// its successful catch-up scan promptly. Once true, shouldSendBuffered is
	// always true.
	shouldSendBuffered := false

	// Because the checkpoint should typically the first event in the buffer, we
	// limit ourselves to sending no more than the current items in the buffer or
	// 1024 event.
	//
	// TODO(ssd): We are considering using the buffered sender during the catch up
	// scan. If we do that, we must remove use of the unbuffered sender here.
	maxUnbufferedSends := min(len(ubr.mu.catchUpBuf), 1024)
	unbufferedSendCount := 0

	publish := func() error {
		for {
			// Prioritize checking ctx.Done() so that we respond to context
			// cancellation quickly even in the presence of a long queue.
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			select {
			case e := <-ubr.mu.catchUpBuf:
				var err error
				if shouldSendBuffered {
					err = ubr.stream.SendBuffered(e.event, e.alloc)
				} else {
					unbufferedSendCount++
					foundCheckpoint := e.event.Checkpoint != nil && !e.event.Checkpoint.ResolvedTS.IsEmpty()
					if foundCheckpoint || unbufferedSendCount >= maxUnbufferedSends {
						shouldSendBuffered = true
						if !foundCheckpoint {
							log.KvExec.Infof(ctx, "no non-empty checkpoint found in %d event(s) before transitioning to buffered sender", unbufferedSendCount)
						}
					}
					err = ubr.stream.SendUnbuffered(e.event)
				}

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

	// Drain without holding locks first to avoid unnecessary blocking on
	// publish() for too long.
	//
	// TODO(ssd): An unfortunate side-effect of this is that we are not
	// coordinated with the disconnected bool, which can result in this call
	// sending events to the buffered sender _after_ an error has already been
	// delivered.
	if err := publish(); err != nil {
		return err
	}

	ubr.mu.Lock()
	defer ubr.mu.Unlock()

	// Drain again with lock held to ensure that events added to the buffer while
	// draining took place are also published.
	//
	// When under lock we always use the buffered sender to avoid blocking.
	shouldSendBuffered = true
	if err := publish(); err != nil {
		return err
	}

	// We check this after publish because even if the catch-up buffer has
	// overflowed, all events in it should still be published.
	if ubr.mu.catchUpOverflowed {
		return newRetryErrBufferCapacityExceeded()
	}

	ubr.mu.catchUpBuf = nil
	return nil
}

// drainAllocations drains catchUpBuf and release all memory held by the events
// in catch-up buffer without publishing them. It should only be called in the
// case of a catch-up scan failure, in which case the registration should have
// already been disconnected.
//
// It is safe to assume that catch-up buffer is empty and nil after this.
func (ubr *unbufferedRegistration) drainAllocations(ctx context.Context) {
	if !ubr.drainAllocationsRequired() {
		return
	}

	for {
		select {
		case e := <-ubr.mu.catchUpBuf:
			e.alloc.Release(ctx)
			putPooledSharedEvent(e)
		default:
			// Buffer is empty, unset catchUpBuf for good measure. But we should never
			// be here on a connected registration.
			ubr.mu.Lock()
			bufLen := len(ubr.mu.catchUpBuf) // nolint:deferunlockcheck
			ubr.mu.catchUpBuf = nil
			ubr.mu.Unlock()
			assertTrue(bufLen == 0, "non-empty catch up buffer after drainAllocation")

			return
		}
	}
}

func (ubr *unbufferedRegistration) drainAllocationsRequired() bool {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if ubr.mu.catchUpBuf == nil {
		return false
	}
	assertTrue(ubr.mu.disconnected, "drainAllocations called on connected registration without a non-nil catchUpBuf")
	return true
}

// detachCatchUpSnap detaches the catchUpSnap that was previously attached.
func (ubr *unbufferedRegistration) detachCatchUpSnap() *CatchUpSnapshot {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()

	catchUpSnap := ubr.mu.catchUpSnap
	ubr.mu.catchUpSnap = nil
	return catchUpSnap
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
		caughtUp := ubr.mu.catchUpBuf == nil
		ubr.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.Errorf("unbufferedRegistration for %v failed to drain its catch-up buffer in time", ubr.Range())
}

func assertTrue(cond bool, msg string) {
	if buildutil.CrdbTestBuild && !cond {
		panic(msg)
	}
}

func assumedUnreachable(msg string) {
	if buildutil.CrdbTestBuild {
		panic(fmt.Sprintf("assumed unreachable code reached: %s", msg))
	}
}
