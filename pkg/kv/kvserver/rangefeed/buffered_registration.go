// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	baseRegistration
	// Input.
	metrics *Metrics

	// Output.
	stream Stream
	// Internal.
	buf           chan *sharedEvent
	blockWhenFull bool // if true, block when buf is full (for tests)

	mu struct {
		syncutil.Mutex
		// True if this registration buffer has overflowed, dropping a live event.
		// This will cause the registration to exit with an error once the buffer
		// has been emptied.
		overflowed bool
		// Management of the output loop goroutine, used to ensure proper teardown.
		outputLoopCancelFn func()
		disconnected       bool

		// catchUpIter is created by replcia under raftMu lock when registration is
		// created. It is detached by output loop for processing and closed.
		// If output loop was not started and catchUpIter is non-nil at the time
		// that disconnect is called, it is closed by disconnect.
		catchUpIter *CatchUpIterator
	}

	// Number of events that have been written to the buffer but
	// not sent. Used only for testing.
	testPendingEventToSend atomic.Int64
}

var _ registration = &bufferedRegistration{}

func newBufferedRegistration(
	streamCtx context.Context,
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
	removeRegFromProcessor func(registration),
) *bufferedRegistration {
	br := &bufferedRegistration{
		baseRegistration: baseRegistration{
			streamCtx:              streamCtx,
			span:                   span,
			catchUpTimestamp:       startTS,
			withDiff:               withDiff,
			withFiltering:          withFiltering,
			withOmitRemote:         withOmitRemote,
			removeRegFromProcessor: removeRegFromProcessor,
		},
		metrics:       metrics,
		stream:        stream,
		buf:           make(chan *sharedEvent, bufferSz),
		blockWhenFull: blockWhenFull,
	}
	br.mu.catchUpIter = catchUpIter
	return br
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catch-up scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
//
// nolint:deferunlockcheck
func (br *bufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	br.assertEvent(ctx, event)
	e := getPooledSharedEvent(sharedEvent{event: br.maybeStripEvent(ctx, event), alloc: alloc})

	br.mu.Lock()
	defer br.mu.Unlock()
	if br.mu.overflowed || br.mu.disconnected {
		return
	}

	alloc.Use(ctx)
	select {
	case br.buf <- e:
		br.testPendingEventToSend.Add(1)
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
				br.testPendingEventToSend.Add(1)
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

// IsDisconnected returns true if the registration has been disconnected.
func (br *bufferedRegistration) IsDisconnected() bool {
	br.mu.Lock()
	defer br.mu.Unlock()
	return br.mu.disconnected
}

// Disconnect cancels the output loop context for the registration and passes an
// error to the output error stream for the registration.
// Safe to run multiple times, but subsequent errors would be discarded.
func (br *bufferedRegistration) Disconnect(pErr *kvpb.Error) {
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
		br.stream.SendError(pErr)
		br.removeRegFromProcessor(br)
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
//
// nolint:deferunlockcheck
func (br *bufferedRegistration) outputLoop(ctx context.Context) error {
	// If the registration has a catch-up scan, run it.
	if err := br.maybeRunCatchUpScan(ctx); err != nil {
		err = errors.Wrap(err, "catch-up scan failed")
		log.Errorf(ctx, "%v", err)
		return err
	}

	var (
		// The following variables facilitate logging when the registration was
		// overflowed before the catchup scan completed.
		//
		// For long catchup scans this is often expected. Hopefully, the buffer
		// contains a checkpoint with a non-empty timestamp. The buffer will always
		// contain a checkpoint since one is added to the buffer during registration;
		// but, it is possible that at the time of registration we did not have a
		// resolved timestamp. We check for this case since it means that the entire
		// catchup scan was wasted work.
		firstIteration                 = true
		wasOverflowedOnFirstIteration  = false
		oneCheckpointWithTimestampSent = false
	)
	// Normal buffered output loop.
	for {
		overflowed := false
		br.mu.Lock()
		if len(br.buf) == 0 {
			overflowed = br.mu.overflowed
		}
		if firstIteration {
			wasOverflowedOnFirstIteration = br.mu.overflowed
		}
		br.mu.Unlock()
		firstIteration = false

		if overflowed {
			if wasOverflowedOnFirstIteration && br.shouldLogOverflow(oneCheckpointWithTimestampSent) {
				log.Warningf(ctx, "rangefeed %s overflowed during catch up scan from %s (useful checkpoint sent: %v)",
					br.span, br.catchUpTimestamp, oneCheckpointWithTimestampSent)
			}

			return newErrBufferCapacityExceeded().GoError()
		}

		select {
		case nextEvent := <-br.buf:
			if wasOverflowedOnFirstIteration && !oneCheckpointWithTimestampSent {
				isCheckpointEvent := nextEvent.event != nil && nextEvent.event.Checkpoint != nil
				oneCheckpointWithTimestampSent = isCheckpointEvent && !nextEvent.event.Checkpoint.ResolvedTS.IsEmpty()
			}

			err := br.stream.SendUnbuffered(nextEvent.event)
			br.testPendingEventToSend.Add(-1)
			nextEvent.alloc.Release(ctx)
			putPooledSharedEvent(nextEvent)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-br.streamCtx.Done():
			return br.streamCtx.Err()
		}
	}
}

// nolint:deferunlockcheck
func (br *bufferedRegistration) runOutputLoop(ctx context.Context, _forStacks roachpb.RangeID) {
	defer br.drainAllocations(ctx)

	br.mu.Lock()
	if br.mu.disconnected {
		// The registration has already been disconnected.
		br.mu.Unlock()
		return
	}
	ctx, br.mu.outputLoopCancelFn = context.WithCancel(ctx)
	br.mu.Unlock()
	err := br.outputLoop(ctx)
	br.Disconnect(kvpb.NewError(err))
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

	return catchUpIter.CatchUpScan(ctx, br.stream.SendUnbuffered, br.withDiff, br.withFiltering, br.withOmitRemote)
}

// Wait for this registration to completely process its internal
// buffer. This is only used when a test sends a sync event to the
// rangefeed processor with testRegCatchupSpan set.
func (br *bufferedRegistration) waitForCaughtUp(ctx context.Context) error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.StartWithCtx(ctx, opts); re.Next(); {
		caughtUp := len(br.buf) == 0 && br.testPendingEventToSend.Load() == 0
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

var overflowLogEvery = log.Every(5 * time.Second)

// shouldLogOverflow returns true if the output loop should log about an
// overflow on the first iteration. We don't want to log every case since we
// expect this on some very busy servers.
func (br *bufferedRegistration) shouldLogOverflow(checkpointSent bool) bool {
	return (!checkpointSent) || log.V(1) || overflowLogEvery.ShouldLog()
}

// Used for testing only.
func (br *bufferedRegistration) getBuf() chan *sharedEvent {
	return br.buf
}

// Used for testing only.
func (br *bufferedRegistration) getOverflowed() bool {
	br.mu.Lock()
	defer br.mu.Unlock()
	return br.mu.overflowed
}
