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

type unbufferedRegistration struct {
	// Input.
	baseRegistration
	metrics *Metrics

	// Output.
	stream BufferedStream

	// Internal.
	mu struct {
		// TODO(wenyihu6): ask why we are using Locker instedad of Mutex during code
		// review
		sync.Locker
		// Once set to true, cannot unset. True if this registration catchUpBuf has
		// overflowed, future live events are dropped. This will
		catchUpOverflowed bool
		// Nil if catch up scan has done (either not needed, success or error). In
		// the case of error, disconnected flag is set. Safe to send to
		// underlying stream if catchUpBuf is nil and disconnected is false. After
		// catch up buffer is done,
		// Nto be protected under mu because it is
		// shared between goroutines and set to nil after it's done.
		catchUpBuf chan *sharedEvent
		// Fine to repeatedly cancel context.
		catchUpScanCancelFn func()
		// Once set, cannot unset.
		disconnected bool
		catchUpIter  *CatchUpIterator
		// Test streams are responsible for
		caughtUp bool

		// add blockwhenfull to catch up buffer - we can't test this with the actual
		// underlying buffered stream though -- will have to disconnect this is test
		// only anyway
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
		// Send to underlying stream directly if catch up scan is not needed.
		br.mu.catchUpBuf = make(chan *sharedEvent, bufferSz)
	}
	return br
}

// publish will call alloc.Use if it successfully sends the event. After this
// function returns, ownership of alloc is now transferred to the stream or
// catch up buffer. It will promise to release it.
func (ubr *unbufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	ubr.assertEvent(ctx, event)
	strippedEvent := ubr.maybeStripEvent(ctx, event)
	if shouldSendToStream := ubr.maybePutInCatchUpBuffer(ctx, strippedEvent, alloc); shouldSendToStream {
		// We are caught up and can send to underlying stream directly.
		if err := ubr.stream.SendBuffered(strippedEvent, alloc); err != nil {
			// Stream buffer is full. Disconnect just in case. We will drain catch up
			// buffer in runOutputLoop. Disconnect from StreamMuxer should happen soon
			// as well. (TOTO wenyihu6: add checks here)
			ubr.disconnect(kvpb.NewError(err))
		}
	}
}

func (ubr *unbufferedRegistration) disconnect(pErr *kvpb.Error) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if alreadyDisconnected := ubr.setDisconnectedIfNotWithLock(); !alreadyDisconnected {
		ubr.stream.Disconnect(pErr)
	}
}

// runOutputLoop promises to drain all allocations when this goroutine ends. No
// more allocations need to be drained when being unregistered from client.
// Caller should make sure never to put anything in catch up buffer after catch
// up buffer has been drained. It will either drop events and disconnect or send
// to underlying stream. In case of disconnect happening early,
// publishCatchUpBuffer will see the stream context cancellation and discard
// catch up buffer.
func (ubr *unbufferedRegistration) runOutputLoop(ctx context.Context, forStacks roachpb.RangeID) {
	ubr.mu.Lock()
	if ubr.mu.disconnected {
		// already disconnected
		ubr.discardCatchUpBufferWithLock()
		ubr.mu.Unlock()
		return
	}

	ctx, ubr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	ubr.mu.Unlock()

	if err := ubr.maybeRunCatchUpScan(ctx); err != nil {
		ubr.disconnect(kvpb.NewError(errors.Wrap(err, "catch-up scan failed")))
		// Important to disconnect before draining to avoid upstream to interpret nil
		// catch up buf as sending to underlying stream directly.
		ubr.discardCatchUpBuffer()
		return
	}

	if err := ubr.publishCatchUpBuffer(ctx); err != nil {
		ubr.disconnect(kvpb.NewError(err))
		// Important to disconnect before draining to avoid upstream to interpret nil
		// catch up buf as sending to underlying stream directly.
		ubr.discardCatchUpBuffer()
		return
	}
	// Success: publishCatchUpBuffer should have drained and set catchUpBuf to nil
	// when it succeeds.
}

// Noop for unbuffered registration since events are not buffered in buf.
// runOutputLoop is responsible for draining catchUpBuf.
func (ubr *unbufferedRegistration) drainAllocations(ctx context.Context) {}

func (ubr *unbufferedRegistration) maybePutInCatchUpBuffer(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) (shouldSendToStream bool) {
	// Try putting in catch up buffer first.
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if ubr.mu.disconnected || ubr.mu.catchUpOverflowed {
		// No memory is allocated yet with Use or with sync.Pool. We don't check for
		// disconnected for buffered registration because they have a dedicated
		// goroutine to watch for stream context cancellation which happens right
		// after Diconnect. But unbuffered registration lacks such luxury, so it has
		// to sync on disconnected because unregistercleint can happen late.
		return false
	}
	if ubr.mu.catchUpBuf == nil {
		return true
	}

	e := getPooledSharedEvent(sharedEvent{event: event, alloc: alloc})
	// alloc.Use()
	select {
	case ubr.mu.catchUpBuf <- e:
		ubr.mu.caughtUp = false
		// success
	default:
		// Catch up buffer is full. Set it to overflowed and we will drop future
		// events. But we will continue draining catch up buffer.
		ubr.mu.catchUpOverflowed = true
		putPooledSharedEvent(e)
		//e.alloc.Release(ctx)
	}
	return false
}

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

func (ubr *unbufferedRegistration) setDisconnectedIfNotWithLock() (alreadyDisconnected bool) {
	// TODO(wenyihu6): think about if you should just drain catchUpBuf here we
	// never publish anything in catch up buf if disconnected. But this might take
	// a long time and you are on a hot path.
	if ubr.mu.disconnected {
		return true
	}
	if ubr.mu.catchUpIter != nil {
		// Catch up scan hasn't started yet.
		ubr.mu.catchUpIter.Close()
		ubr.mu.catchUpIter = nil
	}
	if ubr.mu.catchUpScanCancelFn != nil {
		ubr.mu.catchUpScanCancelFn()
	}
	ubr.mu.disconnected = true
	return false
}

func (ubr *unbufferedRegistration) discardCatchUpBufferWithLock() {
	func() {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				// drain catch up buffer.
				// TODO(wenyihu6): check memorey accounting and context passing
				e.alloc.Release(context.Background())
				putPooledSharedEvent(e)
			default:
				// If catchUpBuf is nil or empty
				return
			}
		}
	}()

	ubr.mu.catchUpBuf = nil
	ubr.mu.caughtUp = true
}

// Caller is responsible for draining catch up buffer again in case of an error.
// Promise to drain and set catchUpBuf to nil properly if succeeds.
func (ubr *unbufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()

	// TODO(wenyihu6): check if we can just drain without holding the lock first ?
	// We shouldn't be reading from the buffer at the same time
	publish := func() error {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				// drain catch up buffer.
				// TODO(wenyihu6): check memorey accounting and context passing
				if err := ubr.stream.SendBuffered(e.event, e.alloc); err != nil {
					return err
				}
				e.alloc.Release(context.Background())
				putPooledSharedEvent(e)
			case <-ctx.Done():
				return ctx.Err()
			case <-ubr.stream.Context().Done():
				return ubr.stream.Context().Err()
			default:
				// If catchUpBuf is nil or empty. Shouldn't be possible to have nil
				// catchUpBuf here.
				return nil
			}
		}
	}

	if err := publish(); err != nil {
		return err
	}

	// We should still publish all events in the catch up buffer even if catch up
	// buffer has overflowed. Caller is responsible for draining it again.
	// Catch up buffer should be drained at this point. But we do not want to
	// set catchUoBuf to nil yet since publish could interpret this as a
	// successful catch-up scan and start sending to underlying stream. We need
	// to make sure that we disconnect first. runOutputLoop will try draining
	// again which should be no-op and will set it to nil properly.
	if ubr.mu.catchUpOverflowed {
		// TODO(wenyihu6): refactor this to a var
		return kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER)
	}

	// success
	ubr.mu.catchUpBuf = nil
	ubr.mu.caughtUp = true
	return nil
}

func (ubr *unbufferedRegistration) discardCatchUpBuffer() {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	// TODO(wenyihu6): check if we can just discard without holding the lock first
	// ? We shouldn't be reading from the buffer at the same time
	ubr.discardCatchUpBufferWithLock()
}

// detachCatchUpIter detaches the catchUpIter that was previously attached.
func (ubr *unbufferedRegistration) detachCatchUpIter() *CatchUpIterator {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	catchUpIter := ubr.mu.catchUpIter
	ubr.mu.catchUpIter = nil
	return catchUpIter
}

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

	// Catch up scan sends events to bypass the buffer and send to underlying
	// stream directly. In the future, we might want a separate queue for catch up
	// scans.
	return catchUpIter.CatchUpScan(ctx, ubr.stream.Send, ubr.withDiff, ubr.withFiltering, ubr.withOmitRemote)
}
