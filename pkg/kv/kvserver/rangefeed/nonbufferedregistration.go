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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type unbufferedRegistration struct {
	baseRegistration
	// Input.
	metrics *Metrics

	// Output.
	stream Stream

	mu struct {
		sync.Locker
		// Once set, cannot unset.
		catchUpOverflowed bool
		// Nil if catch up scan has done (either success or unsuccess). In the case
		// of unsuccess, disconnected flag is set. Safe to send to underlying stream
		// if catchUpBuf is nil and disconnected is false. After catch up buffer is done,
		catchUpBuf chan *sharedEvent
		// Fine to repeated cancel context.
		catchUpScanCancelFn func()
		// Once set, cannot unset.
		disconnected bool
		catchUpIter  *CatchUpIterator
	}
}

var _ registration = &bufferedRegistration{}

func newUnbufferedRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	bufferSz int,
	metrics *Metrics,
	stream Stream,
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
	if br.mu.catchUpIter != nil {
		br.mu.catchUpBuf = make(chan *sharedEvent, bufferSz)
	}
	return br
}

// Publish
func (ubr *unbufferedRegistration) publish(
	ctx context.Context, event *kvpb.RangeFeedEvent, alloc *SharedBudgetAllocation,
) {
	ubr.assertEvent(ctx, event)
	e := getPooledSharedEvent(sharedEvent{event: ubr.maybeStripEvent(ctx, event), alloc: alloc})
	// Try putting in catch up buffer first
	shouldSendToStream := func() bool {
		ubr.mu.Lock()
		defer ubr.mu.Unlock()
		if ubr.mu.catchUpOverflowed || ubr.mu.disconnected {
			// Dropping events. The registration is either disconnected or will be
			// disconnected soon after catch up scan is done and we will disconnect. It
			// will need a catch up scan when it reconnects. Treat these as true to
			// avoid sending to stream.
			return false
		}
		if ubr.mu.catchUpBuf == nil {
			// Catch up buf has been drained and not due to disconnected. Safe to send
			// to underlying stream. Important to check disconnected first since it is
			// nil after draining as well.
			return true
		}

		// If catch up buf is non-nil, catch up scan is still on-going. Put the
		// event in catch up buffer first.
		select {
		case ubr.mu.catchUpBuf <- e:
		default:
			// Dropping events.
			ubr.mu.catchUpOverflowed = true
			putPooledSharedEvent(e)
		}
		return false
	}()

	if shouldSendToStream {
		// not disconnected yet -> should send to underlying stream
		if err := ubr.stream.Send(e.event); err != nil {
			ubr.disconnect(kvpb.NewError(err))
		}
	}
}

func (ubr *unbufferedRegistration) setDisconnectedIfNotWithRMu() (alreadyDisconnected bool) {
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

func (ubr *unbufferedRegistration) setDisconnectedIfNot() {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	ubr.setDisconnectedIfNotWithRMu()
}

func (ubr *unbufferedRegistration) disconnect(pErr *kvpb.Error) {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	if alreadyDisconnected := ubr.setDisconnectedIfNotWithRMu(); !alreadyDisconnected {
		ubr.stream.Disconnect(pErr)
	}
}

// nbr promises to drain all allocations when this goroutine ends. No more
// allocations need to be drained when being unregistered from client. Upstream
// promises never to put anything in catch up buffer after catch up buffer has
// been drained. It will either drop events and disconnect or send to underlying stream.
func (ubr *unbufferedRegistration) runOutputLoop(ctx context.Context, forStacks roachpb.RangeID) {
	ubr.mu.Lock()

	if ubr.mu.disconnected {
		// The registration has already been disconnected.
		ubr.discardCatchUpBufferWithRMu()
		ubr.mu.Unlock()
		return
	}

	ctx, ubr.mu.catchUpScanCancelFn = context.WithCancel(ctx)
	ubr.mu.Unlock()

	if err := ubr.maybeRunCatchUpScan(ctx); err != nil {
		ubr.disconnectAndDiscardCatchUpBuffer(kvpb.NewError(errors.Wrap(err, "catch-up scan failed")))
		return
	}

	if err := ubr.publishCatchUpBuffer(ctx); err != nil {
		ubr.disconnectAndDiscardCatchUpBuffer(kvpb.NewError(err))
		return
	}
	// publishCatchUpBuffer will drain the catch up buffer.
}

func (ubr *unbufferedRegistration) publishCatchUpBuffer(ctx context.Context) error {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()

	// TODO(wenyihu6): check if you can drain first without holding locks lets do
	// it the sfae way first we will revisit
	drainAndPublish := func() error {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				if err := ubr.stream.Send(e.event); err != nil {
					return err
				}
				putPooledSharedEvent(e)
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		}
	}

	if err := drainAndPublish(); err != nil {
		return err
	}

	// Must disconnect first before setting catchUpBuf to nil.
	if ubr.mu.catchUpOverflowed {
		return newErrBufferCapacityExceeded().GoError()
	}

	// success
	ubr.mu.catchUpBuf = nil
	return nil
}

func (ubr *unbufferedRegistration) disconnectAndDiscardCatchUpBuffer(pErr *kvpb.Error) {
	// Important to disconnect before draining to avoid upstream to interpret nil
	// catch up buf as sending to underlying stream directly.
	ubr.disconnect(pErr)
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	ubr.discardCatchUpBufferWithRMu()
}

func (ubr *unbufferedRegistration) discardCatchUpBufferWithRMu() {
	func() {
		for {
			select {
			case e := <-ubr.mu.catchUpBuf:
				putPooledSharedEvent(e)
			default:
				return
			}
		}
	}()

	ubr.mu.catchUpBuf = nil
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

	return catchUpIter.CatchUpScan(ctx, ubr.stream.Send, ubr.withDiff, ubr.withFiltering, ubr.withOmitRemote)
}

func (ubr *unbufferedRegistration) detachCatchUpIter() *CatchUpIterator {
	ubr.mu.Lock()
	defer ubr.mu.Unlock()
	catchUpIter := ubr.mu.catchUpIter
	ubr.mu.catchUpIter = nil
	return catchUpIter
}

// No allocations to drain when disconnected. Catch up goroutine is responsible
// for draining its allocations when goroutine ends.
func (ubr *unbufferedRegistration) drainAllocations(ctx context.Context) {}

func (ubr *unbufferedRegistration) waitForCaughtUp(ctx context.Context) error {
	panic("unimplemented: unbuffered registration does not support waitForCaughtUp")
}
