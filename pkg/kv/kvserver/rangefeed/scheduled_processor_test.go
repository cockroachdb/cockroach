// Copyright 2023 The Cockroach Authors.
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
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduledProcessorUnregisterWaitsStreamDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const totalEvents = 100

	const channelCapacity = totalEvents + 10

	s := cluster.MakeTestingClusterSettings()
	m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
	m.Start(context.Background(), nil, mon.NewStandaloneBudget(math.MaxInt64))

	b := m.MakeBoundAccount()
	fb := NewFeedBudget(&b, 0, &s.SV)

	p, h, stopper := newTestProcessor(t, withBudget(fb), withChanCap(channelCapacity),
		withEventTimeout(time.Millisecond), withProcType(schedulerProcessor))
	ctx := context.Background()
	defer stopper.Stop(ctx)

	unreg := make(chan interface{})
	// Add a registration.
	rStream := newBufferedConsumer(context.Background(), unlimitedCapacity, 2)
	defer func() { rStream.cancel() }()
	ok, _ := p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			rStream.setDone(done)
			return rStream
		},
		func() {
			close(unreg)
		},
	)
	require.True(t, ok, "registration created")
	h.syncEventAndRegistrations()

	p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
		roachpb.Key("k"),
		hlc.Timestamp{WallTime: int64(2)},
		[]byte(fmt.Sprintf("this is value %04d", 2))))

	// Wait for half of the event to be processed by stream then stop processor.
	rStream.WaitPaused(t, 30*time.Second)

	p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
		roachpb.Key("k"),
		hlc.Timestamp{WallTime: int64(3)},
		[]byte(fmt.Sprintf("this is value %04d", 3))))

	rStream.Cancel()

	// Resume event loop in consumer to unblock any internal loops of processor or
	// registrations.
	rStream.Resume(0)

	// Next write should hit cancelled buffered stream. So we should process next
	// event and verify that we didn't stop.
	h.syncEventAndRegistrations()

	require.Equal(t, 1, p.Len(), "registration was removed prior to drain")

	// Notify processor of stream drain so that it can unregister.
	rStream.done()

	// Wait for registration notification to fire indicating that processor removed
	// registration.
	select {
	case <-unreg:
	case <-time.After(30 * time.Second):
		t.Fatal("failed to receive unregistration notification after stream drained")
	}
}

// TestBudgetReleaseOnScheduledProcessorStop
// Note that currently processor will
// stop as soon as request is processed without waiting for drain of pending
// events to complete. Budget is release by closing budget, not by drain op.
// This is probably ok as we do it when replica state changes and new processor
// won't be immediately attached and hoard as much memory meanwhile.
func TestBudgetReleaseOnScheduledProcessorStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const totalEvents = 100

	const channelCapacity = totalEvents + 10

	s := cluster.MakeTestingClusterSettings()
	m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
	m.Start(context.Background(), nil, mon.NewStandaloneBudget(math.MaxInt64))

	b := m.MakeBoundAccount()
	fb := NewFeedBudget(&b, 0, &s.SV)

	p, h, stopper := newTestProcessor(t, withBudget(fb), withChanCap(channelCapacity),
		withEventTimeout(time.Millisecond), withProcType(schedulerProcessor))
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// Add a registration.
	rStream := newBufferedConsumer(context.Background(), unlimitedCapacity, 50)
	defer func() { rStream.cancel() }()
	_, _ = p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			rStream.setDone(done)
			return rStream
		},
		func() {},
	)
	h.syncEventAndRegistrations()

	for i := 0; i < totalEvents; i++ {
		p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
			roachpb.Key("k"),
			hlc.Timestamp{WallTime: int64(i + 2)},
			[]byte(fmt.Sprintf("this is value %04d", i))))
	}

	// Wait for half of the event to be processed by stream then stop processor.
	rStream.WaitPaused(t, 30*time.Second)

	// Since stop is blocking and needs to flush events we need to do that in
	// parallel.
	stopDone := stopProcessor(p)

	// Resume event loop in consumer to unblock any internal loops of processor or
	// registrations.
	rStream.Resume(0)

	// Wait for top function to finish processing before verifying that we
	// consumed all events.
	stopDone(t, 30*time.Second)

	// We need to wait for budget to drain as Stop would only post stop event
	// after flushing the queue, but couldn't determine when main processor loop
	// is actually closed.
	testutils.SucceedsSoon(t, func() error {
		fmt.Printf("Budget now: %d bytes remained, %d events processed\n",
			m.AllocBytes(), rStream.Consumed())
		if m.AllocBytes() != 0 {
			return errors.Errorf(
				"Failed to release all budget after stop: %d bytes remained, %d events processed",
				m.AllocBytes(), rStream.Consumed())
		}
		return nil
	})
}

// TestSameAllocationPassedToMultipleRegistrations verifies that memory
// accounting in presence of multiple buffered streams correctly passes
// allocations to all streams.
func TestSameAllocationPassedToMultipleRegistrations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const totalEvents = 100

	const channelCapacity = totalEvents + 10

	s := cluster.MakeTestingClusterSettings()
	m := mon.NewMonitor("rangefeed", mon.MemoryResource, nil, nil, 1, math.MaxInt64, nil)
	m.Start(context.Background(), nil, mon.NewStandaloneBudget(math.MaxInt64))

	b := m.MakeBoundAccount()
	fb := NewFeedBudget(&b, 0, &s.SV)

	p, h, stopper := newTestProcessor(t, withBudget(fb), withChanCap(channelCapacity),
		withEventTimeout(time.Millisecond), withProcType(schedulerProcessor))
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// Add a registration 1.
	r1Stream := newBufferedConsumer(context.Background(), unlimitedCapacity, 0)
	defer func() { r1Stream.Cancel() }()
	_, _ = p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			r1Stream.setDone(done)
			return r1Stream
		},
		func() {},
	)

	// Add a registration 1.
	r2Stream := newBufferedConsumer(context.Background(), unlimitedCapacity, 0)
	defer func() { r2Stream.Cancel() }()
	_, _ = p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			r2Stream.setDone(done)
			return r2Stream
		},
		func() {},
	)
	h.syncEventAndRegistrations()

	for i := 0; i < totalEvents; i++ {
		p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
			roachpb.Key("k"),
			hlc.Timestamp{WallTime: int64(i + 2)},
			[]byte(fmt.Sprintf("this is value %04d", i))))
	}

	h.syncEventAndRegistrations()

	require.Equal(t, len(r1Stream.allocs), len(r2Stream.allocs), "number of published events")
	for i := range r1Stream.allocs {
		require.Same(t, r1Stream.allocs[i], r2Stream.allocs[i])
	}
}

func TestScheduledProcessorMemoryBudgetExceeded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fb := newTestBudget(40)
	m := NewMetrics()
	p, h, stopper := newTestProcessor(t, withBudget(fb), withChanTimeout(time.Millisecond),
		withMetrics(m), withProcType(schedulerProcessor))
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// Add a registration.
	r1Stream := newBufferedConsumer(ctx, 0, 0)
	_, _ = p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			r1Stream.setDone(done)
			return r1Stream
		},
		func() {},
	)
	h.syncEventAndRegistrations()

	// Write entries in excess of budget.
	for i := 0; i < 10; i++ {
		if !p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
			roachpb.Key("k"),
			hlc.Timestamp{WallTime: int64(i + 2)},
			[]byte(fmt.Sprintf("this is big value %02d", i)))) {
			break
		}
	}
	// Ensure that all events are processed so that it generates an error.
	h.syncEventC()

	// Process error generated by overflow.
	h.syncEventAndRegistrations()

	require.Equal(t, newErrBufferCapacityExceeded(), r1Stream.WaitDone(t, 30*time.Second))
	require.Equal(t, 0, p.Len(), "registration was not removed")
	require.Equal(t, int64(1), m.RangeFeedBudgetExhausted.Count())
}

// TestProcessorBackpressure tests that a processor with EventChanTimeout set to
// 0 will backpressure senders when a consumer isn't keeping up.
func TestScheduledProcessorBackpressure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	span := roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")}

	p, h, stopper := newTestProcessor(t, withSpan(span), withBudget(newTestBudget(math.MaxInt64)),
		withChanCap(1), withEventTimeout(0), withProcType(schedulerProcessor))
	defer stopper.Stop(ctx)
	defer p.Stop()

	// Add a registration.
	stream := newTestStream()
	ok, _ := p.Register(span, hlc.MinTimestamp, nil, false,
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			return NewSingleBufferedStream(ctx, stream.unbuffered(), 3000, false, done)
		},
		nil)
	require.True(t, ok)

	// Wait for the initial checkpoint.
	h.syncEventAndRegistrations()
	require.Len(t, stream.Events(), 1)

	// Block the registration consumer, and spawn a goroutine to post events to
	// the stream, which should block. The rangefeed pipeline buffers a few
	// additional events in intermediate goroutines between channels, so post 10
	// events to be sure.
	unblock := stream.BlockSend()
	defer unblock()

	const numEvents = 10
	doneC := make(chan struct{})
	go func() {
		for i := 0; i < numEvents; i++ {
			assert.True(t, p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: int64(i + 1)}))
		}
		close(doneC)
	}()

	// The sender should be blocked for at least 3 seconds.
	select {
	case <-doneC:
		t.Fatal("send unexpectely succeeded")
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
	}

	// Unblock the sender, and wait for it to complete.
	unblock()
	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatal("sender did not complete")
	}

	// Wait for the final checkpoint event.
	h.syncEventAndRegistrations()
	events := stream.Events()
	require.Equal(t, &kvpb.RangeFeedEvent{
		Checkpoint: &kvpb.RangeFeedCheckpoint{
			Span:       span.AsRawSpanWithNoLocals(),
			ResolvedTS: hlc.Timestamp{WallTime: numEvents},
		},
	}, events[len(events)-1])
}

// TestScheduledProcessorSlowConsumer verifies that if one consumer notifies
// overflow, second consumer would continue processing all events uninterrupted.
func TestScheduledProcessorSlowConsumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p, h, stopper := newTestProcessor(t, withProcType(schedulerProcessor))
	ctx := context.Background()
	defer stopper.Stop(ctx)

	// Add a registration.
	r1Stream := newBufferedConsumer(context.Background(), unlimitedCapacity, 0)
	_, _ = p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			r1Stream.setDone(done)
			return r1Stream
		},
		func() {},
	)
	r2Stream := newBufferedConsumer(context.Background(), unlimitedCapacity, 0)
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		func() Stream {
			t.Fatal("scheduled processor must use buffered streams")
			return nil
		},
		func(done func()) BufferedStream {
			r2Stream.setDone(done)
			return r2Stream
		},
		func() {},
	)
	h.syncEventAndRegistrations()
	require.Equal(t, 2, p.Len())

	// Need one more message to fill the channel because the first one will be
	// sent to the stream and block the registration outputLoop goroutine.
	toFill := testProcessorEventCCap + 1
	for i := 0; i < toFill; i++ {
		ts := hlc.Timestamp{WallTime: int64(i + 2)}
		p.ConsumeLogicalOps(ctx, writeValueOpWithKV(roachpb.Key("k"), ts, []byte("val")))
		// Process events one by one to avoid bulk transfers inside processor.
		h.syncEventAndRegistrations()
		if i == toFill/2 {
			r1Stream.NotifyDrain()
		}
	}

	// Wait for unregistrations to be processed.
	h.syncEventAndRegistrations()
	// At least one event should have been dropped due to overflow. We expect
	// exactly one event to be dropped, but it is possible that multiple events
	// were dropped due to rapid event consumption before the r1's outputLoop
	// began consuming from its event buffer.
	require.LessOrEqual(t, r1Stream.Consumed(), toFill+1)
	testutils.SucceedsSoon(t, func() error {
		if act, exp := p.Len(), 1; exp != act {
			return fmt.Errorf("processor had %d regs, wanted %d", act, exp)
		}
		return nil
	})
}
