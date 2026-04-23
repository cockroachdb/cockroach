// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func newTestScheduler(t *testing.T, stopper *stop.Stopper) *rangefeed.Scheduler {
	t.Helper()
	s := rangefeed.NewScheduler(rangefeed.SchedulerConfig{
		Workers: 1,
		Metrics: rangefeed.NewSchedulerMetrics(time.Second),
	})
	require.NoError(t, s.Start(context.Background(), stopper))
	return s
}

func newTestProcessor(t *testing.T, stopper *stop.Stopper, span roachpb.RSpan) *Processor {
	t.Helper()
	s := newTestScheduler(t, stopper)
	p := NewProcessor(Config{
		AmbientContext: log.MakeTestingAmbientCtxWithNewTracer(),
		Span:           span,
		Stopper:        stopper,
		Scheduler:      s,
	})
	require.NoError(t, p.Start())
	return p
}

var defaultSpan = roachpb.RSpan{
	Key:    roachpb.RKey("a"),
	EndKey: roachpb.RKey("z"),
}

// testStream implements Stream for testing. It captures events and errors.
type testStream struct {
	mu struct {
		syncutil.Mutex
		events []*kvpb.TxnFeedEvent
		err    *kvpb.Error
	}
}

func (s *testStream) SendBuffered(event *kvpb.TxnFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.events = append(s.mu.events, event)
	return nil
}

func (s *testStream) SendUnbuffered(event *kvpb.TxnFeedEvent) error {
	return s.SendBuffered(event)
}

func (s *testStream) SendError(err *kvpb.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.err = err
}

func (s *testStream) getAndClearEvents() []*kvpb.TxnFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.mu.events
	s.mu.events = nil
	return events
}

func (s *testStream) getError() *kvpb.Error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.err
}

// TestProcessorConsumeTxnFeedOps verifies that committed transaction ops are
// delivered to matching registrations.
func TestProcessorConsumeTxnFeedOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p := newTestProcessor(t, stopper, defaultSpan)
	defer p.Stop()

	stream := &testStream{}
	disc, err := p.Register(ctx, defaultSpan, hlc.Timestamp{}, nil, stream)
	require.NoError(t, err)
	require.NotNil(t, disc)

	txnID := uuid.MakeV4()
	ops := &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:           kvserverpb.TxnFeedOp_COMMITTED,
			TxnID:          txnID,
			AnchorKey:      roachpb.Key("b"),
			WriteTimestamp: hlc.Timestamp{WallTime: 10},
			WriteSpans:     []roachpb.Span{{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
		}},
	}

	p.ConsumeTxnFeedOps(ctx, ops)
	p.syncEventC()

	events := stream.getAndClearEvents()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Committed)
	require.Equal(t, txnID, events[0].Committed.TxnID)
	require.Equal(t, roachpb.Key("b"), events[0].Committed.AnchorKey)
	require.Equal(t, hlc.Timestamp{WallTime: 10}, events[0].Committed.CommitTimestamp)
}

// TestProcessorConsumeTxnFeedOpsFiltering verifies that events are only
// delivered to registrations whose span contains the anchor key.
func TestProcessorConsumeTxnFeedOpsFiltering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p := newTestProcessor(t, stopper, defaultSpan)
	defer p.Stop()

	// Register two streams with non-overlapping spans.
	spanAB := roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")}
	spanMZ := roachpb.RSpan{Key: roachpb.RKey("m"), EndKey: roachpb.RKey("z")}

	stream1 := &testStream{}
	disc1, err := p.Register(ctx, spanAB, hlc.Timestamp{}, nil, stream1)
	require.NoError(t, err)
	require.NotNil(t, disc1)

	stream2 := &testStream{}
	disc2, err := p.Register(ctx, spanMZ, hlc.Timestamp{}, nil, stream2)
	require.NoError(t, err)
	require.NotNil(t, disc2)

	// Send an op with anchor key in the first span.
	ops := &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:           kvserverpb.TxnFeedOp_COMMITTED,
			TxnID:          uuid.MakeV4(),
			AnchorKey:      roachpb.Key("b"),
			WriteTimestamp: hlc.Timestamp{WallTime: 10},
		}},
	}
	p.ConsumeTxnFeedOps(ctx, ops)
	p.syncEventC()

	// stream1 should receive the event; stream2 should not.
	events1 := stream1.getAndClearEvents()
	events2 := stream2.getAndClearEvents()
	require.Len(t, events1, 1)
	require.Len(t, events2, 0)
}

// TestProcessorForwardClosedTS verifies that closed timestamp updates are
// delivered to all registrations as checkpoint events.
func TestProcessorForwardClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p := newTestProcessor(t, stopper, defaultSpan)
	defer p.Stop()

	stream := &testStream{}
	_, err := p.Register(ctx, defaultSpan, hlc.Timestamp{}, nil, stream)
	require.NoError(t, err)

	p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 42})
	p.syncEventC()

	events := stream.getAndClearEvents()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Checkpoint)
	require.Equal(t, hlc.Timestamp{WallTime: 42}, events[0].Checkpoint.ResolvedTS)
}

// TestProcessorStopWithErr verifies that stopping the processor disconnects
// all registrations with the given error.
func TestProcessorStopWithErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p := newTestProcessor(t, stopper, defaultSpan)

	stream := &testStream{}
	disc, err := p.Register(ctx, defaultSpan, hlc.Timestamp{}, nil, stream)
	require.NoError(t, err)

	testErr := kvpb.NewErrorf("test stop error")
	p.StopWithErr(testErr)

	// Wait for the processor to fully stop. StopWithErr enqueues a stop
	// request that is processed asynchronously by the scheduler.
	<-p.stoppedC

	require.True(t, disc.IsDisconnected())
	pErr := stream.getError()
	require.NotNil(t, pErr)
	require.Contains(t, pErr.String(), "test stop error")
}

// TestProcessorDisconnectSpan verifies that DisconnectSpanWithErr only
// disconnects registrations whose spans overlap the given span.
func TestProcessorDisconnectSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p := newTestProcessor(t, stopper, defaultSpan)
	defer p.Stop()

	spanAM := roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")}
	spanMZ := roachpb.RSpan{Key: roachpb.RKey("m"), EndKey: roachpb.RKey("z")}

	stream1 := &testStream{}
	disc1, err := p.Register(ctx, spanAM, hlc.Timestamp{}, nil, stream1)
	require.NoError(t, err)

	stream2 := &testStream{}
	disc2, err := p.Register(ctx, spanMZ, hlc.Timestamp{}, nil, stream2)
	require.NoError(t, err)

	require.Equal(t, 2, p.Len())

	// Disconnect only the first span.
	testErr := kvpb.NewErrorf("disconnected")
	p.DisconnectSpanWithErr(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}, testErr)

	// syncEventC flushes the request queue through the event pipeline.
	p.syncEventC()

	require.True(t, disc1.IsDisconnected())
	require.False(t, disc2.IsDisconnected())
	require.Equal(t, 1, p.Len())
}

// TestProcessorNilOps verifies that nil ops are handled gracefully.
func TestProcessorNilOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p := newTestProcessor(t, stopper, defaultSpan)
	defer p.Stop()

	// Should not panic or enqueue anything.
	p.ConsumeTxnFeedOps(ctx, nil)
	p.ForwardClosedTS(ctx, hlc.Timestamp{})
}
