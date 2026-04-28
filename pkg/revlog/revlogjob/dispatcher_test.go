// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud/inmemstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// counterSink counts Flush calls plus the cumulative file count. It
// stands in for the TickSink in dispatcher tests so we don't have to
// stand up the whole TickManager.
type counterSink struct {
	mu    syncutil.Mutex
	calls int
	files int
}

func (s *counterSink) Flush(_ context.Context, msg *revlogpb.Flush) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.files += len(msg.Files)
	return nil
}

func (s *counterSink) snapshot() (calls, files int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.files
}

// seqIDs is an atomic-counter FileIDSource starting at 1.
type seqIDs struct{ n atomic.Int64 }

func (s *seqIDs) Next() int64 { return s.n.Add(1) }

func ts(seconds int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: seconds * int64(time.Second)}
}

// allKeyspace is the conventional "everything" span for tests.
var allKeyspace = roachpb.Span{Key: roachpb.Key("\x00"), EndKey: roachpb.KeyMax}

// newDispatcherTestProducer builds a Producer wired to a counterSink
// over inmem storage — the lightest possible setup for dispatcher
// tests where we only care that events make it from the channel into
// the Producer's accounting.
func newDispatcherTestProducer(t *testing.T) (*Producer, *counterSink) {
	t.Helper()
	es := inmemstorage.New()
	t.Cleanup(func() { _ = es.Close() })
	sink := &counterSink{}
	p, err := NewProducer(es, []roachpb.Span{allKeyspace}, ts(100),
		10*time.Second, &seqIDs{}, sink, ResumeState{})
	require.NoError(t, err)
	return p, sink
}

// TestRunDispatcherDeliversValuesAndCheckpoints verifies the
// dispatcher hands rangefeed values to Producer.OnValue and
// checkpoints to Producer.OnCheckpoint. A checkpoint past the first
// tick boundary should fire one Flush on the sink (one tick's worth
// of files).
func TestRunDispatcherDeliversValuesAndCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prod, sink := newDispatcherTestProducer(t)

	eventsCh := make(chan rangefeedEvent, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- runDispatcher(ctx, prod, eventsCh) }()

	// Two values then a checkpoint past the first tick boundary.
	eventsCh <- rangefeedEvent{value: &kvpb.RangeFeedValue{
		Key: roachpb.Key("a"),
		Value: roachpb.Value{
			RawBytes:  []byte("v1"),
			Timestamp: ts(105),
		},
	}}
	eventsCh <- rangefeedEvent{value: &kvpb.RangeFeedValue{
		Key: roachpb.Key("b"),
		Value: roachpb.Value{
			RawBytes:  []byte("v2"),
			Timestamp: ts(107),
		},
	}}
	eventsCh <- rangefeedEvent{checkpoint: &kvpb.RangeFeedCheckpoint{
		Span:       allKeyspace,
		ResolvedTS: ts(115),
	}}

	require.Eventually(t, func() bool {
		calls, files := sink.snapshot()
		return calls >= 1 && files >= 1
	}, 5*time.Second, 5*time.Millisecond,
		"sink should observe at least one Flush after checkpoint past tick boundary")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// TestRunDispatcherReturnsOnEventsClose verifies the dispatcher
// exits cleanly (no error) when its events channel is closed —
// the production wiring relies on this for shutdown.
func TestRunDispatcherReturnsOnEventsClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prod, _ := newDispatcherTestProducer(t)
	eventsCh := make(chan rangefeedEvent)
	close(eventsCh)
	require.NoError(t, runDispatcher(context.Background(), prod, eventsCh))
}

// TestSetAfterFrontierAdvanceFires verifies the hook installed by
// SetAfterFrontierAdvance is called with the new frontier each time
// a Flush advances the manager's per-span frontier — used by the
// PTS manager (pts.go) to advance the protected timestamp record
// alongside the writer's progress.
func TestSetAfterFrontierAdvanceFires(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	es := inmemstorage.New()
	t.Cleanup(func() { _ = es.Close() })
	mgr, err := NewTickManager(es, []roachpb.Span{allKeyspace}, ts(100),
		10*time.Second)
	require.NoError(t, err)
	mgr.DisableDescFrontier()

	var (
		mu   syncutil.Mutex
		seen []hlc.Timestamp
	)
	mgr.SetAfterFrontierAdvance(func(_ context.Context, f hlc.Timestamp) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, f)
	})

	prod, err := NewProducer(es, []roachpb.Span{allKeyspace}, ts(100),
		10*time.Second, &seqIDs{}, mgr, ResumeState{})
	require.NoError(t, err)

	prod.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v"), nil)
	require.NoError(t, prod.OnCheckpoint(ctx, allKeyspace, ts(115)))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, seen, "hook should have fired at least once after frontier advanced")
	require.Equal(t, ts(115), seen[len(seen)-1],
		"latest hook invocation should observe the advanced frontier")
}
