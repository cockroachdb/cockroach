// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud/inmemstorage"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// fakeDescSource is a test-only descRangefeedSource. Subscribe
// captures the events + errs channels passed by runDescFeed and
// records the requested startTS; the test then pushes synthetic
// values, checkpoints, and errors directly into those channels via
// PushValue / PushCheckpoint / PushError.
type fakeDescSource struct {
	subscribed chan struct{} // closed when Subscribe is called

	mu struct {
		syncutil.Mutex
		startTS hlc.Timestamp
		events  chan<- rangefeedEvent
		errs    chan<- error
		stopped bool
	}
}

func newFakeDescSource() *fakeDescSource {
	return &fakeDescSource{subscribed: make(chan struct{})}
}

func (f *fakeDescSource) Subscribe(
	ctx context.Context, startTS hlc.Timestamp, events chan<- rangefeedEvent, errs chan<- error,
) (func(), error) {
	f.mu.Lock()
	f.mu.startTS = startTS
	f.mu.events = events
	f.mu.errs = errs
	f.mu.Unlock()
	close(f.subscribed)
	return func() {
		f.mu.Lock()
		f.mu.stopped = true
		f.mu.Unlock()
	}, nil
}

// pushValue blocks until the descfeed loop drains the event or ctx
// is cancelled.
func (f *fakeDescSource) pushValue(ctx context.Context, v *kvpb.RangeFeedValue) error {
	<-f.subscribed
	f.mu.Lock()
	ch := f.mu.events
	f.mu.Unlock()
	select {
	case ch <- rangefeedEvent{value: v}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pushCheckpoint blocks until the descfeed loop drains the event or
// ctx is cancelled.
func (f *fakeDescSource) pushCheckpoint(ctx context.Context, ts hlc.Timestamp) error {
	<-f.subscribed
	f.mu.Lock()
	ch := f.mu.events
	f.mu.Unlock()
	select {
	case ch <- rangefeedEvent{checkpoint: &kvpb.RangeFeedCheckpoint{ResolvedTS: ts}}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pushError pushes an internal error into the descfeed loop, which
// should cause runDescFeed to return.
func (f *fakeDescSource) pushError(ctx context.Context, err error) error {
	<-f.subscribed
	f.mu.Lock()
	ch := f.mu.errs
	f.mu.Unlock()
	select {
	case ch <- err:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startTSCalled returns the startTS the descfeed asked Subscribe for.
func (f *fakeDescSource) startTSCalled() hlc.Timestamp {
	<-f.subscribed
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.startTS
}

func (f *fakeDescSource) wasStopped() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mu.stopped
}

// runDescFeedTestRig wires up a fake source + manager + scope and
// starts runDescFeed on a goroutine. Returns the loop's outbound
// signals + a way to stop and join.
type runDescFeedTestRig struct {
	t       *testing.T
	source  *fakeDescSource
	manager *TickManager
	sigs    *descFeedSignals
	done    chan error
	cancel  context.CancelFunc
}

// startRunDescFeed builds the rig and starts runDescFeed on a
// goroutine. The caller must `defer rig.Stop()` immediately so that
// the goroutine join runs in LIFO defer order *before* leaktest.
// (t.Cleanup runs after deferred funcs, which is too late for leaktest
// to see the goroutine as terminated.)
func startRunDescFeed(
	t *testing.T, startHLC hlc.Timestamp, scope Scope, initialSpans []roachpb.Span,
) *runDescFeedTestRig {
	t.Helper()
	es := inmemstorage.New()
	t.Cleanup(func() { _ = es.Close() })

	manager, err := NewTickManager(es, initialSpans, startHLC, 10*time.Second)
	require.NoError(t, err)
	manager.DisableDescFrontier()

	source := newFakeDescSource()
	sigs := newDescFeedSignals()
	done := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Close after sending so Stop can wait on a closed channel
		// even if the test already drained the single value.
		defer close(done)
		done <- runDescFeed(
			ctx, source, keys.SystemSQLCodec, scope, manager, es,
			startHLC, initialSpans, sigs,
		)
	}()

	return &runDescFeedTestRig{
		t:       t,
		source:  source,
		manager: manager,
		sigs:    sigs,
		done:    done,
		cancel:  cancel,
	}
}

// Stop cancels the descfeed and waits for the goroutine to exit.
// Callers should `defer rig.Stop()` immediately after construction.
func (r *runDescFeedTestRig) Stop() {
	r.cancel()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		r.t.Errorf("runDescFeed goroutine did not exit within 5s of cancel")
	}
}

// TestRunDescFeedSubscribesAtStartHLC verifies the descfeed asks
// the source for a subscription at the configured startHLC.
func TestRunDescFeedSubscribesAtStartHLC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	scope := &fakeScope{spansAt: []scopeSpansAt{
		{at: hlc.Timestamp{}, spans: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}},
	}}

	rig := startRunDescFeed(t, startHLC, scope,
		[]roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}})
	defer rig.Stop()

	require.Equal(t, startHLC, rig.source.startTSCalled())
}

// TestRunDescFeedDispatchesValuesAndCheckpoints verifies the loop
// buffers values and processes them on each checkpoint, observable
// via the replan signal once a widening lands.
func TestRunDescFeedDispatchesValuesAndCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	added := roachpb.Span{Key: roachpb.Key("x"), EndKey: roachpb.Key("y")}
	post := append(append([]roachpb.Span(nil), prior...), added)
	wideningTs := hlc.Timestamp{WallTime: int64(150 * time.Second)}

	scope := &fakeScope{spansAt: []scopeSpansAt{
		{at: hlc.Timestamp{}, spans: prior},
		{at: wideningTs, spans: post},
	}}
	rig := startRunDescFeed(t, startHLC, scope, prior)
	defer rig.Stop()

	// Push the widening descriptor and then a checkpoint past it.
	require.NoError(t, rig.source.pushValue(ctx,
		tombstoneValue(t, descpb.ID(42), wideningTs)))
	require.NoError(t, rig.source.pushCheckpoint(ctx,
		hlc.Timestamp{WallTime: int64(160 * time.Second)}))

	// The replan signal should land shortly. Use SucceedsSoon to
	// absorb the goroutine handoff between push and processBatch.
	var sig replanSignal
	require.NoError(t, testutils.SucceedsSoonError(func() error {
		select {
		case sig = <-rig.sigs.replan:
			return nil
		default:
			return errors.New("no replan signal yet")
		}
	}))

	require.Equal(t, post, sig.Spans)
	require.Equal(t, []roachpb.Span{added}, sig.NewSpans)
	require.Equal(t, startHLC, sig.StartTS)
}

// TestRunDescFeedSurfacesSubscriberErrors verifies that an error
// pushed onto the source's err channel causes runDescFeed to return
// it (wrapped) and stop the subscription.
func TestRunDescFeedSurfacesSubscriberErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	scope := &fakeScope{spansAt: []scopeSpansAt{
		{at: hlc.Timestamp{}, spans: prior},
	}}
	rig := startRunDescFeed(t, startHLC, scope, prior)
	defer rig.Stop()

	injected := errors.New("boom")
	require.NoError(t, rig.source.pushError(ctx, injected))

	select {
	case err := <-rig.done:
		require.ErrorContains(t, err, "boom")
		require.ErrorContains(t, err, "descriptor rangefeed internal error")
	case <-time.After(5 * time.Second):
		t.Fatal("runDescFeed did not return after pushed error")
	}
	// Subscription should be torn down (deferred stop ran).
	require.True(t, rig.source.wasStopped())
}

// TestRunDescFeedReturnsOnContextCancel verifies clean shutdown via
// parent ctx cancellation, including stop() of the subscription.
func TestRunDescFeedReturnsOnContextCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	scope := &fakeScope{spansAt: []scopeSpansAt{
		{at: hlc.Timestamp{}, spans: prior},
	}}
	rig := startRunDescFeed(t, startHLC, scope, prior)
	defer rig.Stop()

	// Wait for subscribe to land before cancelling, so we know stop()
	// is wired up.
	_ = rig.source.startTSCalled()
	rig.cancel()

	select {
	case err := <-rig.done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("runDescFeed did not return after ctx cancel")
	}
	require.True(t, rig.source.wasStopped())
}

// TestRunDescFeedTerminatesOnEmptyScope verifies that when the
// scope's resolved-spans set goes empty and Terminated returns true,
// runDescFeed exits with ErrScopeTerminated rather than blocking
// forever.
func TestRunDescFeedTerminatesOnEmptyScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	dropTs := hlc.Timestamp{WallTime: int64(150 * time.Second)}

	scope := &fakeScope{
		spansAt: []scopeSpansAt{
			{at: hlc.Timestamp{}, spans: prior},
			{at: dropTs, spans: nil}, // scope dissolved
		},
		terminated: true,
	}
	rig := startRunDescFeed(t, startHLC, scope, prior)
	defer rig.Stop()

	// Push the dropping descriptor then a checkpoint past it.
	require.NoError(t, rig.source.pushValue(ctx,
		tombstoneValue(t, descpb.ID(42), dropTs)))
	require.NoError(t, rig.source.pushCheckpoint(ctx,
		hlc.Timestamp{WallTime: int64(160 * time.Second)}))

	select {
	case err := <-rig.done:
		require.ErrorIs(t, err, ErrScopeTerminated)
	case <-time.After(5 * time.Second):
		t.Fatal("runDescFeed did not return after scope termination")
	}
}
