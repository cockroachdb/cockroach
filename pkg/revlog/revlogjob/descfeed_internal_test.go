// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// fakeScope is a stub Scope that returns a span set keyed off
// the descriptor's ID at the time the call is made. The test
// configures the per-ts span set in spansAt; Spans walks the map
// to find the entry whose effective ts is <= asOf and returns it.
type fakeScope struct {
	matches    func(*descpb.Descriptor) bool
	spansAt    []scopeSpansAt
	terminated bool
}

type scopeSpansAt struct {
	at    hlc.Timestamp
	spans []roachpb.Span
}

func (s *fakeScope) Matches(d *descpb.Descriptor) bool {
	if s.matches == nil {
		return true
	}
	return s.matches(d)
}

func (s *fakeScope) Spans(_ context.Context, asOf hlc.Timestamp) ([]roachpb.Span, error) {
	var picked []roachpb.Span
	for _, e := range s.spansAt {
		if e.at.LessEq(asOf) {
			picked = e.spans
		}
	}
	return picked, nil
}

func (s *fakeScope) Terminated(context.Context, hlc.Timestamp) (bool, error) {
	return s.terminated, nil
}

func (s *fakeScope) String() string { return "fake" }

// descKey builds a descriptor-row key for the given descriptor ID
// using the system codec, as the descfeed's per-value handler will
// decode it back via DecodeDescMetadataID.
func descKey(t *testing.T, id descpb.ID) roachpb.Key {
	t.Helper()
	return keys.SystemSQLCodec.DescMetadataKey(uint32(id))
}

// tombstoneValue returns a RangeFeedValue with a zero-byte payload
// (descriptor deleted) at ts. The descfeed treats tombstones as
// always-in-scope (no Matches call), which is convenient for tests
// that only care about scope-transition bookkeeping rather than
// descriptor-content handling.
func tombstoneValue(t *testing.T, id descpb.ID, ts hlc.Timestamp) *kvpb.RangeFeedValue {
	t.Helper()
	return &kvpb.RangeFeedValue{
		Key: descKey(t, id),
		Value: roachpb.Value{
			RawBytes:  nil,
			Timestamp: ts,
		},
	}
}

// newDescFeedTestState builds a descFeedState wired to a real
// TickManager and a nodelocal storage. lastSpans is the
// pre-existing coverage as of the test's startHLC.
func newDescFeedTestState(
	t *testing.T, startHLC hlc.Timestamp, scope Scope, lastSpans []roachpb.Span,
) (*descFeedState, *TickManager, cloud.ExternalStorage, *descFeedSignals) {
	t.Helper()
	es := nodelocal.TestingMakeNodelocalStorage(
		t.TempDir(), cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
	t.Cleanup(func() { _ = es.Close() })

	manager, err := NewTickManager(es, lastSpans, startHLC, 10*time.Second)
	require.NoError(t, err)
	manager.DisableDescFrontier()

	sigs := newDescFeedSignals()
	state := &descFeedState{
		scope:        scope,
		manager:      manager,
		es:           es,
		codec:        keys.SystemSQLCodec,
		sigs:         sigs,
		lastSpans:    cloneSpans(lastSpans),
		lastSpansSet: true,
	}
	return state, manager, es, sigs
}

// TestDescFeedWideningSignalsReplan verifies that when a buffered
// descriptor change widens scope, the next checkpoint:
//
//   - registers the newly-introduced spans with the manager's
//     frontier at zero (so the new flow's per-span bookkeeping
//     accounts for them),
//   - sends exactly one replan signal carrying the new full span
//     set, the new-span subset, and the chosen restart start ts.
func TestDescFeedWideningSignalsReplan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	added := roachpb.Span{Key: roachpb.Key("x"), EndKey: roachpb.Key("y")}
	post := append(append([]roachpb.Span(nil), prior...), added)
	wideningTs := hlc.Timestamp{WallTime: int64(150 * time.Second)}

	scope := &fakeScope{
		spansAt: []scopeSpansAt{
			{at: hlc.Timestamp{}, spans: prior},
			{at: wideningTs, spans: post},
		},
	}
	state, manager, _, sigs := newDescFeedTestState(t, startHLC, scope, prior)

	// Buffer one widening tombstone, then drive a checkpoint past
	// it. The checkpoint should drain the value, write the
	// coverage entry, register the new span, and send a replan.
	state.bufferValue(tombstoneValue(t, descpb.ID(42), wideningTs))
	checkpointTs := hlc.Timestamp{WallTime: int64(160 * time.Second)}
	require.NoError(t, state.processBatch(ctx, checkpointTs))

	// Replan signal should be present, with the new span set.
	var sig replanSignal
	select {
	case sig = <-sigs.replan:
	default:
		t.Fatal("expected replan signal, got none")
	}
	require.Equal(t, post, sig.Spans)
	require.Equal(t, []roachpb.Span{added}, sig.NewSpans)
	// StartTS = min(currentDataFrontier=startHLC, wideningTs) = startHLC,
	// since startHLC < wideningTs and the manager's frontier sits at
	// startHLC (no Flush has advanced it).
	require.Equal(t, startHLC, sig.StartTS)

	// The manager's frontier now tracks the new span at zero (since
	// AddSpansAt was called with hlc.Timestamp{}); the global min is
	// therefore zero.
	require.Equal(t, hlc.Timestamp{}, manager.DataFrontier())
}

// TestDescFeedNoWideningSilent verifies that a checkpoint draining
// only non-widening events sends NO replan signal — coverage stays
// stable, only the schema-delta side-effects fire.
func TestDescFeedNoWideningSilent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	scope := &fakeScope{
		spansAt: []scopeSpansAt{
			{at: hlc.Timestamp{}, spans: prior},
		},
	}
	state, _, _, sigs := newDescFeedTestState(t, startHLC, scope, prior)

	state.bufferValue(tombstoneValue(t, descpb.ID(7), hlc.Timestamp{WallTime: int64(120 * time.Second)}))
	require.NoError(t, state.processBatch(ctx, hlc.Timestamp{WallTime: int64(130 * time.Second)}))

	select {
	case sig := <-sigs.replan:
		t.Fatalf("did not expect replan signal, got %+v", sig)
	default:
	}
}

// TestDescFeedBuffersUntilCheckpoint verifies that a value with ts
// strictly above the checkpoint is left in the pending buffer
// rather than processed.
func TestDescFeedBuffersUntilCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	added := roachpb.Span{Key: roachpb.Key("x"), EndKey: roachpb.Key("y")}
	post := append(append([]roachpb.Span(nil), prior...), added)
	wideningTs := hlc.Timestamp{WallTime: int64(200 * time.Second)}

	scope := &fakeScope{
		spansAt: []scopeSpansAt{
			{at: hlc.Timestamp{}, spans: prior},
			{at: wideningTs, spans: post},
		},
	}
	state, _, _, sigs := newDescFeedTestState(t, startHLC, scope, prior)

	state.bufferValue(tombstoneValue(t, descpb.ID(42), wideningTs))

	// Checkpoint at ts < wideningTs leaves the value in the buffer.
	require.NoError(t, state.processBatch(ctx, hlc.Timestamp{WallTime: int64(150 * time.Second)}))
	require.Len(t, state.pending, 1)
	select {
	case sig := <-sigs.replan:
		t.Fatalf("did not expect replan signal yet, got %+v", sig)
	default:
	}

	// Checkpoint at ts >= wideningTs drains it and signals the
	// replan.
	require.NoError(t, state.processBatch(ctx, hlc.Timestamp{WallTime: int64(210 * time.Second)}))
	require.Empty(t, state.pending)
	select {
	case sig := <-sigs.replan:
		require.Equal(t, post, sig.Spans)
		require.Equal(t, []roachpb.Span{added}, sig.NewSpans)
	default:
		t.Fatal("expected replan signal after checkpoint past widening")
	}
}

// TestDescFeedOnePerDistinctTransition verifies that two widening
// values in one batch produce one replan signal whose StartTS is
// the earliest widening's ts and whose NewSpans is the union.
func TestDescFeedOnePerDistinctTransition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	add1 := roachpb.Span{Key: roachpb.Key("x"), EndKey: roachpb.Key("y")}
	mid := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, add1}
	add2 := roachpb.Span{Key: roachpb.Key("p"), EndKey: roachpb.Key("q")}
	post := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		add2,
		add1,
	}
	ts1 := hlc.Timestamp{WallTime: int64(150 * time.Second)}
	ts2 := hlc.Timestamp{WallTime: int64(160 * time.Second)}

	scope := &fakeScope{
		spansAt: []scopeSpansAt{
			{at: hlc.Timestamp{}, spans: prior},
			{at: ts1, spans: mid},
			{at: ts2, spans: post},
		},
	}
	// Force the manager's data frontier high enough that StartTS
	// is bounded by the earliest widening (ts1), not the data
	// frontier — so we can verify the min logic.
	state, _, _, sigs := newDescFeedTestState(t, startHLC, scope, prior)

	state.bufferValue(tombstoneValue(t, descpb.ID(2), ts2))
	state.bufferValue(tombstoneValue(t, descpb.ID(1), ts1)) // out-of-order arrival
	require.NoError(t, state.processBatch(ctx, hlc.Timestamp{WallTime: int64(200 * time.Second)}))

	// Exactly one replan signal, with StartTS = startHLC (since
	// the manager's data frontier hasn't advanced past it, and
	// startHLC < ts1).
	var sig replanSignal
	select {
	case sig = <-sigs.replan:
	default:
		t.Fatal("expected one replan signal")
	}
	select {
	case extra := <-sigs.replan:
		t.Fatalf("expected only one replan signal, got extra %+v", extra)
	default:
	}
	require.Equal(t, startHLC, sig.StartTS)
	// NewSpans is the union of add1 and add2 (sorted).
	require.ElementsMatch(t, []roachpb.Span{add1, add2}, sig.NewSpans)
}

// TestDescFeedTombstoneWritesDeltaWithoutWidening pins down the
// out-of-scope tombstone policy: a tombstone for a descriptor whose
// post-deletion span set is unchanged still writes the schema delta
// (so a reader sees the deletion), but doesn't widen scope and
// doesn't trigger a replan.
//
// The intentional behavior — documented in handleBatchedValue — is
// that tombstones bypass scope.Matches because the prior version's
// row was in scope for us to observe it, and tombstones are tiny.
// This test locks the contract in so a future "filter tombstones
// through Matches" change can't silently lose deletions.
func TestDescFeedTombstoneWritesDeltaWithoutWidening(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	startHLC := hlc.Timestamp{WallTime: int64(100 * time.Second)}
	prior := []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}
	scope := &fakeScope{
		// matches returns false for any non-tombstone — a tombstone's
		// desc==nil short-circuits the Matches call entirely.
		matches: func(*descpb.Descriptor) bool { return false },
		spansAt: []scopeSpansAt{
			{at: hlc.Timestamp{}, spans: prior},
		},
	}
	state, _, es, sigs := newDescFeedTestState(t, startHLC, scope, prior)

	tombstoneTs := hlc.Timestamp{WallTime: int64(150 * time.Second)}
	const tombstoneID = descpb.ID(42)
	state.bufferValue(tombstoneValue(t, tombstoneID, tombstoneTs))
	require.NoError(t, state.processBatch(ctx,
		hlc.Timestamp{WallTime: int64(160 * time.Second)}))

	// No replan — span set didn't change.
	select {
	case sig := <-sigs.replan:
		t.Fatalf("did not expect replan signal, got %+v", sig)
	default:
	}
	require.Equal(t, prior, state.lastSpans, "lastSpans must be unchanged")

	// Schema delta was written: iterate the schema-changes log and
	// expect exactly one entry for the tombstoned ID at tombstoneTs
	// with a nil descriptor.
	var seen []revlog.SchemaChange
	for ch, err := range revlog.IterSchemaChanges(ctx, es,
		hlc.Timestamp{}, hlc.Timestamp{WallTime: int64(200 * time.Second)}) {
		require.NoError(t, err)
		seen = append(seen, ch)
	}
	require.Len(t, seen, 1)
	require.Equal(t, tombstoneID, seen[0].DescID)
	require.Equal(t, tombstoneTs, seen[0].ChangedAt)
	require.Nil(t, seen[0].Descriptor, "tombstone schema delta must record nil descriptor")
}
