// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

// makeFrontierAt builds a span.Frontier with every passed span
// forwarded to ts. Helper for the tests below.
func makeFrontierAt(t *testing.T, ts hlc.Timestamp, spans ...roachpb.Span) span.Frontier {
	t.Helper()
	f, err := span.MakeFrontier(spans...)
	require.NoError(t, err)
	for _, sp := range spans {
		_, err := f.Forward(sp, ts)
		require.NoError(t, err)
	}
	return f
}

// TestBuildRangefeedFrontierFirstRun verifies that with no resume
// entries, every assigned span sits at zero in the returned frontier
// — which means the rangefeed library's init-scan gate scans every
// span at the rangefeed's startTS (the first-run capture path).
func TestBuildRangefeedFrontierFirstRun(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startHLC := hlc.Timestamp{WallTime: int64(100)}
	spans := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
	}

	f, err := buildRangefeedFrontier(spans, startHLC, ResumeState{})
	require.NoError(t, err)
	defer f.Release()

	for _, sp := range spans {
		ts := frontierAt(t, f, sp)
		require.Equal(t, hlc.Timestamp{}, ts,
			"first-run span %s expected zero, got %s", sp, ts)
	}
}

// TestBuildRangefeedFrontierResumeForwardsKnownSpans verifies that
// resume entries forward their spans to the persisted ts, leaving
// non-resumed spans at zero. After a widening replan, the producer's
// resume state describes only old spans; new spans must remain at
// zero so init-scan picks them up.
func TestBuildRangefeedFrontierResumeForwardsKnownSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startHLC := hlc.Timestamp{WallTime: int64(100)}
	old := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	added := roachpb.Span{Key: roachpb.Key("x"), EndKey: roachpb.Key("y")}
	resumeTS := hlc.Timestamp{WallTime: int64(150)}

	f, err := buildRangefeedFrontier([]roachpb.Span{old, added}, startHLC, ResumeState{
		SpanResumes: []SpanResume{{Span: old, Resolved: resumeTS}},
	})
	require.NoError(t, err)
	defer f.Release()

	require.Equal(t, resumeTS, frontierAt(t, f, old),
		"old span should be forwarded to resume ts")
	require.Equal(t, hlc.Timestamp{}, frontierAt(t, f, added),
		"newly-introduced span should remain at zero so init-scan picks it up")
}

// TestBuildRangefeedFrontierResumeBelowStartIgnored verifies that a
// resume entry with ts <= the existing frontier entry's ts is a
// no-op. (Useful invariant: if a newly-introduced span is carried in
// SpanResumes at zero, it stays at zero rather than being explicitly
// "forwarded" backward.)
func TestBuildRangefeedFrontierResumeBelowStartIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sp := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}

	f, err := buildRangefeedFrontier([]roachpb.Span{sp}, hlc.Timestamp{WallTime: 100},
		ResumeState{SpanResumes: []SpanResume{{Span: sp, Resolved: hlc.Timestamp{}}}})
	require.NoError(t, err)
	defer f.Release()

	require.Equal(t, hlc.Timestamp{}, frontierAt(t, f, sp))
}

// TestResumeStateForPartitionFirstRun verifies that with a
// first-run snapshot (Frontier built at startHLC for every assigned
// span), every span gets a SpanResume at startHLC — meaning the
// rangefeed library's init-scan gate skips them and we don't
// re-export the entire backed-up keyspace.
func TestResumeStateForPartitionFirstRun(t *testing.T) {
	defer leaktest.AfterTest(t)()

	startHLC := hlc.Timestamp{WallTime: int64(100)}
	a := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	b := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}

	state := State{Frontier: makeFrontierAt(t, startHLC, a, b)}

	got := ResumeStateForPartition(state, []roachpb.Span{a, b})
	require.Equal(t, []SpanResume{
		{Span: a, Resolved: startHLC},
		{Span: b, Resolved: startHLC},
	}, got.SpanResumes)
	require.Empty(t, got.StartingFlushOrders)
}

// TestResumeStateForPartitionTakesMinAcrossOverlap verifies that
// when persisted spans don't line up exactly with the partition's
// spans, each new span resumes at the min ts across overlapping
// persisted entries — the conservative replay-position so we don't
// miss events held back by the laggiest contributor.
func TestResumeStateForPartitionTakesMinAcrossOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Persisted frontier has two narrow spans within "a"-"d", at
	// different timestamps. The new partition has a single wider
	// span "a"-"d".
	left := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	right := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}
	f, err := span.MakeFrontier(left, right)
	require.NoError(t, err)
	_, err = f.Forward(left, hlc.Timestamp{WallTime: int64(200)})
	require.NoError(t, err)
	_, err = f.Forward(right, hlc.Timestamp{WallTime: int64(150)}) // laggier
	require.NoError(t, err)

	wider := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}
	got := ResumeStateForPartition(State{Frontier: f}, []roachpb.Span{wider})
	require.Len(t, got.SpanResumes, 1)
	require.Equal(t, wider, got.SpanResumes[0].Span)
	require.Equal(t, hlc.Timestamp{WallTime: int64(150)}, got.SpanResumes[0].Resolved,
		"should resume at min across overlapping persisted entries")
}

// TestResumeStateForPartitionEmptyFrontier verifies that a nil/empty
// State.Frontier (no prior persisted state) yields empty SpanResumes
// — buildRangefeedFrontier will then leave every span at zero so
// init-scan covers everything.
func TestResumeStateForPartitionEmptyFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	got := ResumeStateForPartition(State{}, []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	})
	require.Empty(t, got.SpanResumes)
	require.Empty(t, got.StartingFlushOrders)
}

// TestResumeStateForPartitionSkipsNonOverlapping verifies that
// persisted entries which don't overlap any of the partition's
// assigned spans don't show up in the returned ResumeState.
func TestResumeStateForPartitionSkipsNonOverlapping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	persisted := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}
	mine := roachpb.Span{Key: roachpb.Key("x"), EndKey: roachpb.Key("y")}
	f, err := span.MakeFrontier(persisted)
	require.NoError(t, err)
	_, err = f.Forward(persisted, hlc.Timestamp{WallTime: int64(200)})
	require.NoError(t, err)

	got := ResumeStateForPartition(State{Frontier: f}, []roachpb.Span{mine})
	require.Empty(t, got.SpanResumes,
		"non-overlapping persisted entries should not produce a resume")
}

// TestResumeStateForPartitionFlushOrders verifies the per-tick
// starting flushorder is computed as max(prior)+1, so the resumed
// producer's first file for any open tick lands above every prior
// incarnation's contribution and per-key ordering survives the
// restart.
func TestResumeStateForPartitionFlushOrders(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tickA := hlc.Timestamp{WallTime: int64(110)}
	tickB := hlc.Timestamp{WallTime: int64(120)}
	state := State{
		OpenTicks: map[hlc.Timestamp][]revlogpb.File{
			tickA: {{FlushOrder: 0}, {FlushOrder: 2}, {FlushOrder: 5}},
			tickB: {{FlushOrder: 1}},
		},
	}

	got := ResumeStateForPartition(state, []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	})
	require.Equal(t, int32(6), got.StartingFlushOrders[tickA])
	require.Equal(t, int32(2), got.StartingFlushOrders[tickB])
}

// TestResumeSpecRoundTrip verifies that resumeToSpec /
// resumeFromSpec are exact inverses — every per-span resume entry
// and every per-tick StartingFlushOrder survives the round trip
// across the DistSQL spec wire format.
func TestResumeSpecRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tickA := hlc.Timestamp{WallTime: 110}
	tickB := hlc.Timestamp{WallTime: 120}
	in := ResumeState{
		SpanResumes: []SpanResume{
			{
				Span:     roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
				Resolved: hlc.Timestamp{WallTime: 100},
			},
			{
				Span:     roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				Resolved: hlc.Timestamp{WallTime: 105},
			},
		},
		StartingFlushOrders: map[hlc.Timestamp]int32{
			tickA: 3,
			tickB: 7,
		},
	}

	var spec execinfrapb.RevlogSpec
	resumeToSpec(&spec, in)
	out := resumeFromSpec(spec)

	require.Equal(t, in.SpanResumes, out.SpanResumes)
	require.Equal(t, in.StartingFlushOrders, out.StartingFlushOrders)
}

// TestResumeSpecRoundTripEmpty verifies the empty-state case: a
// ResumeState with no SpanResumes and no StartingFlushOrders writes
// nothing onto the spec and reads back equivalent.
func TestResumeSpecRoundTripEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var spec execinfrapb.RevlogSpec
	resumeToSpec(&spec, ResumeState{})
	require.Empty(t, spec.ResumeStarts)
	require.Empty(t, spec.TickStartingFlushOrders)

	out := resumeFromSpec(spec)
	require.Empty(t, out.SpanResumes)
	require.Empty(t, out.StartingFlushOrders)
}

// frontierAt looks up the recorded ts for sp in f via Entries().
// Returns hlc.Timestamp{} if no overlap is found.
func frontierAt(t *testing.T, f span.Frontier, sp roachpb.Span) hlc.Timestamp {
	t.Helper()
	for esp, ts := range f.Entries() {
		if esp.Equal(sp) {
			return ts
		}
	}
	return hlc.Timestamp{}
}
