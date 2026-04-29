// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// seqFileIDs is an atomic-counter FileIDSource starting at 1.
type seqFileIDs struct{ n atomic.Int64 }

func (s *seqFileIDs) Next() int64 { return s.n.Add(1) }

// allSpan is the conventional "everything" span used by the test driver.
var allSpan = roachpb.Span{Key: roachpb.Key("\x00"), EndKey: roachpb.KeyMax}

// ts converts a "logical second" to an hlc.Timestamp in nanoseconds.
// Tick paths only encode whole seconds, so tests place tick
// boundaries on whole-second multiples to get distinct marker paths.
func ts(seconds int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: seconds * int64(time.Second)}
}

func newTestStorage(t *testing.T) cloud.ExternalStorage {
	t.Helper()
	return nodelocal.TestingMakeNodelocalStorage(
		t.TempDir(), cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
}

const testTickWidth = 10 * time.Second

func newTestDriver(
	t *testing.T, startHLC hlc.Timestamp,
) (*revlogjob.Driver, cloud.ExternalStorage) {
	t.Helper()
	es := newTestStorage(t)
	t.Cleanup(func() { es.Close() })
	d, err := revlogjob.NewDriver(es, []roachpb.Span{allSpan}, startHLC, testTickWidth,
		&seqFileIDs{}, revlogjob.ResumeState{})
	require.NoError(t, err)
	return d, es
}

// readBackTicks enumerates closed ticks in (start, end] and returns
// them.
func readBackTicks(
	t *testing.T, ctx context.Context, es cloud.ExternalStorage, start, end hlc.Timestamp,
) []revlog.Tick {
	t.Helper()
	lr := revlog.NewLogReader(es)
	var out []revlog.Tick
	for tk, err := range lr.Ticks(ctx, start, end) {
		require.NoError(t, err)
		out = append(out, tk)
	}
	return out
}

func readBackEvents(
	t *testing.T, ctx context.Context, es cloud.ExternalStorage, tk revlog.Tick,
) []revlog.Event {
	t.Helper()
	lr := revlog.NewLogReader(es)
	var out []revlog.Event
	for ev, err := range lr.GetTickReader(ctx, tk, nil).Events(ctx) {
		require.NoError(t, err)
		out = append(out, ev)
	}
	return out
}

// TestDriverSingleTick runs three KVs into a single tick and
// verifies the driver writes one data file + one manifest, with the
// events readable in (key, ts) order via LogReader.
func TestDriverSingleTick(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es := newTestDriver(t, ts(100))
	d.OnValue(ctx, roachpb.Key("a"), ts(103), []byte("v_a"), nil)
	d.OnValue(ctx, roachpb.Key("b"), ts(105), []byte("v_b"), nil)
	d.OnValue(ctx, roachpb.Key("c"), ts(107), []byte("v_c"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(110)))

	ticks := readBackTicks(t, ctx, es, ts(99), ts(110))
	require.Len(t, ticks, 1)
	require.Equal(t, ts(110), ticks[0].EndTime)
	require.Equal(t, ts(100), ticks[0].Manifest.TickStart)
	require.Len(t, ticks[0].Manifest.Files, 1)

	events := readBackEvents(t, ctx, es, ticks[0])
	require.Len(t, events, 3)
	require.Equal(t, "a", string(events[0].Key))
	require.Equal(t, "b", string(events[1].Key))
	require.Equal(t, "c", string(events[2].Key))
}

// TestDriverMultiTick spreads events across two ticks and verifies a
// single checkpoint past both boundaries closes both, with each
// manifest's TickStart chained off the previous TickEnd.
func TestDriverMultiTick(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es := newTestDriver(t, ts(100))
	d.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v1"), nil)
	d.OnValue(ctx, roachpb.Key("a"), ts(115), []byte("v2"), nil)
	d.OnValue(ctx, roachpb.Key("a"), ts(125), []byte("v3"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(130)))

	ticks := readBackTicks(t, ctx, es, ts(99), ts(130))
	require.Len(t, ticks, 3)
	require.Equal(t, ts(110), ticks[0].EndTime)
	require.Equal(t, ts(100), ticks[0].Manifest.TickStart)
	require.Equal(t, ts(120), ticks[1].EndTime)
	require.Equal(t, ts(110), ticks[1].Manifest.TickStart)
	require.Equal(t, ts(130), ticks[2].EndTime)
	require.Equal(t, ts(120), ticks[2].Manifest.TickStart)

	for i, tk := range ticks {
		events := readBackEvents(t, ctx, es, tk)
		require.Len(t, events, 1, "tick %d", i)
	}
}

// TestDriverMidTickStart starts the log mid-tick and verifies the
// first manifest's TickStart equals startHLC.
func TestDriverMidTickStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es := newTestDriver(t, ts(105))
	d.OnValue(ctx, roachpb.Key("a"), ts(108), []byte("v"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(110)))

	ticks := readBackTicks(t, ctx, es, ts(104), ts(110))
	require.Len(t, ticks, 1)
	require.Equal(t, ts(110), ticks[0].EndTime)
	require.Equal(t, ts(105), ticks[0].Manifest.TickStart)
}

// TestDriverDropsLateEvents verifies events with ts <= startHLC are
// silently dropped (defensive — rangefeed should already filter).
func TestDriverDropsLateEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es := newTestDriver(t, ts(100))
	d.OnValue(ctx, roachpb.Key("a"), ts(50), []byte("stale"), nil)  // before startHLC
	d.OnValue(ctx, roachpb.Key("a"), ts(100), []byte("stale"), nil) // == startHLC
	d.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("kept"), nil)
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(110)))

	ticks := readBackTicks(t, ctx, es, ts(99), ts(110))
	require.Len(t, ticks, 1)
	events := readBackEvents(t, ctx, es, ticks[0])
	require.Len(t, events, 1)
	require.Equal(t, "kept", string(events[0].Value.RawBytes))
}

// TestDriverEmptyTickStillClosed verifies that a tick with no events
// still produces a manifest (empty Files), so readers see no
// coverage gap between adjacent ticks.
func TestDriverEmptyTickStillClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es := newTestDriver(t, ts(100))
	require.NoError(t, d.OnCheckpoint(ctx, allSpan, ts(130)))

	ticks := readBackTicks(t, ctx, es, ts(99), ts(130))
	require.Len(t, ticks, 3)
	for i, tk := range ticks {
		require.Empty(t, tk.Manifest.Files, "tick %d should have no files", i)
	}
}

// TestDriverNoCheckpointNoClose verifies that without a checkpoint,
// no tick is closed even if events are buffered. In-flight buffers
// are dropped on shutdown (TODO(dt) to flush on shutdown).
func TestDriverNoCheckpointNoClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	d, es := newTestDriver(t, ts(100))
	d.OnValue(ctx, roachpb.Key("a"), ts(105), []byte("v"), nil)
	d.OnValue(ctx, roachpb.Key("b"), ts(107), []byte("v"), nil)
	// no checkpoint

	ticks := readBackTicks(t, ctx, es, ts(99), ts(200))
	require.Empty(t, ticks)
}
