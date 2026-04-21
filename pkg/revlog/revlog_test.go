// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog_test

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// newTestStorage returns a cloud.ExternalStorage backed by a temporary
// directory. The directory is cleaned up by t.TempDir.
func newTestStorage(t *testing.T) cloud.ExternalStorage {
	t.Helper()
	return nodelocal.TestingMakeNodelocalStorage(
		t.TempDir(), cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
}

// tickEnd constructs a tick-end timestamp at the given UTC instant.
func tickEnd(year int, month time.Month, day, hour, minute, sec int) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: time.Date(year, month, day, hour, minute, sec, 0, time.UTC).UnixNano(),
	}
}

// writeTick writes one data file with the given events, plus a
// manifest sealing the tick. fileID and flushOrder are forwarded to
// the writer.
func writeTick(
	t *testing.T,
	ctx context.Context,
	es cloud.ExternalStorage,
	te hlc.Timestamp,
	fileID int64,
	flushOrder int32,
	events []revlog.Event,
) revlogpb.File {
	t.Helper()
	w, err := revlog.NewTickWriter(ctx, es, te, fileID, flushOrder)
	require.NoError(t, err)
	for _, ev := range events {
		require.NoError(t, w.Add(ev.Key, ev.Timestamp, ev.Value.RawBytes, ev.PrevValue.RawBytes))
	}
	f, _, err := w.Close()
	require.NoError(t, err)
	return f
}

// testTickWidth is the assumed tick width for test fixtures: every
// sealTick that doesn't pass an explicit TickStart treats the tick
// as covering (te - testTickWidth, te].
const testTickWidth = 10 * time.Second

func sealTick(
	t *testing.T,
	ctx context.Context,
	es cloud.ExternalStorage,
	te hlc.Timestamp,
	files []revlogpb.File,
) {
	t.Helper()
	require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
		TickStart: te.AddDuration(-testTickWidth),
		TickEnd:   te,
		Files:     files,
	}))
}

// collectTicks drains a Ticks iterator into a slice. Any error stops
// iteration and is returned.
func collectTicks(
	t *testing.T, lr *revlog.LogReader, ctx context.Context, start, end hlc.Timestamp,
) []revlog.Tick {
	t.Helper()
	var out []revlog.Tick
	for tk, err := range lr.Ticks(ctx, start, end) {
		require.NoError(t, err)
		out = append(out, tk)
	}
	return out
}

// collectEvents drains a TickReader.Events iterator into a slice.
func collectEvents(t *testing.T, tr *revlog.TickReader, ctx context.Context) []revlog.Event {
	t.Helper()
	var out []revlog.Event
	for ev, err := range tr.Events(ctx) {
		require.NoError(t, err)
		out = append(out, ev)
	}
	return out
}

func key(s string) roachpb.Key {
	return roachpb.Key(s)
}

func ts(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{WallTime: walltime, Logical: logical}
}

func val(s string) roachpb.Value {
	if s == "" {
		return roachpb.Value{}
	}
	return roachpb.Value{RawBytes: []byte(s)}
}

// keysAt returns the keys of events that match the given timestamp,
// failing the test if any event has a different timestamp.
func keysAt(t *testing.T, events []revlog.Event, want hlc.Timestamp) []string {
	t.Helper()
	out := make([]string, 0, len(events))
	for _, e := range events {
		require.Equal(t, want, e.Timestamp, "unexpected timestamp in bucket")
		out = append(out, string(e.Key))
	}
	return out
}

func ev(k string, walltime int64, logical int32, v, prev string) revlog.Event {
	return revlog.Event{
		Key:       key(k),
		Timestamp: ts(walltime, logical),
		Value:     val(v),
		PrevValue: val(prev),
	}
}

func TestRoundTripSingleFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	te := tickEnd(2026, 4, 20, 15, 30, 10)
	in := []revlog.Event{
		ev("a", 1000, 0, "v_a_0", ""),
		ev("a", 2000, 0, "v_a_1", "v_a_0"),
		ev("b", 1500, 0, "v_b", ""),
		ev("c", 1200, 0, "v_c", ""),
	}
	f := writeTick(t, ctx, es, te, 1, 0, in)
	sealTick(t, ctx, es, te, []revlogpb.File{f})

	lr := revlog.NewLogReader(es)
	ticks := collectTicks(t, lr, ctx, te.Prev(), te)
	require.Len(t, ticks, 1)
	require.Equal(t, te, ticks[0].EndTime)
	require.Len(t, ticks[0].Manifest.Files, 1)

	got := collectEvents(t, lr.GetTickReader(ctx, ticks[0], nil), ctx)
	require.Equal(t, in, got)
}

// TestCoverageOverlap verifies the coverage-interval semantics of
// Ticks: a tick T_n covers events in (T_{n-1}, T_n], so a query
// window (start, end] returns ticks whose coverage overlaps that
// window — including the "first higher" tick (whose coverage
// contains end), but NOT a still-later tick (whose coverage starts
// at-or-after end).
func TestCoverageOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	// Four 10s-aligned ticks: t10, t20, t30, t40.
	t10 := tickEnd(2026, 4, 20, 15, 30, 10)
	t20 := tickEnd(2026, 4, 20, 15, 30, 20)
	t30 := tickEnd(2026, 4, 20, 15, 30, 30)
	t40 := tickEnd(2026, 4, 20, 15, 30, 40)
	for i, te := range []hlc.Timestamp{t10, t20, t30, t40} {
		f := writeTick(t, ctx, es, te, int64(i+1), 0, []revlog.Event{
			ev("k", te.WallTime, 0, "v", ""),
		})
		sealTick(t, ctx, es, te, []revlogpb.File{f})
	}

	lr := revlog.NewLogReader(es)

	// Window (15:30:08, 15:30:22]. Caller doesn't know tick width.
	// Coverage: t10 covers (00, 10] → overlaps at (08, 10]; t20
	// covers (10, 20] → overlap at (10, 20]; t30 covers (20, 30]
	// → overlap at (20, 22]; t40 covers (30, 40] → no overlap.
	q8 := hlc.Timestamp{WallTime: time.Date(2026, 4, 20, 15, 30, 8, 0, time.UTC).UnixNano()}
	q22 := hlc.Timestamp{WallTime: time.Date(2026, 4, 20, 15, 30, 22, 0, time.UTC).UnixNano()}
	got := collectTicks(t, lr, ctx, q8, q22)
	require.Equal(t, []hlc.Timestamp{t10, t20, t30},
		[]hlc.Timestamp{got[0].EndTime, got[1].EndTime, got[2].EndTime},
		"expected t10..t30 to overlap (q8, q22]; t40 must not")
	require.Len(t, got, 3)

	// A window aligned exactly on a tick boundary (15:30:10, 15:30:20]
	// returns just that one tick — its coverage *is* the window.
	got = collectTicks(t, lr, ctx, t10, t20)
	require.Len(t, got, 1)
	require.Equal(t, t20, got[0].EndTime)
}

// TestExplicitTickStart verifies the mid-tick log launch case:
// the first tick a log produces carries an explicit Manifest.TickStart
// instead of inheriting "previous tick's end" as its lower bound.
// This narrows the tick's coverage and changes which queries it
// overlaps.
func TestExplicitTickStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	te := tickEnd(2026, 4, 20, 15, 30, 10)
	tickStart := hlc.Timestamp{
		WallTime: time.Date(2026, 4, 20, 15, 30, 7, 0, time.UTC).UnixNano(),
	}

	// First tick of a log: launched at tickStart=15:30:07, sealed
	// at te=15:30:10. Its coverage is (15:30:07, 15:30:10] —
	// strictly narrower than the implicit (15:30:00, 15:30:10].
	w, err := revlog.NewTickWriter(ctx, es, te, 1, 0)
	require.NoError(t, err)
	require.NoError(t, w.Add(key("k"), te, []byte("v"), nil))
	f, _, err := w.Close()
	require.NoError(t, err)
	require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
		TickEnd:   te,
		TickStart: tickStart,
		Files:     []revlogpb.File{f},
	}))

	lr := revlog.NewLogReader(es)

	// Window (15:30:05, 15:30:08]: would overlap the implicit
	// coverage (15:30:00, 15:30:10] — but does NOT overlap the
	// explicit (15:30:07, 15:30:10] at any point past 15:30:08.
	// Actually it does: (15:30:07, 15:30:08] is the overlap.
	q5 := hlc.Timestamp{WallTime: time.Date(2026, 4, 20, 15, 30, 5, 0, time.UTC).UnixNano()}
	q8 := hlc.Timestamp{WallTime: time.Date(2026, 4, 20, 15, 30, 8, 0, time.UTC).UnixNano()}
	got := collectTicks(t, lr, ctx, q5, q8)
	require.Len(t, got, 1)
	require.Equal(t, te, got[0].EndTime)
	require.Equal(t, tickStart, got[0].Manifest.TickStart)

	// Window (15:30:00, 15:30:06] is entirely before the tick's
	// (15:30:07, 15:30:10] coverage; without explicit tick_start
	// the reader would have yielded this tick (implicit coverage
	// (15:30:00, 15:30:10] overlaps), but with tick_start it
	// correctly does not.
	q0 := hlc.Timestamp{WallTime: time.Date(2026, 4, 20, 15, 30, 0, 0, time.UTC).UnixNano()}
	q6 := hlc.Timestamp{WallTime: time.Date(2026, 4, 20, 15, 30, 6, 0, time.UTC).UnixNano()}
	got = collectTicks(t, lr, ctx, q0, q6)
	require.Empty(t, got)
}

func TestEnumerationAcrossHours(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	t1 := tickEnd(2026, 4, 20, 14, 59, 50)
	t2 := tickEnd(2026, 4, 20, 15, 0, 0)
	t3 := tickEnd(2026, 4, 20, 15, 0, 10)
	t4 := tickEnd(2026, 4, 20, 16, 0, 0)
	for i, te := range []hlc.Timestamp{t1, t2, t3, t4} {
		f := writeTick(t, ctx, es, te, int64(i+1), 0, []revlog.Event{
			ev("k", te.WallTime, 0, "v", ""),
		})
		sealTick(t, ctx, es, te, []revlogpb.File{f})
	}

	lr := revlog.NewLogReader(es)

	// Full range: all four ticks via (t1.Prev, t4].
	all := collectTicks(t, lr, ctx, t1.Prev(), t4)
	require.Len(t, all, 4)
	require.Equal(t, []hlc.Timestamp{t1, t2, t3, t4},
		[]hlc.Timestamp{all[0].EndTime, all[1].EndTime, all[2].EndTime, all[3].EndTime})

	// Half-open semantics: (t1, t3] returns t2 and t3 (not t1).
	mid := collectTicks(t, lr, ctx, t1, t3)
	require.Len(t, mid, 2)
	require.Equal(t, t2, mid[0].EndTime)
	require.Equal(t, t3, mid[1].EndTime)

	// (t1.Prev, t1] returns just t1.
	just1 := collectTicks(t, lr, ctx, t1.Prev(), t1)
	require.Len(t, just1, 1)
	require.Equal(t, t1, just1[0].EndTime)

	// Window with no ticks: empty.
	empty := collectTicks(t, lr, ctx,
		tickEnd(2026, 4, 21, 12, 0, 0),
		tickEnd(2026, 4, 21, 13, 0, 0))
	require.Empty(t, empty)
}

func TestSpanFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	te := tickEnd(2026, 4, 20, 15, 30, 10)
	in := []revlog.Event{
		ev("a", 1000, 0, "va", ""),
		ev("b", 1000, 0, "vb", ""),
		ev("c", 1000, 0, "vc", ""),
		ev("d", 1000, 0, "vd", ""),
		ev("e", 1000, 0, "ve", ""),
	}
	f := writeTick(t, ctx, es, te, 1, 0, in)
	sealTick(t, ctx, es, te, []revlogpb.File{f})

	lr := revlog.NewLogReader(es)
	ticks := collectTicks(t, lr, ctx, te.Prev(), te)
	require.Len(t, ticks, 1)

	// [b, d) and [e, e\x00): b, c, e — but not a or d.
	spans := []roachpb.Span{
		{Key: key("b"), EndKey: key("d")},
		{Key: key("e"), EndKey: roachpb.Key("e").Next()},
	}
	got := collectEvents(t, lr.GetTickReader(ctx, ticks[0], spans), ctx)
	wantKeys := []string{"b", "c", "e"}
	gotKeys := make([]string, 0, len(got))
	for _, e := range got {
		gotKeys = append(gotKeys, string(e.Key))
	}
	require.Equal(t, wantKeys, gotKeys)
}

func TestFlushOrderAcrossFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	te := tickEnd(2026, 4, 20, 15, 30, 10)

	// Two files at flush_order 0 covering disjoint spans, plus a
	// file at flush_order 1 that overlaps both. The reader must
	// emit the flush_order=0 events first.
	f0a := writeTick(t, ctx, es, te, 10, 0, []revlog.Event{
		ev("a", 1000, 0, "v_a_0", ""),
		ev("b", 1000, 0, "v_b_0", ""),
	})
	f0b := writeTick(t, ctx, es, te, 11, 0, []revlog.Event{
		ev("y", 1000, 0, "v_y_0", ""),
		ev("z", 1000, 0, "v_z_0", ""),
	})
	f1 := writeTick(t, ctx, es, te, 20, 1, []revlog.Event{
		ev("a", 1500, 0, "v_a_1", "v_a_0"),
		ev("z", 1500, 0, "v_z_1", "v_z_0"),
	})
	// Intentionally pass files in shuffled manifest order to verify
	// the reader sorts by flush_order.
	sealTick(t, ctx, es, te, []revlogpb.File{f1, f0b, f0a})

	lr := revlog.NewLogReader(es)
	ticks := collectTicks(t, lr, ctx, te.Prev(), te)
	require.Len(t, ticks, 1)

	got := collectEvents(t, lr.GetTickReader(ctx, ticks[0], nil), ctx)
	require.Len(t, got, 6)
	// Within a single flush_order bucket the spec admits any
	// interleaving, so we test the per-bucket *set* of keys, then
	// assert that bucket 0 is fully drained before bucket 1.
	bucket0 := keysAt(t, got[:4], hlc.Timestamp{WallTime: 1000})
	bucket1 := keysAt(t, got[4:], hlc.Timestamp{WallTime: 1500})
	require.ElementsMatch(t, []string{"a", "b", "y", "z"}, bucket0)
	require.ElementsMatch(t, []string{"a", "z"}, bucket1)
}

func TestPerKeyAscendingTimestampWithinFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newTestStorage(t)
	defer es.Close()

	te := tickEnd(2026, 4, 20, 15, 30, 10)
	in := []revlog.Event{
		ev("k", 1000, 0, "v0", ""),
		ev("k", 1000, 1, "v1", "v0"),
		ev("k", 2000, 0, "v2", "v1"),
	}
	f := writeTick(t, ctx, es, te, 1, 0, in)
	sealTick(t, ctx, es, te, []revlogpb.File{f})

	lr := revlog.NewLogReader(es)
	ticks := collectTicks(t, lr, ctx, te.Prev(), te)
	require.Len(t, ticks, 1)
	got := collectEvents(t, lr.GetTickReader(ctx, ticks[0], nil), ctx)
	require.Equal(t, in, got)
}

func TestCorruptManifestRejected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tempDir := t.TempDir()
	es := nodelocal.TestingMakeNodelocalStorage(
		tempDir, cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
	defer es.Close()

	te := tickEnd(2026, 4, 20, 15, 30, 10)
	f := writeTick(t, ctx, es, te, 1, 0, []revlog.Event{ev("k", 1000, 0, "v", "")})
	sealTick(t, ctx, es, te, []revlogpb.File{f})

	// Locate the marker on disk and flip a byte in the body. The
	// trailer's CRC32C will no longer match.
	markerPath := filepath.Join(tempDir, revlog.MarkerPath(te))
	body, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	require.Greater(t, len(body), 4)
	body[0] ^= 0xff
	require.NoError(t, os.WriteFile(markerPath, body, 0o644))

	lr := revlog.NewLogReader(es)
	var sawErr error
	for _, err := range lr.Ticks(ctx, te.Prev(), te) {
		if err != nil {
			sawErr = err
			break
		}
	}
	require.Error(t, sawErr)
	require.Contains(t, sawErr.Error(), "CRC32C mismatch")
}

// TestCRC32Format pins the leading-checksum format down: a manifest
// written by WriteTickManifest is a 4-byte little-endian CRC32C of
// the marshaled proto, followed by the marshaled proto.
func TestCRC32Format(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tempDir := t.TempDir()
	es := nodelocal.TestingMakeNodelocalStorage(
		tempDir, cluster.MakeTestingClusterSettings(), cloudpb.ExternalStorage{},
	)
	defer es.Close()

	te := tickEnd(2026, 4, 20, 15, 30, 10)
	require.NoError(t, revlog.WriteTickManifest(ctx, es, revlogpb.Manifest{
		TickStart: te.AddDuration(-testTickWidth),
		TickEnd:   te,
	}))

	buf, err := os.ReadFile(filepath.Join(tempDir, revlog.MarkerPath(te)))
	require.NoError(t, err)
	require.Greater(t, len(buf), 4)
	want := binary.LittleEndian.Uint32(buf[:4])
	got := crc32.Checksum(buf[4:], crc32.MakeTable(crc32.Castagnoli))
	require.Equal(t, got, want)
}
