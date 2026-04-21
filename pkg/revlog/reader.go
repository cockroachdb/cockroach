// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"iter"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
)

// LogReader discovers and opens closed ticks in the revlog.
//
// LogReader is the top-level read entry point. The discovery path
// (Ticks) walks log/resolved/ to enumerate which ticks have closed in
// a given window; the consumption path (GetTickReader) opens the
// data files of one tick for iteration.
type LogReader struct {
	es cloud.ExternalStorage
}

// NewLogReader opens the log at the given external storage. The
// storage handle's root is treated as the "log/" tree described in
// the format spec §3.
func NewLogReader(es cloud.ExternalStorage) *LogReader {
	return &LogReader{es: es}
}

// Tick is a discovered closed tick: its end time plus the parsed
// manifest. Returned by LogReader.Ticks; consumed by
// LogReader.GetTickReader.
type Tick struct {
	EndTime  hlc.Timestamp
	Manifest revlogpb.Manifest
}

// assumedMaxTickWidth is the upper bound on tick width that the
// LIST-narrowing arithmetic in Ticks assumes. Real ticks at the
// 10s working width are well under this; if ticks are ever
// reconfigured to be wider than this, the prefix narrowing here
// must be widened too or the LIST may exclude the "first tick whose
// end is past `end`" — see Ticks for why that tick must be visible.
const assumedMaxTickWidth = time.Minute

// Ticks enumerates closed ticks whose coverage overlaps the time
// window (start, end] — start exclusive, end inclusive. This matches
// replay semantics: the caller already has state as of start (events
// at start are not needed), wants state as of end, and the events
// that take state from start to end live in the ticks whose
// coverage intervals overlap that window. Chained windows
// (`(t0, t1]` then `(t1, t2]`) don't double-count any tick.
//
// "Coverage" is the time range a tick contains: each tick's
// Manifest carries an explicit (TickStart, TickEnd] interval. The
// caller doesn't need to know the tick width or the predecessor
// chain: they say what time window they want and the iterator
// yields whichever ticks' coverage overlaps it.
//
// Implements steps 1–3 of the read protocol (format §6): LIST the
// largest path prefix shared by the smallest and largest possible
// in-window tick ends — for example "log/resolved/2026-04-20/15-30."
// for a one-minute window — then GET + verify each in-window marker.
// The shared prefix degrades gracefully: a one-day window LISTs that
// one day, a one-month window LISTs that month's days, and a
// multi-year window degenerates to LISTing log/resolved/ entirely.
//
// Listing or read errors abort iteration and are yielded once.
func (lr *LogReader) Ticks(ctx context.Context, start, end hlc.Timestamp) iter.Seq2[Tick, error] {
	return func(yield func(Tick, error) bool) {
		// Empty or inverted window: no ticks possible.
		if !start.Less(end) {
			return
		}
		// LIST prefix narrowing: the lower bound is the smallest
		// possible in-window tick (start.Next, since start is
		// exclusive); the upper bound is widened past end by
		// assumedMaxTickWidth so the LIST captures the "first
		// tick whose end exceeds end" — needed for the coverage
		// interval that contains end itself.
		//
		// Round the prefix down to the last "/" so it sits on a
		// directory boundary. Without this, ExternalStorage
		// implementations that strip the LIST prefix from each
		// returned name (notably nodelocal) hand back partial
		// filenames like "6-04-21/14-34.40.pb" when the prefix
		// ends mid-component (e.g. "log/resolved/202") — which
		// our parser can't reconstruct into a tick-end. The cost
		// is a wider LIST inside the last directory; the upper
		// bound is still bounded by assumedMaxTickWidth.
		listPrefix := commonStringPrefix(
			MarkerPath(start.Next()),
			MarkerPath(end.AddDuration(assumedMaxTickWidth)),
		)
		if i := strings.LastIndexByte(listPrefix, '/'); i >= 0 {
			listPrefix = listPrefix[:i+1]
		}

		var names []string
		err := lr.es.List(ctx, listPrefix, cloud.ListOptions{}, func(name string) error {
			names = append(names, name)
			return nil
		})
		if err != nil {
			if isNotFound(err) {
				return
			}
			yield(Tick{}, errors.Wrapf(err, "listing %s", listPrefix))
			return
		}
		slices.Sort(names) // lex == chronological

		// listPrefixRel is the part of listPrefix beyond
		// ResolvedRoot — what we have to prepend to a name that's
		// relative to the LIST prefix to recover its full
		// ResolvedRoot-relative path.
		listPrefixRel := strings.TrimPrefix(listPrefix, ResolvedRoot)

		for _, name := range names {
			// Different backends return List entries with
			// different conventions: some yield the full path
			// relative to the storage root (nodelocal in test
			// mode, with a stray leading slash), some yield the
			// path with the LIST prefix already stripped
			// (nodelocal in production). Normalize: strip a
			// leading slash, strip listPrefix if it's there,
			// then re-prepend the part of listPrefix beyond
			// ResolvedRoot to recover the same canonical
			// ResolvedRoot-relative path.
			rel := strings.TrimPrefix(name, "/")
			rel = strings.TrimPrefix(rel, ResolvedRoot)
			rel = strings.TrimPrefix(rel, listPrefixRel)
			rel = listPrefixRel + rel
			fragment := strings.TrimSuffix(rel, markerExt)
			if fragment == rel {
				yield(Tick{}, errors.Errorf(
					"unexpected marker %q under %s (missing %q suffix)",
					name, listPrefix, markerExt))
				return
			}
			tickEnd, err := ParseTickEnd(fragment)
			if err != nil {
				yield(Tick{}, errors.Wrapf(err, "in %s", listPrefix))
				return
			}
			if !start.Less(tickEnd) {
				// tickEnd <= start; tick is entirely before the
				// window's open lower bound.
				continue
			}
			// tickEnd > start. Read the manifest for the tick's
			// explicit lower bound, then check overlap with
			// (start, end].
			m, err := readManifest(ctx, lr.es, ResolvedRoot+fragment+markerExt)
			if err != nil {
				yield(Tick{}, err)
				return
			}
			if !m.TickStart.Less(end) {
				// (TickStart, TickEnd] is entirely at-or-after
				// end → no overlap. Sorted entries that follow
				// have an even later TickStart, so they can't
				// overlap either.
				return
			}
			if !yield(Tick{EndTime: tickEnd, Manifest: m}, nil) {
				return
			}
		}
	}
}

// commonStringPrefix returns the longest prefix shared by a and b.
func commonStringPrefix(a, b string) string {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a[:n]
}

// GetTickReader opens one tick for iteration. The reader emits events
// whose user_keys fall in any of the requested spans. If spans is
// empty, all events in the tick are emitted.
//
// Spans must be sorted by start key and non-overlapping; the iterator
// advances through them in order. The caller retains ownership of
// the slice.
func (lr *LogReader) GetTickReader(ctx context.Context, t Tick, spans []roachpb.Span) *TickReader {
	return &TickReader{
		es:    lr.es,
		tick:  t,
		spans: spans,
	}
}

// Event is a decoded change emitted by a TickReader.
type Event struct {
	Key       roachpb.Key
	Timestamp hlc.Timestamp
	Value     roachpb.Value
	PrevValue roachpb.Value // zero-value if no diff
}

// TickReader iterates events in one tick, in per-key revision order
// (format §7), restricted to the configured spans.
type TickReader struct {
	es    cloud.ExternalStorage
	tick  Tick
	spans []roachpb.Span
}

// Events iterates events in the tick. Files are processed in
// ascending flush_order; within a single flush_order bucket, files
// are read in manifest order (their disjoint-spans invariant means
// any order satisfies the per-key ordering rule). Within a single
// file, the SSTable iterator emits keys in (user_key, mvcc_ts)
// ascending order.
//
// If reading or decoding any file fails, iteration stops and the
// error is yielded once.
func (tr *TickReader) Events(ctx context.Context) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		// Stable-sort the manifest's files by flush_order. Stable so
		// ties preserve manifest order, matching the spec's "any
		// interleaving is valid within a flush_order bucket".
		files := slices.Clone(tr.tick.Manifest.Files)
		slices.SortStableFunc(files, func(a, b revlogpb.File) int {
			return int(a.FlushOrder) - int(b.FlushOrder)
		})

		for _, f := range files {
			if err := tr.emitFile(ctx, f, yield); err != nil {
				yield(Event{}, err)
				return
			}
		}
	}
}

// emitFile downloads, decodes, and yields events from one data file.
// Returns an error only on a hard failure (read, parse, or decode);
// returning nil with the iteration cut short by yield returning false
// is the "consumer stopped" path.
func (tr *TickReader) emitFile(
	ctx context.Context, f revlogpb.File, yield func(Event, error) bool,
) error {
	name := DataFilePath(tr.tick.EndTime, f.FileID)
	rc, sz, err := tr.es.ReadFile(ctx, name, cloud.ReadOptions{})
	if err != nil {
		return errors.Wrapf(err, "opening %s", name)
	}
	defer func() { _ = rc.Close(ctx) }()

	body, err := ioctx.ReadAllWithScratch(ctx, rc, make([]byte, 0, sz))
	if err != nil {
		return errors.Wrapf(err, "reading %s", name)
	}
	r, err := sstable.NewMemReader(body, sstable.ReaderOptions{})
	if err != nil {
		return errors.Wrapf(err, "opening sstable %s", name)
	}
	defer func() { _ = r.Close() }()

	iterator, err := r.NewIter(sstable.NoTransforms, nil /* lower */, nil /* upper */, sstable.AssertNoBlobHandles)
	if err != nil {
		return errors.Wrapf(err, "creating iterator on %s", name)
	}
	defer func() { _ = iterator.Close() }()

	// spanCursor is the index of the span currently being scanned.
	// It only advances forward.
	spanCursor := 0
	hasSpans := len(tr.spans) > 0

	// SeekGE(nil, 0) seeks to the first key — equivalent to First()
	// — so the no-spans case falls out naturally. kv's underlying
	// type is *base.InternalKV (internal pebble package); we treat
	// it via type inference to avoid importing the internal package.
	var startKey []byte
	if hasSpans {
		startKey = EncodeKeyPrefix(tr.spans[0].Key)
	}
	kv := iterator.SeekGE(startKey, 0 /* flags */)

	for kv != nil {
		userKey, ts, err := DecodeKey(kv.K.UserKey)
		if err != nil {
			return errors.Wrapf(err, "decoding key in %s", name)
		}

		if hasSpans {
			// Advance past spans whose EndKey <= userKey.
			for spanCursor < len(tr.spans) && bytes.Compare(tr.spans[spanCursor].EndKey, userKey) <= 0 {
				spanCursor++
			}
			if spanCursor >= len(tr.spans) {
				return nil
			}
			if bytes.Compare(userKey, tr.spans[spanCursor].Key) < 0 {
				kv = iterator.SeekGE(EncodeKeyPrefix(tr.spans[spanCursor].Key), 0 /* flags */)
				continue
			}
		}

		valBytes, _, err := kv.Value(nil)
		if err != nil {
			return errors.Wrapf(err, "reading value in %s", name)
		}
		var frame revlogpb.ValueFrame
		if err := protoutil.Unmarshal(valBytes, &frame); err != nil {
			return errors.Wrapf(err, "decoding value frame in %s", name)
		}

		// userKey aliases the iterator's key buffer, which Pebble
		// may reuse on the next positioning call. Clone so the
		// emitted event survives further iteration.
		ev := Event{Key: slices.Clone(userKey), Timestamp: ts}
		ev.Value.RawBytes = frame.Value
		ev.PrevValue.RawBytes = frame.PrevValue
		if !yield(ev, nil) {
			return nil
		}
		kv = iterator.Next()
	}
	return nil
}

// readManifest downloads, verifies, and decodes a tick manifest at
// the given object name. The marker layout is a 4-byte little-endian
// CRC32C followed by the marshaled Manifest proto (format §4).
func readManifest(
	ctx context.Context, es cloud.ExternalStorage, name string,
) (revlogpb.Manifest, error) {
	rc, sz, err := es.ReadFile(ctx, name, cloud.ReadOptions{})
	if err != nil {
		return revlogpb.Manifest{}, errors.Wrapf(err, "reading %s", name)
	}
	defer func() { _ = rc.Close(ctx) }()
	buf, err := ioctx.ReadAllWithScratch(ctx, rc, make([]byte, 0, sz))
	if err != nil {
		return revlogpb.Manifest{}, errors.Wrapf(err, "reading %s", name)
	}
	if len(buf) < 4 {
		return revlogpb.Manifest{}, errors.Errorf("manifest %s too short: %d bytes", name, len(buf))
	}
	want := binary.LittleEndian.Uint32(buf[:4])
	body := buf[4:]
	if got := crc32.Checksum(body, crc32cTable); got != want {
		return revlogpb.Manifest{}, errors.Errorf("manifest %s CRC32C mismatch: want %x, got %x", name, want, got)
	}
	var m revlogpb.Manifest
	if err := protoutil.Unmarshal(body, &m); err != nil {
		return revlogpb.Manifest{}, errors.Wrapf(err, "decoding manifest %s", name)
	}
	return m, nil
}

// isNotFound reports whether err is a "this object doesn't exist"
// signal from external storage. Different backends use different
// concrete errors; we recognize the well-known sentinels here.
func isNotFound(err error) bool {
	return errors.Is(err, cloud.ErrFileDoesNotExist) || errors.Is(err, cloud.ErrListingDone)
}
