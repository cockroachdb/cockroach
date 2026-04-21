// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogfeed

import (
	"context"
	"iter"

	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// fakeTickSource is an in-memory tickSource for unit tests. It mirrors
// *revlog.LogReader.Ticks's overlap semantics — yields ticks whose
// (TickStart, TickEnd] intersects (start, end] — without touching real
// external storage or any SSTable machinery.
type fakeTickSource struct {
	// ticks is the full sorted-by-EndTime backing slice. Tests build it
	// with mkTick.
	ticks []revlog.Tick
}

// Ticks implements tickSource.
func (f *fakeTickSource) Ticks(
	_ context.Context, start, end hlc.Timestamp,
) iter.Seq2[revlog.Tick, error] {
	return func(yield func(revlog.Tick, error) bool) {
		if !start.Less(end) {
			return
		}
		for _, t := range f.ticks {
			// Skip ticks entirely at or before start.
			if !start.Less(t.EndTime) {
				continue
			}
			// Stop once a tick begins at or after end (the slice is
			// sorted, so nothing later can overlap).
			if !t.Manifest.TickStart.Less(end) {
				return
			}
			if !yield(t, nil) {
				return
			}
		}
	}
}

// mkTick builds a tick whose coverage interval is (start, end] using
// wall-clock-only timestamps. Sufficient for coverage-check tests; the
// data-file machinery (Files, Stats) is intentionally left empty.
func mkTick(start, end int64) revlog.Tick {
	return revlog.Tick{
		EndTime: hlc.Timestamp{WallTime: end},
		Manifest: revlogpb.Manifest{
			TickStart: hlc.Timestamp{WallTime: start},
			TickEnd:   hlc.Timestamp{WallTime: end},
		},
	}
}
