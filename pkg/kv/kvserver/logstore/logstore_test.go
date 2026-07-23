// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestRaftStorageWrites(t *testing.T) {
	// Run with both SingleDelete and Delete-based clears for raft log
	// truncation and overlap-write. The two modes produce different write
	// batches (Single Delete vs Delete), so each gets its own testdata file.
	//
	// In both modes logAppend's tracked ByteSize must exactly match the
	// engine's SysBytes for the raft log keyspace. Under SingleDelete this
	// requires logAppend to manually compensate for the fact that
	// SingleClearUnversioned does not touch MVCCStats; we assert that
	// compensation is correct by comparing state.ByteSize to ComputeStats.
	testutils.RunTrueAndFalse(t, "single_delete", func(t *testing.T, useSingleDelete bool) {
		// Override the default value of `raftLogSingleDeleteEnv`.
		defer TestingSetRaftLogSingleDelete(useSingleDelete)()
		ctx := context.Background()
		const rangeID = roachpb.RangeID(123)
		sl := NewStateLoader(rangeID)
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		trunc := kvserverpb.RaftTruncatedState{Index: 100, Term: 20}
		state := RaftState{LastIndex: trunc.Index, LastTerm: trunc.Term}
		var output string

		printCommand := func(name, batch string) {
			output += fmt.Sprintf(">> %s\n%s\nState:%+v RaftTruncatedState:%+v\n",
				name, batch, state, trunc)
		}
		printCommand("init", "")

		writeBatch := func(prepare func(rw storage.ReadWriter)) string {
			t.Helper()
			batch := eng.NewBatch()
			defer batch.Close()
			prepare(batch)
			wb := kvserverpb.WriteBatch{Data: batch.Repr()}
			str, err := print.DecodeWriteBatch(wb.GetData())
			require.NoError(t, err)
			require.NoError(t, batch.Commit(true))
			return str
		}
		stats := func() int64 {
			t.Helper()
			prefix := keys.RaftLogPrefix(rangeID)
			prefixEnd := prefix.PrefixEnd()
			ms, err := storage.ComputeStats(ctx, eng, fs.ReplicationReadCategory,
				prefix, prefixEnd, 0 /* nowNanos */)
			require.NoError(t, err)
			return ms.SysBytes
		}
		write := func(name string, hs raftpb.HardState, entries []raftpb.Entry) {
			t.Helper()
			var newState RaftState
			batch := writeBatch(func(rw storage.ReadWriter) {
				require.NoError(t, storeHardState(ctx, rw, sl, hs))
				var err error
				newState, err = logAppend(ctx, sl.RangeIDPrefixBuf, eng, rw, state, entries, false /* enginesSeparated */)
				require.NoError(t, err)
			})
			state = newState
			require.Equal(t, stats(), state.ByteSize)
			printCommand(name, batch)
		}
		truncate := func(name string, ts kvserverpb.RaftTruncatedState) {
			t.Helper()
			batch := writeBatch(func(rw storage.ReadWriter) {
				require.NoError(t, Compact(ctx, trunc, ts, sl, rw, false /* enginesSeparated */))
			})
			trunc = ts
			// Compact does not update state.ByteSize; recompute it from the
			// engine so subsequent assertions stay aligned.
			state.ByteSize = stats()
			printCommand(name, batch)
		}

		write("append (100,103]", raftpb.HardState{
			Term: 21, Vote: 3, Commit: 100, Lead: 3, LeadEpoch: 5,
		}, []raftpb.Entry{
			{Index: 101, Term: 20},
			{Index: 102, Term: 21},
			{Index: 103, Term: 21},
		})
		write("append (101,102] with overlap", raftpb.HardState{
			Term: 22, Commit: 100,
		}, []raftpb.Entry{
			{Index: 102, Term: 22},
		})
		write("append (102,105]", raftpb.HardState{}, []raftpb.Entry{
			{Index: 103, Term: 22},
			{Index: 104, Term: 22},
			{Index: 105, Term: 22},
		})
		write("append (103,107] with overlap extending past tail", raftpb.HardState{}, []raftpb.Entry{
			{Index: 104, Term: 22},
			{Index: 105, Term: 22},
			{Index: 106, Term: 22},
			{Index: 107, Term: 22},
		})
		truncate("truncate at 103", kvserverpb.RaftTruncatedState{Index: 103, Term: 22})
		truncate("truncate all", kvserverpb.RaftTruncatedState{Index: 107, Term: 22})

		// TODO(pav-kv): print the engine content as well.

		output = strings.ReplaceAll(output, "\n\n", "\n")
		output = strings.ReplaceAll(output, "\n\n", "\n")
		echotest.Require(t, output, filepath.Join("testdata", t.Name()))
	})
}

type clearRangeHelper struct {
	t         *testing.T
	rangeID   roachpb.RangeID
	prefixBuf keys.RangeIDPrefixBuf
	eng       storage.Engine
}

func newClearRangeHelper(t *testing.T) *clearRangeHelper {
	const rangeID = roachpb.RangeID(123)
	h := &clearRangeHelper{
		t:         t,
		rangeID:   rangeID,
		prefixBuf: keys.MakeRangeIDPrefixBuf(rangeID),
		eng:       storage.NewDefaultInMemForTesting(),
	}
	return h
}

// close releases the engine.
func (h *clearRangeHelper) close() {
	h.eng.Close()
}

// populate writes the raft log entries [15,16,17,18,19] using logAppend.
func (h *clearRangeHelper) populate(ctx context.Context, firstIndex uint64, lastIndex uint64) {
	h.t.Helper()
	var entries []raftpb.Entry
	for i := firstIndex; i <= lastIndex; i++ {
		entries = append(entries, raftpb.Entry{Index: i, Term: 10})
	}
	b := h.eng.NewBatch()
	defer b.Close()
	_, err := logAppend(
		ctx, h.prefixBuf, h.eng, b, RaftState{}, entries, false, /* enginesSeparated */
	)
	require.NoError(h.t, err)
	require.NoError(h.t, b.Commit(false /* sync */))
}

// countBatchOps tallies the deletion ops recorded in a Pebble batch's wire
// representation, classifying them as point deletes, single deletes, or range
// deletes.
func (h *clearRangeHelper) countBatchOps(batchRepr []byte) (points, singleDels, ranges int) {
	h.t.Helper()
	r, err := storage.NewBatchReader(batchRepr)
	require.NoError(h.t, err)
	for r.Next() {
		switch r.KeyKind() {
		case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
			points++
		case pebble.InternalKeyKindSingleDelete:
			singleDels++
		case pebble.InternalKeyKindRangeDelete:
			ranges++
		}
	}
	return points, singleDels, ranges
}

// raftLogIndices returns the raft log indices present in the engine's raft log.
func (h *clearRangeHelper) raftLogIndices(ctx context.Context) []kvpb.RaftIndex {
	h.t.Helper()
	prefix := h.prefixBuf.RaftLogPrefix()
	iter, err := h.eng.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: prefix, UpperBound: prefix.PrefixEnd(),
	})
	require.NoError(h.t, err)
	defer iter.Close()
	var indices []kvpb.RaftIndex
	iter.SeekGE(storage.MakeMVCCMetadataKey(prefix))
	for {
		ok, err := iter.Valid()
		require.NoError(h.t, err)
		if !ok {
			break
		}
		key := iter.UnsafeKey().Key
		idx, err := keys.DecodeRaftLogKeyFromSuffix(key[len(prefix):])
		require.NoError(h.t, err)
		indices = append(indices, idx)
		iter.Next()
	}
	return indices
}

// TestClearRange verifies the scan-based heuristic in ClearRange: when the
// number of raft log keys actually present in (lo, hi] is below the threshold,
// the function emits per-entry point deletes; otherwise it emits a single
// Pebble range tombstone.
// The test populates raft log indices [15,19].
func TestClearRange(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		lo, hi                 kvpb.RaftIndex
		threshold              int
		wantPoints, wantRanges int
		wantIndices            []kvpb.RaftIndex
	}{
		{lo: 0, hi: math.MaxUint64, threshold: 1, wantPoints: 0, wantRanges: 1},
		{lo: 0, hi: math.MaxUint64, threshold: 6, wantPoints: 5, wantRanges: 0},
		{lo: 10, hi: 15, threshold: 3, wantPoints: 1, wantRanges: 0, wantIndices: []kvpb.RaftIndex{16, 17, 18, 19}},
		{lo: 10, hi: 15, threshold: 6, wantPoints: 1, wantRanges: 0, wantIndices: []kvpb.RaftIndex{16, 17, 18, 19}},
		{lo: 10, hi: 19, threshold: 3, wantPoints: 0, wantRanges: 1},
		{lo: 10, hi: 19, threshold: 6, wantPoints: 5, wantRanges: 0},
		{lo: 14, hi: 18, threshold: 3, wantPoints: 0, wantRanges: 1, wantIndices: []kvpb.RaftIndex{19}},
		{lo: 14, hi: 18, threshold: 6, wantPoints: 4, wantRanges: 0, wantIndices: []kvpb.RaftIndex{19}},
		// Empty / inverted ranges are no-ops.
		{lo: 15, hi: 15, threshold: 1, wantIndices: []kvpb.RaftIndex{15, 16, 17, 18, 19}},
		{lo: 100, hi: 15, threshold: 1, wantIndices: []kvpb.RaftIndex{15, 16, 17, 18, 19}},
	}
	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			h := newClearRangeHelper(t)
			defer h.close()
			h.populate(ctx, 15 /* firstIndex */, 19 /* lastIndex */)
			batch := h.eng.NewBatch()
			defer batch.Close()
			require.NoError(t, ClearRange(ctx, h.eng, batch, h.prefixBuf, tc.lo, tc.hi, tc.threshold))
			points, singles, ranges := h.countBatchOps(batch.Repr())
			require.Equal(t, tc.wantPoints, points)
			require.Equal(t, 0, singles) // ClearRange doesn't use SingleDelete.
			require.Equal(t, tc.wantRanges, ranges)
			require.NoError(t, batch.Commit(false /* sync */))
			require.Equal(t, tc.wantIndices, h.raftLogIndices(ctx))
		})
	}
}

// TestClearRangeSizeKnown verifies that it correctly chooses between point
// deletes, single deletes, and range deletes based on the provided args.
// The test populates raft log indices [15,19].
func TestClearRangeSizeKnown(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		lo, hi                              kvpb.RaftIndex
		threshold                           int
		maybeUseSingleDel                   bool
		wantPoints, wantSingles, wantRanges int
		wantIndices                         []kvpb.RaftIndex
	}{
		{lo: 0, hi: math.MaxUint64, threshold: 0, wantRanges: 1},
		{lo: 0, hi: math.MaxUint64, threshold: 10, maybeUseSingleDel: true, wantRanges: 1},
		{lo: 0, hi: math.MaxUint64, threshold: 1, wantRanges: 1},
		{lo: 0, hi: math.MaxUint64, threshold: 1, maybeUseSingleDel: true, wantRanges: 1},
		{lo: 14, hi: 18, threshold: 3, wantRanges: 1, wantIndices: []kvpb.RaftIndex{19}},
		{lo: 14, hi: 18, threshold: 3, maybeUseSingleDel: true, wantRanges: 1, wantIndices: []kvpb.RaftIndex{19}},
		{lo: 14, hi: 18, threshold: 6, wantPoints: 4, wantIndices: []kvpb.RaftIndex{19}},
		{lo: 14, hi: 18, threshold: 6, maybeUseSingleDel: true, wantSingles: 4, wantIndices: []kvpb.RaftIndex{19}},
		// Empty / inverted ranges are no-ops.
		{lo: 15, hi: 15, threshold: 1, wantIndices: []kvpb.RaftIndex{15, 16, 17, 18, 19}},
		{lo: 100, hi: 15, threshold: 1, wantIndices: []kvpb.RaftIndex{15, 16, 17, 18, 19}},
	}
	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			h := newClearRangeHelper(t)
			defer h.close()
			h.populate(ctx, 15 /* firstIndex */, 19 /* lastIndex */)
			batch := h.eng.NewBatch()
			defer batch.Close()
			require.NoError(t, ClearRangeSizeKnown(
				batch, h.prefixBuf, tc.lo, tc.hi, tc.threshold, tc.maybeUseSingleDel,
			))
			points, singles, ranges := h.countBatchOps(batch.Repr())
			require.Equal(t, tc.wantPoints, points)
			require.Equal(t, tc.wantSingles, singles)
			require.Equal(t, tc.wantRanges, ranges)
			require.NoError(t, batch.Commit(false /* sync */))
			require.Equal(t, tc.wantIndices, h.raftLogIndices(ctx))
		})
	}
}

// TestEmptyLogRange exercises the search for the smallest raft log index
// in (lo, hi].
func TestEmptyLogRange(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		first, last kvpb.RaftIndex // pre-populated raft entry bounds (inclusive)
		lo, hi      kvpb.RaftIndex // EmptyLogRange args
		wantIdx     kvpb.RaftIndex
	}{
		{wantIdx: 0},
		{lo: 100, wantIdx: 0},
		{hi: 100, wantIdx: 100},
		{first: 15, last: 15, wantIdx: 0},
		{first: 15, last: 15, hi: 14, wantIdx: 14},
		{first: 15, last: 15, lo: 10, hi: 14, wantIdx: 14},
		{first: 15, last: 15, lo: 100, hi: 14, wantIdx: 14}, // lo >= hi is a no-op
		{first: 15, last: 15, hi: 15, wantIdx: 14},
		{first: 15, last: 15, lo: 14, hi: 15, wantIdx: 14},
		{first: 15, last: 15, lo: 15, hi: 15, wantIdx: 15}, // lo >= hi is a no-op
		{first: 15, last: 15, hi: 16, wantIdx: 14},
		{first: 15, last: 15, lo: 14, hi: 16, wantIdx: 14},
		{first: 15, last: 16, hi: 15, wantIdx: 14},
		{first: 15, last: 16, lo: 14, hi: 15, wantIdx: 14},
		{first: 15, last: 16, hi: 100, wantIdx: 14},
		{first: 15, last: 16, lo: 14, hi: 100, wantIdx: 14},
	}
	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			h := newClearRangeHelper(t)
			defer h.close()
			if tc.first > 0 {
				h.populate(ctx, uint64(tc.first), uint64(tc.last))
			}
			idx, err := EmptyLogRange(ctx, h.eng, h.prefixBuf, tc.lo, tc.hi)
			require.NoError(t, err)
			require.Equal(t, tc.wantIdx, idx)
		})
	}
}
