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
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestRaftStorageWrites(t *testing.T) {
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
			newState, err = logAppend(ctx, sl.RaftLogPrefix(), rw, state, entries)
			require.NoError(t, err)
		})
		state = newState
		require.Equal(t, stats(), state.ByteSize)
		printCommand(name, batch)
	}
	truncate := func(name string, ts kvserverpb.RaftTruncatedState) {
		t.Helper()
		batch := writeBatch(func(rw storage.ReadWriter) {
			require.NoError(t, Compact(ctx, trunc, ts, sl, rw))
		})
		trunc = ts
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
	truncate("truncate at 103", kvserverpb.RaftTruncatedState{Index: 103, Term: 22})
	truncate("truncate all", kvserverpb.RaftTruncatedState{Index: 105, Term: 22})

	// TODO(pav-kv): print the engine content as well.

	output = strings.ReplaceAll(output, "\n\n", "\n")
	output = strings.ReplaceAll(output, "\n\n", "\n")
	echotest.Require(t, output, filepath.Join("testdata", t.Name()+".txt"))
}

// TestClearRange verifies the choice between per-entry point deletes and a
// single Pebble range tombstone across ClearRange's two modes (sizeKnown vs
// scan-based).
func TestClearRange(t *testing.T) {
	ctx := context.Background()
	const rangeID = roachpb.RangeID(123)
	prefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)

	// countOps tallies point deletes and range deletes recorded in a Pebble
	// batch's wire representation.
	countOps := func(t *testing.T, batchRepr []byte) (points, ranges int) {
		t.Helper()
		r, err := storage.NewBatchReader(batchRepr)
		require.NoError(t, err)
		for r.Next() {
			switch r.KeyKind() {
			case pebble.InternalKeyKindDelete, pebble.InternalKeyKindDeleteSized:
				points++
			case pebble.InternalKeyKindRangeDelete:
				ranges++
			}
		}
		return points, ranges
	}

	// Populate the following Raft log entries: [5,6,7,8,9].
	populate := func(t *testing.T, eng storage.Engine) {
		t.Helper()
		entries := []raftpb.Entry{
			{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1},
			{Index: 8, Term: 1}, {Index: 9, Term: 1},
		}
		b := eng.NewBatch()
		defer b.Close()
		_, err := logAppend(ctx, prefixBuf.RaftLogPrefix(), b, RaftState{}, entries)
		require.NoError(t, err)
		require.NoError(t, b.Commit(true /* sync */))
	}

	// Returns the raft log indices still present in the engine.
	remainingIndices := func(t *testing.T, eng storage.Engine) []kvpb.RaftIndex {
		t.Helper()
		prefix := prefixBuf.RaftLogPrefix()
		iter, err := eng.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
			LowerBound: prefix, UpperBound: prefix.PrefixEnd(),
		})
		require.NoError(t, err)
		defer iter.Close()
		var indices []kvpb.RaftIndex
		iter.SeekGE(storage.MakeMVCCMetadataKey(prefix))
		for {
			ok, err := iter.Valid()
			require.NoError(t, err)
			if !ok {
				break
			}
			key := iter.UnsafeKey().Key
			idx, err := keys.DecodeRaftLogKeyFromSuffix(key[len(prefix):])
			require.NoError(t, err)
			indices = append(indices, idx)
			iter.Next()
		}
		return indices
	}

	tests := []struct {
		lo, hi                 kvpb.RaftIndex
		threshold              int
		sizeKnown              bool
		wantPoints, wantRanges int
		wantIndices            []kvpb.RaftIndex // raft log indices remaining in eng
	}{
		{lo: 0, hi: math.MaxUint64, threshold: 1, sizeKnown: false, wantPoints: 0, wantRanges: 1},
		{lo: 0, hi: math.MaxUint64, threshold: 1, sizeKnown: true, wantPoints: 0, wantRanges: 1},
		{lo: 0, hi: math.MaxUint64, threshold: 6, sizeKnown: false, wantPoints: 5, wantRanges: 0},
		// If sizeKnown=true, the count is calculated by hi-lo.
		{lo: 0, hi: math.MaxUint64, threshold: 6, sizeKnown: true, wantPoints: 0, wantRanges: 1},

		{lo: 1, hi: 6, threshold: 3, sizeKnown: false, wantPoints: 1, wantRanges: 0, wantIndices: []kvpb.RaftIndex{6, 7, 8, 9}},
		{lo: 1, hi: 6, threshold: 3, sizeKnown: true, wantPoints: 0, wantRanges: 1, wantIndices: []kvpb.RaftIndex{6, 7, 8, 9}},
		{lo: 1, hi: 6, threshold: 6, sizeKnown: false, wantPoints: 1, wantRanges: 0, wantIndices: []kvpb.RaftIndex{6, 7, 8, 9}},
		{lo: 1, hi: 6, threshold: 6, sizeKnown: true, wantPoints: 5, wantRanges: 0, wantIndices: []kvpb.RaftIndex{6, 7, 8, 9}},

		{lo: 1, hi: 11, threshold: 3, sizeKnown: false, wantPoints: 0, wantRanges: 1},
		{lo: 1, hi: 11, threshold: 3, sizeKnown: true, wantPoints: 0, wantRanges: 1},
		{lo: 1, hi: 11, threshold: 6, sizeKnown: false, wantPoints: 5, wantRanges: 0},
		{lo: 1, hi: 11, threshold: 6, sizeKnown: true, wantPoints: 0, wantRanges: 1},

		{lo: 5, hi: 9, threshold: 3, sizeKnown: false, wantPoints: 0, wantRanges: 1, wantIndices: []kvpb.RaftIndex{9}},
		{lo: 5, hi: 9, threshold: 3, sizeKnown: true, wantPoints: 0, wantRanges: 1, wantIndices: []kvpb.RaftIndex{9}},
		{lo: 5, hi: 9, threshold: 6, sizeKnown: false, wantPoints: 4, wantRanges: 0, wantIndices: []kvpb.RaftIndex{9}},
		{lo: 5, hi: 9, threshold: 6, sizeKnown: true, wantPoints: 4, wantRanges: 0, wantIndices: []kvpb.RaftIndex{9}},

		{lo: 5, hi: 5, threshold: 1, sizeKnown: true, wantPoints: 0, wantRanges: 0, wantIndices: []kvpb.RaftIndex{5, 6, 7, 8, 9}},  // no-op
		{lo: 10, hi: 5, threshold: 1, sizeKnown: true, wantPoints: 0, wantRanges: 0, wantIndices: []kvpb.RaftIndex{5, 6, 7, 8, 9}}, // no-op
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()
			populate(t, eng)
			batch := eng.NewBatch()
			defer batch.Close()
			var r storage.Reader
			if !tc.sizeKnown {
				r = eng
			}
			require.NoError(t, ClearRange(
				ctx, r, batch, prefixBuf, tc.lo, tc.hi, tc.threshold, tc.sizeKnown,
			))
			points, ranges := countOps(t, batch.Repr())
			require.Equal(t, tc.wantPoints, points)
			require.Equal(t, tc.wantRanges, ranges)
			require.NoError(t, batch.Commit(true /* sync */))
			require.Equal(t, tc.wantIndices, remainingIndices(t, eng), "remaining raft indices")
		})
	}
}
