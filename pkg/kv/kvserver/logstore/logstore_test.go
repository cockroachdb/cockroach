// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

func TestRaftStorageWrites(t *testing.T) {
	// Run with both SingleDelete and Delete-based clears for raft log
	// truncation and overlap-write. The two modes produce different write
	// batches (Single Delete vs Delete), so each gets its own testdata file.
	//
	// Under SingleDelete, logAppend's tracked ByteSize is a known upper bound
	// on the on-disk size: SingleClearUnversioned writes directly to the
	// batch without updating MVCCStats, so the bytes of entries that are
	// displaced (overlap-write) or trimmed (trailing-cut) are never
	// subtracted from diff. We predict that drift exactly by tracking each
	// live entry's size, and assert
	//   state.ByteSize == stats() + expectedDrift
	// across both modes (drift stays 0 under Delete).
	testutils.RunTrueAndFalse(t, "single_delete", func(t *testing.T, useSingleDelete bool) {
		UseRaftLogSingleDelete = useSingleDelete
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
		// entrySize returns the SysBytes contribution of an inline raft log
		// entry, mirroring the encoding used inside MVCCBlindPut. Used to
		// predict the SingleDelete drift: when an entry is displaced or
		// trimmed via SingleClearUnversioned, its bytes remain charged to
		// state.ByteSize until the next truncate recalibration.
		entrySize := func(ent raftpb.Entry) int64 {
			key := keys.RaftLogKey(rangeID, kvpb.RaftIndex(ent.Index))
			var v roachpb.Value
			require.NoError(t, v.SetProto(&ent))
			v.InitChecksum(key)
			return int64(storage.MVCCKey{Key: key}.EncodedSize()) +
				int64((&enginepb.MVCCMetadata{RawBytes: v.RawBytes}).Size())
		}

		// SingleDelete-only bookkeeping. liveSizes holds the SysBytes
		// contribution of each currently-live raft log entry; expectedDrift
		// is the cumulative number of bytes by which logAppend's tracked
		// ByteSize is expected to exceed the engine's SysBytes due to
		// SingleClearUnversioned not updating MVCCStats.
		liveSizes := map[kvpb.RaftIndex]int64{}
		expectedDrift := int64(0)
		write := func(name string, hs raftpb.HardState, entries []raftpb.Entry) {
			t.Helper()
			if useSingleDelete {
				// Compute the drift contributed by this call before mutating
				// liveSizes: every overlap-write displaces a live entry, and
				// every trailing-cut trims one, but logAppend doesn't
				// subtract their bytes from diff under SingleDelete.
				for _, e := range entries {
					if old, ok := liveSizes[kvpb.RaftIndex(e.Index)]; ok {
						expectedDrift += old
					}
				}
				newLast := kvpb.RaftIndex(entries[len(entries)-1].Index)
				for i := newLast + 1; i <= state.LastIndex; i++ {
					if sz, ok := liveSizes[i]; ok {
						expectedDrift += sz
					}
				}
				for i := newLast + 1; i <= state.LastIndex; i++ {
					delete(liveSizes, i)
				}
				for _, e := range entries {
					liveSizes[kvpb.RaftIndex(e.Index)] = entrySize(e)
				}
			}

			var newState RaftState
			batch := writeBatch(func(rw storage.ReadWriter) {
				require.NoError(t, storeHardState(ctx, rw, sl, hs))
				var err error
				newState, err = logAppend(ctx, sl.RaftLogPrefix(), rw, state, entries)
				require.NoError(t, err)
			})
			state = newState
			if useSingleDelete {
				require.Equal(t, stats()+expectedDrift, state.ByteSize)
			} else {
				require.Equal(t, stats(), state.ByteSize)
			}
			printCommand(name, batch)
		}
		truncate := func(name string, ts kvserverpb.RaftTruncatedState) {
			t.Helper()
			batch := writeBatch(func(rw storage.ReadWriter) {
				require.NoError(t, Compact(ctx, trunc, ts, sl, rw))
			})
			trunc = ts
			if useSingleDelete {
				// Drop truncated entries from liveSizes and reset the drift;
				// state.ByteSize is recalibrated below.
				for i := range liveSizes {
					if i <= ts.Index {
						delete(liveSizes, i)
					}
				}
				expectedDrift = 0
			}
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
		echotest.Require(t, output, filepath.Join("testdata", t.Name()))
	})
}
