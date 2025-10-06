// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestMatch(t *testing.T) {
	init := entryID{}.append(1, 2, 3)
	ids := make([]entryID, init.lastIndex()+1)
	for i := range ids {
		ids[i] = entryID{term: init.termAt(uint64(i)), index: uint64(i)}
	}
	for _, tt := range []struct {
		sl    LogSlice
		notOk bool
		want  uint64
	}{
		// prev does not match the log
		{sl: entryID{term: 10, index: 1}.append(), notOk: true},
		{sl: entryID{term: 4, index: 1}.append(4, 4), notOk: true},
		{sl: entryID{term: 5, index: 2}.append(5, 6), notOk: true},
		// no conflict, empty slice
		{sl: LogSlice{}, want: 0},
		// no conflict
		{sl: ids[0].append(1, 2, 3), want: 3},
		{sl: ids[1].append(2, 3), want: 3},
		{sl: ids[2].append(3), want: 3},
		// no conflict, but has new entries
		{sl: ids[0].append(1, 2, 3, 4, 4), want: 3},
		{sl: ids[1].append(2, 3, 4, 4), want: 3},
		{sl: ids[2].append(3, 4, 4), want: 3},
		{sl: ids[3].append(4, 4), want: 3},
		// passes prev check, but conflicts with existing entries
		{sl: ids[0].append(4, 4), want: 0},
		{sl: ids[1].append(1, 4, 4), want: 1},
		{sl: ids[2].append(2, 2, 4, 4), want: 2},
		// out of bounds
		{sl: entryID{term: 3, index: 10}.append(3), notOk: true},
		// just touching the right bound, but still out of bounds
		{sl: entryID{term: 3, index: 4}.append(3, 3, 4), notOk: true},
	} {
		t.Run("", func(t *testing.T) {
			log := newLog(NewMemoryStorage(), raftlogger.DiscardLogger)
			require.True(t, log.append(init))
			match, ok := log.match(tt.sl)
			require.Equal(t, !tt.notOk, ok)
			require.Equal(t, tt.want, match)
		})
	}
}

func TestFindConflictByTerm(t *testing.T) {
	noSnap := entryID{}
	snap10 := entryID{term: 3, index: 10}
	for _, tt := range []struct {
		sl    LogSlice
		index uint64
		term  uint64
		want  uint64
	}{
		// Log starts from index 1.
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 100, term: 2, want: 100}, // ErrUnavailable
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 5, term: 6, want: 5},
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 5, term: 5, want: 5},
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 5, term: 4, want: 2},
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 5, term: 2, want: 2},
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 5, term: 1, want: 0},
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 1, term: 2, want: 1},
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 1, term: 1, want: 0},
		{sl: noSnap.append(2, 2, 5, 5, 5), index: 0, term: 0, want: 0},
		// Log with compacted entries.
		{sl: snap10.append(3, 3, 4, 4, 4), index: 30, term: 3, want: 30}, // ErrUnavailable
		{sl: snap10.append(3, 3, 4, 4, 4), index: 14, term: 9, want: 14},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 14, term: 4, want: 14},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 14, term: 3, want: 12},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 14, term: 2, want: 9},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 11, term: 5, want: 11},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 10, term: 5, want: 10},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 10, term: 3, want: 10},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 10, term: 2, want: 9},
		{sl: snap10.append(3, 3, 4, 4, 4), index: 9, term: 2, want: 9}, // ErrCompacted
		{sl: snap10.append(3, 3, 4, 4, 4), index: 4, term: 2, want: 4}, // ErrCompacted
		{sl: snap10.append(3, 3, 4, 4, 4), index: 0, term: 0, want: 0}, // ErrCompacted
	} {
		t.Run("", func(t *testing.T) {
			st := NewMemoryStorage()
			st.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{
				Term:  tt.sl.prev.term,
				Index: tt.sl.prev.index,
			}})
			l := newLog(st, raftlogger.DiscardLogger)
			require.True(t, l.append(tt.sl))

			index, term := l.findConflictByTerm(tt.index, tt.term)
			require.Equal(t, tt.want, index)
			wantTerm, err := l.term(index)
			wantTerm = l.zeroTermOnOutOfBounds(wantTerm, err)
			require.Equal(t, wantTerm, term)
		})
	}
}

func TestIsUpToDate(t *testing.T) {
	init := entryID{}.append(1, 1, 2, 2, 3)
	raftLog := newLog(NewMemoryStorage(), raftlogger.DiscardLogger)
	require.True(t, raftLog.append(init))
	last := raftLog.lastEntryID()
	require.Equal(t, entryID{term: 3, index: 5}, last)
	for _, tt := range []struct {
		term  uint64
		index uint64
		want  bool
	}{
		// greater term, ignore last index
		{term: last.term + 1, index: last.index - 1, want: true},
		{term: last.term + 1, index: last.index, want: true},
		{term: last.term + 1, index: last.index + 1, want: true},
		// smaller term, ignore last index
		{term: last.term - 1, index: last.index - 1, want: false},
		{term: last.term - 1, index: last.index, want: false},
		{term: last.term - 1, index: last.index + 1, want: false},
		// equal term, lager or equal index wins
		{term: last.term, index: last.index - 1, want: false},
		{term: last.term, index: last.index, want: true},
		{term: last.term, index: last.index + 1, want: true},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.want, raftLog.isUpToDate(entryID{term: tt.term, index: tt.index}))
		})
	}
}

func TestAppend(t *testing.T) {
	init := entryID{}.append(1, 2, 2)
	for _, tt := range []struct {
		app   LogSlice
		want  LogSlice
		notOk bool
	}{
		// appends not at the end of the log
		{app: LogSlice{}, notOk: true},
		{app: entryID{term: 1, index: 1}.append(3), notOk: true},
		{app: entryID{term: 2, index: 4}.append(3), notOk: true},
		// appends at the end of the log
		{
			app:  entryID{term: 2, index: 3}.append(2),
			want: entryID{}.append(1, 2, 2, 2),
		}, {
			app:  entryID{term: 2, index: 3}.append(),
			want: entryID{}.append(1, 2, 2),
		},
	} {
		t.Run("", func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.SetHardState(pb.HardState{Term: init.term}))
			require.NoError(t, storage.Append(init.entries))
			raftLog := newLog(storage, raftlogger.DiscardLogger)

			require.Equal(t, !tt.notOk, raftLog.append(tt.app))
			if tt.notOk {
				require.Equal(t, init.entries, raftLog.allEntries())
				return
			}
			// TODO(pav-kv): check the term and prev too.
			require.Equal(t, tt.want.entries, raftLog.allEntries())
			if len(tt.app.entries) != 0 {
				require.Equal(t, tt.app.lastIndex(), raftLog.lastIndex())
				require.Equal(t, tt.app.prev, raftLog.unstable.prev)
			}
		})
	}
}

// TestLogMaybeAppend tests the behaviour of maybeAppend method:
//  1. The request is rejected if the "previous entry" check fails, when the
//     corresponding entry is missing or has a mismatching term.
//  2. If the request is accepted, the slice is appended to the log, and all the
//     entries at higher indices are truncated.
func TestLogMaybeAppend(t *testing.T) {
	init := entryID{}.append(1, 2, 3)
	last := init.lastEntryID()
	commit := uint64(1)

	for _, tt := range []struct {
		app   LogSlice
		want  []pb.Entry
		notOk bool
		panic bool
	}{
		// rejected appends
		{
			app: entryID{term: 2, index: 3}.append(4), want: init.entries,
			notOk: true, // term mismatch
		}, {
			app: entryID{term: 3, index: 4}.append(4), want: init.entries,
			notOk: true, // out of bounds
		},
		// appends at the end of the log
		{app: last.append(), want: init.entries},
		{app: last.append(4), want: index(1).terms(1, 2, 3, 4)},
		{app: last.append(4, 4), want: index(1).terms(1, 2, 3, 4, 4)},
		// appends from before the end of the log
		{app: LogSlice{}, want: init.entries},
		{app: entryID{term: 1, index: 1}.append(4), want: index(1).terms(1, 4)},
		{app: entryID{term: 1, index: 1}.append(4, 4), want: index(1).terms(1, 4, 4)},
		{app: entryID{term: 2, index: 2}.append(4), want: index(1).terms(1, 2, 4)},
		// panics
		{
			app:   entryID{}.append(4),
			panic: true, // conflict with existing committed entry
		},
	} {
		// TODO(pav-kv): for now, we pick a high enough app.term so that it
		// represents a valid append message. The maybeAppend currently ignores it,
		// but it must check that the append does not regress the term.
		app := tt.app
		app.term = 100
		require.NoError(t, app.valid())

		raftLog := newLog(NewMemoryStorage(), raftlogger.DiscardLogger)
		require.True(t, raftLog.append(init))
		raftLog.committed = commit

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.panic)
				}
			}()
			ok := raftLog.maybeAppend(app)
			require.Equal(t, !tt.notOk, ok)
			require.False(t, tt.panic)
			require.Equal(t, commit, raftLog.committed) // commit index did not change
			require.Equal(t, tt.want, raftLog.allEntries())
		})
	}
}

// TestCompactionSideEffects ensures that all the log related functionality
// works correctly after a compaction.
func TestCompactionSideEffects(t *testing.T) {
	// Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
	stable := entryID{}.append(intRange[uint64](1, 751)...)
	unstable := stable.lastEntryID().append(intRange[uint64](751, 1001)...)

	storage := NewMemoryStorage()
	require.NoError(t, storage.SetHardState(pb.HardState{Term: stable.term}))
	require.NoError(t, storage.Append(stable.entries))
	raftLog := newLog(storage, raftlogger.DiscardLogger)
	require.True(t, raftLog.append(unstable))

	raftLog.commitTo(unstable.mark())
	raftLog.appliedTo(raftLog.committed, 0 /* size */)

	offset := uint64(500)
	require.NoError(t, storage.Compact(offset))
	require.Equal(t, unstable.lastEntryID(), raftLog.lastEntryID())

	for j := offset; j <= raftLog.lastIndex(); j++ {
		require.Equal(t, j, mustTerm(raftLog.term(j)))
	}
	for j := offset; j <= raftLog.lastIndex(); j++ {
		require.True(t, raftLog.matchTerm(entryID{term: j, index: j}))
	}

	unstableEnts := raftLog.nextUnstableEnts()
	require.Equal(t, 250, len(unstableEnts))
	require.Equal(t, uint64(751), unstableEnts[0].Index)

	last := raftLog.lastEntryID()
	require.True(t, raftLog.append(last.append(last.term+1)))
	require.Equal(t, last.index+1, raftLog.lastIndex())

	want := append(stable.entries[offset:], unstable.entries...)
	want = append(want, pb.Entry{Term: last.term + 1, Index: last.index + 1})
	require.Equal(t, want, raftLog.allEntries())
}

func TestHasNextCommittedEnts(t *testing.T) {
	snap := snapshot{
		term: 1,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{Term: 1, Index: 3}},
	}
	init := entryID{term: 1, index: 3}.append(1, 1, 1)
	for _, tt := range []struct {
		applied       uint64
		applying      uint64
		allowUnstable bool
		paused        bool
		snap          bool
		whasNext      bool
	}{
		{applied: 3, applying: 3, allowUnstable: true, whasNext: true},
		{applied: 3, applying: 4, allowUnstable: true, whasNext: true},
		{applied: 3, applying: 5, allowUnstable: true, whasNext: false},
		{applied: 4, applying: 4, allowUnstable: true, whasNext: true},
		{applied: 4, applying: 5, allowUnstable: true, whasNext: false},
		{applied: 5, applying: 5, allowUnstable: true, whasNext: false},
		// Don't allow unstable entries.
		{applied: 3, applying: 3, allowUnstable: false, whasNext: true},
		{applied: 3, applying: 4, allowUnstable: false, whasNext: false},
		{applied: 3, applying: 5, allowUnstable: false, whasNext: false},
		{applied: 4, applying: 4, allowUnstable: false, whasNext: false},
		{applied: 4, applying: 5, allowUnstable: false, whasNext: false},
		{applied: 5, applying: 5, allowUnstable: false, whasNext: false},
		// Paused.
		{applied: 3, applying: 3, allowUnstable: true, paused: true, whasNext: false},
		// With snapshot.
		{applied: 3, applying: 3, allowUnstable: true, snap: true, whasNext: false},
	} {
		t.Run("", func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap.snap))
			raftLog := newLog(storage, raftlogger.DiscardLogger)
			require.True(t, raftLog.append(init))
			require.NoError(t, storage.Append(init.entries[:1]))

			raftLog.stableTo(LogMark{Term: init.term, Index: 4})
			raftLog.commitTo(LogMark{Term: init.term, Index: 5})
			raftLog.appliedTo(tt.applied, 0 /* size */)
			raftLog.acceptApplying(tt.applying, 0 /* size */, tt.allowUnstable)
			raftLog.applyingEntsPaused = tt.paused
			if tt.snap {
				newSnap := snap
				newSnap.snap.Metadata.Index = init.lastIndex() + 1
				require.True(t, raftLog.restore(newSnap))
			}
			require.Equal(t, tt.whasNext, raftLog.hasNextCommittedEnts(tt.allowUnstable))
		})
	}
}

func TestNextCommittedEnts(t *testing.T) {
	snap := snapshot{
		term: 1,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{Term: 1, Index: 3}},
	}
	init := entryID{term: 1, index: 3}.append(1, 1, 1)
	for _, tt := range []struct {
		applied       uint64
		applying      uint64
		allowUnstable bool
		paused        bool
		snap          bool
		wents         []pb.Entry
	}{
		{applied: 3, applying: 3, allowUnstable: true, wents: init.entries[:2]},
		{applied: 3, applying: 4, allowUnstable: true, wents: init.entries[1:2]},
		{applied: 3, applying: 5, allowUnstable: true, wents: nil},
		{applied: 4, applying: 4, allowUnstable: true, wents: init.entries[1:2]},
		{applied: 4, applying: 5, allowUnstable: true, wents: nil},
		{applied: 5, applying: 5, allowUnstable: true, wents: nil},
		// Don't allow unstable entries.
		{applied: 3, applying: 3, allowUnstable: false, wents: init.entries[:1]},
		{applied: 3, applying: 4, allowUnstable: false, wents: nil},
		{applied: 3, applying: 5, allowUnstable: false, wents: nil},
		{applied: 4, applying: 4, allowUnstable: false, wents: nil},
		{applied: 4, applying: 5, allowUnstable: false, wents: nil},
		{applied: 5, applying: 5, allowUnstable: false, wents: nil},
		// Paused.
		{applied: 3, applying: 3, allowUnstable: true, paused: true, wents: nil},
		// With snapshot.
		{applied: 3, applying: 3, allowUnstable: true, snap: true, wents: nil},
	} {
		t.Run("", func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap.snap))
			raftLog := newLog(storage, raftlogger.DiscardLogger)
			require.True(t, raftLog.append(init))
			require.NoError(t, storage.Append(init.entries[:1]))

			raftLog.stableTo(LogMark{Term: init.term, Index: 4})
			raftLog.commitTo(LogMark{Term: init.term, Index: 5})
			raftLog.appliedTo(tt.applied, 0 /* size */)
			raftLog.acceptApplying(tt.applying, 0 /* size */, tt.allowUnstable)
			raftLog.applyingEntsPaused = tt.paused
			if tt.snap {
				newSnap := snap
				newSnap.snap.Metadata.Index = init.lastIndex() + 1
				require.True(t, raftLog.restore(newSnap))
			}
			require.Equal(t, tt.wents, raftLog.nextCommittedEnts(tt.allowUnstable))
		})
	}
}

func TestAcceptApplying(t *testing.T) {
	maxSize := entryEncodingSize(100)
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	init := entryID{term: 1, index: 3}.append(1, 1, 1)
	for _, tt := range []struct {
		index         uint64
		allowUnstable bool
		size          entryEncodingSize
		wpaused       bool
	}{
		{index: 3, allowUnstable: true, size: maxSize - 1, wpaused: true},
		{index: 3, allowUnstable: true, size: maxSize, wpaused: true},
		{index: 3, allowUnstable: true, size: maxSize + 1, wpaused: true},
		{index: 4, allowUnstable: true, size: maxSize - 1, wpaused: true},
		{index: 4, allowUnstable: true, size: maxSize, wpaused: true},
		{index: 4, allowUnstable: true, size: maxSize + 1, wpaused: true},
		{index: 5, allowUnstable: true, size: maxSize - 1, wpaused: false},
		{index: 5, allowUnstable: true, size: maxSize, wpaused: true},
		{index: 5, allowUnstable: true, size: maxSize + 1, wpaused: true},
		// Don't allow unstable entries.
		{index: 3, allowUnstable: false, size: maxSize - 1, wpaused: true},
		{index: 3, allowUnstable: false, size: maxSize, wpaused: true},
		{index: 3, allowUnstable: false, size: maxSize + 1, wpaused: true},
		{index: 4, allowUnstable: false, size: maxSize - 1, wpaused: false},
		{index: 4, allowUnstable: false, size: maxSize, wpaused: true},
		{index: 4, allowUnstable: false, size: maxSize + 1, wpaused: true},
		{index: 5, allowUnstable: false, size: maxSize - 1, wpaused: false},
		{index: 5, allowUnstable: false, size: maxSize, wpaused: true},
		{index: 5, allowUnstable: false, size: maxSize + 1, wpaused: true},
	} {
		t.Run("", func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			raftLog := newLogWithSize(storage, raftlogger.DiscardLogger, maxSize)
			require.True(t, raftLog.append(init))
			require.NoError(t, storage.Append(init.entries[:1]))

			raftLog.stableTo(LogMark{Term: init.term, Index: 4})
			raftLog.commitTo(LogMark{Term: init.term, Index: 5})
			raftLog.appliedTo(3, 0 /* size */)

			raftLog.acceptApplying(tt.index, tt.size, tt.allowUnstable)
			require.Equal(t, tt.wpaused, raftLog.applyingEntsPaused)
		})
	}
}

func TestAppliedTo(t *testing.T) {
	maxSize := entryEncodingSize(100)
	overshoot := entryEncodingSize(5)
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	init := entryID{term: 1, index: 3}.append(1, 1, 1)
	for _, tt := range []struct {
		index         uint64
		size          entryEncodingSize
		wapplyingSize entryEncodingSize
		wpaused       bool
	}{
		// Apply some of in-progress entries (applying = 5 below).
		{index: 4, size: overshoot - 1, wapplyingSize: maxSize + 1, wpaused: true},
		{index: 4, size: overshoot, wapplyingSize: maxSize, wpaused: true},
		{index: 4, size: overshoot + 1, wapplyingSize: maxSize - 1, wpaused: false},
		// Apply all of in-progress entries.
		{index: 5, size: overshoot - 1, wapplyingSize: maxSize + 1, wpaused: true},
		{index: 5, size: overshoot, wapplyingSize: maxSize, wpaused: true},
		{index: 5, size: overshoot + 1, wapplyingSize: maxSize - 1, wpaused: false},
		// Apply all of outstanding bytes.
		{index: 4, size: maxSize + overshoot, wapplyingSize: 0, wpaused: false},
		// Apply more than outstanding bytes.
		// Incorrect accounting doesn't underflow applyingSize.
		{index: 4, size: maxSize + overshoot + 1, wapplyingSize: 0, wpaused: false},
	} {
		t.Run("", func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			raftLog := newLogWithSize(storage, raftlogger.DiscardLogger, maxSize)
			require.True(t, raftLog.append(init))
			require.NoError(t, storage.Append(init.entries[:1]))

			raftLog.stableTo(LogMark{Term: init.term, Index: 4})
			raftLog.commitTo(LogMark{Term: init.term, Index: 5})
			raftLog.appliedTo(3, 0 /* size */)
			raftLog.acceptApplying(5, maxSize+overshoot, false /* allowUnstable */)

			raftLog.appliedTo(tt.index, tt.size)
			require.Equal(t, tt.index, raftLog.applied)
			require.Equal(t, uint64(5), raftLog.applying)
			require.Equal(t, tt.wapplyingSize, raftLog.applyingEntsSize)
			require.Equal(t, tt.wpaused, raftLog.applyingEntsPaused)
		})
	}
}

// TestNextUnstableEnts ensures nextUnstableEnts returns the unstable part of
// the entries correctly, before and after making them stable.
func TestNextUnstableEnts(t *testing.T) {
	init := entryID{}.append(1, 2)
	for _, tt := range []LogSlice{
		init.lastEntryID().append(),
		init.lastEntryID().append(2, 2),
		init.lastEntryID().append(3, 4, 5, 6),
	} {
		t.Run("", func(t *testing.T) {
			// append stable entries to storage
			storage := NewMemoryStorage()
			require.NoError(t, storage.SetHardState(pb.HardState{Term: init.term}))
			require.NoError(t, storage.Append(init.entries))

			// append unstable entries to raftLog
			raftLog := newLog(storage, raftlogger.DiscardLogger)
			require.True(t, raftLog.append(tt))
			require.Equal(t, tt.prev, raftLog.unstable.prev)

			require.Equal(t, len(tt.entries) != 0, raftLog.hasNextUnstableEnts())
			require.Equal(t, tt.entries, raftLog.nextUnstableEnts())
			if len(tt.entries) != 0 {
				raftLog.stableTo(tt.mark())
			}
			require.Equal(t, tt.lastEntryID(), raftLog.unstable.prev)
		})
	}
}

func TestCommitTo(t *testing.T) {
	init := entryID{}.append(1, 2, 3)
	commit := uint64(2)
	for _, tt := range []struct {
		commit LogMark
		want   uint64
		panic  bool
	}{
		{commit: LogMark{Term: 3, Index: 3}, want: 3},
		{commit: LogMark{Term: 3, Index: 2}, want: 2},     // commit does not regress
		{commit: LogMark{Term: 3, Index: 4}, panic: true}, // commit out of range -> panic
		// TODO(pav-kv): add commit marks with a different term.
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.panic)
				}
			}()
			raftLog := newLog(NewMemoryStorage(), raftlogger.DiscardLogger)
			require.True(t, raftLog.append(init))
			raftLog.committed = commit
			raftLog.commitTo(tt.commit)
			require.Equal(t, tt.want, raftLog.committed)
		})
	}
}

func TestStableTo(t *testing.T) {
	init := entryID{}.append(1, 2)
	for _, tt := range []struct {
		mark LogMark
		want uint64 // prev.index
	}{
		// out of bounds
		{mark: LogMark{Term: 2, Index: 0}, want: 0},
		{mark: LogMark{Term: 2, Index: 3}, want: 0},
		// outdated accepted term
		{mark: LogMark{Term: 1, Index: 1}, want: 0},
		{mark: LogMark{Term: 1, Index: 2}, want: 0},
		// successful acknowledgements
		{mark: LogMark{Term: 2, Index: 1}, want: 1},
		{mark: LogMark{Term: 2, Index: 2}, want: 2},
	} {
		t.Run("", func(t *testing.T) {
			raftLog := newLog(NewMemoryStorage(), raftlogger.DiscardLogger)
			require.True(t, raftLog.append(init))
			raftLog.stableTo(tt.mark)
			require.Equal(t, tt.want, raftLog.unstable.prev.index)
		})
	}
}

func TestStableToWithSnap(t *testing.T) {
	snapID := entryID{term: 2, index: 5}
	snap := pb.Snapshot{Metadata: pb.SnapshotMetadata{Term: snapID.term, Index: snapID.index}}
	for _, tt := range []struct {
		sl   LogSlice
		to   LogMark
		want uint64 // prev.index
	}{
		// out of bounds
		{sl: snapID.append(), to: LogMark{Term: 1, Index: 2}, want: 5},
		{sl: snapID.append(), to: LogMark{Term: 2, Index: 6}, want: 5},
		{sl: snapID.append(), to: LogMark{Term: 2, Index: 7}, want: 5},
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 2, Index: 4}, want: 5},
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 2, Index: 10}, want: 5},
		// successful acknowledgements
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 8, Index: 5}, want: 5},
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 8, Index: 6}, want: 6},
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 8, Index: 7}, want: 7},
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 8, Index: 8}, want: 8},
		// mismatching accepted term
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 3, Index: 6}, want: 5},
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 3, Index: 7}, want: 5},
		{sl: snapID.append(6, 6, 8), to: LogMark{Term: 3, Index: 8}, want: 5},
	} {
		t.Run("", func(t *testing.T) {
			s := NewMemoryStorage()
			require.NoError(t, s.SetHardState(pb.HardState{Term: snapID.term}))
			require.NoError(t, s.ApplySnapshot(snap))
			raftLog := newLog(s, raftlogger.DiscardLogger)
			require.True(t, raftLog.append(tt.sl))
			raftLog.stableTo(tt.to)
			require.Equal(t, tt.want, raftLog.unstable.prev.index)
		})
	}
}

// TestCompaction ensures that the number of log entries is correct after compactions.
func TestCompaction(t *testing.T) {
	for _, tt := range []struct {
		lastIndex uint64
		compact   []uint64
		wleft     []int
		wallow    bool
	}{
		// out of upper bound
		{1000, []uint64{1001}, []int{-1}, false},
		{1000, []uint64{300, 500, 800, 900}, []int{700, 500, 200, 100}, true},
		// out of lower bound
		{1000, []uint64{300, 299}, []int{700, -1}, false},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.False(t, tt.wallow)
				}
			}()
			storage := NewMemoryStorage()
			init := entryID{}.append(intRange(1, tt.lastIndex+1)...)
			require.NoError(t, storage.Append(init.entries))
			raftLog := newLog(storage, raftlogger.DiscardLogger)
			raftLog.commitTo(init.mark())

			raftLog.appliedTo(raftLog.committed, 0 /* size */)
			for j := 0; j < len(tt.compact); j++ {
				err := storage.Compact(tt.compact[j])
				if err != nil {
					require.False(t, tt.wallow)
					continue
				}
				require.Equal(t, tt.wleft[j], len(raftLog.allEntries()))
			}
		})
	}
}

func TestLogRestore(t *testing.T) {
	index := uint64(1000)
	term := uint64(1000)
	snap := pb.SnapshotMetadata{Index: index, Term: term}
	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: snap})
	raftLog := newLog(storage, raftlogger.DiscardLogger)

	require.Zero(t, len(raftLog.allEntries()))
	require.Equal(t, index+1, raftLog.firstIndex())
	require.Equal(t, index, raftLog.committed)
	require.Equal(t, index, raftLog.unstable.prev.index)
	require.Equal(t, term, mustTerm(raftLog.term(index)))
}

func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Term: 1, Index: offset}})
	l := newLog(storage, raftlogger.DiscardLogger)
	require.True(t, l.append(entryID{term: 1, index: offset}.
		append(intRange(offset+1, offset+num+1)...)))

	first := offset + 1
	for _, tt := range []struct {
		lo, hi        uint64
		wpanic        bool
		wErrCompacted bool
	}{
		{lo: first - 3, hi: first, wErrCompacted: true},
		{lo: first - 2, hi: first, wErrCompacted: true},
		{lo: first - 1, hi: first},
		{lo: first + num/2, hi: first + num/2},
		{lo: first + num - 1, hi: first + num - 1},
		{lo: first + num - 2, hi: first + num - 1},
		{lo: first + num, hi: first + num, wpanic: true},
		{lo: first + num, hi: first + num + 1, wpanic: true},
		{lo: first + num + 1, hi: first + num + 1, wpanic: true},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.wpanic)
				}
			}()
			err := l.snap(l.storage).mustCheckOutOfBounds(tt.lo, tt.hi)
			require.False(t, tt.wpanic)
			require.False(t, tt.wErrCompacted && err != ErrCompacted)
			require.False(t, !tt.wErrCompacted && err != nil)
		})
	}
}

func TestTerm(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: offset, Term: 1}})
	l := newLog(storage, raftlogger.DiscardLogger)
	require.True(t, l.append(entryID{term: 1, index: offset}.append(intRange(1, num)...)))

	for _, tt := range []struct {
		idx  uint64
		term uint64
		err  error
	}{
		{idx: offset - 1, err: ErrCompacted},
		{idx: offset, term: 1},
		{idx: offset + num/2, term: num / 2},
		{idx: offset + num - 1, term: num - 1},
		{idx: offset + num, err: ErrUnavailable},
	} {
		t.Run("", func(t *testing.T) {
			term, err := l.term(tt.idx)
			require.Equal(t, tt.term, term)
			require.Equal(t, tt.err, err)
		})
	}
}

func TestTermWithUnstableSnapshot(t *testing.T) {
	storagesnapi := uint64(100)
	unstablesnapi := storagesnapi + 5

	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: storagesnapi, Term: 1}})
	l := newLog(storage, raftlogger.DiscardLogger)
	require.True(t, l.restore(snapshot{
		term: 1,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: unstablesnapi, Term: 1}},
	}))

	for _, tt := range []struct {
		idx  uint64
		term uint64
		err  error
	}{
		// cannot get term from storage
		{idx: storagesnapi, err: ErrCompacted},
		// cannot get term from the gap between storage ents and unstable snapshot
		{idx: storagesnapi + 1, err: ErrCompacted},
		{idx: unstablesnapi - 1, err: ErrCompacted},
		// get term from unstable snapshot index
		{idx: unstablesnapi, term: 1},
		// the log beyond the unstable snapshot is empty
		{idx: unstablesnapi + 1, err: ErrUnavailable},
	} {
		t.Run("", func(t *testing.T) {
			term, err := l.term(tt.idx)
			require.Equal(t, tt.term, term)
			require.Equal(t, tt.err, err)
		})
	}
}

func TestSlice(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	last := offset + num
	half := offset + num/2
	halfe := pb.Entry{Index: half, Term: half}

	entries := func(lo, hi uint64) []pb.Entry { // (lo, hi]
		return index(lo+1).termRange(lo+1, hi+1)
	}

	storage := NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Index: offset}}))
	require.NoError(t, storage.Append(entries(offset, half)))
	l := newLog(storage, raftlogger.DiscardLogger)
	require.True(t, l.append(pbEntryID(&halfe).append(intRange(half+1, last+1)...)))

	for _, tt := range []struct {
		lo  uint64
		hi  uint64
		lim uint64

		w      []pb.Entry
		wpanic bool
	}{
		// ErrCompacted.
		{lo: offset - 2, hi: offset, lim: noLimit, w: nil},
		{lo: offset - 1, hi: offset, lim: noLimit, w: nil},
		// panics
		{lo: half, hi: half - 1, lim: noLimit, wpanic: true},     // lo and hi inversion
		{lo: last - 1, hi: last + 1, lim: noLimit, wpanic: true}, // hi is out of bounds

		// No limit.
		{lo: offset, hi: offset, lim: noLimit, w: nil},
		{lo: offset, hi: half - 1, lim: noLimit, w: entries(offset, half-1)},
		{lo: offset, hi: half, lim: noLimit, w: entries(offset, half)},
		{lo: offset, hi: half + 1, lim: noLimit, w: entries(offset, half+1)},
		{lo: offset, hi: last, lim: noLimit, w: entries(offset, last)},
		{lo: half - 1, hi: half, lim: noLimit, w: entries(half-1, half)},
		{lo: half - 1, hi: half + 1, lim: noLimit, w: entries(half-1, half+1)},
		{lo: half - 1, hi: last, lim: noLimit, w: entries(half-1, last)},
		{lo: half, hi: half + 1, lim: noLimit, w: entries(half, half+1)},
		{lo: half, hi: last, lim: noLimit, w: entries(half, last)},
		{lo: last - 1, hi: last, lim: noLimit, w: entries(last-1, last)},

		// At least one entry is always returned.
		{lo: offset, hi: last, lim: 0, w: entries(offset, offset+1)},
		{lo: half - 1, hi: half + 1, lim: 0, w: entries(half-1, half)},
		{lo: half, hi: last, lim: 0, w: entries(half, half+1)},
		{lo: half + 1, hi: last, lim: 0, w: entries(half+1, half+2)},
		// Low limit.
		{lo: offset, hi: last, lim: uint64(halfe.Size() - 1), w: entries(offset, offset+1)},
		{lo: half - 1, hi: half + 1, lim: uint64(halfe.Size() - 1), w: entries(half-1, half)},
		{lo: half, hi: last, lim: uint64(halfe.Size() - 1), w: entries(half, half+1)},
		// Just enough for one limit.
		{lo: offset, hi: last, lim: uint64(halfe.Size()), w: entries(offset, offset+1)},
		{lo: half - 1, hi: half + 1, lim: uint64(halfe.Size()), w: entries(half-1, half)},
		{lo: half, hi: last, lim: uint64(halfe.Size()), w: entries(half, half+1)},
		// Not enough for two limit.
		{lo: offset, hi: last, lim: uint64(halfe.Size() + 1), w: entries(offset, offset+1)},
		{lo: half - 1, hi: half + 1, lim: uint64(halfe.Size() + 1), w: entries(half-1, half)},
		{lo: half, hi: last, lim: uint64(halfe.Size() + 1), w: entries(half, half+1)},
		// Enough for two limit.
		{lo: offset, hi: last, lim: uint64(halfe.Size() * 2), w: entries(offset, offset+2)},
		{lo: half - 2, hi: half + 1, lim: uint64(halfe.Size() * 2), w: entries(half-2, half)},
		{lo: half - 1, hi: half + 1, lim: uint64(halfe.Size() * 2), w: entries(half-1, half+1)},
		{lo: half, hi: last, lim: uint64(halfe.Size() * 2), w: entries(half, half+2)},
		// Not enough for three.
		{lo: half - 2, hi: half + 1, lim: uint64(halfe.Size()*3 - 1), w: entries(half-2, half)},
		// Enough for three.
		{lo: half - 1, hi: half + 2, lim: uint64(halfe.Size() * 3), w: entries(half-1, half+2)},
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.wpanic)
				}
			}()
			g, err := l.slice(tt.lo, tt.hi, entryEncodingSize(tt.lim))
			require.False(t, tt.lo < offset && err != ErrCompacted)
			require.False(t, tt.lo >= offset && err != nil)
			require.Equal(t, tt.w, g)
		})
	}
}

func TestScan(t *testing.T) {
	offset := uint64(47)
	num := uint64(20)
	last := offset + num
	half := offset + num/2
	entries := func(from, to uint64) []pb.Entry {
		return index(from).termRange(from, to)
	}
	entrySize := entsSize(entries(half, half+1))

	storage := NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Index: offset}}))
	require.NoError(t, storage.Append(entries(offset+1, half)))
	l := newLog(storage, raftlogger.DiscardLogger)
	require.True(t, l.append(entryID{term: half - 1, index: half - 1}.
		append(intRange(half, last+1)...)))

	// Test that scan() returns the same entries as slice(), on all inputs.
	for _, pageSize := range []entryEncodingSize{0, 1, 10, 100, entrySize, entrySize + 1} {
		for lo := offset; lo < last; lo++ {
			for hi := lo + 1; hi <= last; hi++ {
				var got []pb.Entry
				require.NoError(t, l.scan(lo, hi, pageSize, func(e []pb.Entry) error {
					got = append(got, e...)
					require.True(t, len(e) == 1 || entsSize(e) <= pageSize)
					return nil
				}))
				want, err := l.slice(lo, hi, noLimit)
				require.NoError(t, err)
				require.Equal(t, want, got, "scan() and slice() mismatch on [%d, %d) @ %d", lo, hi, pageSize)
			}
		}
	}

	// Test that the callback error is propagated to the caller.
	iters := 0
	require.ErrorIs(t, errBreak, l.scan(offset+1, half, 0, func([]pb.Entry) error {
		iters++
		if iters == 2 {
			return errBreak
		}
		return nil
	}))
	require.Equal(t, 2, iters)

	// Test that we max out the limit, and not just always return a single entry.
	// NB: this test works only because the requested range length is even.
	require.NoError(t, l.scan(offset+1, offset+11, entrySize*2, func(ents []pb.Entry) error {
		require.Len(t, ents, 2)
		require.Equal(t, entrySize*2, entsSize(ents))
		return nil
	}))
}

func mustTerm(term uint64, err error) uint64 {
	if err != nil {
		panic(err)
	}
	return term
}

// index is a helper type for generating slices of pb.Entry. The value of index
// is the first entry index in the generated slices.
type index uint64

// terms generates a slice of entries at indices [index, index+len(terms)), with
// the given terms of each entry. Terms must be non-decreasing.
func (i index) terms(terms ...uint64) []pb.Entry {
	if len(terms) == 0 {
		return nil
	}
	index := uint64(i)
	entries := make([]pb.Entry, 0, len(terms))
	for _, term := range terms {
		entries = append(entries, pb.Entry{Term: term, Index: index})
		index++
	}
	return entries
}

// termRange generates a slice of to-from entries, at consecutive indices
// starting from i, and consecutive terms in [from, to).
// TODO(pav-kv): remove.
func (i index) termRange(from, to uint64) []pb.Entry {
	return i.terms(intRange(from, to)...)
}

// append generates a valid LogSlice of entries appended after the given entry
// ID, at indices [id.index+1, id.index+len(terms)], with the given terms of
// each entry. Terms must be >= id.term, and non-decreasing.
func (id entryID) append(terms ...uint64) LogSlice {
	term := id.term
	if ln := len(terms); ln != 0 {
		term = terms[ln-1]
	}
	ls := LogSlice{
		term:    term,
		prev:    id,
		entries: index(id.index + 1).terms(terms...),
	}
	if err := ls.valid(); err != nil {
		panic(err)
	}
	return ls
}

// intRange returns a slice containing integers in [from, to) interval.
func intRange[T constraints.Integer](from, to T) []T {
	slice := make([]T, to-from)
	for i := range slice {
		slice[i] = from + T(i)
	}
	return slice
}
