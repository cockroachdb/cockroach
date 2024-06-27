// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
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
	"fmt"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func (l *raftLog) bootstrap(t testing.TB, entries []pb.Entry) {
	last := l.lastEntryID()
	term := last.term
	if ln := len(entries); ln != 0 {
		term = entries[ln-1].Term
	}
	app := logSlice{term: term, prev: last, entries: entries}
	require.NoError(t, app.valid())
	require.True(t, l.maybeAppend(app))
}

func TestFindConflict(t *testing.T) {
	previousEnts := index(1).terms(1, 2, 3)
	ids := make([]entryID, 1, len(previousEnts)+1) // dummy (0, 0) at index 0
	for i := range previousEnts {
		ids = append(ids, pbEntryID(&previousEnts[i]))
	}
	for _, tt := range []struct {
		prev  entryID
		ents  []pb.Entry
		notOk bool
		want  entryID
	}{
		// prev does not match the log
		{prev: entryID{term: 10, index: 1}, notOk: true},
		{prev: entryID{term: 4, index: 1}, ents: index(2).terms(4, 4), notOk: true},
		{prev: entryID{term: 5, index: 2}, ents: index(3).terms(5, 6), notOk: true},
		// no conflict, empty entries
		{ents: nil, want: ids[0]},
		// no conflict
		{prev: ids[0], ents: index(1).terms(1, 2, 3), want: ids[3]},
		{prev: ids[1], ents: index(2).terms(2, 3), want: ids[3]},
		{prev: ids[2], ents: index(3).terms(3), want: ids[3]},
		// no conflict, but has new entries
		{prev: ids[0], ents: index(1).terms(1, 2, 3, 4, 4), want: ids[3]},
		{prev: ids[1], ents: index(2).terms(2, 3, 4, 4), want: ids[3]},
		{prev: ids[2], ents: index(3).terms(3, 4, 4), want: ids[3]},
		{prev: ids[3], ents: index(4).terms(4, 4), want: ids[3]},
		// passes prev check, but conflicts with existing entries
		{prev: ids[0], ents: index(1).terms(4, 4), want: ids[0]},
		{prev: ids[1], ents: index(2).terms(1, 4, 4), want: ids[1]},
		{prev: ids[2], ents: index(3).terms(2, 2, 4, 4), want: ids[2]},
		// out of bounds
		{prev: entryID{term: 3, index: 10}, ents: index(11).terms(3), notOk: true},
		// just touching the right bound, but still out of bounds
		{prev: entryID{term: 3, index: 4}, ents: index(5).terms(3, 3, 4), notOk: true},
	} {
		t.Run("", func(t *testing.T) {
			log := newLog(NewMemoryStorage(), discardLogger)
			log.bootstrap(t, previousEnts)
			app := logSlice{term: 100, prev: tt.prev, entries: tt.ents}
			require.NoError(t, app.valid())
			match, ok := log.findConflict(app)
			require.Equal(t, !tt.notOk, ok)
			require.Equal(t, tt.want, match)
		})
	}
}

func TestFindConflictByTerm(t *testing.T) {
	for _, tt := range []struct {
		ents  []pb.Entry // ents[0] contains the (index, term) of the snapshot
		index uint64
		term  uint64
		want  uint64
	}{
		// Log starts from index 1.
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 100, term: 2, want: 100}, // ErrUnavailable
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 5, term: 6, want: 5},
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 5, term: 5, want: 5},
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 5, term: 4, want: 2},
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 5, term: 2, want: 2},
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 5, term: 1, want: 0},
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 1, term: 2, want: 1},
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 1, term: 1, want: 0},
		{ents: index(0).terms(0, 2, 2, 5, 5, 5), index: 0, term: 0, want: 0},
		// Log with compacted entries.
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 30, term: 3, want: 30}, // ErrUnavailable
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 14, term: 9, want: 14},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 14, term: 4, want: 14},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 14, term: 3, want: 12},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 14, term: 2, want: 9},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 11, term: 5, want: 11},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 10, term: 5, want: 10},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 10, term: 3, want: 10},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 10, term: 2, want: 9},
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 9, term: 2, want: 9}, // ErrCompacted
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 4, term: 2, want: 4}, // ErrCompacted
		{ents: index(10).terms(3, 3, 3, 4, 4, 4), index: 0, term: 0, want: 0}, // ErrCompacted
	} {
		t.Run("", func(t *testing.T) {
			st := NewMemoryStorage()
			require.NotEmpty(t, tt.ents)
			st.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{
				Index: tt.ents[0].Index,
				Term:  tt.ents[0].Term,
			}})
			l := newLog(st, raftLogger)
			l.bootstrap(t, tt.ents[1:])

			index, term := l.findConflictByTerm(tt.index, tt.term)
			require.Equal(t, tt.want, index)
			wantTerm, err := l.term(index)
			wantTerm = l.zeroTermOnOutOfBounds(wantTerm, err)
			require.Equal(t, wantTerm, term)
		})
	}
}

func TestIsUpToDate(t *testing.T) {
	previousEnts := index(1).terms(1, 2, 3)
	raftLog := newLog(NewMemoryStorage(), raftLogger)
	raftLog.bootstrap(t, previousEnts)
	tests := []struct {
		lastIndex uint64
		term      uint64
		wUpToDate bool
	}{
		// greater term, ignore lastIndex
		{raftLog.lastIndex() - 1, 4, true},
		{raftLog.lastIndex(), 4, true},
		{raftLog.lastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{raftLog.lastIndex() - 1, 2, false},
		{raftLog.lastIndex(), 2, false},
		{raftLog.lastIndex() + 1, 2, false},
		// equal term, equal or lager lastIndex wins
		{raftLog.lastIndex() - 1, 3, false},
		{raftLog.lastIndex(), 3, true},
		{raftLog.lastIndex() + 1, 3, true},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, tt.wUpToDate, raftLog.isUpToDate(entryID{term: tt.term, index: tt.lastIndex}))
		})
	}
}

// TODO(pav-kv): merge into TestLogMaybeAppend when unstable is integrated with
// logSlice, and does all the necessary correctness checks.
func TestLogAppend(t *testing.T) {
	initial := logSlice{term: 2, entries: index(1).terms(1, 2)}
	require.NoError(t, initial.valid())
	last := initial.lastEntryID()

	for _, tt := range []struct {
		app       logSlice
		windex    uint64
		wents     []pb.Entry
		wunstable uint64
	}{
		{
			app:       logSlice{},
			windex:    2,
			wents:     index(1).terms(1, 2),
			wunstable: 3,
		}, {
			app:       last.terms(2),
			windex:    3,
			wents:     index(1).terms(1, 2, 2),
			wunstable: 3,
		}, { // conflicts with index 1
			app:       entryID{}.terms(2),
			windex:    1,
			wents:     index(1).terms(2),
			wunstable: 1,
		}, { // conflicts with index 2
			app:       entryID{term: 1, index: 1}.terms(3, 3),
			windex:    3,
			wents:     index(1).terms(1, 3, 3),
			wunstable: 2,
		},
	} {
		t.Run("", func(t *testing.T) {
			storage := NewMemoryStorage()
			storage.Append(initial.entries)
			raftLog := newLog(storage, discardLogger)

			// TODO(pav-kv): vary the term when maybeAppend respects it.
			require.NoError(t, tt.app.valid())
			raftLog.maybeAppend(tt.app)
			require.Equal(t, tt.windex, raftLog.lastIndex())
			g, err := raftLog.entries(1, noLimit)
			require.NoError(t, err)
			require.Equal(t, tt.wents, g)
			require.Equal(t, tt.wunstable, raftLog.unstable.offset)
		})
	}
}

// TestLogMaybeAppend tests the behaviour of maybeAppend method:
//  1. The request is rejected if the "previous entry" check fails, when the
//     corresponding entry is missing or has a mismatching term.
//  2. If the request is accepted, the slice is appended to the log, and all the
//     entries at higher indices are truncated.
func TestLogMaybeAppend(t *testing.T) {
	init := logSlice{term: 3, entries: index(1).terms(1, 2, 3)}
	require.NoError(t, init.valid())
	last := init.lastEntryID()
	commit := uint64(1)

	for _, tt := range []struct {
		app   logSlice
		want  []pb.Entry
		notOk bool
		panic bool
	}{
		// rejected appends
		{
			app: entryID{term: 2, index: 3}.terms(4), want: init.entries,
			notOk: true, // term mismatch
		}, {
			app: entryID{term: 3, index: 4}.terms(4), want: init.entries,
			notOk: true, // out of bounds
		},
		// appends at the end of the log
		{app: last.terms(), want: init.entries},
		{app: last.terms(4), want: index(1).terms(1, 2, 3, 4)},
		{app: last.terms(4, 4), want: index(1).terms(1, 2, 3, 4, 4)},
		// appends from before the end of the log
		{app: logSlice{}, want: init.entries},
		{app: entryID{term: 1, index: 1}.terms(4), want: index(1).terms(1, 4)},
		{app: entryID{term: 1, index: 1}.terms(4, 4), want: index(1).terms(1, 4, 4)},
		{app: entryID{term: 2, index: 2}.terms(4), want: index(1).terms(1, 2, 4)},
		// panics
		{
			app:   entryID{}.terms(4),
			panic: true, // conflict with existing committed entry
		},
	} {
		// TODO(pav-kv): for now, we pick a high enough app.term so that it
		// represents a valid append message. The maybeAppend currently ignores it,
		// but it must check that the append does not regress the term.
		app := tt.app
		app.term = 100
		require.NoError(t, app.valid())

		raftLog := newLog(NewMemoryStorage(), discardLogger)
		require.True(t, raftLog.maybeAppend(init))
		raftLog.committed = commit

		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.panic)
				}
			}()
			ok := raftLog.maybeAppend(app)
			require.Equal(t, !tt.notOk, ok)
			require.Equal(t, commit, raftLog.committed) // commit index did not change
			require.Equal(t, tt.want, raftLog.allEntries())
		})
	}
}

// TestCompactionSideEffects ensures that all the log related functionality works correctly after
// a compaction.
func TestCompactionSideEffects(t *testing.T) {
	// Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
	lastIndex := uint64(1000)
	unstableIndex := uint64(750)
	storage := NewMemoryStorage()
	require.NoError(t, storage.Append(index(1).termRange(1, unstableIndex+1)))
	raftLog := newLog(storage, raftLogger)
	raftLog.bootstrap(t, index(unstableIndex+1).termRange(unstableIndex+1, lastIndex+1))

	require.True(t, raftLog.maybeCommit(raftLog.lastEntryID()))
	raftLog.appliedTo(raftLog.committed, 0 /* size */)

	offset := uint64(500)
	require.NoError(t, storage.Compact(offset))
	require.Equal(t, lastIndex, raftLog.lastIndex())

	for j := offset; j <= raftLog.lastIndex(); j++ {
		require.Equal(t, j, mustTerm(raftLog.term(j)))
	}

	for j := offset; j <= raftLog.lastIndex(); j++ {
		require.True(t, raftLog.matchTerm(entryID{term: j, index: j}))
	}

	unstableEnts := raftLog.nextUnstableEnts()
	require.Equal(t, 250, len(unstableEnts))
	require.Equal(t, uint64(751), unstableEnts[0].Index)

	prev := raftLog.lastIndex()
	raftLog.bootstrap(t, []pb.Entry{{Index: prev + 1, Term: prev + 1}})
	require.Equal(t, prev+1, raftLog.lastIndex())

	ents, err := raftLog.entries(raftLog.lastIndex(), noLimit)
	require.NoError(t, err)
	require.Equal(t, 1, len(ents))
}

func TestHasNextCommittedEnts(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := index(4).terms(1, 1, 1)
	tests := []struct {
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
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLog(storage, raftLogger)
			raftLog.bootstrap(t, ents[1:])
			raftLog.stableTo(entryID{term: 1, index: 4})
			raftLog.maybeCommit(entryID{term: 1, index: 5})
			raftLog.appliedTo(tt.applied, 0 /* size */)
			raftLog.acceptApplying(tt.applying, 0 /* size */, tt.allowUnstable)
			raftLog.applyingEntsPaused = tt.paused
			if tt.snap {
				newSnap := snap
				newSnap.Metadata.Index++
				raftLog.restore(newSnap)
			}
			require.Equal(t, tt.whasNext, raftLog.hasNextCommittedEnts(tt.allowUnstable))
		})
	}
}

func TestNextCommittedEnts(t *testing.T) {
	snap := pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Term: 1, Index: 3},
	}
	ents := index(4).terms(1, 1, 1)
	tests := []struct {
		applied       uint64
		applying      uint64
		allowUnstable bool
		paused        bool
		snap          bool
		wents         []pb.Entry
	}{
		{applied: 3, applying: 3, allowUnstable: true, wents: ents[:2]},
		{applied: 3, applying: 4, allowUnstable: true, wents: ents[1:2]},
		{applied: 3, applying: 5, allowUnstable: true, wents: nil},
		{applied: 4, applying: 4, allowUnstable: true, wents: ents[1:2]},
		{applied: 4, applying: 5, allowUnstable: true, wents: nil},
		{applied: 5, applying: 5, allowUnstable: true, wents: nil},
		// Don't allow unstable entries.
		{applied: 3, applying: 3, allowUnstable: false, wents: ents[:1]},
		{applied: 3, applying: 4, allowUnstable: false, wents: nil},
		{applied: 3, applying: 5, allowUnstable: false, wents: nil},
		{applied: 4, applying: 4, allowUnstable: false, wents: nil},
		{applied: 4, applying: 5, allowUnstable: false, wents: nil},
		{applied: 5, applying: 5, allowUnstable: false, wents: nil},
		// Paused.
		{applied: 3, applying: 3, allowUnstable: true, paused: true, wents: nil},
		// With snapshot.
		{applied: 3, applying: 3, allowUnstable: true, snap: true, wents: nil},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLog(storage, raftLogger)
			raftLog.bootstrap(t, ents[1:])
			raftLog.stableTo(entryID{term: 1, index: 4})
			raftLog.maybeCommit(entryID{term: 1, index: 5})
			raftLog.appliedTo(tt.applied, 0 /* size */)
			raftLog.acceptApplying(tt.applying, 0 /* size */, tt.allowUnstable)
			raftLog.applyingEntsPaused = tt.paused
			if tt.snap {
				newSnap := snap
				newSnap.Metadata.Index++
				raftLog.restore(newSnap)
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
	ents := index(4).terms(1, 1, 1)
	tests := []struct {
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
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLogWithSize(storage, raftLogger, maxSize)
			raftLog.bootstrap(t, ents[1:])
			raftLog.stableTo(entryID{term: 1, index: 4})
			raftLog.maybeCommit(entryID{term: 1, index: 5})
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
	ents := index(4).terms(1, 1, 1)
	tests := []struct {
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
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			storage := NewMemoryStorage()
			require.NoError(t, storage.ApplySnapshot(snap))
			require.NoError(t, storage.Append(ents[:1]))

			raftLog := newLogWithSize(storage, raftLogger, maxSize)
			raftLog.bootstrap(t, ents[1:])
			raftLog.stableTo(entryID{term: 1, index: 4})
			raftLog.maybeCommit(entryID{term: 1, index: 5})
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

// TestNextUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
func TestNextUnstableEnts(t *testing.T) {
	previousEnts := index(1).terms(1, 2)
	tests := []struct {
		unstable uint64
		wents    []pb.Entry
	}{
		{3, nil},
		{1, previousEnts},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			// append stable entries to storage
			storage := NewMemoryStorage()
			require.NoError(t, storage.Append(previousEnts[:tt.unstable-1]))

			// append unstable entries to raftlog
			raftLog := newLog(storage, raftLogger)
			raftLog.bootstrap(t, previousEnts[tt.unstable-1:])

			ents := raftLog.nextUnstableEnts()
			if l := len(ents); l > 0 {
				raftLog.stableTo(pbEntryID(&ents[l-1]))
			}
			require.Equal(t, tt.wents, ents)
			require.Equal(t, previousEnts[len(previousEnts)-1].Index+1, raftLog.unstable.offset)
		})
	}
}

func TestCommitTo(t *testing.T) {
	initial := entryID{}.terms(1, 2, 3)
	commit := uint64(2)
	for _, tt := range []struct {
		commit uint64
		want   uint64
		panic  bool
	}{
		{commit: 3, want: 3},
		{commit: 1, want: 2},     // commit does not regress
		{commit: 4, panic: true}, // commit out of range -> panic
	} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.panic)
				}
			}()
			raftLog := newLog(NewMemoryStorage(), discardLogger)
			raftLog.maybeAppend(initial)
			raftLog.committed = commit
			raftLog.commitTo(tt.commit)
			require.Equal(t, tt.want, raftLog.committed)
		})
	}
}

func TestStableTo(t *testing.T) {
	tests := []struct {
		stablei   uint64
		stablet   uint64
		wunstable uint64
	}{
		{1, 1, 2},
		{2, 2, 3},
		{2, 1, 1}, // bad term
		{3, 1, 1}, // bad index
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			raftLog := newLog(NewMemoryStorage(), raftLogger)
			raftLog.bootstrap(t, index(1).terms(1, 2))
			raftLog.stableTo(entryID{term: tt.stablet, index: tt.stablei})
			require.Equal(t, tt.wunstable, raftLog.unstable.offset)
		})
	}
}

func TestStableToWithSnap(t *testing.T) {
	snapi, snapt := uint64(5), uint64(2)
	tests := []struct {
		stablei uint64
		stablet uint64
		newEnts []pb.Entry

		wunstable uint64
	}{
		{snapi + 1, snapt, nil, snapi + 1},
		{snapi, snapt, nil, snapi + 1},
		{snapi - 1, snapt, nil, snapi + 1},

		{snapi + 1, snapt + 1, nil, snapi + 1},
		{snapi, snapt + 1, nil, snapi + 1},
		{snapi - 1, snapt + 1, nil, snapi + 1},

		{snapi + 1, snapt, index(snapi + 1).terms(snapt), snapi + 2},
		{snapi, snapt, index(snapi + 1).terms(snapt), snapi + 1},
		{snapi - 1, snapt, index(snapi + 1).terms(snapt), snapi + 1},

		{snapi + 1, snapt + 1, index(snapi + 1).terms(snapt), snapi + 1},
		{snapi, snapt + 1, index(snapi + 1).terms(snapt), snapi + 1},
		{snapi - 1, snapt + 1, index(snapi + 1).terms(snapt), snapi + 1},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s := NewMemoryStorage()
			require.NoError(t, s.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: snapi, Term: snapt}}))
			raftLog := newLog(s, raftLogger)
			raftLog.bootstrap(t, tt.newEnts)
			raftLog.stableTo(entryID{term: tt.stablet, index: tt.stablei})
			require.Equal(t, tt.wunstable, raftLog.unstable.offset)
		})
	}
}

// TestCompaction ensures that the number of log entries is correct after compactions.
func TestCompaction(t *testing.T) {
	tests := []struct {
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
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.False(t, tt.wallow)
				}
			}()
			storage := NewMemoryStorage()
			require.NoError(t, storage.Append(index(1).termRange(1, tt.lastIndex+1)))
			raftLog := newLog(storage, raftLogger)
			raftLog.maybeCommit(entryID{term: 0, index: tt.lastIndex}) // TODO(pav-kv): this is a no-op

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
	raftLog := newLog(storage, raftLogger)

	require.Zero(t, len(raftLog.allEntries()))
	require.Equal(t, index+1, raftLog.firstIndex())
	require.Equal(t, index, raftLog.committed)
	require.Equal(t, index+1, raftLog.unstable.offset)
	require.Equal(t, term, mustTerm(raftLog.term(index)))
}

func TestIsOutOfBounds(t *testing.T) {
	offset := uint64(100)
	num := uint64(100)
	storage := NewMemoryStorage()
	storage.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: offset}})
	l := newLog(storage, raftLogger)
	l.bootstrap(t, index(offset+1).termRange(offset+1, offset+num+1))

	first := offset + 1
	tests := []struct {
		lo, hi        uint64
		wpanic        bool
		wErrCompacted bool
	}{
		{
			first - 2, first + 1,
			false,
			true,
		},
		{
			first - 1, first + 1,
			false,
			true,
		},
		{
			first, first,
			false,
			false,
		},
		{
			first + num/2, first + num/2,
			false,
			false,
		},
		{
			first + num - 1, first + num - 1,
			false,
			false,
		},
		{
			first + num, first + num,
			false,
			false,
		},
		{
			first + num, first + num + 1,
			true,
			false,
		},
		{
			first + num + 1, first + num + 1,
			true,
			false,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					require.True(t, tt.wpanic)
				}
			}()
			err := l.mustCheckOutOfBounds(tt.lo, tt.hi)
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
	l := newLog(storage, raftLogger)
	l.bootstrap(t, index(offset+1).termRange(1, num))

	for i, tt := range []struct {
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
		t.Run(fmt.Sprint(i), func(t *testing.T) {
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
	l := newLog(storage, raftLogger)
	l.restore(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: unstablesnapi, Term: 1}})

	for i, tt := range []struct {
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
		t.Run(fmt.Sprint(i), func(t *testing.T) {
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

	entries := func(from, to uint64) []pb.Entry {
		return index(from).termRange(from, to)
	}

	storage := NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(pb.Snapshot{
		Metadata: pb.SnapshotMetadata{Index: offset}}))
	require.NoError(t, storage.Append(entries(offset+1, half)))
	l := newLog(storage, raftLogger)
	l.bootstrap(t, entries(half, last))

	for _, tt := range []struct {
		lo  uint64
		hi  uint64
		lim uint64

		w      []pb.Entry
		wpanic bool
	}{
		// ErrCompacted.
		{lo: offset - 1, hi: offset + 1, lim: noLimit, w: nil},
		{lo: offset, hi: offset + 1, lim: noLimit, w: nil},
		// panics
		{lo: half, hi: half - 1, lim: noLimit, wpanic: true}, // lo and hi inversion
		{lo: last, hi: last + 1, lim: noLimit, wpanic: true}, // hi is out of bounds

		// No limit.
		{lo: offset + 1, hi: offset + 1, lim: noLimit, w: nil},
		{lo: offset + 1, hi: half - 1, lim: noLimit, w: entries(offset+1, half-1)},
		{lo: offset + 1, hi: half, lim: noLimit, w: entries(offset+1, half)},
		{lo: offset + 1, hi: half + 1, lim: noLimit, w: entries(offset+1, half+1)},
		{lo: offset + 1, hi: last, lim: noLimit, w: entries(offset+1, last)},
		{lo: half - 1, hi: half, lim: noLimit, w: entries(half-1, half)},
		{lo: half - 1, hi: half + 1, lim: noLimit, w: entries(half-1, half+1)},
		{lo: half - 1, hi: last, lim: noLimit, w: entries(half-1, last)},
		{lo: half, hi: half + 1, lim: noLimit, w: entries(half, half+1)},
		{lo: half, hi: last, lim: noLimit, w: entries(half, last)},
		{lo: last - 1, hi: last, lim: noLimit, w: entries(last-1, last)},

		// At least one entry is always returned.
		{lo: offset + 1, hi: last, lim: 0, w: entries(offset+1, offset+2)},
		{lo: half - 1, hi: half + 1, lim: 0, w: entries(half-1, half)},
		{lo: half, hi: last, lim: 0, w: entries(half, half+1)},
		{lo: half + 1, hi: last, lim: 0, w: entries(half+1, half+2)},
		// Low limit.
		{lo: offset + 1, hi: last, lim: uint64(halfe.Size() - 1), w: entries(offset+1, offset+2)},
		{lo: half - 1, hi: half + 1, lim: uint64(halfe.Size() - 1), w: entries(half-1, half)},
		{lo: half, hi: last, lim: uint64(halfe.Size() - 1), w: entries(half, half+1)},
		// Just enough for one limit.
		{lo: offset + 1, hi: last, lim: uint64(halfe.Size()), w: entries(offset+1, offset+2)},
		{lo: half - 1, hi: half + 1, lim: uint64(halfe.Size()), w: entries(half-1, half)},
		{lo: half, hi: last, lim: uint64(halfe.Size()), w: entries(half, half+1)},
		// Not enough for two limit.
		{lo: offset + 1, hi: last, lim: uint64(halfe.Size() + 1), w: entries(offset+1, offset+2)},
		{lo: half - 1, hi: half + 1, lim: uint64(halfe.Size() + 1), w: entries(half-1, half)},
		{lo: half, hi: last, lim: uint64(halfe.Size() + 1), w: entries(half, half+1)},
		// Enough for two limit.
		{lo: offset + 1, hi: last, lim: uint64(halfe.Size() * 2), w: entries(offset+1, offset+3)},
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
			require.False(t, tt.lo <= offset && err != ErrCompacted)
			require.False(t, tt.lo > offset && err != nil)
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
	l := newLog(storage, raftLogger)
	l.bootstrap(t, entries(half, last))

	// Test that scan() returns the same entries as slice(), on all inputs.
	for _, pageSize := range []entryEncodingSize{0, 1, 10, 100, entrySize, entrySize + 1} {
		for lo := offset + 1; lo < last; lo++ {
			for hi := lo; hi <= last; hi++ {
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
func (i index) termRange(from, to uint64) []pb.Entry {
	index := uint64(i)
	entries := make([]pb.Entry, 0, to-from)
	for term := from; term < to; term++ {
		entries = append(entries, pb.Entry{Term: term, Index: index})
		index++
	}
	return entries
}

// terms generates a logSlice of entries appended after the given entry ID, at
// indices [id.index+1, id.index+len(terms)], with the given terms of each
// entry. Terms must be >= id.term, and non-decreasing.
func (id entryID) terms(terms ...uint64) logSlice {
	term := id.term
	if ln := len(terms); ln != 0 {
		term = terms[ln-1]
	}
	return logSlice{
		term:    term,
		prev:    id,
		entries: index(id.index + 1).terms(terms...),
	}
}
