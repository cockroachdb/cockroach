// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2024 The etcd Authors
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

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestEntryID(t *testing.T) {
	// Some obvious checks first.
	require.Equal(t, entryID{term: 5, index: 10}, entryID{term: 5, index: 10})
	require.NotEqual(t, entryID{term: 4, index: 10}, entryID{term: 5, index: 10})
	require.NotEqual(t, entryID{term: 5, index: 9}, entryID{term: 5, index: 10})

	for _, tt := range []struct {
		entry pb.Entry
		want  entryID
	}{
		{entry: pb.Entry{}, want: entryID{term: 0, index: 0}},
		{entry: pb.Entry{Term: 1, Index: 2, Data: []byte("data")}, want: entryID{term: 1, index: 2}},
		{entry: pb.Entry{Term: 10, Index: 123}, want: entryID{term: 10, index: 123}},
	} {
		require.Equal(t, tt.want, pbEntryID(&tt.entry))
	}
}

func TestLogSlice(t *testing.T) {
	id := func(index, term uint64) entryID {
		return entryID{term: term, index: index}
	}
	e := func(index, term uint64) pb.Entry {
		return pb.Entry{Term: term, Index: index}
	}
	for _, tt := range []struct {
		term    uint64
		prev    entryID
		entries []pb.Entry

		notOk bool
		last  entryID
	}{
		// Empty "dummy" slice, starting at (0, 0) origin of the log.
		{last: id(0, 0)},
		// Empty slice with a given prev ID. Valid only if term >= prev.term.
		{prev: id(123, 10), notOk: true},
		{term: 9, prev: id(123, 10), notOk: true},
		{term: 10, prev: id(123, 10), last: id(123, 10)},
		{term: 11, prev: id(123, 10), last: id(123, 10)},
		// A single entry.
		{term: 0, entries: []pb.Entry{e(1, 1)}, notOk: true},
		{term: 1, entries: []pb.Entry{e(1, 1)}, last: id(1, 1)},
		{term: 2, entries: []pb.Entry{e(1, 1)}, last: id(1, 1)},
		// Multiple entries.
		{term: 2, entries: []pb.Entry{e(2, 1), e(3, 1), e(4, 2)}, notOk: true},
		{term: 1, prev: id(1, 1), entries: []pb.Entry{e(2, 1), e(3, 1), e(4, 2)}, notOk: true},
		{term: 2, prev: id(1, 1), entries: []pb.Entry{e(2, 1), e(3, 1), e(4, 2)}, last: id(4, 2)},
		// First entry inconsistent with prev.
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(111, 5)}, notOk: true},
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(124, 4)}, notOk: true},
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(234, 6)}, notOk: true},
		{term: 10, prev: id(123, 5), entries: []pb.Entry{e(124, 6)}, last: id(124, 6)},
		// Inconsistent entries.
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(12, 2)}, notOk: true},
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(15, 2)}, notOk: true},
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(14, 1)}, notOk: true},
		{term: 10, prev: id(12, 2), entries: []pb.Entry{e(13, 2), e(14, 3)}, last: id(14, 3)},
	} {
		t.Run("", func(t *testing.T) {
			s := LogSlice{term: tt.term, prev: tt.prev, entries: tt.entries}
			require.Equal(t, tt.notOk, s.valid() != nil)
			if tt.notOk {
				return
			}
			last := s.lastEntryID()
			require.Equal(t, tt.last, last)
			require.Equal(t, last.index, s.lastIndex())
			require.Equal(t, LogMark{Term: tt.term, Index: last.index}, s.mark())

			require.Equal(t, tt.prev.term, s.termAt(tt.prev.index))
			for _, e := range tt.entries {
				require.Equal(t, e.Term, s.termAt(e.Index))
			}
		})
	}
}

func TestLogSliceForward(t *testing.T) {
	id := func(index, term uint64) entryID {
		return entryID{term: term, index: index}
	}
	ls := func(prev entryID, terms ...uint64) LogSlice {
		empty := make([]pb.Entry, 0) // hack to canonicalize empty slices
		return LogSlice{
			term:    8,
			prev:    prev,
			entries: append(empty, index(prev.index+1).terms(terms...)...),
		}
	}
	for _, tt := range []struct {
		ls   LogSlice
		to   uint64
		want LogSlice
	}{
		{ls: LogSlice{}, to: 0, want: LogSlice{}},
		{ls: ls(id(5, 1)), to: 5, want: ls(id(5, 1))},
		{ls: ls(id(10, 3), 3, 4, 5), to: 10, want: ls(id(10, 3), 3, 4, 5)},
		{ls: ls(id(10, 3), 3, 4, 5), to: 11, want: ls(id(11, 3), 4, 5)},
		{ls: ls(id(10, 3), 3, 4, 5), to: 12, want: ls(id(12, 4), 5)},
		{ls: ls(id(10, 3), 3, 4, 5), to: 13, want: ls(id(13, 5))},
	} {
		t.Run("", func(t *testing.T) {
			require.NoError(t, tt.ls.valid())
			require.Equal(t, tt.want, tt.ls.forward(tt.to))
		})
	}
}

func TestSnapshot(t *testing.T) {
	id := func(index, term uint64) entryID {
		return entryID{term: term, index: index}
	}
	snap := func(index, term uint64) pb.Snapshot {
		return pb.Snapshot{Metadata: pb.SnapshotMetadata{
			Term: term, Index: index,
		}}
	}
	for _, tt := range []struct {
		term  uint64
		snap  pb.Snapshot
		notOk bool
		last  entryID
	}{
		// Empty "dummy" snapshot, at (0, 0) origin of the log.
		{last: id(0, 0)},
		// Valid only if term >= Metadata.Term.
		{term: 10, snap: snap(123, 9), last: id(123, 9)},
		{term: 10, snap: snap(123, 10), last: id(123, 10)},
		{term: 10, snap: snap(123, 11), notOk: true},
	} {
		t.Run("", func(t *testing.T) {
			s := snapshot{term: tt.term, snap: tt.snap}
			require.Equal(t, tt.notOk, s.valid() != nil)
			if tt.notOk {
				return
			}
			last := s.lastEntryID()
			require.Equal(t, tt.last, last)
			require.Equal(t, last.index, s.lastIndex())
			require.Equal(t, LogMark{Term: tt.term, Index: last.index}, s.mark())
		})
	}
}
