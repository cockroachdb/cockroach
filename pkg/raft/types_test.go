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
			s := logSlice{term: tt.term, prev: tt.prev, entries: tt.entries}
			require.Equal(t, tt.notOk, s.valid() != nil)
			if !tt.notOk {
				last := s.lastEntryID()
				require.Equal(t, tt.last, last)
				require.Equal(t, last.index, s.lastIndex())
			}
		})
	}
}
