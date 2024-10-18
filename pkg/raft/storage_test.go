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
	"math"
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestStorageTerm(t *testing.T) {
	prev3 := entryID{index: 3, term: 3}
	ls := prev3.append(4, 5)
	tests := []struct {
		i uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, ErrUnavailable, 0, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ls: ls}
			if tt.wpanic {
				require.Panics(t, func() {
					_, _ = s.Term(tt.i)
				})
			}
			term, err := s.Term(tt.i)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wterm, term)
		})
	}
}

func TestStorageEntries(t *testing.T) {
	prev3 := entryID{index: 3, term: 3}
	ls := prev3.append(4, 5, 6)
	ents := ls.entries
	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []pb.Entry
	}{
		{2, 6, math.MaxUint64, ErrCompacted, nil},
		{3, 4, math.MaxUint64, ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, index(4).terms(4)},
		{4, 6, math.MaxUint64, nil, index(4).terms(4, 5)},
		{4, 7, math.MaxUint64, nil, index(4).terms(4, 5, 6)},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, index(4).terms(4)},
		// limit to 2
		{4, 7, uint64(ents[0].Size() + ents[1].Size()), nil, index(4).terms(4, 5)},
		// limit to 2
		{4, 7, uint64(ents[0].Size() + ents[1].Size() + ents[2].Size()/2), nil, index(4).terms(4, 5)},
		{4, 7, uint64(ents[0].Size() + ents[1].Size() + ents[2].Size() - 1), nil, index(4).terms(4, 5)},
		// all
		{4, 7, uint64(ents[0].Size() + ents[1].Size() + ents[2].Size()), nil, index(4).terms(4, 5, 6)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ls: ls}
			entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wentries, entries)
		})
	}
}

func TestStorageLastIndex(t *testing.T) {
	s := &MemoryStorage{ls: entryID{index: 3, term: 3}.append(4, 5)}
	require.Equal(t, uint64(5), s.LastIndex())
	require.NoError(t, s.Append(index(6).terms(5)))
	require.Equal(t, uint64(6), s.LastIndex())
}

func TestStorageFirstIndex(t *testing.T) {
	s := &MemoryStorage{ls: entryID{index: 3, term: 3}.append(4, 5)}
	require.Equal(t, uint64(4), s.FirstIndex())
	require.NoError(t, s.Compact(4))
	require.Equal(t, uint64(5), s.FirstIndex())
}

func TestStorageCompact(t *testing.T) {
	ls := entryID{index: 3, term: 3}.append(4, 5)
	tests := []struct {
		i uint64

		werr   error
		windex uint64
		wterm  uint64
		wlen   int
	}{
		{2, ErrCompacted, 3, 3, 2},
		{3, ErrCompacted, 3, 3, 2},
		{4, nil, 4, 4, 1},
		{5, nil, 5, 5, 0},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ls: ls}
			require.Equal(t, tt.werr, s.Compact(tt.i))
			require.Equal(t, tt.windex, s.ls.prev.index)
			require.Equal(t, tt.wterm, s.ls.prev.term)
			require.Equal(t, tt.wlen, len(s.ls.entries))
		})
	}
}

func TestStorageCreateSnapshot(t *testing.T) {
	ls := entryID{index: 3, term: 3}.append(4, 5)
	cs := &pb.ConfState{Voters: []pb.PeerID{1, 2, 3}}
	data := []byte("data")

	tests := []struct {
		i uint64

		werr  error
		wsnap pb.Snapshot
	}{
		{4, nil, pb.Snapshot{Data: data, Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}}},
		{5, nil, pb.Snapshot{Data: data, Metadata: pb.SnapshotMetadata{Index: 5, Term: 5, ConfState: *cs}}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ls: ls}
			snap, err := s.CreateSnapshot(tt.i, cs, data)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wsnap, snap)
		})
	}
}

func TestStorageAppend(t *testing.T) {
	ls := entryID{index: 3, term: 3}.append(4, 5)
	tests := []struct {
		entries []pb.Entry

		werr     error
		wentries []pb.Entry
	}{
		{
			index(1).terms(1, 2),
			ErrCompacted,
			index(4).terms(4, 5),
		},
		{
			index(3).terms(3, 4, 5),
			ErrCompacted,
			index(4).terms(4, 5),
		},
		{
			index(4).terms(6, 6),
			nil,
			index(4).terms(6, 6),
		},
		{
			index(4).terms(4, 5, 5),
			nil,
			index(4).terms(4, 5, 5),
		},
		// Truncate the existing entries and append.
		{
			index(4).terms(5),
			nil,
			index(4).terms(5),
		},
		// Direct append.
		{
			index(6).terms(5),
			nil,
			index(4).terms(4, 5, 5),
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ls: ls}
			require.Equal(t, tt.werr, s.Append(tt.entries))
			require.Equal(t, tt.wentries, s.ls.entries)
		})
	}
}

func TestStorageApplySnapshot(t *testing.T) {
	cs := &pb.ConfState{Voters: []pb.PeerID{1, 2, 3}}
	data := []byte("data")

	tests := []pb.Snapshot{{Data: data, Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}},
		{Data: data, Metadata: pb.SnapshotMetadata{Index: 3, Term: 3, ConfState: *cs}},
	}

	s := NewMemoryStorage()

	i := 0
	tt := tests[i]
	require.NoError(t, s.ApplySnapshot(tt))

	// ApplySnapshot fails due to ErrSnapOutOfDate.
	i = 1
	tt = tests[i]
	require.Equal(t, ErrSnapOutOfDate, s.ApplySnapshot(tt))
}

func TestStorageLogSnapshot(t *testing.T) {
	s := NewMemoryStorage()
	require.NoError(t, s.Append(index(1).terms(1, 2, 3)))
	snap := s.LogSnapshot()
	// The snapshot must be immutable regardless of mutations on the storage.
	check := func() {
		require.Equal(t, uint64(1), snap.FirstIndex())
		require.Equal(t, uint64(3), snap.LastIndex())
		entries, err := snap.Entries(snap.FirstIndex(), snap.LastIndex()+1, math.MaxUint64)
		require.NoError(t, err)
		require.Equal(t, index(1).terms(1, 2, 3), entries)
	}
	check()
	require.NoError(t, s.Append(index(4).terms(4, 5))) // regular append
	check()
	require.NoError(t, s.Append(index(2).terms(7, 7, 7))) // truncation and append
	check()
	require.NoError(t, s.Compact(4)) // compaction
	check()
}
