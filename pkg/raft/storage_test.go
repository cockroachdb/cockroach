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
	ents := index(3).terms(3, 4, 5)
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
			s := &MemoryStorage{ents: ents}

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
	ents := index(3).terms(3, 4, 5, 6)
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
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, index(4).terms(4, 5)},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, index(4).terms(4, 5)},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, index(4).terms(4, 5)},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, index(4).terms(4, 5, 6)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ents: ents}
			entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wentries, entries)
		})
	}
}

func TestStorageLastIndex(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	s := &MemoryStorage{ents: ents}

	last, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)

	require.NoError(t, s.Append(index(6).terms(5)))
	last, err = s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), last)
}

func TestStorageFirstIndex(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	s := &MemoryStorage{ents: ents}

	first, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), first)

	require.NoError(t, s.Compact(4))
	first, err = s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), first)
}

func TestStorageCompact(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	tests := []struct {
		i uint64

		werr   error
		windex uint64
		wterm  uint64
		wlen   int
	}{
		{2, ErrCompacted, 3, 3, 3},
		{3, ErrCompacted, 3, 3, 3},
		{4, nil, 4, 4, 2},
		{5, nil, 5, 5, 1},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ents: ents}
			require.Equal(t, tt.werr, s.Compact(tt.i))
			require.Equal(t, tt.windex, s.ents[0].Index)
			require.Equal(t, tt.wterm, s.ents[0].Term)
			require.Equal(t, tt.wlen, len(s.ents))
		})
	}
}

func TestStorageCreateSnapshot(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}}
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
			s := &MemoryStorage{ents: ents}
			snap, err := s.CreateSnapshot(tt.i, cs, data)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wsnap, snap)
		})
	}
}

func TestStorageAppend(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	tests := []struct {
		entries []pb.Entry

		werr     error
		wentries []pb.Entry
	}{
		{
			index(1).terms(1, 2),
			nil,
			index(3).terms(3, 4, 5),
		},
		{
			index(3).terms(3, 4, 5),
			nil,
			index(3).terms(3, 4, 5),
		},
		{
			index(3).terms(3, 6, 6),
			nil,
			index(3).terms(3, 6, 6),
		},
		{
			index(3).terms(3, 4, 5, 5),
			nil,
			index(3).terms(3, 4, 5, 5),
		},
		// Truncate incoming entries, truncate the existing entries and append.
		{
			index(2).terms(3, 3, 5),
			nil,
			index(3).terms(3, 5),
		},
		// Truncate the existing entries and append.
		{
			index(4).terms(5),
			nil,
			index(3).terms(3, 5),
		},
		// Direct append.
		{
			index(6).terms(5),
			nil,
			index(3).terms(3, 4, 5, 5),
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := &MemoryStorage{ents: ents}
			require.Equal(t, tt.werr, s.Append(tt.entries))
			require.Equal(t, tt.wentries, s.ents)
		})
	}
}

func TestStorageApplySnapshot(t *testing.T) {
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}}
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
