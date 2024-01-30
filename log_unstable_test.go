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

	"github.com/stretchr/testify/require"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestUnstableMaybeFirstIndex(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// no snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			false, 0,
		},
		{
			[]pb.Entry{}, 0, nil,
			false, 0,
		},
		// has snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 5,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 5,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries:  tt.entries,
				offset:   tt.offset,
				snapshot: tt.snap,
				logger:   raftLogger,
			}
			index, ok := u.maybeFirstIndex()
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.windex, index)
		})
	}
}

func TestMaybeLastIndex(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// last in entries
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			true, 5,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 5,
		},
		// last in snapshot
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			true, 4,
		},
		// empty unstable
		{
			[]pb.Entry{}, 0, nil,
			false, 0,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries:  tt.entries,
				offset:   tt.offset,
				snapshot: tt.snap,
				logger:   raftLogger,
			}
			index, ok := u.maybeLastIndex()
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.windex, index)
		})
	}
}

func TestUnstableMaybeTerm(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
		offset  uint64
		snap    *pb.Snapshot
		index   uint64

		wok   bool
		wterm uint64
	}{
		// term from entries
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			5,
			true, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			6,
			false, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, nil,
			4,
			false, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,
			true, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			6,
			false, 0,
		},
		// term from snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			4,
			true, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			3,
			false, 0,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,
			false, 0,
		},
		{
			[]pb.Entry{}, 5, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			4,
			true, 1,
		},
		{
			[]pb.Entry{}, 0, nil,
			5,
			false, 0,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries:  tt.entries,
				offset:   tt.offset,
				snapshot: tt.snap,
				logger:   raftLogger,
			}
			term, ok := u.maybeTerm(tt.index)
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.wterm, term)
		})
	}
}

func TestUnstableRestore(t *testing.T) {
	u := unstable{
		entries:            []pb.Entry{{Index: 5, Term: 1}},
		offset:             5,
		offsetInProgress:   6,
		snapshot:           &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
		snapshotInProgress: true,
		logger:             raftLogger,
	}
	s := pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 6, Term: 2}}
	u.restore(s)

	require.Equal(t, s.Metadata.Index+1, u.offset)
	require.Equal(t, s.Metadata.Index+1, u.offsetInProgress)
	require.Zero(t, len(u.entries))
	require.Equal(t, &s, u.snapshot)
	require.False(t, u.snapshotInProgress)
}

func TestUnstableNextEntries(t *testing.T) {
	tests := []struct {
		entries          []pb.Entry
		offset           uint64
		offsetInProgress uint64

		wentries []pb.Entry
	}{
		// nothing in progress
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, 5,
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}},
		},
		// partially in progress
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, 6,
			[]pb.Entry{{Index: 6, Term: 1}},
		},
		// everything in progress
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, 7,
			nil, // nil, not empty slice
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries:          tt.entries,
				offset:           tt.offset,
				offsetInProgress: tt.offsetInProgress,
				logger:           raftLogger,
			}
			res := u.nextEntries()
			require.Equal(t, tt.wentries, res)
		})
	}
}

func TestUnstableNextSnapshot(t *testing.T) {
	s := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	tests := []struct {
		snapshot           *pb.Snapshot
		snapshotInProgress bool

		wsnapshot *pb.Snapshot
	}{
		// snapshot not unstable
		{
			nil, false,
			nil,
		},
		// snapshot not in progress
		{
			s, false,
			s,
		},
		// snapshot in progress
		{
			s, true,
			nil,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				snapshot:           tt.snapshot,
				snapshotInProgress: tt.snapshotInProgress,
			}
			res := u.nextSnapshot()
			require.Equal(t, tt.wsnapshot, res)
		})
	}
}

func TestUnstableAcceptInProgress(t *testing.T) {
	tests := []struct {
		entries            []pb.Entry
		snapshot           *pb.Snapshot
		offsetInProgress   uint64
		snapshotInProgress bool

		woffsetInProgress   uint64
		wsnapshotInProgress bool
	}{
		{
			[]pb.Entry{}, nil,
			5,     // no entries
			false, // snapshot not already in progress
			5, false,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, nil,
			5,     // entries not in progress
			false, // snapshot not already in progress
			6, false,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, nil,
			5,     // entries not in progress
			false, // snapshot not already in progress
			7, false,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, nil,
			6,     // in-progress to the first entry
			false, // snapshot not already in progress
			7, false,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, nil,
			7,     // in-progress to the second entry
			false, // snapshot not already in progress
			7, false,
		},
		// with snapshot
		{
			[]pb.Entry{}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,     // no entries
			false, // snapshot not already in progress
			5, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,     // entries not in progress
			false, // snapshot not already in progress
			6, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,     // entries not in progress
			false, // snapshot not already in progress
			7, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			6,     // in-progress to the first entry
			false, // snapshot not already in progress
			7, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			7,     // in-progress to the second entry
			false, // snapshot not already in progress
			7, true,
		},
		{
			[]pb.Entry{}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,    // entries not in progress
			true, // snapshot already in progress
			5, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,    // entries not in progress
			true, // snapshot already in progress
			6, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5,    // entries not in progress
			true, // snapshot already in progress
			7, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			6,    // in-progress to the first entry
			true, // snapshot already in progress
			7, true,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			7,    // in-progress to the second entry
			true, // snapshot already in progress
			7, true,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries:            tt.entries,
				snapshot:           tt.snapshot,
				offsetInProgress:   tt.offsetInProgress,
				snapshotInProgress: tt.snapshotInProgress,
			}
			u.acceptInProgress()
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
			require.Equal(t, tt.wsnapshotInProgress, u.snapshotInProgress)
		})
	}
}

func TestUnstableStableTo(t *testing.T) {
	tests := []struct {
		entries          []pb.Entry
		offset           uint64
		offsetInProgress uint64
		snap             *pb.Snapshot
		index, term      uint64

		woffset           uint64
		woffsetInProgress uint64
		wlen              int
	}{
		{
			[]pb.Entry{}, 0, 0, nil,
			5, 1,
			0, 0, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 6, nil,
			5, 1, // stable to the first entry
			6, 6, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, 6, nil,
			5, 1, // stable to the first entry
			6, 6, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, 7, nil,
			5, 1, // stable to the first entry and in-progress ahead
			6, 7, 1,
		},
		{
			[]pb.Entry{{Index: 6, Term: 2}}, 6, 7, nil,
			6, 1, // stable to the first entry and term mismatch
			6, 7, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 6, nil,
			4, 1, // stable to old entry
			5, 6, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 6, nil,
			4, 2, // stable to old entry
			5, 6, 1,
		},
		// with snapshot
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5, 1, // stable to the first entry
			6, 6, 0,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5, 1, // stable to the first entry
			6, 6, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}}, 5, 7, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			5, 1, // stable to the first entry and in-progress ahead
			6, 7, 1,
		},
		{
			[]pb.Entry{{Index: 6, Term: 2}}, 6, 7, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 5, Term: 1}},
			6, 1, // stable to the first entry and term mismatch
			6, 7, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
			4, 1, // stable to snapshot
			5, 6, 1,
		},
		{
			[]pb.Entry{{Index: 5, Term: 2}}, 5, 6, &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 2}},
			4, 1, // stable to old entry
			5, 6, 1,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries:          tt.entries,
				offset:           tt.offset,
				offsetInProgress: tt.offsetInProgress,
				snapshot:         tt.snap,
				logger:           raftLogger,
			}
			u.stableTo(entryID{term: tt.term, index: tt.index})
			require.Equal(t, tt.woffset, u.offset)
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
			require.Equal(t, tt.wlen, len(u.entries))
		})
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	tests := []struct {
		entries          []pb.Entry
		offset           uint64
		offsetInProgress uint64
		snap             *pb.Snapshot
		toappend         []pb.Entry

		woffset           uint64
		woffsetInProgress uint64
		wentries          []pb.Entry
	}{
		// append to the end
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 5, nil,
			[]pb.Entry{{Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5, 5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 6, nil,
			[]pb.Entry{{Index: 6, Term: 1}, {Index: 7, Term: 1}},
			5, 6, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}},
		},
		// replace the unstable entries
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 5, nil,
			[]pb.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}},
			5, 5, []pb.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 5, nil,
			[]pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}},
			4, 4, []pb.Entry{{Index: 4, Term: 2}, {Index: 5, Term: 2}, {Index: 6, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}}, 5, 6, nil,
			[]pb.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}},
			5, 5, []pb.Entry{{Index: 5, Term: 2}, {Index: 6, Term: 2}},
		},
		// truncate the existing entries and append
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, 5, nil,
			[]pb.Entry{{Index: 6, Term: 2}},
			5, 5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, 5, nil,
			[]pb.Entry{{Index: 7, Term: 2}, {Index: 8, Term: 2}},
			5, 5, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 2}, {Index: 8, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, 6, nil,
			[]pb.Entry{{Index: 6, Term: 2}},
			5, 6, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 2}},
		},
		{
			[]pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 1}, {Index: 7, Term: 1}}, 5, 7, nil,
			[]pb.Entry{{Index: 6, Term: 2}},
			5, 6, []pb.Entry{{Index: 5, Term: 1}, {Index: 6, Term: 2}},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			u := unstable{
				entries:          tt.entries,
				offset:           tt.offset,
				offsetInProgress: tt.offsetInProgress,
				snapshot:         tt.snap,
				logger:           raftLogger,
			}
			u.truncateAndAppend(tt.toappend)
			require.Equal(t, tt.woffset, u.offset)
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
			require.Equal(t, tt.wentries, u.entries)
		})
	}
}
