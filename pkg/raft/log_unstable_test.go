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
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func newUnstableForTesting(ls logSlice, snap *pb.Snapshot) unstable {
	return unstable{
		snapshot:         snap,
		entries:          ls.entries,
		offset:           ls.prev.index + 1,
		offsetInProgress: ls.prev.index + 1,
		logger:           discardLogger,
	}
}

func (u *unstable) checkInvariants(t testing.TB) {
	t.Helper()
	require.GreaterOrEqual(t, u.offsetInProgress, u.offset)
	require.LessOrEqual(t, u.offsetInProgress-u.offset, uint64(len(u.entries)))
	if u.snapshot != nil {
		require.Equal(t, u.snapshot.Metadata.Index+1, u.offset)
	} else {
		require.False(t, u.snapshotInProgress)
	}
	if len(u.entries) != 0 {
		require.Equal(t, u.entries[0].Index, u.offset)
	}
	if u.offsetInProgress > u.offset && u.snapshot != nil {
		require.True(t, u.snapshotInProgress)
	}
}

func TestUnstableMaybeFirstIndex(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls   logSlice
		snap *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// no snapshot
		{
			prev4.append(1), nil,
			false, 0,
		},
		{
			logSlice{}, nil,
			false, 0,
		},
		// has snapshot
		{
			prev4.append(1), snap4,
			true, 5,
		},
		{
			prev4.append(), snap4,
			true, 5,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.checkInvariants(t)

			index, ok := u.maybeFirstIndex()
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.windex, index)
		})
	}
}

func TestMaybeLastIndex(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls   logSlice
		snap *pb.Snapshot

		wok    bool
		windex uint64
	}{
		// last in entries
		{
			prev4.append(1), nil,
			true, 5,
		},
		{
			prev4.append(1), snap4,
			true, 5,
		},
		// last in snapshot
		{
			prev4.append(), snap4,
			true, 4,
		},
		// empty unstable
		{
			logSlice{}, nil,
			false, 0,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.checkInvariants(t)

			index, ok := u.maybeLastIndex()
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.windex, index)
		})
	}
}

func TestUnstableMaybeTerm(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls    logSlice
		snap  *pb.Snapshot
		index uint64

		wok   bool
		wterm uint64
	}{
		// term from entries
		{
			prev4.append(1), nil,
			5,
			true, 1,
		},
		{
			prev4.append(1), nil,
			6,
			false, 0,
		},
		{
			prev4.append(1), nil,
			4,
			false, 0,
		},
		{
			prev4.append(1), snap4,
			5,
			true, 1,
		},
		{
			prev4.append(1), snap4,
			6,
			false, 0,
		},
		// term from snapshot
		{
			prev4.append(1), snap4,
			4,
			true, 1,
		},
		{
			prev4.append(1), snap4,
			3,
			false, 0,
		},
		{
			prev4.append(), snap4,
			5,
			false, 0,
		},
		{
			prev4.append(), snap4,
			4,
			true, 1,
		},
		{
			prev4.append(), nil,
			5,
			false, 0,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.checkInvariants(t)

			term, ok := u.maybeTerm(tt.index)
			require.Equal(t, tt.wok, ok)
			require.Equal(t, tt.wterm, term)
		})
	}
}

func TestUnstableRestore(t *testing.T) {
	u := newUnstableForTesting(
		entryID{term: 1, index: 4}.append(1),
		&pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
	)
	u.snapshotInProgress = true
	u.offsetInProgress = 6
	u.checkInvariants(t)

	s := snapshot{
		term: 2,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 6, Term: 2}},
	}
	u.restore(s)
	u.checkInvariants(t)

	require.Equal(t, s.lastIndex()+1, u.offset)
	require.Equal(t, s.lastIndex()+1, u.offsetInProgress)
	require.Zero(t, len(u.entries))
	require.Equal(t, &s.snap, u.snapshot)
	require.False(t, u.snapshotInProgress)
}

func TestUnstableNextEntries(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	for _, tt := range []struct {
		ls               logSlice
		offsetInProgress uint64

		wentries []pb.Entry
	}{
		// nothing in progress
		{
			prev4.append(1, 1), 5,
			index(5).terms(1, 1),
		},
		// partially in progress
		{
			prev4.append(1, 1), 6,
			index(6).terms(1),
		},
		// everything in progress
		{
			prev4.append(1, 1), 7,
			nil, // nil, not empty slice
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, nil /* snap */)
			u.offsetInProgress = tt.offsetInProgress
			u.checkInvariants(t)
			require.Equal(t, tt.wentries, u.nextEntries())
		})
	}
}

func TestUnstableNextSnapshot(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		prev               entryID
		snap               *pb.Snapshot
		snapshotInProgress bool

		wsnapshot *pb.Snapshot
	}{
		// snapshot not unstable
		{
			entryID{}, nil, false,
			nil,
		},
		// snapshot not in progress
		{
			prev4, snap4, false,
			snap4,
		},
		// snapshot in progress
		{
			prev4, snap4, true,
			nil,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.prev.append(), tt.snap)
			u.snapshotInProgress = tt.snapshotInProgress
			u.checkInvariants(t)
			require.Equal(t, tt.wsnapshot, u.nextSnapshot())
		})
	}
}

func TestUnstableAcceptInProgress(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls                 logSlice
		snap               *pb.Snapshot
		offsetInProgress   uint64
		snapshotInProgress bool

		woffsetInProgress   uint64
		wsnapshotInProgress bool
	}{
		{
			prev4.append(), nil, // no entries
			5,
			false, // snapshot not already in progress
			5, false,
		},
		{
			prev4.append(1), nil,
			5,     // entries not in progress
			false, // snapshot not already in progress
			6, false,
		},
		{
			prev4.append(1, 1), nil,
			5,     // entries not in progress
			false, // snapshot not already in progress
			7, false,
		},
		{
			prev4.append(1, 1), nil,
			6,     // in-progress to the first entry
			false, // snapshot not already in progress
			7, false,
		},
		{
			prev4.append(1, 1), nil,
			7,     // in-progress to the second entry
			false, // snapshot not already in progress
			7, false,
		},
		// with snapshot
		{
			prev4.append(), snap4, // no entries
			5,
			false, // snapshot not already in progress
			5, true,
		},
		{
			prev4.append(1), snap4,
			5,     // entries not in progress
			false, // snapshot not already in progress
			6, true,
		},
		{
			prev4.append(1, 1), snap4,
			5,     // entries not in progress
			false, // snapshot not already in progress
			7, true,
		},
		{
			prev4.append(), snap4,
			5,    // entries not in progress
			true, // snapshot already in progress
			5, true,
		},
		{
			prev4.append(1), snap4,
			5,    // entries not in progress
			true, // snapshot already in progress
			6, true,
		},
		{
			prev4.append(1, 1), snap4,
			5,    // entries not in progress
			true, // snapshot already in progress
			7, true,
		},
		{
			prev4.append(1, 1), snap4,
			6,    // in-progress to the first entry
			true, // snapshot already in progress
			7, true,
		},
		{
			prev4.append(1, 1), snap4,
			7,    // in-progress to the second entry
			true, // snapshot already in progress
			7, true,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.snapshotInProgress = tt.snapshotInProgress
			u.offsetInProgress = tt.offsetInProgress
			u.checkInvariants(t)

			u.acceptInProgress()
			u.checkInvariants(t)
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
			require.Equal(t, tt.wsnapshotInProgress, u.snapshotInProgress)
		})
	}
}

func TestUnstableStableTo(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls               logSlice
		offsetInProgress uint64
		snap             *pb.Snapshot
		index, term      uint64

		woffset           uint64
		woffsetInProgress uint64
		wlen              int
	}{
		{
			logSlice{}, 1, nil,
			5, 1,
			1, 1, 0,
		},
		{
			prev4.append(1), 6, nil,
			5, 1, // stable to the first entry
			6, 6, 0,
		},
		{
			prev4.append(1, 1), 6, nil,
			5, 1, // stable to the first entry
			6, 6, 1,
		},
		{
			prev4.append(1, 1), 7, nil,
			5, 1, // stable to the first entry and in-progress ahead
			6, 7, 1,
		},
		{
			entryID{term: 1, index: 5}.append(2), 7, nil,
			6, 1, // stable to the first entry and term mismatch
			6, 7, 1,
		},
		{
			prev4.append(1), 6, nil,
			4, 1, // stable to old entry
			5, 6, 1,
		},
		{
			prev4.append(1), 6, nil,
			4, 2, // stable to old entry
			5, 6, 1,
		},
		// with snapshot
		{
			prev4.append(1), 6, snap4,
			5, 1, // stable to the first entry
			6, 6, 0,
		},
		{
			prev4.append(1, 1), 6, snap4,
			5, 1, // stable to the first entry
			6, 6, 1,
		},
		{
			prev4.append(1, 1), 7, snap4,
			5, 1, // stable to the first entry and in-progress ahead
			6, 7, 1,
		},
		{
			entryID{term: 1, index: 5}.append(2), 7,
			&pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 5, Term: 1}},
			6, 1, // stable to the first entry and term mismatch
			6, 7, 1,
		},
		{
			prev4.append(1), 6, snap4,
			4, 1, // stable to snapshot
			5, 6, 1,
		},
		{
			prev4.append(2), 6, snap4,
			4, 1, // stable to old entry
			5, 6, 1,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.offsetInProgress = tt.offsetInProgress
			u.snapshotInProgress = u.snapshot != nil && u.offsetInProgress > u.offset
			u.checkInvariants(t)

			if u.snapshotInProgress {
				u.stableSnapTo(u.snapshot.Metadata.Index)
			}
			u.checkInvariants(t)
			u.stableTo(entryID{term: tt.term, index: tt.index})
			u.checkInvariants(t)
			require.Equal(t, tt.woffset, u.offset)
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
			require.Equal(t, tt.wlen, len(u.entries))
		})
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	for _, tt := range []struct {
		ls               logSlice
		offsetInProgress uint64
		snap             *pb.Snapshot
		app              logSlice

		want              logSlice
		woffsetInProgress uint64
	}{
		// append to the end
		{
			prev4.append(1), 5, nil,
			entryID{term: 1, index: 5}.append(1, 1),
			prev4.append(1, 1, 1),
			5,
		},
		{
			prev4.append(1), 6, nil,
			entryID{term: 1, index: 5}.append(1, 1),
			prev4.append(1, 1, 1),
			6,
		},
		// replace the unstable entries
		{
			prev4.append(1), 5, nil,
			prev4.append(2, 2),
			prev4.append(2, 2),
			5,
		},
		{
			prev4.append(1), 5, nil,
			entryID{term: 1, index: 3}.append(2, 2, 2),
			entryID{term: 1, index: 3}.append(2, 2, 2),
			4,
		},
		{
			prev4.append(1), 6, nil,
			prev4.append(2, 2),
			prev4.append(2, 2),
			5,
		},
		// truncate the existing entries and append
		{
			prev4.append(1, 1, 1), 5, nil,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			5,
		},
		{
			prev4.append(1, 1, 1), 5, nil,
			entryID{term: 1, index: 6}.append(2, 2),
			prev4.append(1, 1, 2, 2),
			5,
		},
		{
			prev4.append(1, 1, 1), 6, nil,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			6,
		},
		{
			prev4.append(1, 1, 1), 7, nil,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			6,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.offsetInProgress = tt.offsetInProgress
			u.snapshotInProgress = u.snapshot != nil && u.offsetInProgress > u.offset
			u.checkInvariants(t)

			u.truncateAndAppend(tt.app)
			u.checkInvariants(t)
			require.Equal(t, tt.want.prev.index+1, u.offset)
			require.Equal(t, tt.want.entries, u.entries)
			require.Equal(t, tt.woffsetInProgress, u.offsetInProgress)
		})
	}
}
