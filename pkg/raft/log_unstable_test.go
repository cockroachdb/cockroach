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
)

func newUnstableForTesting(ls LogSlice, snap *pb.Snapshot) unstable {
	return unstable{
		snapshot:        snap,
		LogSlice:        ls,
		entryInProgress: ls.prev.index,
		logger:          raftlogger.DiscardLogger,
	}
}

func (u *unstable) checkInvariants(t testing.TB) {
	t.Helper()
	require.NoError(t, u.LogSlice.valid())
	require.GreaterOrEqual(t, u.entryInProgress, u.prev.index)
	require.LessOrEqual(t, u.entryInProgress, u.lastIndex())
	if u.snapshot != nil {
		require.Equal(t, u.snapshot.Metadata.Term, u.prev.term)
		require.Equal(t, u.snapshot.Metadata.Index, u.prev.index)
	} else {
		require.False(t, u.snapshotInProgress)
	}
	if u.entryInProgress > u.prev.index && u.snapshot != nil {
		require.True(t, u.snapshotInProgress)
	}
}

func TestUnstableMaybeFirstIndex(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls   LogSlice
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
			LogSlice{}, nil,
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

func TestLastIndex(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls     LogSlice
		snap   *pb.Snapshot
		windex uint64
	}{
		{prev4.append(1), nil, 5}, // last in entries
		{prev4.append(1), snap4, 5},
		{prev4.append(), snap4, 4}, // last in snapshot
		{LogSlice{}, nil, 0},       // empty unstable
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.checkInvariants(t)
			require.Equal(t, tt.windex, u.lastIndex())
		})
	}
}

func TestUnstableRestore(t *testing.T) {
	u := newUnstableForTesting(
		entryID{term: 1, index: 4}.append(1),
		&pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}},
	)
	u.snapshotInProgress = true
	u.entryInProgress = 5
	u.checkInvariants(t)

	s := snapshot{
		term: 2,
		snap: pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 6, Term: 2}},
	}
	require.True(t, u.restore(s))
	u.checkInvariants(t)

	require.Equal(t, uint64(6), u.entryInProgress)
	require.Zero(t, len(u.entries))
	require.Equal(t, &s.snap, u.snapshot)
	require.False(t, u.snapshotInProgress)
}

func TestUnstableNextEntries(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	for _, tt := range []struct {
		ls              LogSlice
		entryInProgress uint64

		wentries []pb.Entry
	}{
		// nothing in progress
		{
			prev4.append(1, 1), 4,
			index(5).terms(1, 1),
		},
		// partially in progress
		{
			prev4.append(1, 1), 5,
			index(6).terms(1),
		},
		// everything in progress
		{
			prev4.append(1, 1), 6,
			nil, // nil, not empty slice
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, nil /* snap */)
			u.entryInProgress = tt.entryInProgress
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
		ls                 LogSlice
		snap               *pb.Snapshot
		entryInProgress    uint64
		snapshotInProgress bool

		wentryInProgress    uint64
		wsnapshotInProgress bool
	}{
		{
			prev4.append(), nil, // no entries
			4,
			false, // snapshot not already in progress
			4, false,
		},
		{
			prev4.append(1), nil,
			4,     // entries not in progress
			false, // snapshot not already in progress
			5, false,
		},
		{
			prev4.append(1, 1), nil,
			4,     // entries not in progress
			false, // snapshot not already in progress
			6, false,
		},
		{
			prev4.append(1, 1), nil,
			5,     // in-progress to the first entry
			false, // snapshot not already in progress
			6, false,
		},
		{
			prev4.append(1, 1), nil,
			6,     // in-progress to the second entry
			false, // snapshot not already in progress
			6, false,
		},
		// with snapshot
		{
			prev4.append(), snap4, // no entries
			4,
			false, // snapshot not already in progress
			4, true,
		},
		{
			prev4.append(1), snap4,
			4,     // entries not in progress
			false, // snapshot not already in progress
			5, true,
		},
		{
			prev4.append(1, 1), snap4,
			4,     // entries not in progress
			false, // snapshot not already in progress
			6, true,
		},
		{
			prev4.append(), snap4,
			4,    // entries not in progress
			true, // snapshot already in progress
			4, true,
		},
		{
			prev4.append(1), snap4,
			4,    // entries not in progress
			true, // snapshot already in progress
			5, true,
		},
		{
			prev4.append(1, 1), snap4,
			4,    // entries not in progress
			true, // snapshot already in progress
			6, true,
		},
		{
			prev4.append(1, 1), snap4,
			5,    // in-progress to the first entry
			true, // snapshot already in progress
			6, true,
		},
		{
			prev4.append(1, 1), snap4,
			6,    // in-progress to the second entry
			true, // snapshot already in progress
			6, true,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.snapshotInProgress = tt.snapshotInProgress
			u.entryInProgress = tt.entryInProgress
			u.checkInvariants(t)

			u.acceptInProgress()
			u.checkInvariants(t)
			require.Equal(t, tt.wentryInProgress, u.entryInProgress)
			require.Equal(t, tt.wsnapshotInProgress, u.snapshotInProgress)
		})
	}
}

func TestUnstableStableTo(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	snap4 := &pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 4, Term: 1}}
	for _, tt := range []struct {
		ls              LogSlice
		entryInProgress uint64
		snap            *pb.Snapshot
		index, term     uint64

		wprev            uint64
		wentryInProgress uint64
		wlen             int
	}{
		{
			LogSlice{}, 0, nil,
			5, 1,
			0, 0, 0,
		},
		{
			prev4.append(1), 5, nil,
			5, 1, // stable to the first entry
			5, 5, 0,
		},
		{
			prev4.append(1, 1), 5, nil,
			5, 1, // stable to the first entry
			5, 5, 1,
		},
		{
			prev4.append(1, 1), 6, nil,
			5, 1, // stable to the first entry and in-progress ahead
			5, 6, 1,
		},
		{
			entryID{term: 1, index: 5}.append(2), 6, nil,
			6, 1, // stable to the first entry and term mismatch
			5, 6, 1,
		},
		{
			prev4.append(1), 5, nil,
			4, 1, // stable to old entry
			4, 5, 1,
		},
		{
			prev4.append(1), 5, nil,
			4, 2, // stable to old entry
			4, 5, 1,
		},
		// with snapshot
		{
			prev4.append(1), 5, snap4,
			5, 1, // stable to the first entry
			5, 5, 0,
		},
		{
			prev4.append(1, 1), 5, snap4,
			5, 1, // stable to the first entry
			5, 5, 1,
		},
		{
			prev4.append(1, 1), 6, snap4,
			5, 1, // stable to the first entry and in-progress ahead
			5, 6, 1,
		},
		{
			entryID{term: 1, index: 5}.append(2), 6,
			&pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 5, Term: 1}},
			6, 1, // stable to the first entry and term mismatch
			5, 6, 1,
		},
		{
			prev4.append(1), 5, snap4,
			4, 1, // stable to snapshot
			4, 5, 1,
		},
		{
			prev4.append(2), 5, snap4,
			4, 1, // stable to old entry
			4, 5, 1,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.entryInProgress = tt.entryInProgress
			u.snapshotInProgress = u.snapshot != nil && u.entryInProgress > u.prev.index
			u.checkInvariants(t)

			if u.snapshotInProgress {
				u.stableSnapTo(u.snapshot.Metadata.Index)
			}
			u.checkInvariants(t)
			u.stableTo(LogMark{Term: tt.term, Index: tt.index})
			u.checkInvariants(t)
			require.Equal(t, tt.wprev, u.prev.index)
			require.Equal(t, tt.wentryInProgress, u.entryInProgress)
			require.Equal(t, tt.wlen, len(u.entries))
		})
	}
}

func TestUnstableTruncateAndAppend(t *testing.T) {
	prev4 := entryID{term: 1, index: 4}
	for _, tt := range []struct {
		ls              LogSlice
		entryInProgress uint64
		snap            *pb.Snapshot
		app             LogSlice

		want             LogSlice
		wentryInProgress uint64
	}{
		// append to the end
		{
			prev4.append(1), 4, nil,
			entryID{term: 1, index: 5}.append(1, 1),
			prev4.append(1, 1, 1),
			4,
		},
		{
			prev4.append(1), 5, nil,
			entryID{term: 1, index: 5}.append(1, 1),
			prev4.append(1, 1, 1),
			5,
		},
		// replace the unstable entries
		{
			prev4.append(1), 4, nil,
			prev4.append(2, 2),
			prev4.append(2, 2),
			4,
		},
		{
			prev4.append(1), 4, nil,
			entryID{term: 1, index: 3}.append(2, 2, 2),
			entryID{term: 1, index: 3}.append(2, 2, 2),
			3,
		},
		{
			prev4.append(1), 5, nil,
			prev4.append(2, 2),
			prev4.append(2, 2),
			4,
		},
		// truncate the existing entries and append
		{
			prev4.append(1, 1, 1), 4, nil,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			4,
		},
		{
			prev4.append(1, 1, 1), 4, nil,
			entryID{term: 1, index: 6}.append(2, 2),
			prev4.append(1, 1, 2, 2),
			4,
		},
		{
			prev4.append(1, 1, 1), 5, nil,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			5,
		},
		{
			prev4.append(1, 1, 1), 6, nil,
			entryID{term: 1, index: 5}.append(2),
			prev4.append(1, 2),
			5,
		},
	} {
		t.Run("", func(t *testing.T) {
			u := newUnstableForTesting(tt.ls, tt.snap)
			u.entryInProgress = tt.entryInProgress
			u.snapshotInProgress = u.snapshot != nil && u.entryInProgress > u.prev.index
			u.checkInvariants(t)

			u.truncateAndAppend(tt.app)
			u.checkInvariants(t)
			require.Equal(t, tt.want, u.LogSlice)
			require.Equal(t, tt.wentryInProgress, u.entryInProgress)
		})
	}
}
