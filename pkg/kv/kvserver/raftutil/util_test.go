// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/tracker"
)

func TestReplicaIsBehind(t *testing.T) {
	const replicaID = 3
	makeStatus := func(f func(*raft.Status)) *raft.Status {
		st := new(raft.Status)
		st.Commit = 10
		st.Progress = make(map[uint64]tracker.Progress)
		f(st)
		return st
	}

	tests := []struct {
		name   string
		st     *raft.Status
		expect bool
	}{
		{
			name: "local follower",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateFollower
			}),
			expect: true,
		},
		{
			name: "local candidate",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateCandidate
			}),
			expect: true,
		},
		{
			name: "local leader, no progress for peer",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
			}),
			expect: true,
		},
		{
			name: "local leader, peer leader",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateReplicate}
				st.Lead = replicaID
			}),
			expect: false,
		},
		{
			name: "local leader, peer state probe",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateProbe}
			}),
			expect: true,
		},
		{
			name: "local leader, peer state snapshot",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateSnapshot}
			}),
			expect: true,
		},
		{
			name: "local leader, peer state replicate, match < commit",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateReplicate, Match: 9}
			}),
			expect: true,
		},
		{
			name: "local leader, peer state replicate, match == commit",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateReplicate, Match: 10}
			}),
			expect: false,
		},
		{
			name: "local leader, peer state replicate, match > commit",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateReplicate, Match: 11}
			}),
			expect: false,
		},
		{
			name:   "nil raft status",
			st:     nil,
			expect: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, ReplicaIsBehind(tt.st, replicaID))
		})
	}
}

func TestReplicaMayNeedSnapshot(t *testing.T) {
	const firstIndex = 10
	const replicaID = 3
	makeStatus := func(f func(*raft.Status)) *raft.Status {
		st := new(raft.Status)
		st.Commit = 10
		st.Progress = make(map[uint64]tracker.Progress)
		f(st)
		return st
	}

	tests := []struct {
		name   string
		st     *raft.Status
		expect ReplicaNeedsSnapshotStatus
	}{
		{
			name: "local follower",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateFollower
			}),
			expect: LocalReplicaNotLeader,
		},
		{
			name: "local candidate",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateCandidate
			}),
			expect: LocalReplicaNotLeader,
		},
		{
			name: "local leader, no progress for peer",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
			}),
			expect: ReplicaUnknown,
		},
		{
			name: "local leader, peer state probe",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateProbe}
			}),
			expect: ReplicaStateProbe,
		},
		{
			name: "local leader, peer state snapshot",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateSnapshot}
			}),
			expect: ReplicaStateSnapshot,
		},
		{
			name: "local leader, peer state replicate, match+1 < firstIndex",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateReplicate, Match: 8}
			}),
			expect: ReplicaMatchBelowLeadersFirstIndex,
		},
		{
			name: "local leader, peer state replicate, match+1 == firstIndex",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateReplicate, Match: 9}
			}),
			expect: NoSnapshotNeeded,
		},
		{
			name: "local leader, peer state replicate, match+1 == firstIndex",
			st: makeStatus(func(st *raft.Status) {
				st.RaftState = raft.StateLeader
				st.Progress[replicaID] = tracker.Progress{State: tracker.StateReplicate, Match: 10}
			}),
			expect: NoSnapshotNeeded,
		},
		{
			name:   "nil raft status",
			st:     nil,
			expect: NoRaftStatusAvailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, ReplicaMayNeedSnapshot(tt.st, firstIndex, replicaID))
		})
	}
}
