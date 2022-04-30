// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

type testSnapshoter struct {
	progress map[uint64]tracker.Progress
	lease    roachpb.Lease
}

func (s testSnapshoter) raftWithProgressIfLeader(f withProgressVisitor) {
	for id, pr := range s.progress {
		typ := raft.ProgressTypePeer
		if pr.IsLearner {
			typ = raft.ProgressTypeLearner
		}
		f(id, typ, pr)
	}
}

func (s testSnapshoter) getLeaseRLocked() (cur, next roachpb.Lease) {
	return s.lease, roachpb.Lease{}
}

func TestRaftSnapshotQueuePriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		// if true, needs snapshot
		leaseholder bool
		voter       bool
		learner     bool
		// expected priority
		expected float64
	}{
		{false, false, false, -1},
		{false, false, true, raftSnapshotToLearnerPriority},
		{false, true, false, raftSnapshotToVoterPriority},
		{false, true, true, raftSnapshotToVoterPriority},
		{true, false, false, raftSnapshotToLeaseholderPriority},
		{true, false, true, raftSnapshotToLeaseholderPriority},
		{true, true, false, raftSnapshotToLeaseholderPriority},
		{true, true, true, raftSnapshotToLeaseholderPriority},
	}
	for _, c := range testCases {
		var parts []string
		if c.leaseholder {
			parts = append(parts, "leaseholder")
		}
		if c.voter {
			parts = append(parts, "voter")
		}
		if c.learner {
			parts = append(parts, "learner")
		}
		name := strings.Join(parts, "+")
		if name == "" {
			name = "none"
		}

		t.Run(name, func(t *testing.T) {
			leaderState := tracker.StateReplicate
			leaseholderState := tracker.StateReplicate
			voterState := tracker.StateReplicate
			learnerState := tracker.StateReplicate
			if c.leaseholder {
				leaseholderState = tracker.StateSnapshot
			}
			if c.voter {
				voterState = tracker.StateSnapshot
			}
			if c.learner {
				learnerState = tracker.StateSnapshot
			}
			s := testSnapshoter{
				progress: map[uint64]tracker.Progress{
					1: {State: leaderState},
					2: {State: leaseholderState},
					3: {State: voterState},
					4: {State: learnerState, IsLearner: true},
				},
				lease: roachpb.Lease{
					Replica: roachpb.ReplicaDescriptor{ReplicaID: 2},
				},
			}

			priority := raftSnapshotQueuePriority(s)
			require.Equal(t, c.expected, priority)
		})
	}
}
