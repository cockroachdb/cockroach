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

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
)

// Status contains information about this Raft peer and its view of the system.
// The Progress is only populated on the leader.
type Status struct {
	BasicStatus
	Config   tracker.Config
	Progress map[pb.PeerID]tracker.Progress
}

// BasicStatus contains basic information about the Raft peer. It does not allocate.
type BasicStatus struct {
	ID pb.PeerID

	pb.HardState
	SoftState

	Applied uint64

	LeadTransferee pb.PeerID
}

// SparseStatus is a variant of Status without Config and Progress.Inflights,
// which are expensive to copy.
type SparseStatus struct {
	BasicStatus
	Progress map[pb.PeerID]tracker.Progress
}

// withProgress calls the supplied visitor to introspect the progress for the
// supplied raft group. Cannot be used to introspect p.Inflights.
func withProgress(r *raft, visitor func(id pb.PeerID, typ ProgressType, pr tracker.Progress)) {
	r.trk.Visit(func(id pb.PeerID, pr *tracker.Progress) {
		typ := ProgressTypePeer
		if pr.IsLearner {
			typ = ProgressTypeLearner
		}
		p := *pr
		p.Inflights = nil
		visitor(id, typ, p)
	})
}

func getProgressCopy(r *raft) map[pb.PeerID]tracker.Progress {
	m := make(map[pb.PeerID]tracker.Progress, len(r.trk.Progress))
	r.trk.Visit(func(id pb.PeerID, pr *tracker.Progress) {
		p := *pr
		p.Inflights = pr.Inflights.Clone()
		pr = nil

		m[id] = p
	})
	return m
}

func getBasicStatus(r *raft) BasicStatus {
	s := BasicStatus{
		ID:             r.id,
		LeadTransferee: r.leadTransferee,
	}
	s.HardState = r.hardState()
	s.SoftState = r.softState()
	s.Applied = r.raftLog.applied
	if s.RaftState == StateFollower && s.Lead == r.id {
		// A raft leader's term ends when it is shut down. It'll rejoin its peers as
		// a follower when it comes back up, but its Lead and Term field may still
		// correspond to its pre-restart leadership term. We expect this to quickly
		// be updated when it hears from the new leader, if one was elected in its
		// absence, or when it campaigns.
		//
		// The layers above raft (in particular kvserver) do not handle the case
		// where a raft node's state is StateFollower but its lead field points to
		// itself. They expect the Lead field to correspond to the current leader,
		// which we know we are not. For their benefit, we overwrite the Lead field
		// to None.
		//
		// TODO(arul): the layers above should not conflate Lead with current
		// leader. Fix that and get rid of this overwrite.
		s.HardState.Lead = None
	}
	return s
}

// getStatus gets a copy of the current raft status.
func getStatus(r *raft) Status {
	var s Status
	s.BasicStatus = getBasicStatus(r)
	if s.RaftState == StateLeader {
		s.Progress = getProgressCopy(r)
	}
	s.Config = r.trk.Config.Clone()
	return s
}

// getSparseStatus gets a sparse[*] copy of the current raft status.
//
// [*] See struct definition for what this entails.
func getSparseStatus(r *raft) SparseStatus {
	status := SparseStatus{
		BasicStatus: getBasicStatus(r),
	}
	if status.RaftState == StateLeader {
		status.Progress = make(map[pb.PeerID]tracker.Progress, len(r.trk.Progress))
		withProgress(r, func(id pb.PeerID, _ ProgressType, pr tracker.Progress) {
			status.Progress[id] = pr
		})
	}
	return status
}

// MarshalJSON translates the raft status into JSON.
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"applied":%d,"progress":{`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState, s.Applied)

	if len(s.Progress) == 0 {
		j += "},"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%x":{"match":%d,"next":%d,"state":%q},`, k, v.Match, v.Next, v.State)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "},"
	}

	j += fmt.Sprintf(`"leadtransferee":"%x"}`, s.LeadTransferee)
	return []byte(j), nil
}

func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		getLogger().Panicf("unexpected error: %v", err)
	}
	return string(b)
}
