// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
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

package quorum

import (
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// JointConfig is a configuration of two groups of (possibly overlapping)
// majority configurations. Decisions require the support of both majorities.
type JointConfig [2]MajorityConfig

func (c JointConfig) String() string {
	if len(c[1]) > 0 {
		return c[0].String() + "&&" + c[1].String()
	}
	return c[0].String()
}

// IDs returns a newly initialized map representing the set of voters present
// in the joint configuration.
func (c JointConfig) IDs() map[pb.PeerID]struct{} {
	m := make(map[pb.PeerID]struct{}, len(c[0])+len(c[1]))
	for _, cc := range c {
		for id := range cc {
			m[id] = struct{}{}
		}
	}
	return m
}

// Visit calls the given function for each unique voter ID in the joint
// configuration.
func (c JointConfig) Visit(f func(pb.PeerID)) {
	for id := range c[0] {
		f(id)
	}
	for id := range c[1] {
		if _, ok := c[0][id]; ok {
			continue // skip duplicate
		}
		f(id)
	}
}

// Describe returns a (multi-line) representation of the commit indexes for the
// given lookuper.
func (c JointConfig) Describe(l AckedIndexer) string {
	return MajorityConfig(c.IDs()).Describe(l)
}

// CommittedIndex returns the largest committed index for the given joint
// quorum. An index is jointly committed if it is committed in both constituent
// majorities.
func (c JointConfig) CommittedIndex(l AckedIndexer) Index {
	return min(c[0].CommittedIndex(l), c[1].CommittedIndex(l))
}

// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending, lost, or won. A joint quorum
// requires both majority quorums to vote in favor.
func (c JointConfig) VoteResult(votes map[pb.PeerID]bool) VoteResult {
	r1 := c[0].VoteResult(votes)
	r2 := c[1].VoteResult(votes)

	if r1 == r2 {
		// If they agree, return the agreed state.
		return r1
	}
	if r1 == VoteLost || r2 == VoteLost {
		// If either config has lost, loss is the only possible outcome.
		return VoteLost
	}
	// One side won, the other one is pending, so the whole outcome is.
	return VotePending
}

// LeadSupportExpiration takes slices of timestamps peers in both configurations
// have promised a leader support until and returns the timestamp until which
// the leader is guaranteed support until.
//
// Note that we accept two slices of timestamps instead of one map of a joint
// configuration for performance reasons. Having two contiguous slices is more
// performant than one map based on the ComputeLeadSupportUntil microbenchmark.
func (c JointConfig) LeadSupportExpiration(
	supportedC0 []hlc.Timestamp, supportedC1 []hlc.Timestamp,
) hlc.Timestamp {
	qse := c[0].LeadSupportExpiration(supportedC0)
	qse.Backward(c[1].LeadSupportExpiration(supportedC1))
	return qse
}
