// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserverpb

import "github.com/cockroachdb/cockroach/pkg/storage/enginepb"

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCPersistentStats) ToStats() enginepb.MVCCStats {
	return enginepb.MVCCStats(*ms)
}

// ToStatsPtr converts the receiver to a *MVCCStats.
func (ms *MVCCPersistentStats) ToStatsPtr() *enginepb.MVCCStats {
	return (*enginepb.MVCCStats)(ms)
}

// ToRangeAppliedState converts the ReplicaState to a RangeAppliedState.
func (m *ReplicaState) ToRangeAppliedState() RangeAppliedState {
	// NB: RangeAppliedState materializes in the state machine on every raft
	// commands batch application, and must be byte-to-byte consistent across
	// replicas. If you need to change the returned fields here, most likely this
	// needs to be accompanied by a below-raft migration.
	return RangeAppliedState{
		RaftAppliedIndex:     m.RaftAppliedIndex,
		LeaseAppliedIndex:    m.LeaseAppliedIndex,
		RangeStats:           MVCCPersistentStats(*m.Stats),
		RaftClosedTimestamp:  m.RaftClosedTimestamp,
		RaftAppliedIndexTerm: m.RaftAppliedIndexTerm,
	}
}
