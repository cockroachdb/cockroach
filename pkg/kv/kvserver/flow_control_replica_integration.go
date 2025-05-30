// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"

type replicaForRACv2 Replica

var _ replica_rac2.ReplicaForTesting = &replicaForRACv2{}

// IsScratchRange implements replica_rac2.ReplicaForTesting.
func (r *replicaForRACv2) IsScratchRange() bool {
	return (*Replica)(r).IsScratchRange()
}

// MuLock implements replica_rac2.ReplicaForRaftNode.
func (r *replicaForRACv2) MuLock() {
	r.mu.Lock()
}

// MuUnlock implements replica_rac2.ReplicaForRaftNode.
func (r *replicaForRACv2) MuUnlock() {
	r.mu.Unlock()
}
