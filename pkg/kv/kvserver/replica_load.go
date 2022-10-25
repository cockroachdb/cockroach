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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ReplicaLoad tracks a sliding window of throughput on a replica. By default,
// there are 6, 5 minute sliding windows.
type ReplicaLoad struct {
	batchRequests *replicastats.ReplicaStats
	requests      *replicastats.ReplicaStats
	writeKeys     *replicastats.ReplicaStats
	readKeys      *replicastats.ReplicaStats
	writeBytes    *replicastats.ReplicaStats
	readBytes     *replicastats.ReplicaStats
	nanos         *replicastats.ReplicaStats
}

// NewReplicaLoad returns a new ReplicaLoad, which may be used to track the
// request throughput of a replica.
func NewReplicaLoad(clock *hlc.Clock, getNodeLocality replicastats.LocalityOracle) *ReplicaLoad {
	// NB: We only wish to record the locality of a request for QPS, where it
	// as only follow-the-workload lease transfers use this per-locality
	// request count. Maintaining more than one bucket for client requests
	// increases the memory footprint O(localities).
	return &ReplicaLoad{
		batchRequests: replicastats.NewReplicaStats(clock, getNodeLocality),
		requests:      replicastats.NewReplicaStats(clock, nil),
		writeKeys:     replicastats.NewReplicaStats(clock, nil),
		readKeys:      replicastats.NewReplicaStats(clock, nil),
		writeBytes:    replicastats.NewReplicaStats(clock, nil),
		readBytes:     replicastats.NewReplicaStats(clock, nil),
		nanos:         replicastats.NewReplicaStats(clock, nil),
	}
}

// split will distribute the load in the calling struct, evenly between itself
// and other.
func (rl *ReplicaLoad) split(other *ReplicaLoad) {
	rl.batchRequests.SplitRequestCounts(other.batchRequests)
	rl.requests.SplitRequestCounts(other.requests)
	rl.writeKeys.SplitRequestCounts(other.writeKeys)
	rl.readKeys.SplitRequestCounts(other.readKeys)
	rl.writeBytes.SplitRequestCounts(other.writeBytes)
	rl.readBytes.SplitRequestCounts(other.readBytes)
	rl.nanos.SplitRequestCounts(other.nanos)
}

// merge will combine the tracked load in other, into the calling struct.
func (rl *ReplicaLoad) merge(other *ReplicaLoad) {
	rl.batchRequests.MergeRequestCounts(other.batchRequests)
	rl.requests.MergeRequestCounts(other.requests)
	rl.writeKeys.MergeRequestCounts(other.writeKeys)
	rl.readKeys.MergeRequestCounts(other.readKeys)
	rl.writeBytes.MergeRequestCounts(other.writeBytes)
	rl.readBytes.MergeRequestCounts(other.readBytes)
	rl.nanos.MergeRequestCounts(other.nanos)
}

// reset will clear all recorded history.
func (rl *ReplicaLoad) reset() {
	rl.batchRequests.ResetRequestCounts()
	rl.requests.ResetRequestCounts()
	rl.writeKeys.ResetRequestCounts()
	rl.readKeys.ResetRequestCounts()
	rl.writeBytes.ResetRequestCounts()
	rl.readBytes.ResetRequestCounts()
	rl.nanos.ResetRequestCounts()
}
