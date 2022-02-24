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

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// ReplicaLoad tracks a sliding window of throughput on a replica. By default,
// there are 6, 5 minute sliding windows.
type ReplicaLoad struct {
	batchRequests *replicaStats
	requests      *replicaStats
	writeKeys     *replicaStats
	readKeys      *replicaStats
	writeBytes    *replicaStats
	readBytes     *replicaStats
}

func newReplicaLoad(clock *hlc.Clock, getNodeLocality localityOracle) *ReplicaLoad {
	return &ReplicaLoad{
		batchRequests: newReplicaStats(clock, getNodeLocality),
		requests:      newReplicaStats(clock, getNodeLocality),
		writeKeys:     newReplicaStats(clock, getNodeLocality),
		readKeys:      newReplicaStats(clock, getNodeLocality),
		writeBytes:    newReplicaStats(clock, getNodeLocality),
		readBytes:     newReplicaStats(clock, getNodeLocality),
	}
}

// split will distribute the load in the calling struct, evenly between itself
// and other.
func (rl *ReplicaLoad) split(other *ReplicaLoad) {
	rl.batchRequests.splitRequestCounts(other.batchRequests)
	rl.requests.splitRequestCounts(other.requests)
	rl.writeKeys.splitRequestCounts(other.writeKeys)
	rl.readKeys.splitRequestCounts(other.readKeys)
	rl.writeBytes.splitRequestCounts(other.writeBytes)
	rl.readBytes.splitRequestCounts(other.readBytes)
}

// merge will combine the tracked load in other, into the calling struct.
func (rl *ReplicaLoad) merge(other *ReplicaLoad) {
	rl.batchRequests.mergeRequestCounts(other.batchRequests)
	rl.requests.mergeRequestCounts(other.requests)
	rl.writeKeys.mergeRequestCounts(other.writeKeys)
	rl.readKeys.mergeRequestCounts(other.readKeys)
	rl.writeBytes.mergeRequestCounts(other.writeBytes)
	rl.readBytes.mergeRequestCounts(other.readBytes)
}

// reset will clear all recorded history.
func (rl *ReplicaLoad) reset() {
	rl.batchRequests.resetRequestCounts()
	rl.requests.resetRequestCounts()
	rl.writeKeys.resetRequestCounts()
	rl.readKeys.resetRequestCounts()
	rl.writeBytes.resetRequestCounts()
	rl.readBytes.resetRequestCounts()
}
