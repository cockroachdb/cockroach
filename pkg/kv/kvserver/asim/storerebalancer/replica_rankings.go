// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storerebalancer

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

func hottestRanges(
	state state.State, storeID state.StoreID, dim load.Dimension,
) []kvserver.CandidateReplica {
	replRankings := kvserver.NewReplicaRankings()
	accumulator := kvserver.NewReplicaAccumulator(dim)
	// NB: This follows the actual implementation, where replicas are included
	// regardless of whether the replica is a lease holder. These are later
	// filtered out in the store rebalancer.
	for _, repl := range state.Replicas(storeID) {
		candidateReplica := newSimulatorReplica(repl, state)
		accumulator.AddReplica(candidateReplica)
	}
	replRankings.Update(accumulator)
	return replRankings.TopLoad(dim)
}
