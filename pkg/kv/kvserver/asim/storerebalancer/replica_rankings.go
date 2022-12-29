// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	return replRankings.TopLoad()
}
