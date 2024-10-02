// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storerebalancer

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/stretchr/testify/require"
)

func TestHottestRanges(t *testing.T) {
	settings := config.DefaultSimulationSettings()
	s := state.NewStateWithReplCounts(map[state.StoreID]int{1: 7, 2: 7, 3: 7}, 3, 1000 /* keyspace */, settings)
	// Set the QPS to be a testing rate to be rangeID * 100 for each range.
	// NB: Normally the subsequent lease transfer would erase the QPS, however
	// here the testing rate remains constants despite resets of actual counts.
	for i := 2; i < 8; i++ {
		state.TestingSetRangeQPS(s, state.RangeID(i), float64(i*100))
	}

	mapper := func(candidates []kvserver.CandidateReplica, f func(kvserver.CandidateReplica) int) []int {
		ret := []int{}
		for _, candidate := range candidates {
			ret = append(ret, f(candidate))
		}
		return ret
	}

	qpsF := func(c kvserver.CandidateReplica) int {
		return int(c.RangeUsageInfo().QueriesPerSecond)
	}
	ridF := func(c kvserver.CandidateReplica) int {
		return int(c.GetRangeID())
	}

	// Transfer leases so that the leases are: s1[r2,r3], s2[r4,r5], s3[r6,r7].
	s.TransferLease(2, 1)
	s.TransferLease(3, 1)
	s.TransferLease(4, 2)
	s.TransferLease(5, 2)
	s.TransferLease(6, 3)
	s.TransferLease(7, 3)

	hot1 := hottestRanges(s, 1, load.Queries)
	hot2 := hottestRanges(s, 2, load.Queries)
	hot3 := hottestRanges(s, 3, load.Queries)

	// NB: We only assert on the ranges where the store holds a lease. The
	// other replicas will be included, however will have a QPS of zero and be
	// filtered out in the store rebalancer. This matches the
	// replica_rankings.go behavior in kvserver.
	require.Equal(t, []int{300, 200}, mapper(hot1, qpsF)[:2])
	require.Equal(t, []int{500, 400}, mapper(hot2, qpsF)[:2])
	require.Equal(t, []int{700, 600}, mapper(hot3, qpsF)[:2])

	require.Equal(t, []int{3, 2}, mapper(hot1, ridF)[:2])
	require.Equal(t, []int{5, 4}, mapper(hot2, ridF)[:2])
	require.Equal(t, []int{7, 6}, mapper(hot3, ridF)[:2])
}
