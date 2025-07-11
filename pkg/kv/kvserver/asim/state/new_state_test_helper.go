// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
)

// NewStateWithDistribution returns a State where the stores given are
// initialized with the specified % of the replicas. This is done on a best
// effort basis, given the replication factor. It may be impossible to satisfy
// some distributions, for example: percentOfReplicas {1: 0.40, 2: 0.20, 3:
// 0.20}, replicationFactor 3, would be impossible to satisfy as the only
// distribution possible is {1: 0.33, 2: 0.33, 3: 0.33} given a replication
// factor of 3. A best effort distribution is applied in these cases.
func NewStateWithDistribution(
	percentOfReplicas []float64,
	ranges, replicationFactor, keyspace int,
	settings *config.SimulationSettings,
) State {
	numNodes := len(percentOfReplicas)
	// Currently multi-store is not tested for correctness. Default to a single
	// store per node.
	clusterInfo := ClusterInfoWithStoreCount(numNodes, 1 /* storesPerNode */)
	s := LoadClusterInfo(clusterInfo, settings)

	stores := make([]StoreID, numNodes)
	for i, store := range s.Stores() {
		stores[i] = store.StoreID()
	}
	spanConfig := defaultSpanConfig
	spanConfig.NumReplicas = int32(replicationFactor)
	spanConfig.NumVoters = int32(replicationFactor)

	rangesInfo := RangesInfoWithDistribution(
		stores,
		percentOfReplicas,
		percentOfReplicas,
		ranges,
		DefaultSpanConfigWithRF(replicationFactor),
		int64(MinKey),
		int64(keyspace),
		0, /* rangeSize */
	)
	LoadRangeInfo(s, rangesInfo...)
	return s
}

// NewStateWithReplCounts returns a new test state where each store is
// initialized the given number of replicas. The required number of ranges is
// inferred from the replication factor and the replica count.
func NewStateWithReplCounts(
	replCounts map[StoreID]int, replicationFactor, keyspace int, settings *config.SimulationSettings,
) State {
	clusterInfo := ClusterInfoWithStoreCount(len(replCounts), 1 /* storesPerNode */)
	rangesInfo := RangesInfoWithReplicaCounts(replCounts, keyspace, replicationFactor, 0 /* rangeSize */)
	return LoadConfig(clusterInfo, rangesInfo, settings)
}

// NewStateEvenDistribution returns a new State where the replica count per
// store is equal.
func NewStateEvenDistribution(
	stores, ranges, replicationFactor, keyspace int, settings *config.SimulationSettings,
) State {
	clusterInfo := ClusterInfoWithStoreCount(stores, 1 /* storesPerNode*/)
	rangesInfo := RangesInfoEvenDistribution(stores, ranges, int64(MinKey), int64(keyspace), replicationFactor, 0 /* rangeSize */)
	return LoadConfig(clusterInfo, rangesInfo, settings)
}

// NewStateSkewedDistribution returns a new State where the replica count per
// store is skewed.
func NewStateSkewedDistribution(
	stores, ranges, replicationFactor, keyspace int, settings *config.SimulationSettings,
) State {
	clusterInfo := ClusterInfoWithStoreCount(stores, 1 /* storesPerNode */)
	rangesInfo := RangesInfoSkewedDistribution(stores, ranges, int64(MinKey), int64(keyspace), replicationFactor, 0 /* rangeSize */)
	return LoadConfig(clusterInfo, rangesInfo, settings)
}

// NewStateRandDistribution returns a new State where the replica count per
// store is randomized.
func NewStateRandDistribution(
	seed int64,
	stores int,
	ranges int,
	keyspace int,
	replicationFactor int,
	settings *config.SimulationSettings,
) State {
	randSource := rand.New(rand.NewSource(seed))
	clusterInfo := ClusterInfoWithStoreCount(stores, 1 /* storesPerNode */)
	rangesInfo := RangesInfoRandDistribution(randSource, stores, ranges, int64(MinKey),
		int64(keyspace), replicationFactor, 0 /* rangeSize */)
	return LoadConfig(clusterInfo, rangesInfo, settings)
}

// NewStateWeightedRandDistribution returns a new State where the replica count
// per store is weighted randomized based on weightedStores.
func NewStateWeightedRandDistribution(
	seed int64,
	weightedStores []float64,
	ranges int,
	keyspace int,
	replicationFactor int,
	settings *config.SimulationSettings,
) State {
	randSource := rand.New(rand.NewSource(seed))
	clusterInfo := ClusterInfoWithStoreCount(len(weightedStores), 1 /* storesPerNode */)
	rangesInfo := RangesInfoWeightedRandDistribution(randSource, weightedStores, ranges, int64(MinKey),
		int64(keyspace), replicationFactor, 0 /* rangeSize */)
	return LoadConfig(clusterInfo, rangesInfo, settings)
}
