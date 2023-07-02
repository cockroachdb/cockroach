// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package state

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type requestCount struct {
	req int
	id  int
}

type requestCounts []requestCount

func (s requestCounts) Len() int { return len(s) }
func (s requestCounts) Less(i, j int) bool {
	return s[i].req > s[j].req
}
func (s requestCounts) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func evenDistribution(n int) []float64 {
	distribution := []float64{}
	frac := 1.0 / float64(n)
	for i := 0; i < n; i++ {
		distribution = append(distribution, frac)
	}
	return distribution
}

func skewedDistribution(n, k int) []float64 {
	distribution := []float64{}
	rem := k
	for i := 0; i < n; i++ {
		rem /= 2
		distribution = append(distribution, float64(rem))
	}
	return distribution
}

func exactDistribution(counts []int) []float64 {
	distribution := make([]float64, len(counts))
	total := 0
	for _, count := range counts {
		total += count
	}
	for i, count := range counts {
		distribution[i] = float64(count) / float64(total)
	}
	return distribution
}

// RangesInfoWithDistribution returns a RangesInfo, where the stores given are
// initialized with the specified % of the replicas. This is done on a best
// effort basis, given the replication factor. It may be impossible to satisfy
// some distributions, for example: percentOfReplicas {1: 0.40, 2: 0.20, 3:
// 0.20}, replicationFactor 3, would be impossible to satisfy as the only
// distribution possible is {1: 0.33, 2: 0.33, 3: 0.33} given a replication
// factor of 3. A best effort distribution is applied in these cases.
func RangesInfoWithDistribution(
	stores []StoreID,
	replicaWeights []float64,
	leaseWeights []float64,
	numRanges int,
	config roachpb.SpanConfig,
	minKey, maxKey, rangeSize int64,
) RangesInfo {
	ret := make([]RangeInfo, numRanges)
	rf := int(config.NumReplicas)

	targetReplicaCount := make(requestCounts, len(stores))
	targetLeaseCount := map[StoreID]int{}
	for i, store := range stores {
		requiredReplicas := int(float64(numRanges*rf) * (replicaWeights[i]))
		requiredLeases := int(float64(numRanges) * (leaseWeights[i]))
		targetReplicaCount[i] = requestCount{
			req: requiredReplicas,
			id:  int(store),
		}
		targetLeaseCount[store] = requiredLeases
	}

	// If there are no ranges specified, default to 1 range.
	if numRanges == 0 {
		numRanges = 1
	}

	// There cannot be less keys than there are ranges.
	if int64(numRanges) > maxKey-minKey {
		panic(fmt.Sprintf(
			"The number of ranges specified (%d) is less than num keys in startKey-endKey (%d %d) ",
			numRanges, minKey, maxKey))
	}
	// We create each range in sorted order by start key. Then assign replicas
	// to stores by finding the store with the highest remaining target replica
	// count remaining; repeating for each replica.
	rangeInterval := int(float64(maxKey-minKey+1) / float64(numRanges))
	for rngIdx := 0; rngIdx < numRanges; rngIdx++ {
		key := Key(int64(rngIdx*rangeInterval)) + Key(minKey)
		configCopy := config
		rangeInfo := RangeInfo{
			Descriptor: roachpb.RangeDescriptor{
				StartKey: key.ToRKey(),
				InternalReplicas: make(
					[]roachpb.ReplicaDescriptor, configCopy.NumReplicas),
			},
			Config:      &configCopy,
			Leaseholder: 0,
			Size:        rangeSize,
		}

		sort.Sort(targetReplicaCount)
		maxLeaseRequestedIdx := 0
		for replCandidateIdx := 0; replCandidateIdx < rf; replCandidateIdx++ {
			targetReplicaCount[replCandidateIdx].req--
			storeID := StoreID(targetReplicaCount[replCandidateIdx].id)
			rangeInfo.Descriptor.InternalReplicas[replCandidateIdx] = roachpb.ReplicaDescriptor{
				StoreID: roachpb.StoreID(storeID),
			}
			if targetLeaseCount[storeID] >
				targetLeaseCount[StoreID(rangeInfo.Descriptor.InternalReplicas[maxLeaseRequestedIdx].StoreID)] {
				maxLeaseRequestedIdx = replCandidateIdx
			}
		}

		// Similar to finding the stores with the highest remaining target
		// replica count, use the store with the highest remaining target
		// leaseholder count as the leaseholder.
		lhStore := rangeInfo.Descriptor.InternalReplicas[maxLeaseRequestedIdx].StoreID
		targetLeaseCount[StoreID(lhStore)]--
		rangeInfo.Leaseholder = StoreID(lhStore)
		ret[rngIdx] = rangeInfo
	}

	return ret
}

// ClusterInfoWithDistribution returns a ClusterInfo. The ClusterInfo regions
// have nodes added to them according to the regionNodeWeights and numNodes
// given. In cases where the numNodes does not divide among the regions given
// their weights, a best effort apporach is taken so that the total number of
// aggregate matches numNodes.
func ClusterInfoWithDistribution(
	nodeCount int, storesPerNode int, regions []string, regionNodeWeights []float64,
) ClusterInfo {
	ret := ClusterInfo{}

	ret.Regions = make([]Region, len(regions))
	availableNodes := nodeCount
	for i, name := range regions {
		allocatedNodes := int(float64(nodeCount) * (regionNodeWeights[i]))
		if allocatedNodes > availableNodes {
			allocatedNodes = availableNodes
		}
		availableNodes -= allocatedNodes
		ret.Regions[i] = Region{
			Name:  name,
			Zones: []Zone{{Name: name + "_1", NodeCount: allocatedNodes, StoresPerNode: storesPerNode}},
		}
	}

	return ret
}

// ClusterInfoWithStoreCount returns a new ClusterInfo with the specified number of
// stores. There will be storesPerNode stores per node and a single region and zone.
func ClusterInfoWithStoreCount(nodeCount int, storesPerNode int) ClusterInfo {
	return ClusterInfoWithDistribution(
		nodeCount,
		storesPerNode,
		[]string{"AU_EAST"}, /* regions */
		[]float64{1},        /* regionNodeWeights */
	)
}

func makeStoreList(stores int) []StoreID {
	storeList := make([]StoreID, stores)
	for i := 0; i < stores; i++ {
		storeList[i] = StoreID(i + 1)
	}
	return storeList
}

func RangesInfoSkewedDistribution(
	stores int, ranges int, keyspace int, replicationFactor int, rangeSize int64,
) RangesInfo {
	distribution := skewedDistribution(stores, ranges)
	storeList := makeStoreList(stores)

	spanConfig := defaultSpanConfig
	spanConfig.NumReplicas = int32(replicationFactor)
	spanConfig.NumVoters = int32(replicationFactor)

	return RangesInfoWithDistribution(
		storeList, distribution, distribution, ranges, spanConfig,
		int64(MinKey), int64(keyspace), rangeSize)
}

func RangesInfoWithReplicaCounts(
	replCounts map[StoreID]int, keyspace, replicationFactor int, rangeSize int64,
) RangesInfo {
	stores := len(replCounts)
	counts := make([]int, stores)
	total := 0
	for store, count := range replCounts {
		counts[int(store-1)] = count
		total += count
	}
	ranges := total / replicationFactor

	distribution := exactDistribution(counts)
	storeList := makeStoreList(stores)

	spanConfig := defaultSpanConfig
	spanConfig.NumReplicas = int32(replicationFactor)
	spanConfig.NumVoters = int32(replicationFactor)

	return RangesInfoWithDistribution(
		storeList, distribution, distribution, ranges, spanConfig,
		int64(MinKey), int64(keyspace), rangeSize)
}

func RangesInfoEvenDistribution(
	stores int, ranges int, keyspace int, replicationFactor int, rangeSize int64,
) RangesInfo {
	distribution := evenDistribution(stores)
	storeList := makeStoreList(stores)

	spanConfig := defaultSpanConfig
	spanConfig.NumReplicas = int32(replicationFactor)
	spanConfig.NumVoters = int32(replicationFactor)

	return RangesInfoWithDistribution(
		storeList, distribution, distribution, ranges, spanConfig,
		int64(MinKey), int64(keyspace), rangeSize)
}

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
		spanConfig,
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
	rangesInfo := RangesInfoEvenDistribution(stores, ranges, keyspace, replicationFactor, 0 /* rangeSize */)
	return LoadConfig(clusterInfo, rangesInfo, settings)
}

// NewStateSkewedDistribution returns a new State where the replica count per
// store is skewed.
func NewStateSkewedDistribution(
	stores, ranges, replicationFactor, keyspace int, settings *config.SimulationSettings,
) State {
	clusterInfo := ClusterInfoWithStoreCount(stores, 1 /* storesPerNode */)
	rangesInfo := RangesInfoSkewedDistribution(stores, ranges, keyspace, replicationFactor, 0 /* rangeSize */)
	return LoadConfig(clusterInfo, rangesInfo, settings)
}
