// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type requestCount struct {
	req int
	id  int
}

type requestCounts []requestCount

func (s requestCounts) Len() int { return len(s) }
func (s requestCounts) Less(i, j int) bool {
	if s[i].req == s[j].req {
		return i < j
	}
	return s[i].req > s[j].req
}
func (s requestCounts) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func evenDistribution(numOfStores int) []float64 {
	distribution := []float64{}
	frac := 1.0 / float64(numOfStores)
	for i := 0; i < numOfStores; i++ {
		distribution = append(distribution, frac)
	}
	return distribution
}

func skewedDistribution(numOfStores int) []float64 {
	weights := make([]float64, numOfStores)
	// Sum of weights. Since weights computed won't add up to 1, we normalize it
	// by dividing the sum of weights. Sum is pre-computed here using the partial
	// sum formula of a geometric series: sum of 2^(-i) from i = 0 to k gives
	// 2-2^(-k).
	// Example: given 3 stores, cur(weights before normalization) is 1, 0.5, 0.25,
	// sum is 2.0-2^(-2) = 1.75. After normalization, weights are 0.57, 0.29,
	// 0.14.
	sum := 2.0 - math.Pow(2, float64(-(numOfStores-1)))
	cur := float64(1)
	for i := 0; i < numOfStores; i++ {
		// cur is 1, 0.5, 0.25, ...
		weights[i] = cur / sum
		cur /= 2
	}
	return weights
}

func exactDistribution(storeReplicaCount []int) []float64 {
	distribution := make([]float64, len(storeReplicaCount))
	total := 0
	for _, count := range storeReplicaCount {
		total += count
	}
	for i, count := range storeReplicaCount {
		distribution[i] = float64(count) / float64(total)
	}
	return distribution
}

func DefaultSpanConfigWithRF(rf int) roachpb.SpanConfig {
	spanConfig := defaultSpanConfig
	spanConfig.NumReplicas = int32(rf)
	spanConfig.NumVoters = int32(rf)
	return spanConfig
}

// weighted struct handles weighted random index selection from an input array,
// weightedStores.
//
// For example, consider input weightedStores = [0.1, 0.2, 0.7].
// - newWeighted constructs cumulative weighs, creating cumulativeWeighted [0.1,
// 0.3, 1.0].
// - rand function then randomly selects a number n within the range of [0.0,
// 1.0) and finds which bucket ([0.0, 0.1], (0.1, 0.3], (0.3, 1.0]) n falls
// under. It finds the smallest index within cumulativeWeights that >= n. Thus,
// indices with greater weights have a higher probability of being selected as
// they cover larger cumulative weights range. For instance, if it selects 0.5,
// Rand would return index 2 since 0.7 is the smallest index that is >= 0.5.
type weighted struct {
	cumulativeWeights []float64
}

// newWeighted constructs cumulative weights that are used later to select a
// single random index from weightedStores based on the associated weights.
func newWeighted(weightedStores []float64) weighted {
	cumulativeWeights := make([]float64, len(weightedStores))
	prefixSumWeight := float64(0)
	for i, item := range weightedStores {
		prefixSumWeight += item
		cumulativeWeights[i] = prefixSumWeight
	}
	const epsilon = 1e-10
	if math.Abs(cumulativeWeights[len(weightedStores)-1]-float64(1)) > epsilon {
		panic(fmt.Sprintf("total cumulative weights for all stores should sum up to 1 but got %.2f\n",
			cumulativeWeights[len(weightedStores)-1]))
	}
	return weighted{cumulativeWeights: cumulativeWeights}
}

// rand randomly picks an index from weightedStores based on the associated
// weights.
func (w weighted) rand(randSource *rand.Rand) int {
	r := randSource.Float64()
	index := sort.Search(len(w.cumulativeWeights), func(i int) bool { return w.cumulativeWeights[i] >= r })
	return index
}

// weightedRandDistribution generates a weighted random distribution across
// stores. It achieves this by randomly selecting an index from weightedStores
// 10 times while considering the weights, and repeating this process ten times.
// The output is a weighted random distribution reflecting the selections made.
func weightedRandDistribution(randSource *rand.Rand, weightedStores []float64) []float64 {
	w := newWeighted(weightedStores)
	numSamples := 10
	votes := make([]int, len(weightedStores))
	for i := 0; i < numSamples; i++ {
		index := w.rand(randSource)
		votes[index] += 1
	}
	return exactDistribution(votes)
}

// randDistribution generates a random distribution across stores. It achieves
// this by creating an array of size n, selecting random numbers from [0, 10)
// for each index, and returning the exact distribution of this result.
func randDistribution(randSource *rand.Rand, numOfStores int) []float64 {
	total := float64(0)
	distribution := make([]float64, numOfStores)
	for i := 0; i < numOfStores; i++ {
		num := float64(randSource.Intn(10))
		distribution[i] = num
		total += num
	}

	for i := 0; i < numOfStores; i++ {
		distribution[i] = distribution[i] / total
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
) (RangesInfo, string) {
	ret := make([]RangeInfo, numRanges)
	rf := int(config.NumReplicas)

	targetReplicaCount := make(requestCounts, len(stores))
	targetLeaseCount := map[StoreID]int{}
	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, "[")
	for i, store := range stores {
		requiredReplicas := int(float64(numRanges*rf) * (replicaWeights[i]))
		requiredLeases := int(float64(numRanges) * (leaseWeights[i]))
		targetReplicaCount[i] = requestCount{
			req: requiredReplicas,
			id:  int(store),
		}
		targetLeaseCount[store] = requiredLeases
		_, _ = fmt.Fprintf(&buf, "s%d:(%d,%d*)",
			int(store), requiredReplicas, requiredLeases)
		if i != len(stores)-1 {
			_, _ = fmt.Fprintf(&buf, ",")
		}
	}
	_, _ = fmt.Fprintf(&buf, "]")

	// If there are no ranges specified, default to 1 range.
	if numRanges == 0 {
		numRanges = 1
	}

	// There cannot be fewekeys than there are ranges.
	if int64(numRanges) > maxKey-minKey {
		panic(fmt.Sprintf(
			"The number of ranges specified (%d) is larger than num keys in startKey-endKey (%d %d) ",
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

	return ret, buf.String()
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
			Zones: []Zone{NewZone(name+"_1", allocatedNodes, storesPerNode)},
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
	stores int, ranges int, minKey int64, maxKey int64, replicationFactor int, rangeSize int64,
) (RangesInfo, string) {
	distribution := skewedDistribution(stores)
	storeList := makeStoreList(stores)

	return RangesInfoWithDistribution(
		storeList, distribution, distribution, ranges,
		DefaultSpanConfigWithRF(replicationFactor), minKey, maxKey, rangeSize)
}

func RangesInfoWithReplicaCounts(
	replCounts map[StoreID]int, keyspace, replicationFactor int, rangeSize int64,
) (RangesInfo, string) {
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

	return RangesInfoWithDistribution(
		storeList, distribution, distribution, ranges,
		DefaultSpanConfigWithRF(replicationFactor), int64(MinKey), int64(keyspace), rangeSize)
}

func RangesInfoEvenDistribution(
	stores int, ranges int, minKey int64, maxKey int64, replicationFactor int, rangeSize int64,
) (RangesInfo, string) {
	distribution := evenDistribution(stores)
	storeList := makeStoreList(stores)

	return RangesInfoWithDistribution(
		storeList, distribution, distribution, ranges,
		DefaultSpanConfigWithRF(replicationFactor), minKey, maxKey, rangeSize)
}

// RangesInfoWeightedRandDistribution returns a RangesInfo, where ranges are
// generated with a weighted random distribution across stores.
func RangesInfoWeightedRandDistribution(
	randSource *rand.Rand,
	weightedStores []float64,
	ranges int,
	minKey, maxKey int64,
	replicationFactor int,
	rangeSize int64,
) (RangesInfo, string) {
	if randSource == nil || len(weightedStores) == 0 {
		panic("randSource cannot be nil and weightedStores must be non-empty in order to generate weighted random range info")
	}
	distribution := weightedRandDistribution(randSource, weightedStores)
	storeList := makeStoreList(len(weightedStores))
	return RangesInfoWithDistribution(
		storeList,
		distribution,
		distribution,
		ranges,
		DefaultSpanConfigWithRF(replicationFactor),
		minKey, maxKey,
		rangeSize,
	)
}

// RangesInfoRandDistribution returns a RangesInfo, where ranges are generated
// with a random distribution across stores.
func RangesInfoRandDistribution(
	randSource *rand.Rand,
	stores int,
	ranges int,
	minKey, maxKey int64,
	replicationFactor int,
	rangeSize int64,
) (RangesInfo, string) {
	if randSource == nil {
		panic("randSource cannot be nil in order to generate random range info")
	}
	distribution := randDistribution(randSource, stores)
	storeList := makeStoreList(stores)

	spanConfig := defaultSpanConfig
	spanConfig.NumReplicas = int32(replicationFactor)
	spanConfig.NumVoters = int32(replicationFactor)

	return RangesInfoWithDistribution(
		storeList, distribution, distribution, ranges, DefaultSpanConfigWithRF(replicationFactor),
		minKey, maxKey, rangeSize)
}

func RangesInfoWithReplicaPlacement(
	rp ReplicaPlacement, numRanges int, config roachpb.SpanConfig, minKey, maxKey, rangeSize int64,
) (RangesInfo, string) {
	// If there are no ranges specified, default to 1 range.
	if numRanges == 0 {
		numRanges = 1
	}

	ret := initializeRangesInfoWithSpanConfigs(numRanges, config, minKey, maxKey, rangeSize)
	result := rp.findReplicaPlacementForEveryStoreSet(numRanges)
	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, "[")
	for i, ratio := range result {
		_, _ = fmt.Fprintf(&buf, "{")
		for j, sid := range ratio.StoreIDs {
			_, _ = fmt.Fprintf(&buf, "s%v", sid)
			if sid == ratio.LeaseholderID {
				_, _ = fmt.Fprintf(&buf, "*")
			}
			if j != len(ratio.StoreIDs)-1 {
				_, _ = fmt.Fprintf(&buf, ",")
			}
		}
		_, _ = fmt.Fprintf(&buf, "}:%d", ratio.Ranges)
		if i != len(result)-1 {
			_, _ = fmt.Fprintf(&buf, ",")
		}
	}
	_, _ = fmt.Fprintf(&buf, "]")

	rf := int(config.NumReplicas)

	for rngIdx := 0; rngIdx < len(ret); rngIdx++ {
		nextStoreSet := 0
		for i := 0; i < len(result); i++ {
			if result[i].Ranges > 0 {
				nextStoreSet = i
				break
			}
		}
		ratio := result[nextStoreSet]
		if len(ratio.StoreIDs) != rf {
			panic(fmt.Sprintf("expected %d replicas, got %d", rf, len(ratio.StoreIDs)))
		}
		if len(ratio.StoreIDs) != len(ratio.Types) {
			panic(fmt.Sprintf("expected %d types, got %d", len(ratio.StoreIDs), len(ratio.Types)))
		}
		for j := 0; j < len(ratio.StoreIDs); j++ {
			ret[rngIdx].Descriptor.InternalReplicas[j] = roachpb.ReplicaDescriptor{
				StoreID: roachpb.StoreID(ratio.StoreIDs[j]),
				Type:    ratio.Types[j],
			}
		}
		ret[rngIdx].Leaseholder = StoreID(ratio.LeaseholderID)
		result[nextStoreSet].Ranges--
	}
	return ret, buf.String()
}
