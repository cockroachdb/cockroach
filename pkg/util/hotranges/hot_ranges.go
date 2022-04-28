// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hotranges

import (
	"math"
	"sort"
)

// HotReplica defines the structure of the ranges that are received by HottestRangesAggregated.
type HotReplica struct {
	qps      float64
	startKey string
	endKey   string
}

// HottestRangesAggregated is the main function which implements range aggregation
// for historical hot ranges.
func HottestRangesAggregated(ranges []HotReplica, budget int) []HotReplica {
	// If we are under budget simply return the ranges.
	if len(ranges) <= budget {
		return ranges
	}
	var nonZeroRanges []HotReplica
	// Filter out all zeroed qps ranges first to see if we can get at or under the budget.
	for _, r := range ranges {
		if r.qps != 0 {
			nonZeroRanges = append(nonZeroRanges, r)
		}
	}
	if len(nonZeroRanges) <= budget {
		return nonZeroRanges
	}

	// Group the ranges we can potentially aggregate.
	groupedRanges := make(map[int][]HotReplica)
	var prevEndKey string
	groupKey := 0
	for _, r := range ranges {
		// If starting out or the start key doesn't match the previous end key then
		// create a new group.
		if prevEndKey == "" || r.startKey != prevEndKey {
			groupKey++
			groupedRanges[groupKey] = make([]HotReplica, 0)

		}
		groupedRanges[groupKey] = append(groupedRanges[groupKey], r)
		prevEndKey = r.endKey
	}
	aggregatedRanges := mergeRanges(nonZeroRanges, groupedRanges, budget)
	// If we exhausted all candidates and are still over budget, then take the hottest ranges within budget.
	if len(aggregatedRanges) > budget {
		sort.Slice(aggregatedRanges, func(i, j int) bool {
			return aggregatedRanges[i].qps > aggregatedRanges[j].qps
		})
		return aggregatedRanges[:budget]
	}
	return aggregatedRanges
}

// mergeRanges is a helper function which performs the grouping of ranges by
// lexicographic adjacency. It ultimately returns the final aggregated array
// of ranges.
func mergeRanges(ranges []HotReplica, groupedRanges map[int][]HotReplica, budget int) []HotReplica {
	if len(ranges) <= budget {
		return ranges
	}

	isValidCandidate := false
	var groupCandidateKey int
	var lowestMeanQPS float64
	for !isValidCandidate {
		// If we've exhausted all candidate groups return the aggregated ranges thus far.
		if len(groupedRanges) == 0 {
			return ranges
		}
		groupCandidateKey, lowestMeanQPS = findGroupedRangesCandidate(groupedRanges)
		// Edge case if a candidate cannot be found then return the aggregated ranges thus far.
		if groupCandidateKey == 0 {
			return ranges
		}
		isValidCandidate = validateGroupRangesCandidate(groupedRanges[groupCandidateKey], lowestMeanQPS)
		// If the candidate group is not valid, remove it from the map.
		if !isValidCandidate {
			delete(groupedRanges, groupCandidateKey)
		}
	}
	// At this point, we found a valid candidate group to aggregate.
	validCandidate := groupedRanges[groupCandidateKey]
	aggregatedHotReplica := HotReplica{
		qps:      lowestMeanQPS,
		startKey: validCandidate[0].startKey,
		endKey:   validCandidate[len(validCandidate)-1].endKey,
	}
	// Build the new ranges slice, excluding the ranges that fall in the newly aggregated range and instead
	// insert the newly aggregated range.
	aggregatedRanges := make([]HotReplica, 0)
	omitRanges := false
	for _, v := range ranges {
		if v.startKey == aggregatedHotReplica.startKey {
			omitRanges = true
			aggregatedRanges = append(aggregatedRanges, aggregatedHotReplica)
			continue
		} else if v.endKey == aggregatedHotReplica.endKey {
			omitRanges = false
			continue
		}
		if !omitRanges {
			aggregatedRanges = append(aggregatedRanges, v)
		}
	}
	// Now that aggregated range is added into the array, delete the associated candidate group.
	delete(groupedRanges, groupCandidateKey)
	// Continue executing this until we either get under the budget or run out of candidate groups.
	return mergeRanges(aggregatedRanges, groupedRanges, budget)
}

// findGroupedRangesCandidate is a helper function which determines the candidate group
// of ranges which should be merged. This is determined by finding the lowest mean QPS of
// a group.
func findGroupedRangesCandidate(groupedRanges map[int][]HotReplica) (int, float64) {
	// Find the merge candidate by lowest mean QPS.
	lowestMeanQPS := math.MaxFloat64
	var groupCandidateKey int
	for k, v := range groupedRanges {
		// If the group only has 1 range, we cannot aggregate it so it should not be considered.
		if len(v) == 1 {
			continue
		}
		var sumQPS float64
		for _, r := range v {
			sumQPS += r.qps
		}
		meanQPS := sumQPS / float64(len(v))
		if meanQPS < lowestMeanQPS {
			lowestMeanQPS = meanQPS
			groupCandidateKey = k
		}
	}
	return groupCandidateKey, lowestMeanQPS
}

// validateGroupRangesCandidate is a helper function which determines whether the given
// mean QPS of the candidate ranges group is less than that groups' median QPS.
func validateGroupRangesCandidate(candidateRanges []HotReplica, meanQPS float64) bool {
	arrLen := len(candidateRanges)
	copyCandidateRanges := make([]HotReplica, len(candidateRanges))
	copy(copyCandidateRanges, candidateRanges)
	// Sort the ranges by qps.
	sort.Slice(copyCandidateRanges, func(i, j int) bool {
		return copyCandidateRanges[i].qps < copyCandidateRanges[j].qps
	})
	// Median calculation for even length array of ranges.
	if arrLen%2 == 0 {
		medianQPS := (copyCandidateRanges[arrLen/2].qps + copyCandidateRanges[(arrLen/2)-1].qps) / 2
		return meanQPS < medianQPS
	}
	// Median calculation for odd length array of ranges.
	medianCandidate := copyCandidateRanges[(arrLen-1)/2]
	return meanQPS < medianCandidate.qps
}
