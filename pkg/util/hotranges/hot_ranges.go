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

import "sort"

// SampleKeySpan defines the structure of the key spans that are received by HottestKeySpansAggregated.
type SampleKeySpan struct {
	qps      float64
	startKey string
	endKey   string
}

// HottestKeySpansAggregated is the main function which implements key span aggregation
// for historical hot ranges.
func HottestKeySpansAggregated(keySpans []SampleKeySpan, budget int) []SampleKeySpan {

	// If we are under budget simply return the key spans.
	if len(keySpans) <= budget {
		return keySpans
	}
	var nonZeroKeySpans []SampleKeySpan
	// Filter out all zeroed qps key spans first to see if we can get at or under the budget.
	for _, r := range keySpans {
		if r.qps != 0 {
			nonZeroKeySpans = append(nonZeroKeySpans, r)
		}
	}
	if len(nonZeroKeySpans) <= budget {
		return nonZeroKeySpans
	}

	// Group the non zero key spans we can potentially aggregate.
	adjacentKeySpans := make([][]SampleKeySpan, 0)
	var prevEndKey string
	groupIdx := -1
	for _, r := range nonZeroKeySpans {
		// If starting out or the start key doesn't match the previous end key then
		// create a new group.
		if prevEndKey == "" || r.startKey != prevEndKey {
			groupIdx++
			adjacentKeySpans = append(adjacentKeySpans, make([]SampleKeySpan, 0))
		}
		adjacentKeySpans[groupIdx] = append(adjacentKeySpans[groupIdx], r)
		prevEndKey = r.endKey
	}

	// Order the groups by lowest QPS.
	sort.Slice(adjacentKeySpans, func(i, j int) bool {
		var iQPS float64
		var jQPS float64
		for _, v := range adjacentKeySpans[i] {
			iQPS += v.qps
		}
		iQPS /= float64(len(adjacentKeySpans[i]))
		for _, v := range adjacentKeySpans[j] {
			jQPS += v.qps
		}
		jQPS /= float64(len(adjacentKeySpans[j]))
		return iQPS < jQPS
	})
	aggregatedKeySpans := findAggregatedKeySpans(adjacentKeySpans, budget, len(nonZeroKeySpans))
	// If we exhausted all candidates and are still over budget, then take the hottest key spans within budget.
	if len(aggregatedKeySpans) > budget {
		sort.Slice(aggregatedKeySpans, func(i, j int) bool {
			return aggregatedKeySpans[i].qps > aggregatedKeySpans[j].qps
		})
		return aggregatedKeySpans[:budget]
	}
	return aggregatedKeySpans
}

func findAggregatedKeySpans(
	groupedKeySpans [][]SampleKeySpan, targetBudget int, currentBudget int,
) []SampleKeySpan {
	aggregatedKeySpans := make([]SampleKeySpan, 0)
	for _, group := range groupedKeySpans {
		// If we already meet the budget or have a case where the slice is too small and the median would be equal to the mean,
		// simply append the current slice to the aggregatedKeySpans slice.
		if currentBudget <= targetBudget || len(group) == 1 || len(group) == 2 {
			aggregatedKeySpans = append(aggregatedKeySpans, group...)
			continue
		}
		// Calculate the mean qps of the group.
		var meanQPS float64
		for _, v := range group {
			meanQPS += v.qps
		}
		meanQPS /= float64(len(group))
		// See if it is a valid group to aggregate, otherwise just add the group un-aggregated.
		if shouldMergeAdjacentKeySpans(group, meanQPS) {
			aggregatedKeySpan := SampleKeySpan{
				qps:      meanQPS,
				startKey: group[0].startKey,
				endKey:   group[len(group)-1].endKey,
			}
			aggregatedKeySpans = append(aggregatedKeySpans, aggregatedKeySpan)
			// Reduce the current budget by number of key spans in the group + 1 since we add a new resulting key span.
			currentBudget = currentBudget - len(group) + 1
		} else {
			aggregatedKeySpans = append(aggregatedKeySpans, group...)
		}
	}
	return aggregatedKeySpans
}

// shouldMergeAdjacentKeySpans is a helper function which determines whether the given
// mean QPS of the candidate key spans group is less than that groups' median QPS.
func shouldMergeAdjacentKeySpans(candidateKeySpans []SampleKeySpan, meanQPS float64) bool {
	arrLen := len(candidateKeySpans)
	copyCandidateKeySpans := make([]SampleKeySpan, len(candidateKeySpans))
	copy(copyCandidateKeySpans, candidateKeySpans)
	// Sort the key spans by qps.
	sort.Slice(copyCandidateKeySpans, func(i, j int) bool {
		return copyCandidateKeySpans[i].qps < copyCandidateKeySpans[j].qps
	})
	// Median calculation for even length array of key spans.
	if arrLen%2 == 0 {
		medianQPS := (copyCandidateKeySpans[arrLen/2].qps + copyCandidateKeySpans[(arrLen/2)-1].qps) / 2
		return meanQPS < medianQPS
	}
	// Median calculation for odd length array of key spans.
	medianCandidate := copyCandidateKeySpans[(arrLen-1)/2]
	return meanQPS < medianCandidate.qps
}
