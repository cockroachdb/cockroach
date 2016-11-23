// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// The number of random candidates to select from a larger list of possible
// candidates. Because the allocator heuristics are being run on every node it
// is actually not desirable to set this value higher. Doing so can lead to
// situations where the allocator determistically selects the "best" node for a
// decision and all of the nodes pile on allocations to that node. See "power
// of two random choices":
// https://brooker.co.za/blog/2012/01/17/two-random.html and
// https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf.
const allocatorRandomCount = 2

type nodeIDSet map[roachpb.NodeID]struct{}

func formatCandidates(
	selected roachpb.StoreDescriptor, candidates []roachpb.StoreDescriptor,
) string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("[")
	for i := range candidates {
		candidate := candidates[i]
		if i > 0 {
			_, _ = buf.WriteString(" ")
		}
		fmt.Fprintf(&buf, "%d:%d", candidate.StoreID, candidate.Capacity.RangeCount)
		if candidate.StoreID == selected.StoreID {
			_, _ = buf.WriteString("*")
		}
	}
	_, _ = buf.WriteString("]")
	return buf.String()
}

// rangeCountBalancer attempts to balance ranges across the cluster while
// considering only the number of ranges being serviced each store.
type rangeCountBalancer struct {
	rand allocatorRand
}

func (rangeCountBalancer) selectBest(sl StoreList) (roachpb.StoreDescriptor, bool) {
	if len(sl.stores) == 0 {
		return roachpb.StoreDescriptor{}, false
	}
	best := sl.stores[0]
	for i := 1; i < len(sl.stores); i++ {
		if sl.stores[i].Capacity.RangeCount < best.Capacity.RangeCount {
			best = sl.stores[i]
		}
	}

	// NB: logging of the best candidate is performed by the caller (selectGood
	// or improve).
	return best, true
}

func (rcb rangeCountBalancer) selectGood(
	sl StoreList, excluded nodeIDSet,
) (roachpb.StoreDescriptor, bool) {
	// Consider a random sample of stores from the store list.
	sl.stores = selectRandom(rcb.rand, allocatorRandomCount, sl, excluded)
	good, ok := rcb.selectBest(sl)

	if log.V(2) {
		log.Infof(context.TODO(), "selected good: mean=%.1f %s",
			sl.candidateCount.mean, formatCandidates(good, sl.stores))
	}
	return good, ok
}

func (rcb rangeCountBalancer) selectBad(sl StoreList) (roachpb.StoreDescriptor, bool) {
	var bad roachpb.StoreDescriptor
	var ok bool
	if len(sl.stores) > 0 {
		// Find the list of removal candidates that are on stores that have more
		// than the average numbers of ranges.
		candidates := make([]roachpb.StoreDescriptor, 0, len(sl.stores))
		for i := range sl.stores {
			candidate := sl.stores[i]
			if rebalanceFromConvergesOnMean(sl, candidate) {
				candidates = append(candidates, candidate)
			}
		}

		rcb.rand.Lock()
		if len(candidates) > 0 {
			// Randomly choose a store from one of the above average range count
			// candidates.
			bad = candidates[rcb.rand.Intn(len(candidates))]
			ok = true
		} else {
			// Fallback to choosing a random store to remove from.
			bad = sl.stores[rcb.rand.Intn(len(sl.stores))]
			ok = true
		}
		rcb.rand.Unlock()
	}

	if log.V(2) {
		log.Infof(context.TODO(), "selected bad: mean=%.1f %s",
			sl.candidateCount.mean, formatCandidates(bad, sl.stores))
	}
	return bad, ok
}

// improve returns a candidate StoreDescriptor to rebalance a replica to. The
// strategy is to always converge on the mean range count. If that isn't
// possible, we don't return any candidate.
func (rcb rangeCountBalancer) improve(
	sl StoreList, excluded nodeIDSet,
) (roachpb.StoreDescriptor, bool) {
	// Attempt to select a better candidate from the supplied list.
	sl.stores = selectRandom(rcb.rand, allocatorRandomCount, sl, excluded)
	candidate, ok := rcb.selectBest(sl)
	if !ok {
		if log.V(2) {
			log.Infof(context.TODO(), "not rebalancing: no valid candidate targets: %s",
				formatCandidates(roachpb.StoreDescriptor{}, sl.stores))
		}
		return roachpb.StoreDescriptor{}, false
	}

	// Adding a replica to the candidate must make its range count converge on the
	// mean range count.
	rebalanceConvergesOnMean := rebalanceToConvergesOnMean(sl, candidate)
	if !rebalanceConvergesOnMean {
		if log.V(2) {
			log.Infof(context.TODO(), "not rebalancing: %s wouldn't converge on the mean %.1f",
				formatCandidates(candidate, sl.stores), sl.candidateCount.mean)
		}
		return roachpb.StoreDescriptor{}, false
	}

	if log.V(2) {
		log.Infof(context.TODO(), "rebalancing: mean=%.1f %s",
			sl.candidateCount.mean, formatCandidates(candidate, sl.stores))
	}
	return candidate, true
}

// selectRandom chooses up to count random store descriptors from the given
// store list, excluding any stores that are too full to accept more replicas.
func selectRandom(
	randGen allocatorRand, count int, sl StoreList, excluded nodeIDSet,
) []roachpb.StoreDescriptor {
	var descs []roachpb.StoreDescriptor
	// Randomly permute available stores matching the required attributes.
	randGen.Lock()
	defer randGen.Unlock()
	for _, idx := range randGen.Perm(len(sl.stores)) {
		desc := sl.stores[idx]
		// Skip if store is in excluded set.
		if _, ok := excluded[desc.Node.NodeID]; ok {
			continue
		}

		// Don't overfill stores.
		if desc.Capacity.FractionUsed() > maxFractionUsedThreshold {
			continue
		}

		// Add this store; exit loop if we've satisfied count.
		descs = append(descs, sl.stores[idx])
		if len(descs) >= count {
			break
		}
	}
	return descs
}

func rebalanceFromConvergesOnMean(sl StoreList, candidate roachpb.StoreDescriptor) bool {
	return float64(candidate.Capacity.RangeCount) > sl.candidateCount.mean+0.5
}

func rebalanceToConvergesOnMean(sl StoreList, candidate roachpb.StoreDescriptor) bool {
	return float64(candidate.Capacity.RangeCount) < sl.candidateCount.mean-0.5
}
