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
	"math"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

const allocatorRandomCount = 10

type nodeIDSet map[roachpb.NodeID]struct{}

func formatCandidates(
	selected *roachpb.StoreDescriptor,
	candidates []roachpb.StoreDescriptor,
) string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("[")
	for i := range candidates {
		candidate := &candidates[i]
		if i > 0 {
			_, _ = buf.WriteString(" ")
		}
		fmt.Fprintf(&buf, "%d:%d", candidate.StoreID, candidate.Capacity.RangeCount)
		if candidate == selected {
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

func (rcb rangeCountBalancer) selectBest(sl StoreList) *roachpb.StoreDescriptor {
	var best *roachpb.StoreDescriptor
	for i := range sl.stores {
		candidate := &sl.stores[i]
		if best == nil {
			best = candidate
			continue
		}
		if candidate.Capacity.RangeCount < best.Capacity.RangeCount {
			best = candidate
		}
	}

	// NB: logging of the best candidate is performed by the caller (selectGood
	// or improve).
	return best
}

func (rcb rangeCountBalancer) selectGood(
	sl StoreList, excluded nodeIDSet,
) *roachpb.StoreDescriptor {
	// Consider a random sample of stores from the store list.
	sl.stores = selectRandom(rcb.rand, allocatorRandomCount, sl, excluded)
	good := rcb.selectBest(sl)

	if log.V(2) {
		log.Infof(context.TODO(), "selected good: mean=%.1f %s",
			sl.candidateCount.mean, formatCandidates(good, sl.stores))
	}
	return good
}

func (rcb rangeCountBalancer) selectBad(sl StoreList) *roachpb.StoreDescriptor {
	var worst *roachpb.StoreDescriptor
	for i := range sl.stores {
		candidate := &sl.stores[i]
		if worst == nil {
			worst = candidate
			continue
		}
		if candidate.Capacity.RangeCount > worst.Capacity.RangeCount {
			worst = candidate
		}
	}

	if log.V(2) {
		log.Infof(context.TODO(), "selected bad: mean=%.1f %s",
			sl.candidateCount.mean, formatCandidates(worst, sl.stores))
	}
	return worst
}

// improve returns a candidate StoreDescriptor to rebalance a replica to. The
// strategy is to always converge on the mean range count. If that isn't
// possible, we don't return any candidate.
func (rcb rangeCountBalancer) improve(
	sl StoreList, excluded nodeIDSet,
) *roachpb.StoreDescriptor {
	// Attempt to select a better candidate from the supplied list.
	sl.stores = selectRandom(rcb.rand, allocatorRandomCount, sl, excluded)
	candidate := rcb.selectBest(sl)
	if candidate == nil {
		if log.V(2) {
			log.Infof(context.TODO(), "not rebalancing: no valid candidate targets: %s",
				formatCandidates(nil, sl.stores))
		}
		return nil
	}

	// Adding a replica to the candidate must make its range count converge on the
	// mean range count.
	if math.Abs(float64(candidate.Capacity.RangeCount+1)-sl.candidateCount.mean) >=
		math.Abs(float64(candidate.Capacity.RangeCount)-sl.candidateCount.mean) {
		if log.V(2) {
			log.Infof(context.TODO(), "not rebalancing: %s wouldn't converge on the mean %.1f",
				formatCandidates(candidate, sl.stores), sl.candidateCount.mean)
		}
		return nil
	}

	if log.V(2) {
		log.Infof(context.TODO(), "rebalancing: mean=%.1f %s",
			sl.candidateCount.mean, formatCandidates(candidate, sl.stores))
	}
	return candidate
}

func (rcb rangeCountBalancer) shouldRebalance(
	store roachpb.StoreDescriptor, sl StoreList,
) bool {
	// Moving a replica from the given store makes its range count converge on
	// the mean range count.
	//
	// TODO(peter,bram,cuong): The FractionUsed check seems suspicious. When a
	// node becomes fuller than maxFractionUsedThreshold we will always select it
	// for rebalancing. This is currently utilized by tests.
	if store.Capacity.FractionUsed() <= maxFractionUsedThreshold &&
		(math.Abs(float64(store.Capacity.RangeCount-1)-sl.candidateCount.mean) >
			math.Abs(float64(store.Capacity.RangeCount)-sl.candidateCount.mean)) {
		return false
	}
	return true
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
