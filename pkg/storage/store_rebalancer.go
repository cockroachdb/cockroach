// Copyright 2018 The Cockroach Authors.
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

package storage

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const (
	// storeRebalancerTimerDuration is how frequently to check the store-level
	// balance of the cluster.
	storeRebalancerTimerDuration = time.Minute

	// minQPSThresholdDifference is the minimum QPS difference from the cluster
	// mean that this system should care about. In other words, we won't worry
	// about rebalancing for QPS reasons if a store's QPS differs from the mean
	// by less than this amount even if the amount is greater than the percentage
	// threshold. This avoids too many lease transfers in lightly loaded clusters.
	minQPSThresholdDifference = 100
)

// StoreRebalancer is responsible for examining how the associated store's load
// compares to the load on other stores in the cluster and transferring leases
// or replicas away if the local store is overloaded.
//
// This isn't implemented as a Queue because the Queues all operate on one
// replica at a time, making a local decision about each replica. Queues don't
// really know how the replica they're looking at compares to other replicas on
// the store. Our goal is balancing stores, though, so it's preferable to make
// decisions about each store and then carefully pick replicas to move that
// will best accomplish the store-level goals.
type StoreRebalancer struct {
	log.AmbientContext
	st           *cluster.Settings
	rq           *replicateQueue
	replRankings *replicaRankings
}

// NewStoreRebalancer creates a StoreRebalancer to work in tandem with the
// provided replicateQueue.
func NewStoreRebalancer(
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	rq *replicateQueue,
	replRankings *replicaRankings,
) *StoreRebalancer {
	ambientCtx.AddLogTag("store-rebalancer", nil)
	return &StoreRebalancer{
		AmbientContext: ambientCtx,
		st:             st,
		rq:             rq,
		replRankings:   replRankings,
	}
}

// Start runs an infinite loop in a goroutine which regularly checks whether
// the store is overloaded along any important dimension (e.g. range count,
// QPS, disk usage), and if so attempts to correct that by moving leases or
// replicas elsewhere.
//
// This worker acts on store-level imbalances, whereas the replicate queue
// makes decisions based on the zone config constraints and diversity of
// individual ranges. This means that there are two different workers that
// could potentially be making decisions about a given range, so they have to
// be careful to avoid stepping on each others' toes.
//
// TODO(a-robinson): Expose metrics to make this understandable without having
// to dive into logspy.
func (sr *StoreRebalancer) Start(
	ctx context.Context, stopper *stop.Stopper, storeID roachpb.StoreID,
) {
	ctx = sr.AnnotateCtx(ctx)

	// Start a goroutine that watches and proactively renews certain
	// expiration-based leases.
	stopper.RunWorker(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(storeRebalancerTimerDuration)
		defer ticker.Stop()
		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some qps/wps stats to
			// accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
			}

			if !EnableStatsBasedRebalancing.Get(&sr.st.SV) {
				continue
			}

			localDesc, found := sr.rq.allocator.storePool.getStoreDescriptor(storeID)
			if !found {
				log.Warningf(ctx, "StorePool missing descriptor for local store")
				continue
			}
			storeList, _, _ := sr.rq.allocator.storePool.getStoreList(roachpb.RangeID(0), storeFilterNone)
			sr.rebalanceStore(ctx, localDesc, storeList)
		}
	})
}

func (sr *StoreRebalancer) rebalanceStore(
	ctx context.Context, localDesc roachpb.StoreDescriptor, storeList StoreList,
) {

	statThreshold := statRebalanceThreshold.Get(&sr.st.SV)

	// First check if we should transfer leases away to better balance QPS.
	qpsMinThreshold := math.Min(storeList.candidateQueriesPerSecond.mean*(1-statThreshold),
		storeList.candidateQueriesPerSecond.mean-minQPSThresholdDifference)
	qpsMaxThreshold := math.Max(storeList.candidateQueriesPerSecond.mean*(1+statThreshold),
		storeList.candidateQueriesPerSecond.mean+minQPSThresholdDifference)

	if !(localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold) {
		log.VEventf(ctx, 1, "local QPS %.2f is below max threshold %.2f (mean=%.2f); no rebalancing needed",
			localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold, storeList.candidateQueriesPerSecond.mean)
		return
	}

	storeMap := storeListToMap(storeList)
	sysCfg, cfgOk := sr.rq.allocator.storePool.gossip.GetSystemConfig()
	if !cfgOk {
		log.VEventf(ctx, 1, "no system config available, unable to choose lease transfer targets")
		return
	}

	log.Infof(ctx, "considering load-based lease transfers for s%d with %.2f qps (mean=%.2f, upperThreshold=%.2f)",
		localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storeList.candidateQueriesPerSecond.mean, qpsMaxThreshold)

	hottestRanges := sr.replRankings.topQPS()
	for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		replWithStats, target := sr.chooseLeaseToTransfer(
			ctx, sysCfg, &hottestRanges, localDesc, storeList, storeMap, qpsMinThreshold, qpsMaxThreshold)
		if replWithStats.repl == nil {
			log.Infof(ctx,
				"ran out of leases worth transferring and qps (%.2f) is still above desired threshold (%.2f)",
				localDesc.Capacity.QueriesPerSecond, qpsMaxThreshold)
			break
		}
		log.VEventf(ctx, 1, "transferring r%d (%.2f qps) to s%d to better balance load",
			replWithStats.repl.RangeID, replWithStats.qps, target.StoreID)
		replCtx := replWithStats.repl.AnnotateCtx(ctx)
		if err := sr.rq.transferLease(replCtx, replWithStats.repl, target); err != nil {
			log.Errorf(replCtx, "unable to transfer lease to s%d: %v", target.StoreID, err)
			continue
		}
		// Finally, update our local copies of the descriptors so that if
		// additional transfers are needed we'll be making the decisions with more
		// up-to-date info.
		localDesc.Capacity.LeaseCount--
		localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
		if otherDesc := storeMap[target.StoreID]; otherDesc != nil {
			otherDesc.Capacity.LeaseCount++
			otherDesc.Capacity.QueriesPerSecond += replWithStats.qps
		}
	}
}

// TODO(a-robinson): Should we take the number of leases on each store into
// account here or just continue to let that happen in allocator.go?
func (sr *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context,
	sysCfg config.SystemConfig,
	hottestRanges *[]replicaWithStats,
	localDesc roachpb.StoreDescriptor,
	storeList StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replicaWithStats, roachpb.ReplicaDescriptor) {
	now := sr.rq.store.Clock().Now()
	for {
		if len(*hottestRanges) == 0 {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}
		}
		replWithStats := (*hottestRanges)[0]
		*hottestRanges = (*hottestRanges)[1:]

		// We're all out of replicas.
		if replWithStats.repl == nil {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}
		}

		if !replWithStats.repl.OwnsValidLease(now) {
			log.VEventf(ctx, 3, "store doesn't own the lease for r%d", replWithStats.repl.RangeID)
			continue
		}

		if localDesc.Capacity.QueriesPerSecond-replWithStats.qps < minQPS {
			log.VEventf(ctx, 3, "moving r%d's %.2f qps would bring s%d below the min threshold (%.2f)",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, minQPS)
			continue
		}

		// Don't bother moving leases whose QPS is below some small fraction of the
		// store's QPS (unless the store has extra leases to spare anyway). It's
		// just unnecessary churn with no benefit to move leases responsible for,
		// for example, 1 qps on a store with 5000 qps.
		const minQPSFraction = .001
		if replWithStats.qps < localDesc.Capacity.QueriesPerSecond*minQPSFraction &&
			float64(localDesc.Capacity.LeaseCount) <= storeList.candidateLeases.mean {
			log.VEventf(ctx, 5, "r%d's %.2f qps is too little to matter relative to s%d's %.2f total qps",
				replWithStats.repl.RangeID, replWithStats.qps, localDesc.StoreID, localDesc.Capacity.QueriesPerSecond)
			continue
		}

		desc := replWithStats.repl.Desc()
		log.VEventf(ctx, 3, "considering lease transfer for r%d with %.2f qps", desc.RangeID, replWithStats.qps)

		// Check all the other replicas in order of increasing qps.
		replicas := make([]roachpb.ReplicaDescriptor, len(desc.Replicas))
		copy(replicas, desc.Replicas)
		sort.Slice(replicas, func(i, j int) bool {
			var iQPS, jQPS float64
			if desc := storeMap[replicas[i].StoreID]; desc != nil {
				iQPS = desc.Capacity.QueriesPerSecond
			}
			if desc := storeMap[replicas[j].StoreID]; desc != nil {
				jQPS = desc.Capacity.QueriesPerSecond
			}
			return iQPS < jQPS
		})

		for _, candidate := range replicas {
			if candidate.StoreID == localDesc.StoreID {
				continue
			}
			storeDesc, ok := storeMap[candidate.StoreID]
			if !ok {
				log.VEventf(ctx, 3, "missing store descriptor for s%d", candidate.StoreID)
				continue
			}

			newCandidateQPS := storeDesc.Capacity.QueriesPerSecond + replWithStats.qps
			if storeDesc.Capacity.QueriesPerSecond < minQPS {
				if newCandidateQPS > maxQPS {
					log.VEventf(ctx, 3,
						"r%d's %.2f qps would push s%d over the max threshold (%.2f) with %.2f qps afterwards",
						desc.RangeID, replWithStats.qps, candidate.StoreID, maxQPS, newCandidateQPS)
					continue
				}
			} else if newCandidateQPS > storeList.candidateQueriesPerSecond.mean {
				log.VEventf(ctx, 3,
					"r%d's %.2f qps would push s%d over the mean (%.2f) with %.2f qps afterwards",
					desc.RangeID, replWithStats.qps, candidate.StoreID,
					storeList.candidateQueriesPerSecond.mean, newCandidateQPS)
				continue
			}

			zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
			if err != nil {
				log.Error(ctx, err)
				return replicaWithStats{}, roachpb.ReplicaDescriptor{}
			}
			preferred := sr.rq.allocator.preferredLeaseholders(zone, desc.Replicas)
			if len(preferred) > 0 && !storeHasReplica(candidate.StoreID, preferred) {
				log.VEventf(ctx, 3, "s%d not a preferred leaseholder; preferred: %v", candidate.StoreID, preferred)
				continue
			}

			filteredStoreList := storeList.filter(zone.Constraints)
			if sr.rq.allocator.followTheWorkloadPrefersLocal(
				ctx,
				filteredStoreList,
				localDesc,
				candidate.StoreID,
				desc.Replicas,
				replWithStats.repl.leaseholderStats,
			) {
				log.VEventf(ctx, 3, "r%d is on s%d due to follow-the-workload; skipping",
					desc.RangeID, localDesc.StoreID)
				continue
			}

			return replWithStats, candidate
		}
	}
}

func storeListToMap(sl StoreList) map[roachpb.StoreID]*roachpb.StoreDescriptor {
	storeMap := make(map[roachpb.StoreID]*roachpb.StoreDescriptor)
	for i := range sl.stores {
		storeMap[sl.stores[i].StoreID] = &sl.stores[i]
	}
	return storeMap
}
