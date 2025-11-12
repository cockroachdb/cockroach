// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var mmaid = atomic.Int64{}

// Called periodically, say every 10s.
//
// We do not want to shed replicas for CPU from a remote store until its had a
// chance to shed leases.
func (cs *clusterState) rebalanceStores(
	ctx context.Context, localStoreID roachpb.StoreID, rng *rand.Rand, dsm *diversityScoringMemo,
) []PendingRangeChange {
	now := cs.ts.Now()
	ctx = logtags.AddTag(ctx, "mmaid", mmaid.Add(1))
	log.KvDistribution.VInfof(ctx, 2, "rebalanceStores begins")
	// To select which stores are overloaded, we use a notion of overload that
	// is based on cluster means (and of course individual store/node
	// capacities). We do not want to loop through all ranges in the cluster,
	// and for each range and its constraints expression decide whether any of
	// the replica stores is overloaded, since O(num-ranges) work during each
	// allocator pass is not scalable.
	//
	// If cluster mean is too low, more will be considered overloaded. This is
	// ok, since then when we look at ranges we will have a different mean for
	// the constraint satisfying candidates and if that mean is higher we may
	// not do anything. There is wasted work, but we can bound it by typically
	// only looking at a random K ranges for each store.
	//
	// If the cluster mean is too high, we will not rebalance across subsets
	// that have a low mean. Seems fine, if we accept that rebalancing is not
	// responsible for equalizing load across two nodes that have 30% and 50%
	// cpu utilization while the cluster mean is 70% utilization (as an
	// example).
	clusterMeans := cs.meansMemo.getMeans(nil)
	type sheddingStore struct {
		roachpb.StoreID
		storeLoadSummary
	}
	var sheddingStores []sheddingStore
	log.KvDistribution.Infof(ctx,
		"cluster means: (stores-load %s) (stores-capacity %s) (nodes-cpu-load %d) (nodes-cpu-capacity %d)",
		clusterMeans.storeLoad.load, clusterMeans.storeLoad.capacity,
		clusterMeans.nodeLoad.loadCPU, clusterMeans.nodeLoad.capacityCPU)
	// NB: We don't attempt to shed replicas or leases from a store which is
	// fdDrain or fdDead, nor do we attempt to shed replicas from a store which
	// is storeMembershipRemoving (decommissioning). These are currently handled
	// via replicate_queue.go.
	for storeID, ss := range cs.stores {
		sls := cs.meansMemo.getStoreLoadSummary(ctx, clusterMeans, storeID, ss.loadSeqNum)
		log.KvDistribution.VInfof(ctx, 2, "evaluating s%d: node load %s, store load %s, worst dim %s",
			storeID, sls.nls, sls.sls, sls.worstDim)

		if sls.sls >= overloadSlow {
			if ss.overloadEndTime != (time.Time{}) {
				if now.Sub(ss.overloadEndTime) > overloadGracePeriod {
					ss.overloadStartTime = now
					log.KvDistribution.Infof(ctx, "overload-start s%v (%v) - grace period expired", storeID, sls)
				} else {
					// Else, extend the previous overload interval.
					log.KvDistribution.Infof(ctx, "overload-continued s%v (%v) - within grace period", storeID, sls)
				}
				ss.overloadEndTime = time.Time{}
			}
			// The pending decrease must be small enough to continue shedding
			if ss.maxFractionPendingDecrease < maxFractionPendingThreshold &&
				// There should be no pending increase, since that can be an overestimate.
				ss.maxFractionPendingIncrease < epsilon {
				log.KvDistribution.VInfof(ctx, 2, "store s%v was added to shedding store list", storeID)
				sheddingStores = append(sheddingStores, sheddingStore{StoreID: storeID, storeLoadSummary: sls})
			} else {
				log.KvDistribution.VInfof(ctx, 2,
					"skipping overloaded store s%d (worst dim: %s): pending decrease %.2f >= threshold %.2f or pending increase %.2f >= epsilon",
					storeID, sls.worstDim, ss.maxFractionPendingDecrease, maxFractionPendingThreshold, ss.maxFractionPendingIncrease)
			}
		} else if sls.sls < loadNoChange && ss.overloadEndTime == (time.Time{}) {
			// NB: we don't stop the overloaded interval if the store is at
			// loadNoChange, since a store can hover at the border of the two.
			log.KvDistribution.Infof(ctx, "overload-end s%v (%v) - load dropped below no-change threshold", storeID, sls)
			ss.overloadEndTime = now
		}
	}

	// We have used storeLoadSummary.sls to filter above. But when sorting, we
	// first compare using nls. Consider the following scenario with 4 stores
	// per node: node n1 is at 90% cpu utilization and has 4 stores each
	// contributing 22.5% cpu utilization. Node n2 is at 60% utilization, and
	// has 1 store contributing all 60%. When looking at rebalancing, we want to
	// first shed load from the stores of n1, before we shed load from the store
	// on n2.
	slices.SortFunc(sheddingStores, func(a, b sheddingStore) int {
		// Prefer to shed from the local store first.
		if a.StoreID == localStoreID {
			return -1
		} else if b.StoreID == localStoreID {
			return 1
		}
		// Use StoreID for tie-breaker for determinism.
		return cmp.Or(-cmp.Compare(a.nls, b.nls), -cmp.Compare(a.sls, b.sls),
			cmp.Compare(a.StoreID, b.StoreID))
	})

	if log.V(2) {
		log.KvDistribution.Info(ctx, "sorted shedding stores:")
		for _, store := range sheddingStores {
			log.KvDistribution.Infof(ctx, "  (s%d: %s)", store.StoreID, store.sls)
		}
	}

	var changes []PendingRangeChange
	var disj [1]constraintsConj
	var storesToExclude storeSet
	var storesToExcludeForRange storeSet
	scratchNodes := map[roachpb.NodeID]*NodeLoad{}
	scratchStores := map[roachpb.StoreID]struct{}{}
	// The caller has a fixed concurrency limit it can move ranges at, when it
	// is the sender of the snapshot. So we don't want to create too many
	// changes, since then the allocator gets too far ahead of what has been
	// enacted, and its decision-making is no longer based on recent
	// information. We don't have this issue with lease transfers since they are
	// very fast, so we set a much higher limit.
	//
	// TODO: revisit these constants.
	const maxRangeMoveCount = 1
	const maxLeaseTransferCount = 8
	// See the long comment where rangeState.lastFailedChange is declared.
	const lastFailedChangeDelayDuration time.Duration = 60 * time.Second
	rangeMoveCount := 0
	leaseTransferCount := 0
	for idx /*logging only*/, store := range sheddingStores {
		shouldReturnEarly := false
		shouldContinue := false
		{
			log.KvDistribution.Infof(ctx, "start processing shedding store s%d: cpu node load %s, store load %s, worst dim %s",
				store.StoreID, store.nls, store.sls, store.worstDim)
			ss := cs.stores[store.StoreID]

			doneShedding := false
			if true {
				// Debug logging.
				topKRanges := ss.adjusted.topKRanges[localStoreID]
				n := topKRanges.len()
				if n > 0 {
					var b strings.Builder
					for i := 0; i < n; i++ {
						rangeID := topKRanges.index(i)
						rstate := cs.ranges[rangeID]
						load := rstate.load.Load
						if !ss.adjusted.replicas[rangeID].IsLeaseholder {
							load[CPURate] = rstate.load.RaftCPU
						}
						fmt.Fprintf(&b, " r%d:%v", rangeID, load)
					}
					log.KvDistribution.Infof(ctx, "top-K[%s] ranges for s%d with lease on local s%d:%s",
						topKRanges.dim, store.StoreID, localStoreID, b.String())
				} else {
					log.KvDistribution.Infof(ctx, "no top-K[%s] ranges found for s%d with lease on local s%d",
						topKRanges.dim, store.StoreID, localStoreID)
				}
			}

			// TODO(tbg): it's somewhat akward that we only enter this branch for
			// ss.StoreID == localStoreID and not for *any* calling local store.
			// More generally, does it make sense that rebalanceStores is called on
			// behalf of a particular store (vs. being called on behalf of the set
			// of local store IDs)?
			if ss.StoreID == localStoreID && store.dimSummary[CPURate] >= overloadSlow {
				log.KvDistribution.VInfof(ctx, 2, "local store s%d is CPU overloaded (%v >= %v), attempting lease transfers first",
					store.StoreID, store.dimSummary[CPURate], overloadSlow)
				// This store is local, and cpu overloaded. Shed leases first.
				//
				// NB: any ranges at this store that don't have pending changes must
				// have this local store as the leaseholder.
				topKRanges := ss.adjusted.topKRanges[localStoreID]
				n := topKRanges.len()
				for i := 0; i < n; i++ {
					rangeID := topKRanges.index(i)
					rstate := cs.ranges[rangeID]
					if len(rstate.pendingChanges) > 0 {
						// If the range has pending changes, don't make more changes.
						log.KvDistribution.VInfof(ctx, 2, "skipping r%d: has pending changes", rangeID)
						continue
					}
					for _, repl := range rstate.replicas {
						if repl.StoreID != localStoreID { // NB: localStoreID == ss.StoreID == store.StoreID
							continue
						}
						if !repl.IsLeaseholder {
							// TODO(tbg): is this true? Can't there be ranges with replicas on
							// multiple local stores, and wouldn't this assertion fire in that
							// case once rebalanceStores is invoked on whichever of the two
							// stores doesn't hold the lease?
							//
							// TODO(tbg): see also the other assertion below (leaseholderID !=
							// store.StoreID) which seems similar to this one.
							log.KvDistribution.Fatalf(ctx, "internal state inconsistency: replica considered for lease shedding has no pending"+
								" changes but is not leaseholder: %+v", rstate)
						}
					}
					if now.Sub(rstate.lastFailedChange) < lastFailedChangeDelayDuration {
						log.KvDistribution.VInfof(ctx, 2, "skipping r%d: too soon after failed change", rangeID)
						continue
					}
					if !cs.ensureAnalyzedConstraints(rstate) {
						log.KvDistribution.VInfof(ctx, 2, "skipping r%d: constraints analysis failed", rangeID)
						continue
					}
					if rstate.constraints.leaseholderID != store.StoreID {
						// We should not panic here since the leaseQueue may have shed the
						// lease and informed MMA, since the last time MMA computed the
						// top-k ranges. This is useful for debugging in the prototype, due
						// to the lack of unit tests.
						//
						// TODO(tbg): can the above scenario currently happen? ComputeChanges
						// first processes the leaseholder message and then, still under the
						// lock, immediately calls into rebalanceStores (i.e. this store).
						// Doesn't this mean that the leaseholder view is up to date?
						panic(fmt.Sprintf("internal state inconsistency: "+
							"store=%v range_id=%v should be leaseholder but isn't",
							store.StoreID, rangeID))
					}
					cands, _ := rstate.constraints.candidatesToMoveLease()
					var candsPL storeSet
					for _, cand := range cands {
						candsPL.insert(cand.storeID)
					}
					// Always consider the local store (which already holds the lease) as a
					// candidate, so that we don't move the lease away if keeping it would be
					// the better option overall.
					// TODO(tbg): is this really needed? We intentionally exclude the leaseholder
					// in candidatesToMoveLease, so why reinsert it now?
					candsPL.insert(store.StoreID)
					if len(candsPL) <= 1 {
						continue // leaseholder is the only candidate
					}
					clear(scratchNodes)
					means := computeMeansForStoreSet(cs, candsPL, scratchNodes, scratchStores)
					sls := cs.computeLoadSummary(ctx, store.StoreID, &means.storeLoad, &means.nodeLoad)
					log.KvDistribution.VInfof(ctx, 2, "considering lease-transfer r%v from s%v: candidates are %v", rangeID, store.StoreID, candsPL)
					if sls.dimSummary[CPURate] < overloadSlow {
						// This store is not cpu overloaded relative to these candidates for
						// this range.
						log.KvDistribution.VInfof(ctx, 2, "result(failed): skipping r%d since store not overloaded relative to candidates", rangeID)
						continue
					}
					var candsSet candidateSet
					for _, cand := range cands {
						if disp := cs.stores[cand.storeID].adjusted.replicas[rangeID].LeaseDisposition; disp != LeaseDispositionOK {
							// Don't transfer lease to a store that is lagging.
							log.KvDistribution.Infof(ctx, "skipping store s%d for lease transfer: lease disposition %v",
								cand.storeID, disp)
							continue
						}
						candSls := cs.computeLoadSummary(ctx, cand.storeID, &means.storeLoad, &means.nodeLoad)
						candsSet.candidates = append(candsSet.candidates, candidateInfo{
							StoreID:              cand.storeID,
							storeLoadSummary:     candSls,
							diversityScore:       0,
							leasePreferenceIndex: cand.leasePreferenceIndex,
						})
					}
					if len(candsSet.candidates) == 0 {
						log.KvDistribution.Infof(
							ctx,
							"result(failed): no candidates to move lease from n%vs%v for r%v before sortTargetCandidateSetAndPick [pre_filter_candidates=%v]",
							ss.NodeID, ss.StoreID, rangeID, candsPL)
						continue
					}
					// Have candidates. We set ignoreLevel to
					// ignoreHigherThanLoadThreshold since this is the only allocator that
					// can shed leases for this store, and lease shedding is cheap, and it
					// will only add CPU to the target store (so it is ok to ignore other
					// dimensions on the target).
					targetStoreID := sortTargetCandidateSetAndPick(
						ctx, candsSet, sls.sls, ignoreHigherThanLoadThreshold, CPURate, rng)
					if targetStoreID == 0 {
						log.KvDistribution.Infof(
							ctx,
							"result(failed): no candidates to move lease from n%vs%v for r%v after sortTargetCandidateSetAndPick",
							ss.NodeID, ss.StoreID, rangeID)
						continue
					}
					targetSS := cs.stores[targetStoreID]
					var addedLoad LoadVector
					// Only adding leaseholder CPU.
					addedLoad[CPURate] = rstate.load.Load[CPURate] - rstate.load.RaftCPU
					if addedLoad[CPURate] < 0 {
						// TODO(sumeer): remove this panic once we are not in an
						// experimental phase.
						addedLoad[CPURate] = 0
						panic("raft cpu higher than total cpu")
					}
					if !cs.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, &means, true, CPURate) {
						log.KvDistribution.VInfof(ctx, 2, "result(failed): cannot shed from s%d to s%d for r%d: delta load %v",
							store.StoreID, targetStoreID, rangeID, addedLoad)
						continue
					}
					addTarget := roachpb.ReplicationTarget{
						NodeID:  targetSS.NodeID,
						StoreID: targetSS.StoreID,
					}
					removeTarget := roachpb.ReplicationTarget{
						NodeID:  ss.NodeID,
						StoreID: ss.StoreID,
					}
					replicaChanges := MakeLeaseTransferChanges(
						rangeID, rstate.replicas, rstate.load, addTarget, removeTarget)
					leaseChange := MakePendingRangeChange(rangeID, replicaChanges[:])
					if err := cs.preCheckOnApplyReplicaChanges(leaseChange.pendingReplicaChanges); err != nil {
						panic(errors.Wrapf(err, "pre-check failed for lease transfer %v", leaseChange))
					}
					cs.addPendingRangeChange(leaseChange)
					changes = append(changes, leaseChange)
					leaseTransferCount++
					if changes[len(changes)-1].IsChangeReplicas() || !changes[len(changes)-1].IsTransferLease() {
						panic(fmt.Sprintf("lease transfer is invalid: %v", changes[len(changes)-1]))
					}
					log.KvDistribution.Infof(ctx,
						"result(success): shedding r%v lease from s%v to s%v [change:%v] with "+
							"resulting loads source:%v target:%v (means: %v) (frac_pending: (src:%.2f,target:%.2f) (src:%.2f,target:%.2f))",
						rangeID, removeTarget.StoreID, addTarget.StoreID, changes[len(changes)-1],
						ss.adjusted.load, targetSS.adjusted.load, means.storeLoad.load,
						ss.maxFractionPendingIncrease, ss.maxFractionPendingDecrease,
						targetSS.maxFractionPendingIncrease, targetSS.maxFractionPendingDecrease)
					if leaseTransferCount >= maxLeaseTransferCount {
						log.KvDistribution.VInfof(ctx, 2, "reached max lease transfer count %d, returning", maxLeaseTransferCount)
						shouldReturnEarly = true
						break
					}
					doneShedding = ss.maxFractionPendingDecrease >= maxFractionPendingThreshold
					if doneShedding {
						log.KvDistribution.VInfof(ctx, 2, "s%d has reached pending decrease threshold(%.2f>=%.2f) after lease transfers: done shedding with %d left in topK",
							store.StoreID, ss.maxFractionPendingDecrease, maxFractionPendingThreshold, n-(i+1))
						break
					}
				}
				if doneShedding || leaseTransferCount > 0 {
					// If managed to transfer a lease, wait for it to be done, before
					// shedding replicas from this store (which is more costly). Otherwise
					// we may needlessly start moving replicas. Note that the store
					// rebalancer will call the rebalance method again after the lease
					// transfer is done and we may still be considering those transfers as
					// pending from a load perspective, so we *may* not be able to do more
					// lease transfers -- so be it.
					log.KvDistribution.VInfof(ctx, 2, "skipping replica transfers for s%d: done shedding=%v, lease_transfers=%d",
						store.StoreID, doneShedding, leaseTransferCount)
					shouldContinue = true
					break
				}
			} else {
				log.KvDistribution.VInfof(ctx, 2, "skipping lease shedding: s%v != local store s%s or cpu is not overloaded: %v",
					ss.StoreID, localStoreID, store.dimSummary[CPURate])
			}

			log.KvDistribution.VInfof(ctx, 2, "attempting to shed replicas next")

			if store.StoreID != localStoreID && store.dimSummary[CPURate] >= overloadSlow &&
				now.Sub(ss.overloadStartTime) < remoteStoreLeaseSheddingGraceDuration {
				log.KvDistribution.VInfof(ctx, 2, "skipping remote store s%d: in lease shedding grace period", store.StoreID)
				shouldContinue = true
				break
			}
			// If the node is cpu overloaded, or the store/node is not fdOK, exclude
			// the other stores on this node from receiving replicas shed by this
			// store.
			excludeStoresOnNode := store.nls > overloadSlow
			storesToExclude = storesToExclude[:0]
			if excludeStoresOnNode {
				nodeID := ss.NodeID
				for _, storeID := range cs.nodes[nodeID].stores {
					storesToExclude.insert(storeID)
				}
				log.KvDistribution.VInfof(ctx, 2, "excluding all stores on n%d due to overload/fd status", nodeID)
			} else {
				// This store is excluded of course.
				storesToExclude.insert(store.StoreID)
			}

			// Iterate over top-K ranges first and try to move them.
			topKRanges := ss.adjusted.topKRanges[localStoreID]
			n := topKRanges.len()
			loadDim := topKRanges.dim
			for i := 0; i < n; i++ {
				rangeID := topKRanges.index(i)
				// TODO(sumeer): the following code belongs in a closure, since we will
				// repeat it for some random selection of non topKRanges.
				rstate := cs.ranges[rangeID]
				if len(rstate.pendingChanges) > 0 {
					// If the range has pending changes, don't make more changes.
					log.KvDistribution.VInfof(ctx, 2, "skipping r%d: has pending changes", rangeID)
					continue
				}
				if now.Sub(rstate.lastFailedChange) < lastFailedChangeDelayDuration {
					log.KvDistribution.VInfof(ctx, 2, "skipping r%d: too soon after failed change", rangeID)
					continue
				}
				if !cs.ensureAnalyzedConstraints(rstate) {
					log.KvDistribution.VInfof(ctx, 2, "skipping r%d: constraints analysis failed", rangeID)
					continue
				}
				isVoter, isNonVoter := rstate.constraints.replicaRole(store.StoreID)
				if !isVoter && !isNonVoter {
					// We should not panic here since the replicateQueue may have shed the
					// lease and informed MMA, since the last time MMA computed the top-k
					// ranges. This is useful for debugging in the prototype, due to the
					// lack of unit tests.
					panic(fmt.Sprintf("internal state inconsistency: "+
						"store=%v range_id=%v pending-changes=%v "+
						"rstate_replicas=%v rstate_constraints=%v",
						store.StoreID, rangeID, rstate.pendingChanges, rstate.replicas, rstate.constraints))
				}
				var conj constraintsConj
				var err error
				if isVoter {
					conj, err = rstate.constraints.candidatesToReplaceVoterForRebalance(store.StoreID)
				} else {
					conj, err = rstate.constraints.candidatesToReplaceNonVoterForRebalance(store.StoreID)
				}
				if err != nil {
					// This range has some constraints that are violated. Let those be
					// fixed first.
					log.KvDistribution.VInfof(ctx, 2, "skipping r%d: constraint violation needs fixing first: %v", rangeID, err)
					continue
				}
				disj[0] = conj
				storesToExcludeForRange = append(storesToExcludeForRange[:0], storesToExclude...)
				// Also exclude all stores on nodes that have existing replicas.
				for _, replica := range rstate.replicas {
					storeID := replica.StoreID
					if storeID == store.StoreID {
						// We don't exclude other stores on this node, since we are allowed to
						// transfer the range to them. If the node is overloaded or not fdOK,
						// we have already excluded those stores above.
						continue
					}
					nodeID := cs.stores[storeID].NodeID
					for _, storeID := range cs.nodes[nodeID].stores {
						storesToExcludeForRange.insert(storeID)
					}
				}
				// TODO(sumeer): eliminate cands allocations by passing a scratch slice.
				cands, ssSLS := cs.computeCandidatesForRange(ctx, disj[:], storesToExcludeForRange, store.StoreID)
				log.KvDistribution.VInfof(ctx, 2, "considering replica-transfer r%v from s%v: store load %v",
					rangeID, store.StoreID, ss.adjusted.load)
				if log.V(2) {
					log.KvDistribution.Infof(ctx, "candidates are:")
					for _, c := range cands.candidates {
						log.KvDistribution.Infof(ctx, " s%d: %s", c.StoreID, c.storeLoadSummary)
					}
				}

				if len(cands.candidates) == 0 {
					log.KvDistribution.VInfof(ctx, 2, "result(failed): no candidates found for r%d after exclusions", rangeID)
					continue
				}
				var rlocalities replicasLocalityTiers
				if isVoter {
					rlocalities = rstate.constraints.voterLocalityTiers
				} else {
					rlocalities = rstate.constraints.replicaLocalityTiers
				}
				localities := dsm.getExistingReplicaLocalities(rlocalities)
				isLeaseholder := rstate.constraints.leaseholderID == store.StoreID
				// Set the diversity score and lease preference index of the candidates.
				for _, cand := range cands.candidates {
					cand.diversityScore = localities.getScoreChangeForRebalance(
						ss.localityTiers, cs.stores[cand.StoreID].localityTiers)
					if isLeaseholder {
						cand.leasePreferenceIndex = matchedLeasePreferenceIndex(
							cand.StoreID, rstate.constraints.spanConfig.leasePreferences, cs.constraintMatcher)
					}
				}
				// Consider a cluster where s1 is overloadSlow, s2 is loadNoChange, and
				// s3, s4 are loadNormal. Now s4 is considering rebalancing load away
				// from s1, but the candidate top-k range has replicas {s1, s3, s4}. So
				// the only way to shed load from s1 is a s1 => s2 move. But there may
				// be other ranges at other leaseholder stores which can be moved from
				// s1 => {s3, s4}. So we should not be doing this sub-optimal transfer
				// of load from s1 => s2 unless s1 is not seeing any load shedding for
				// some interval of time. We need a way to capture this information in a
				// simple but effective manner. For now, we capture this using these
				// grace duration thresholds.
				ignoreLevel := ignoreLoadNoChangeAndHigher
				overloadDur := now.Sub(ss.overloadStartTime)
				if overloadDur > ignoreHigherThanLoadThresholdGraceDuration {
					ignoreLevel = ignoreHigherThanLoadThreshold
					log.KvDistribution.VInfof(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
						ignoreLevel, ssSLS.sls, rangeID, overloadDur)
				} else if overloadDur > ignoreLoadThresholdAndHigherGraceDuration {
					ignoreLevel = ignoreLoadThresholdAndHigher
					log.KvDistribution.VInfof(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
						ignoreLevel, ssSLS.sls, rangeID, overloadDur)
				}
				targetStoreID := sortTargetCandidateSetAndPick(
					ctx, cands, ssSLS.sls, ignoreLevel, loadDim, rng)
				if targetStoreID == 0 {
					log.KvDistribution.VInfof(ctx, 2, "result(failed): no suitable target found among candidates for r%d "+
						"(threshold %s; %s)", rangeID, ssSLS.sls, ignoreLevel)
					continue
				}
				targetSS := cs.stores[targetStoreID]
				addedLoad := rstate.load.Load
				if !isLeaseholder {
					addedLoad[CPURate] = rstate.load.RaftCPU
				}
				if !cs.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, cands.means, false, loadDim) {
					log.KvDistribution.VInfof(ctx, 2, "result(failed): cannot shed from s%d to s%d for r%d: delta load %v",
						store.StoreID, targetStoreID, rangeID, addedLoad)
					continue
				}
				addTarget := roachpb.ReplicationTarget{
					NodeID:  targetSS.NodeID,
					StoreID: targetSS.StoreID,
				}
				removeTarget := roachpb.ReplicationTarget{
					NodeID:  ss.NodeID,
					StoreID: ss.StoreID,
				}
				if addTarget.StoreID == removeTarget.StoreID {
					panic(fmt.Sprintf("internal state inconsistency: "+
						"add=%v==remove_target=%v range_id=%v candidates=%v",
						addTarget, removeTarget, rangeID, cands.candidates))
				}
				replicaChanges := makeRebalanceReplicaChanges(
					rangeID, rstate.replicas, rstate.load, addTarget, removeTarget)
				rangeChange := MakePendingRangeChange(rangeID, replicaChanges[:])
				if err = cs.preCheckOnApplyReplicaChanges(rangeChange.pendingReplicaChanges); err != nil {
					panic(errors.Wrapf(err, "pre-check failed for replica changes: %v for %v",
						replicaChanges, rangeID))
				}
				cs.addPendingRangeChange(rangeChange)
				changes = append(changes, rangeChange)
				rangeMoveCount++
				log.KvDistribution.VInfof(ctx, 2,
					"result(success): rebalancing r%v from s%v to s%v [change: %v] with resulting loads source: %v target: %v",
					rangeID, removeTarget.StoreID, addTarget.StoreID, changes[len(changes)-1], ss.adjusted.load, targetSS.adjusted.load)
				if rangeMoveCount >= maxRangeMoveCount {
					log.KvDistribution.VInfof(ctx, 2, "s%d has reached max range move count %d: mma returning with %d stores left in shedding stores", store.StoreID, maxRangeMoveCount, len(sheddingStores)-(idx+1))
					shouldReturnEarly = true
					break
				}
				doneShedding = ss.maxFractionPendingDecrease >= maxFractionPendingThreshold
				if doneShedding {
					log.KvDistribution.VInfof(ctx, 2, "s%d has reached pending decrease threshold(%.2f>=%.2f) after rebalancing: done shedding with %d left in topk",
						store.StoreID, ss.maxFractionPendingDecrease, maxFractionPendingThreshold, n-(i+1))
					break
				}
			}
			// TODO(sumeer): For regular rebalancing, we will wait until those top-K
			// move and then continue with the rest. There is a risk that the top-K
			// have some constraint that prevents rebalancing, while the rest can be
			// moved. Running with underprovisioned clusters and expecting load-based
			// rebalancing to work well is not in scope.
			if doneShedding {
				log.KvDistribution.VInfof(ctx, 2, "store s%d is done shedding, moving to next store", store.StoreID)
				shouldContinue = true
				break
			}
		}
		if shouldReturnEarly {
			return changes
		}
		if shouldContinue {
			continue
		}
	}
	return changes
}
