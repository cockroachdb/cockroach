// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

type MMAMetrics struct {
	DroppedDueToStateInconsistency  *metric.Counter
	ExternalFailedToRegister        *metric.Counter
	ExternaRegisterSuccess          *metric.Counter
	ExternalReplicaRebalanceSuccess *metric.Counter
	ExternalReplicaRebalanceFailure *metric.Counter
	ExternalLeaseTransferSuccess    *metric.Counter
	ExternalLeaseTransferFailure    *metric.Counter
	MMAReplicaRebalanceSuccess      *metric.Counter
	MMAReplicaRebalanceFailure      *metric.Counter
	MMALeaseTransferSuccess         *metric.Counter
	MMALeaseTransferFailure         *metric.Counter
	MMARegisterLeaseSuccess         *metric.Counter
	MMARegisterRebalanceSuccess     *metric.Counter
}

func makeMMAMetrics() *MMAMetrics {
	return &MMAMetrics{
		DroppedDueToStateInconsistency:  metric.NewCounter(metaDroppedDueToStateInconsistency),
		ExternalFailedToRegister:        metric.NewCounter(metaExternalFailedToRegister),
		ExternaRegisterSuccess:          metric.NewCounter(metaExternaRegisterSuccess),
		MMARegisterLeaseSuccess:         metric.NewCounter(metaMMARegisterLeaseSuccess),
		MMARegisterRebalanceSuccess:     metric.NewCounter(metaMMARegisterRebalanceSuccess),
		ExternalReplicaRebalanceSuccess: metric.NewCounter(metaExternalReplicaRebalanceSuccess),
		ExternalReplicaRebalanceFailure: metric.NewCounter(metaExternalReplicaRebalanceFailure),
		ExternalLeaseTransferSuccess:    metric.NewCounter(metaExternalLeaseTransferSuccess),
		ExternalLeaseTransferFailure:    metric.NewCounter(metaExternalLeaseTransferFailure),
		MMAReplicaRebalanceSuccess:      metric.NewCounter(metaMMAReplicaRebalanceSuccess),
		MMAReplicaRebalanceFailure:      metric.NewCounter(metaMMAReplicaRebalanceFailure),
		MMALeaseTransferSuccess:         metric.NewCounter(metaMMALeaseTransferSuccess),
		MMALeaseTransferFailure:         metric.NewCounter(metaMMALeaseTransferFailure),
	}
}

var (
	metaDroppedDueToStateInconsistency = metric.Metadata{
		Name:        "mma.dropped",
		Help:        "Number of operations dropped due to MMA state inconsistency",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalFailedToRegister = metric.Metadata{
		Name:        "mma.external.dropped",
		Help:        "Number of external operations that failed to register with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaExternaRegisterSuccess = metric.Metadata{
		Name:        "mma.external.success",
		Help:        "Number of external operations successfully registered with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaMMARegisterLeaseSuccess = metric.Metadata{
		Name:        "mma.lease.register.success",
		Help:        "Number of lease transfers successfully registered with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaMMARegisterRebalanceSuccess = metric.Metadata{
		Name:        "mma.rebalance.register.success",
		Help:        "Number of rebalance operations successfully registered with MMA",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}
	metaExternalReplicaRebalanceSuccess = metric.Metadata{
		Name:        "mma.rebalances.external.success",
		Help:        "Number of successful external replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaExternalLeaseTransferSuccess = metric.Metadata{
		Name:        "mma.lease.external.success",
		Help:        "Number of successful external lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}

	metaExternalReplicaRebalanceFailure = metric.Metadata{
		Name:        "mma.rebalances.external.failure",
		Help:        "Number of failed external replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaExternalLeaseTransferFailure = metric.Metadata{
		Name:        "mma.lease.external.failure",
		Help:        "Number of failed external lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}

	metaMMAReplicaRebalanceSuccess = metric.Metadata{
		Name:        "mma.rebalance.success",
		Help:        "Number of successful MMA-initiated replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaMMAReplicaRebalanceFailure = metric.Metadata{
		Name:        "mma.rebalance.failure",
		Help:        "Number of failed MMA-initiated replica rebalance operations",
		Measurement: "Range Rebalances",
		Unit:        metric.Unit_COUNT,
	}

	metaMMALeaseTransferSuccess = metric.Metadata{
		Name:        "mma.lease.success",
		Help:        "Number of successful MMA-initiated lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}

	metaMMALeaseTransferFailure = metric.Metadata{
		Name:        "mma.lease.failure",
		Help:        "Number of failed MMA-initiated lease transfer operations",
		Measurement: "Lease Transfers",
		Unit:        metric.Unit_COUNT,
	}
)

type allocatorState struct {
	// Locking.
	//
	// Now (for prototype):
	//
	// mu is a crude stopgap to make everything in the MMA thread-safe. Every
	// method that implements the Allocator interface must acquire this mutex.
	//
	// Future (for production):
	//
	// The main concern with the cure approach is that mu is held while doing
	// slow decision-making. That prevents (a) other decision-making, (b) blocks
	// updates from AllocatorSync from other components that are not currently
	// using MMA.
	//
	// Option 1 Release mu frequently:
	//
	// The allocatorState is deliberately tightly coupled and we want it to be
	// internally consistent. We expect allocatorState.rebalanceStores to be the
	// longest lived holder of mu. We could modify it so that after it computes
	// the cluster means and computes the overloaded stores it releases mu, and
	// then reacquires it when trying to shed for each overloaded store. Note
	// that when replicateQueue and leaseQueue also call into MMA, those calls
	// will be per range and will hold mu for a very short time.
	//
	// This option doesn't change the fact that effectively everything done by
	// MMA is single threaded, so it cannot scale to consume more than 1 vCPU.
	// However, we also would like MMA to be efficient enough that even beefy
	// nodes don't spend more than 1 vCPU on MMA, so perhaps this is ok.
	//
	// Option 2: Copy-on-write:
	//
	// Unclear how to make cow efficient for these data-structures.
	//
	// Option 3: Queue updates:
	//
	// Read lock would be held for computing decisions and then these decisions
	// would be queued for adding as pending changes to MMA state. Decisions can
	// be returned to enacting module with the expected range descriptor,
	// without waiting for the addition to the pending changes in MMA. The
	// assumption here is that there will be few conflicts on a single range,
	// and those rare conflicts will result in some decisions becoming noops.
	// The problem is that long-lived read locks with write locks result in
	// read-read contention (to prevent writer starvation). If we had a
	// try-write-lock that could quickly return with failure then we could avoid
	// this. We could of course build our own queueing mechanism instead of
	// relying on the queueing in mutex.
	mmaMetrics *MMAMetrics
	mu         syncutil.Mutex
	cs         *clusterState

	// Ranges that are under-replicated, over-replicated, don't satisfy
	// constraints, have low diversity etc. Avoids iterating through all ranges.
	// A range is removed from this map if it has pending changes -- when those
	// pending changes go away it gets added back so we can check on it.
	rangesNeedingAttention map[roachpb.RangeID]struct{}

	diversityScoringMemo *diversityScoringMemo

	rand *rand.Rand
}

var _ Allocator = &allocatorState{}

// TODO(sumeer): temporary constants.
const (
	maxFractionPendingThreshold = 0.1
)

func NewAllocatorState(ts timeutil.TimeSource, rand *rand.Rand) *allocatorState {
	interner := newStringInterner()
	cs := newClusterState(ts, interner)
	return &allocatorState{
		cs:                     cs,
		rangesNeedingAttention: map[roachpb.RangeID]struct{}{},
		diversityScoringMemo:   newDiversityScoringMemo(),
		rand:                   rand,
		mmaMetrics:             makeMMAMetrics(),
	}
}

// These constants are semi-arbitrary.

// Don't start moving ranges from a cpu overloaded remote store, to give it
// some time to shed its leases.
const remoteStoreLeaseSheddingGraceDuration = 2 * time.Minute
const overloadGracePeriod = time.Minute

func (a *allocatorState) Metrics() *MMAMetrics {
	return a.mmaMetrics
}

func (a *allocatorState) LoadSummaryForAllStores() string {
	return a.cs.loadSummaryForAllStores()
}

var mmaid = atomic.Int64{}

// Called periodically, say every 10s.
//
// We do not want to shed replicas for CPU from a remote store until its had a
// chance to shed leases.
func (a *allocatorState) rebalanceStores(
	ctx context.Context, localStoreID roachpb.StoreID,
) []PendingRangeChange {
	now := a.cs.ts.Now()
	ctx = logtags.AddTag(ctx, "mmaid", mmaid.Add(1))
	log.VInfof(ctx, 2, "rebalanceStores begins")
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
	clusterMeans := a.cs.meansMemo.getMeans(nil)
	type sheddingStore struct {
		roachpb.StoreID
		storeLoadSummary
	}
	var sheddingStores []sheddingStore
	log.Infof(ctx,
		"cluster means: (stores-load %s) (stores-capacity %s) (nodes-cpu-load %d) (nodes-cpu-capacity %d)",
		clusterMeans.storeLoad.load, clusterMeans.storeLoad.capacity,
		clusterMeans.nodeLoad.loadCPU, clusterMeans.nodeLoad.capacityCPU)
	// NB: We don't attempt to shed replicas or leases from a store which is
	// fdDrain or fdDead, nor do we attempt to shed replicas from a store which
	// is storeMembershipRemoving (decommissioning). These are currently handled
	// via replicate_queue.go.
	for storeID, ss := range a.cs.stores {
		sls := a.cs.meansMemo.getStoreLoadSummary(clusterMeans, storeID, ss.loadSeqNum)
		log.VInfof(ctx, 2, "evaluating s%d: node load %s, store load %s, worst dim %s",
			storeID, sls.nls, sls.sls, sls.worstDim)

		if sls.sls >= overloadSlow {
			if ss.overloadEndTime != (time.Time{}) {
				if now.Sub(ss.overloadEndTime) > overloadGracePeriod {
					ss.overloadStartTime = now
					log.Infof(ctx, "overload-start s%v (%v) - grace period expired", storeID, sls)
				} else {
					// Else, extend the previous overload interval.
					log.Infof(ctx, "overload-continued s%v (%v) - within grace period", storeID, sls)
				}
				ss.overloadEndTime = time.Time{}
			}
			// The pending decrease must be small enough to continue shedding
			if ss.maxFractionPendingDecrease < maxFractionPendingThreshold &&
				// There should be no pending increase, since that can be an overestimate.
				ss.maxFractionPendingIncrease < epsilon {
				log.VInfof(ctx, 2, "store s%v was added to shedding store list", storeID)
				sheddingStores = append(sheddingStores, sheddingStore{StoreID: storeID, storeLoadSummary: sls})
			} else {
				log.VInfof(ctx, 2,
					"skipping overloaded store s%d (worst dim: %s): pending decrease %.2f >= threshold %.2f or pending increase %.2f >= epsilon",
					storeID, sls.worstDim, ss.maxFractionPendingDecrease, maxFractionPendingThreshold, ss.maxFractionPendingIncrease)
			}
		} else if sls.sls < loadNoChange && ss.overloadEndTime == (time.Time{}) {
			// NB: we don't stop the overloaded interval if the store is at
			// loadNoChange, since a store can hover at the border of the two.
			log.Infof(ctx, "overload-end s%v (%v) - load dropped below no-change threshold", storeID, sls)
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
		log.Info(ctx, "sorted shedding stores:")
		for _, store := range sheddingStores {
			log.Infof(ctx, "  (s%d: %s)", store.StoreID, store.sls)
		}
	}

	var changes []PendingRangeChange
	var disj [1]constraintsConj
	var storesToExclude storeIDPostingList
	var storesToExcludeForRange storeIDPostingList
	scratchNodes := map[roachpb.NodeID]*NodeLoad{}
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
		log.Infof(ctx, "start processing shedding store s%d: cpu node load %s, store load %s, worst dim %s",
			store.StoreID, store.nls, store.sls, store.worstDim)
		ss := a.cs.stores[store.StoreID]

		doneShedding := false
		if true {
			// Debug logging.
			topKRanges := ss.adjusted.topKRanges[localStoreID]
			n := topKRanges.len()
			if n > 0 {
				var b strings.Builder
				for i := 0; i < n; i++ {
					rangeID := topKRanges.index(i)
					rstate := a.cs.ranges[rangeID]
					load := rstate.load.Load
					if !ss.adjusted.replicas[rangeID].IsLeaseholder {
						load[CPURate] = rstate.load.RaftCPU
					}
					fmt.Fprintf(&b, " r%d:%v", rangeID, load)
				}
				log.Infof(ctx, "top-K[%s] ranges for s%d with lease on local s%d:%s",
					topKRanges.dim, store.StoreID, localStoreID, b.String())
			} else {
				log.Infof(ctx, "no top-K[%s] ranges found for s%d with lease on local s%d",
					topKRanges.dim, store.StoreID, localStoreID)
			}
		}

		// TODO(tbg): it's somewhat akward that we only enter this branch for
		// ss.StoreID == localStoreID and not for *any* calling local store.
		// More generally, does it make sense that rebalanceStores is called on
		// behalf of a particular store (vs. being called on behalf of the set
		// of local store IDs)?
		if ss.StoreID == localStoreID && store.dimSummary[CPURate] >= overloadSlow {
			log.VInfof(ctx, 2, "local store s%d is CPU overloaded (%v >= %v), attempting lease transfers first",
				store.StoreID, store.dimSummary[CPURate], overloadSlow)
			// This store is local, and cpu overloaded. Shed leases first.
			//
			// NB: any ranges at this store that don't have pending changes must
			// have this local store as the leaseholder.
			topKRanges := ss.adjusted.topKRanges[localStoreID]
			n := topKRanges.len()
			for i := 0; i < n; i++ {
				rangeID := topKRanges.index(i)
				rstate := a.cs.ranges[rangeID]
				if len(rstate.pendingChanges) > 0 {
					// If the range has pending changes, don't make more changes.
					log.VInfof(ctx, 2, "skipping r%d: has pending changes", rangeID)
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
						log.Fatalf(ctx, "internal state inconsistency: replica considered for lease shedding has no pending"+
							" changes but is not leaseholder: %+v", rstate)
					}
				}
				if now.Sub(rstate.lastFailedChange) < lastFailedChangeDelayDuration {
					log.VInfof(ctx, 2, "skipping r%d: too soon after failed change", rangeID)
					continue
				}
				if !a.ensureAnalyzedConstraints(rstate) {
					log.VInfof(ctx, 2, "skipping r%d: constraints analysis failed", rangeID)
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
				var candsPL storeIDPostingList
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
				var means meansForStoreSet
				clear(scratchNodes)
				means.stores = candsPL
				computeMeansForStoreSet(a.cs, &means, scratchNodes)
				sls := a.cs.computeLoadSummary(store.StoreID, &means.storeLoad, &means.nodeLoad)
				log.VInfof(ctx, 2, "considering lease-transfer r%v from s%v: candidates are %v", rangeID, store.StoreID, candsPL)
				if sls.dimSummary[CPURate] < overloadSlow {
					// This store is not cpu overloaded relative to these candidates for
					// this range.
					log.VInfof(ctx, 2, "result(failed): skipping r%d since store not overloaded relative to candidates", rangeID)
					continue
				}
				var candsSet candidateSet
				for _, cand := range cands {
					if a.cs.stores[cand.storeID].adjusted.replicas[rangeID].VoterIsLagging {
						// Don't transfer lease to a store that is lagging.
						log.Infof(ctx, "skipping store s%d for lease transfer: replica is lagging",
							cand.storeID)
						continue
					}
					candSls := a.cs.computeLoadSummary(cand.storeID, &means.storeLoad, &means.nodeLoad)
					if sls.fd != fdOK {
						log.VInfof(ctx, 2, "skipping store s%d: failure detection status not OK", cand.storeID)
						continue
					}
					candsSet.candidates = append(candsSet.candidates, candidateInfo{
						StoreID:              cand.storeID,
						storeLoadSummary:     candSls,
						diversityScore:       0,
						leasePreferenceIndex: cand.leasePreferenceIndex,
					})
				}
				if len(candsSet.candidates) == 0 {
					log.Infof(
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
					ctx, candsSet, sls.sls, ignoreHigherThanLoadThreshold, CPURate, a.rand)
				if targetStoreID == 0 {
					log.Infof(
						ctx,
						"result(failed): no candidates to move lease from n%vs%v for r%v after sortTargetCandidateSetAndPick",
						ss.NodeID, ss.StoreID, rangeID)
					continue
				}
				targetSS := a.cs.stores[targetStoreID]
				var addedLoad LoadVector
				// Only adding leaseholder CPU.
				addedLoad[CPURate] = rstate.load.Load[CPURate] - rstate.load.RaftCPU
				if addedLoad[CPURate] < 0 {
					// TODO(sumeer): remove this panic once we are not in an
					// experimental phase.
					addedLoad[CPURate] = 0
					panic("raft cpu higher than total cpu")
				}
				if !a.cs.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, &means, true, CPURate) {
					log.VInfof(ctx, 2, "result(failed): cannot shed from s%d to s%d for r%d: delta load %v",
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
				leaseChanges := MakeLeaseTransferChanges(
					rangeID, rstate.replicas, rstate.load, addTarget, removeTarget)
				if valid, reason := a.cs.preCheckOnApplyReplicaChanges(leaseChanges[:]); !valid {
					panic(fmt.Sprintf("pre-check failed for lease transfer %v: due to %v",
						leaseChanges, reason))
				}
				pendingChanges := a.cs.createPendingChanges(leaseChanges[:]...)
				changes = append(changes, PendingRangeChange{
					RangeID:               rangeID,
					pendingReplicaChanges: pendingChanges[:],
				})
				leaseTransferCount++
				if changes[len(changes)-1].IsChangeReplicas() || !changes[len(changes)-1].IsTransferLease() {
					panic(fmt.Sprintf("lease transfer is invalid: %v", changes[len(changes)-1]))
				}
				log.Infof(ctx,
					"result(success): shedding r%v lease from s%v to s%v [change:%v] with "+
						"resulting loads source:%v target:%v (means: %v) (frac_pending: (src:%.2f,target:%.2f) (src:%.2f,target:%.2f))",
					rangeID, removeTarget.StoreID, addTarget.StoreID, changes[len(changes)-1],
					ss.adjusted.load, targetSS.adjusted.load, means.storeLoad.load,
					ss.maxFractionPendingIncrease, ss.maxFractionPendingDecrease,
					targetSS.maxFractionPendingIncrease, targetSS.maxFractionPendingDecrease)
				if leaseTransferCount >= maxLeaseTransferCount {
					log.VInfof(ctx, 2, "reached max lease transfer count %d, returning", maxLeaseTransferCount)
					return changes
				}
				doneShedding = ss.maxFractionPendingDecrease >= maxFractionPendingThreshold
				if doneShedding {
					log.VInfof(ctx, 2, "s%d has reached pending decrease threshold(%.2f>=%.2f) after lease transfers: done shedding with %d left in topK",
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
				log.VInfof(ctx, 2, "skipping replica transfers for s%d: done shedding=%v, lease_transfers=%d",
					store.StoreID, doneShedding, leaseTransferCount)
				continue
			}
		} else {
			log.VInfof(ctx, 2, "skipping lease shedding: s%v != local store s%s or cpu is not overloaded: %v",
				ss.StoreID, localStoreID, store.dimSummary[CPURate])
		}

		log.VInfof(ctx, 2, "attempting to shed replicas next")

		if store.StoreID != localStoreID && store.dimSummary[CPURate] >= overloadSlow &&
			now.Sub(ss.overloadStartTime) < remoteStoreLeaseSheddingGraceDuration {
			log.VInfof(ctx, 2, "skipping remote store s%d: in lease shedding grace period", store.StoreID)
			continue
		}
		// If the node is cpu overloaded, or the store/node is not fdOK, exclude
		// the other stores on this node from receiving replicas shed by this
		// store.
		excludeStoresOnNode := store.nls > overloadSlow || store.fd != fdOK
		storesToExclude = storesToExclude[:0]
		if excludeStoresOnNode {
			nodeID := ss.NodeID
			for _, storeID := range a.cs.nodes[nodeID].stores {
				storesToExclude.insert(storeID)
			}
			log.VInfof(ctx, 2, "excluding all stores on n%d due to overload/fd status", nodeID)
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
			rstate := a.cs.ranges[rangeID]
			if len(rstate.pendingChanges) > 0 {
				// If the range has pending changes, don't make more changes.
				log.VInfof(ctx, 2, "skipping r%d: has pending changes", rangeID)
				continue
			}
			if now.Sub(rstate.lastFailedChange) < lastFailedChangeDelayDuration {
				log.VInfof(ctx, 2, "skipping r%d: too soon after failed change", rangeID)
				continue
			}
			if !a.ensureAnalyzedConstraints(rstate) {
				log.VInfof(ctx, 2, "skipping r%d: constraints analysis failed", rangeID)
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
				log.VInfof(ctx, 2, "skipping r%d: constraint violation needs fixing first: %v", rangeID, err)
				continue
			}
			disj[0] = conj
			storesToExcludeForRange = append(storesToExcludeForRange[:0], storesToExclude...)
			// Also exclude all stores on nodes that have existing replicas.
			for _, replica := range rstate.replicas {
				storeID := replica.StoreID
				nodeID := a.cs.stores[storeID].NodeID
				for _, storeID := range a.cs.nodes[nodeID].stores {
					storesToExcludeForRange.insert(storeID)
				}
			}
			// TODO(sumeer): eliminate cands allocations by passing a scratch slice.
			cands, ssSLS := a.computeCandidatesForRange(disj[:], storesToExcludeForRange, store.StoreID)
			log.VInfof(ctx, 2, "considering replica-transfer r%v from s%v: store load %v",
				rangeID, store.StoreID, ss.adjusted.load)
			if log.V(2) {
				log.Infof(ctx, "candidates are:")
				for _, c := range cands.candidates {
					log.Infof(ctx, " s%d: %s", c.StoreID, c.storeLoadSummary)
				}
			}

			if len(cands.candidates) == 0 {
				log.VInfof(ctx, 2, "result(failed): no candidates found for r%d after exclusions", rangeID)
				continue
			}
			var rlocalities replicasLocalityTiers
			if isVoter {
				rlocalities = rstate.constraints.voterLocalityTiers
			} else {
				rlocalities = rstate.constraints.replicaLocalityTiers
			}
			localities := a.diversityScoringMemo.getExistingReplicaLocalities(rlocalities)
			isLeaseholder := rstate.constraints.leaseholderID == store.StoreID
			// Set the diversity score and lease preference index of the candidates.
			for _, cand := range cands.candidates {
				cand.diversityScore = localities.getScoreChangeForRebalance(
					ss.localityTiers, a.cs.stores[cand.StoreID].localityTiers)
				if isLeaseholder {
					cand.leasePreferenceIndex = matchedLeasePreferenceIndex(
						cand.StoreID, rstate.constraints.spanConfig.leasePreferences, a.cs.constraintMatcher)
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
				log.VInfof(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
					ignoreLevel, ssSLS.sls, rangeID, overloadDur)
			} else if overloadDur > ignoreLoadThresholdAndHigherGraceDuration {
				ignoreLevel = ignoreLoadThresholdAndHigher
				log.VInfof(ctx, 3, "using level %v (threshold:%v) for r%d based on overload duration %v",
					ignoreLevel, ssSLS.sls, rangeID, overloadDur)
			}
			targetStoreID := sortTargetCandidateSetAndPick(
				ctx, cands, ssSLS.sls, ignoreLevel, loadDim, a.rand)
			if targetStoreID == 0 {
				log.VInfof(ctx, 2, "result(failed): no suitable target found among candidates for r%d "+
					"(threshold %s; %s)", rangeID, ssSLS.sls, ignoreLevel)
				continue
			}
			targetSS := a.cs.stores[targetStoreID]
			addedLoad := rstate.load.Load
			if !isLeaseholder {
				addedLoad[CPURate] = rstate.load.RaftCPU
			}
			if !a.cs.canShedAndAddLoad(ctx, ss, targetSS, addedLoad, cands.means, false, loadDim) {
				log.VInfof(ctx, 2, "result(failed): cannot shed from s%d to s%d for r%d: delta load %v",
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
			if valid, reason := a.cs.preCheckOnApplyReplicaChanges(replicaChanges[:]); !valid {
				panic(fmt.Sprintf("pre-check failed for replica changes: %v due to %v for %v",
					replicaChanges, reason, rangeID))
			}
			pendingChanges := a.cs.createPendingChanges(replicaChanges[:]...)
			changes = append(changes, PendingRangeChange{
				RangeID:               rangeID,
				pendingReplicaChanges: pendingChanges[:],
			})
			rangeMoveCount++
			log.VInfof(ctx, 2,
				"result(success): rebalancing r%v from s%v to s%v [change: %v] with resulting loads source: %v target: %v",
				rangeID, removeTarget.StoreID, addTarget.StoreID, changes[len(changes)-1], ss.adjusted.load, targetSS.adjusted.load)
			if rangeMoveCount >= maxRangeMoveCount {
				log.VInfof(ctx, 2, "s%d has reached max range move count %d: mma returning with %d stores left in shedding stores", store.StoreID, maxRangeMoveCount, len(sheddingStores)-(idx+1))
				return changes
			}
			doneShedding = ss.maxFractionPendingDecrease >= maxFractionPendingThreshold
			if doneShedding {
				log.VInfof(ctx, 2, "s%d has reached pending decrease threshold(%.2f>=%.2f) after rebalancing: done shedding with %d left in topk",
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
			log.VInfof(ctx, 2, "store s%d is done shedding, moving to next store", store.StoreID)
			continue
		}
	}
	return changes
}

// SetStore implements the Allocator interface.
func (a *allocatorState) SetStore(store StoreAttributesAndLocality) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cs.setStore(store)
}

// RemoveNodeAndStores implements the Allocator interface.
func (a *allocatorState) RemoveNodeAndStores(nodeID roachpb.NodeID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	panic("unimplemented")
}

// UpdateFailureDetectionSummary implements the Allocator interface.
func (a *allocatorState) UpdateFailureDetectionSummary(
	nodeID roachpb.NodeID, fd failureDetectionSummary,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	panic("unimplemented")
}

// ProcessStoreLeaseholderMsg implements the Allocator interface.
func (a *allocatorState) ProcessStoreLoadMsg(ctx context.Context, msg *StoreLoadMsg) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cs.processStoreLoadMsg(ctx, msg)
}

// AdjustPendingChangesDisposition implements the Allocator interface.
func (a *allocatorState) AdjustPendingChangesDisposition(changeIDs []ChangeID, success bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !success {
		replicaChanges := make([]ReplicaChange, 0, len(changeIDs))
		for _, changeID := range changeIDs {
			change, ok := a.cs.pendingChanges[changeID]
			if !ok {
				return
			}
			replicaChanges = append(replicaChanges, change.ReplicaChange)
		}
		if valid, reason := a.cs.preCheckOnUndoReplicaChanges(replicaChanges); !valid {
			log.Infof(context.Background(), "did not undo change %v: due to %v", changeIDs, reason)
			return
		}
	}

	for _, changeID := range changeIDs {
		// We set !requireFound, since a StoreLeaseholderMsg that happened after
		// the pending change was created and before this call to
		// AdjustPendingChangesDisposition may have already removed the pending
		// change.
		if success {
			a.cs.pendingChangeEnacted(changeID, a.cs.ts.Now(), false)
		} else {
			a.cs.undoPendingChange(changeID, false)
		}
	}
}

// RegisterExternalChanges implements the Allocator interface. All changes should
// correspond to the same range, panic otherwise.
func (a *allocatorState) RegisterExternalChanges(changes []ReplicaChange) []ChangeID {
	a.mu.Lock()
	defer a.mu.Unlock()
	if valid, reason := a.cs.preCheckOnApplyReplicaChanges(changes); !valid {
		a.mmaMetrics.ExternalFailedToRegister.Inc(1)
		log.Infof(context.Background(),
			"did not register external changes: due to %v", reason)
		return nil
	} else {
		a.mmaMetrics.ExternaRegisterSuccess.Inc(1)
	}
	pendingChanges := a.cs.createPendingChanges(changes...)
	changeIDs := make([]ChangeID, len(pendingChanges))
	for i, pendingChange := range pendingChanges {
		changeIDs[i] = pendingChange.ChangeID
	}
	return changeIDs
}

// ComputeChanges implements the Allocator interface.
func (a *allocatorState) ComputeChanges(
	ctx context.Context, msg *StoreLeaseholderMsg, opts ChangeOptions,
) []PendingRangeChange {
	a.mu.Lock()
	defer a.mu.Unlock()
	if msg.StoreID != opts.LocalStoreID {
		panic(fmt.Sprintf("ComputeChanges: expected StoreID %d, got %d", opts.LocalStoreID, msg.StoreID))
	}
	a.cs.processStoreLeaseholderMsg(ctx, msg, a.mmaMetrics)
	return a.rebalanceStores(ctx, opts.LocalStoreID)
}

// AdminRelocateOne implements the Allocator interface.
func (a *allocatorState) AdminRelocateOne(
	desc *roachpb.RangeDescriptor,
	conf *roachpb.SpanConfig,
	leaseholderStore roachpb.StoreID,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) ([]pendingReplicaChange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	panic("unimplemented")
}

// AdminScatterOne implements the Allocator interface.
func (a *allocatorState) AdminScatterOne(
	rangeID roachpb.RangeID, canTransferLease bool, opts ChangeOptions,
) ([]pendingReplicaChange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	panic("unimplemented")
}

// KnownStores implements the Allocator interface.
func (a *allocatorState) KnownStores() map[roachpb.StoreID]struct{} {
	stores := make(map[roachpb.StoreID]struct{})
	a.mu.Lock()
	defer a.mu.Unlock()
	// The allocatorState is a wrapper around the clusterState, which contains
	// all the stores.
	for storeID := range a.cs.stores {
		stores[storeID] = struct{}{}
	}
	return stores
}

type candidateInfo struct {
	roachpb.StoreID
	storeLoadSummary
	// Higher is better.
	diversityScore float64
	// Lower is better.
	leasePreferenceIndex int32
}

type candidateSet struct {
	candidates []candidateInfo
	means      *meansForStoreSet
}

type ignoreLevel uint8

const (
	// Default.
	ignoreLoadNoChangeAndHigher ignoreLevel = iota

	// NB: loadThreshold is always > loadNoChange.
	//
	// Has been overloaded over ignoreLoadThresholdAndHigherGraceDuration
	// (5*time.Minute). We are getting desperate and consider more overloaded
	// stores as long as they are < loadThreshold.
	ignoreLoadThresholdAndHigher

	// Has been overloaded over ignoreHigherThanLoadThresholdGraceDuration
	// (8*time.Minute). We are getting desperate and consider more overloaded
	// stores as long as they are <= loadThreshold.
	ignoreHigherThanLoadThreshold
)

const ignoreLoadThresholdAndHigherGraceDuration = 5 * time.Minute
const ignoreHigherThanLoadThresholdGraceDuration = 8 * time.Minute

func (i ignoreLevel) String() string {
	switch i {
	case ignoreLoadNoChangeAndHigher:
		return "only consider targets < LoadNoChange"
	case ignoreLoadThresholdAndHigher:
		return "only consider targets < load threshold"
	case ignoreHigherThanLoadThreshold:
		return "only consider targets <= load threshold"
	default:
		panic(fmt.Sprintf("unknown: %d", i))
	}
}

// SafeFormat implements the redact.SafeFormatter interface.
func (i ignoreLevel) SafeFormat(s interfaces.SafePrinter, verb rune) {
	s.SafeInt(redact.SafeInt(i))
}

// The logic in sortTargetCandidateSetAndPick related to load is a heuristic
// motivated by the following observations:
//
// 1. The high level objective is mostly to move towards the mean along all
// resource dimensions. For now, we are fuzzy on whether this should be the
// usage mean or the utilization mean, since historically the allocated has
// considered the former only, while mmaprototype in loadSummaryForDimension considers
// both.
//
// 2. We want to minimize range movement (and to a lesser extent lease
// movement) when moving towards the mean, so it is preferable to first move
// to stores that are not overloaded along any dimension, since they are
// guaranteed to not have to shed the moved load. Hence sorting based on the
// overall store loadSummary first for the targets.
//
// 3. It is possible (as also demonstrated by asim tests) that with multiple
// metrics, we can have scenarios where every store is loadNoChange or
// overloaded along some dimension while underloaded along another dimension
// (the latter is necessarily true if overload and underload are based on the
// mean). We have to be able to rebalance out of such local optima. One way to
// do so would be introduce the ability to decide changes to multiple ranges
// at once, however this adds code complexity. Instead, we can ignore the fact
// that the target is overall overloaded, and pick a candidate that is most
// underloaded in the dimension that the source wants to shed. This should
// allow the target to accept the range and later shed some other range. There
// is the possibility that will result in more range movement until we reach
// the goal state, but so be it.

// In the set of candidates it is possible that some are overloaded, or have
// too many pending changes. It divides them into sets with equal diversity
// score and sorts such that the set with higher diversity score is considered
// before one with lower diversity score. Then it finds the best diversity set
// which has some candidates that are not overloaded wrt disk capacity. Within
// this set it will exclude candidates that are overloaded or have too many
// pending changes, and then pick randomly among the least loaded ones.
//
// Our enum for loadSummary is quite coarse. This has pros and cons: the pros
// are (a) the coarseness increases the probability that we do eventually find
// something that can handle the multidimensional load of this range, (b)
// random picking in a large set results in different stores with allocators
// making different decisions, which reduces thrashing. The con is that we may
// not select the candidate that is the very best. Normally, we only consider
// candidates in the best equivalence class defined by the loadSummary
// aggregated across all dimensions, which is already coarse as mentioned
// above. However, when ignoreHigherThanLoadThreshold is set and an
// overloadedDim is provided, we extend beyond the first equivalence class, to
// consider all candidates that are underloaded in the overloadedDim.
//
// The caller must not exclude any candidates based on load or
// maxFractionPendingIncrease. That filtering must happen here. Depending on
// the value of ignoreLevel, only candidates < loadThreshold may be
// considered.
//
// overloadDim, if not set to NumLoadDimensions, represents the dimension that
// is overloaded in the source. It is used to narrow down the candidates to
// those that are most underloaded in that dimension, when all the candidates
// have an aggregate load summary (across all dimensions) that is >=
// loadNoChange. This function guarantees that when overloadedDim is set, all
// candidates returned will be < loadNoChange in that dimension.
//
// overloadDim will be set to NumLoadDimensions when the source is not
// shedding due to overload (say due to (impending) failure). In this case the
// caller should set loadThreshold to overloadSlow and ignoreLevel to
// ignoreHigherThanLoadThreshold, to maximize the probability of finding a
// candidate.
func sortTargetCandidateSetAndPick(
	ctx context.Context,
	cands candidateSet,
	loadThreshold loadSummary,
	ignoreLevel ignoreLevel,
	overloadedDim LoadDimension,
	rng *rand.Rand,
) roachpb.StoreID {
	var b strings.Builder
	for i := range cands.candidates {
		fmt.Fprintf(&b, " s%v(%v)", cands.candidates[i].StoreID, cands.candidates[i].storeLoadSummary)
	}
	if loadThreshold <= loadNoChange {
		panic("loadThreshold must be > loadNoChange")
	}
	slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
		if diversityScoresAlmostEqual(a.diversityScore, b.diversityScore) {
			return cmp.Or(cmp.Compare(a.sls, b.sls),
				cmp.Compare(a.leasePreferenceIndex, b.leasePreferenceIndex),
				cmp.Compare(a.StoreID, b.StoreID))
		}
		return -cmp.Compare(a.diversityScore, b.diversityScore)
	})
	bestDiversity := cands.candidates[0].diversityScore
	j := 0
	// Iterate over candidates with the same diversity. First such set that is
	// not disk capacity constrained is where we stop. Even if they can't accept
	// because they have too many pending changes or can't handle the addition
	// of the range. That is, we are not willing to reduce diversity when
	// rebalancing ranges. When rebalancing leases, the diversityScore of all
	// the candidates will be 0.
	for _, cand := range cands.candidates {
		if !diversityScoresAlmostEqual(bestDiversity, cand.diversityScore) {
			if j == 0 {
				// Don't have any candidates yet.
				bestDiversity = cand.diversityScore
			} else {
				// Have a set of candidates.
				break
			}
		}
		// Diversity is the same. Include if not reaching disk capacity.
		if !cand.highDiskSpaceUtilization {
			cands.candidates[j] = cand
			j++
		}
	}
	if j == 0 {
		log.VInfof(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to disk space util")
		return 0
	}
	// Every candidate in [0:j] has same diversity and is sorted by increasing
	// load, and within the same load by increasing leasePreferenceIndex.
	cands.candidates = cands.candidates[:j]
	j = 0
	// Filter out the candidates that are overloaded or have too many pending
	// changes.
	//
	// lowestLoad is the load of the set of candidates with the lowest load,
	// among which we will later pick. If the set is found to be empty in the
	// following loop, the lowestLoad is updated.
	//
	// Consider the series of sets of candidates that have the same sls. The
	// only reason we will consider a set later than the first one is if the
	// earlier sets get fully discarded solely because of nls and have no
	// pending changes, or because of ignoreHigherThanLoadThreshold.
	lowestLoadSet := cands.candidates[0].sls
	currentLoadSet := lowestLoadSet
	discardedCandsHadNoPendingChanges := true
	for _, cand := range cands.candidates {
		if cand.sls > currentLoadSet {
			if !discardedCandsHadNoPendingChanges {
				// Never go to the next set if we have discarded candidates that have
				// pending changes. We will wait for those to have no pending changes
				// before we consider later sets.
				break
			}
			currentLoadSet = cand.sls
		}
		if cand.sls > lowestLoadSet {
			if j == 0 {
				// This is the lowestLoad set being considered now.
				lowestLoadSet = cand.sls
			} else if ignoreLevel < ignoreHigherThanLoadThreshold || overloadedDim == NumLoadDimensions {
				// Past the lowestLoad set. We don't care about these.
				break
			}
			// Else ignoreLevel >= ignoreHigherThanLoadThreshold && overloadedDim !=
			// NumLoadDimensions, so keep going and consider all candidates with
			// cand.sls <= loadThreshold.
		}
		if cand.sls > loadThreshold {
			break
		}
		candDiscardedByNLS := cand.nls > loadThreshold ||
			(cand.nls == loadThreshold && ignoreLevel < ignoreHigherThanLoadThreshold)
		candDiscardedByOverloadDim := overloadedDim != NumLoadDimensions &&
			cand.dimSummary[overloadedDim] >= loadNoChange
		if candDiscardedByNLS || candDiscardedByOverloadDim ||
			cand.maxFractionPendingIncrease >= maxFractionPendingThreshold {
			// Discard this candidate.
			if cand.maxFractionPendingIncrease > epsilon && discardedCandsHadNoPendingChanges {
				discardedCandsHadNoPendingChanges = false
			}
			log.VInfof(ctx, 2,
				"candiate store %v was discarded: sls=%v", cand.StoreID, cand.storeLoadSummary)
			continue
		}
		cands.candidates[j] = cand
		j++
	}
	if j == 0 {
		log.VInfof(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to load")
		return 0
	}
	lowestLoadSet = cands.candidates[0].sls
	highestLoadSet := cands.candidates[j-1].sls
	cands.candidates = cands.candidates[:j]
	// The set of candidates we will consider all have load <= loadThreshold.
	// They may all be lowestLoad, or we may have allowed additional candidates
	// because of ignoreHigherThanLoadThreshold and a specified overloadedDim.
	// When the overloadedDim is specified, all these candidates will be <
	// loadNoChange in that dimension.
	//
	// If this set has some members that are load >= loadNoChange, we have a set
	// that we would not ordinarily consider as candidates. But we are willing
	// to shed to from overloadUrgent => {overloadSlow, loadNoChange} or
	// overloadSlow => loadNoChange, when absolutely necessary. This necessity
	// is defined by the fact that we didn't have any candidate in an earlier or
	// this set that was ignored because of pending changes. Because if a
	// candidate was ignored because of pending work, we want to wait for that
	// pending work to finish and then see if we can transfer to those. Note
	// that we used the condition cand.maxFractionPendingIncrease>epsilon and
	// not cand.maxFractionPendingIncrease>=maxFractionPendingThreshold when
	// setting discardedCandsHadNoPendingChanges. This is an additional
	// conservative choice, since pending added work is slightly inflated in
	// size, and we want to have a true picture of all of these potential
	// candidates before we start using the ones with load >= loadNoChange.
	if lowestLoadSet > loadThreshold {
		panic("candidates should not have lowestLoad > loadThreshold")
	}
	// INVARIANT: lowestLoad <= loadThreshold.
	if lowestLoadSet == loadThreshold && ignoreLevel < ignoreHigherThanLoadThreshold {
		log.VInfof(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to equal to loadThreshold")
		return 0
	}
	// INVARIANT: lowestLoad < loadThreshold ||
	// (lowestLoad <= loadThreshold && ignoreLevel >= ignoreHigherThanLoadThreshold).

	// < loadNoChange is fine. We need to check whether the following cases can continue.
	// [loadNoChange, loadThreshold), or loadThreshold && ignoreHigherThanLoadThreshold.
	if lowestLoadSet >= loadNoChange &&
		(!discardedCandsHadNoPendingChanges || ignoreLevel == ignoreLoadNoChangeAndHigher) {
		log.VInfof(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to loadNoChange")
		return 0
	}
	if lowestLoadSet != highestLoadSet {
		slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
			return cmp.Or(
				cmp.Compare(a.leasePreferenceIndex, b.leasePreferenceIndex),
				cmp.Compare(a.StoreID, b.StoreID))
		})
	}
	// Candidates are sorted by non-decreasing leasePreferenceIndex. Eliminate
	// ones that have notMatchedLeasePreferenceIndex.
	j = 0
	for _, cand := range cands.candidates {
		if cand.leasePreferenceIndex == notMatchedLeasePreferencIndex {
			break
		}
		j++
	}
	if j == 0 {
		log.VInfof(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to lease preference")
		return 0
	}
	cands.candidates = cands.candidates[:j]
	if lowestLoadSet != highestLoadSet || (lowestLoadSet >= loadNoChange && overloadedDim != NumLoadDimensions) {
		// Sort candidates from lowest to highest along overloaded dimension. We
		// limit when we do this, since this will further restrict the pool of
		// candidates and in general we don't want to restrict the pool.
		slices.SortFunc(cands.candidates, func(a, b candidateInfo) int {
			return cmp.Compare(a.dimSummary[overloadedDim], b.dimSummary[overloadedDim])
		})
		lowestOverloadedLoad := cands.candidates[0].dimSummary[overloadedDim]
		if lowestOverloadedLoad >= loadNoChange {
			log.VInfof(ctx, 2, "sortTargetCandidateSetAndPick: no candidates due to overloadedDim")
			return 0
		}
		j = 1
		for j < len(cands.candidates) {
			if cands.candidates[j].dimSummary[overloadedDim] > lowestOverloadedLoad {
				break
			}
			j++
		}
		cands.candidates = cands.candidates[:j]
	}
	b.Reset()
	for i := range cands.candidates {
		fmt.Fprintf(&b, " s%v(%v)", cands.candidates[i].StoreID, cands.candidates[i].sls)
	}
	j = rng.Intn(j)
	log.VInfof(ctx, 2, "sortTargetCandidateSetAndPick: candidates:%s, picked s%v", b.String(), cands.candidates[j].StoreID)
	if ignoreLevel == ignoreLoadNoChangeAndHigher && cands.candidates[j].sls >= loadNoChange ||
		ignoreLevel == ignoreLoadThresholdAndHigher && cands.candidates[j].sls >= loadThreshold ||
		ignoreLevel == ignoreHigherThanLoadThreshold && cands.candidates[j].sls > loadThreshold {
		panic(errors.AssertionFailedf("saw higher load %v candidate than expected, ignoreLevel=%v",
			cands.candidates[j].sls, ignoreLevel))
	}
	return cands.candidates[j].StoreID
}

func (a *allocatorState) ensureAnalyzedConstraints(rstate *rangeState) bool {
	if rstate.constraints != nil {
		return true
	}
	// Populate the constraints.
	rac := newRangeAnalyzedConstraints()
	buf := rac.stateForInit()
	leaseholder := roachpb.StoreID(-1)
	for _, replica := range rstate.replicas {
		buf.tryAddingStore(replica.StoreID, replica.ReplicaIDAndType.ReplicaType.ReplicaType,
			a.cs.stores[replica.StoreID].localityTiers)
		if replica.IsLeaseholder {
			leaseholder = replica.StoreID
		}
	}
	if leaseholder < 0 {
		// Very dubious why the leaseholder (which must be a local store since there
		// are no pending changes) is not known.
		// TODO(sumeer): log an error.
		releaseRangeAnalyzedConstraints(rac)
		return false
	}
	rac.finishInit(rstate.conf, a.cs.constraintMatcher, leaseholder)
	rstate.constraints = rac
	return true
}

// Consider the core logic for a change, rebalancing or recovery.
//
// - There is a constraint expression for the target: constraintDisj
//
// - There are stores to exclude, since may already be playing roles for that
//   range (or have another replica on the same node -- so not all of these
//   stores to exclude will satisfy the constraint expression). For something
//   like candidatesVoterConstraintsUnsatisfied(), the toRemoveVoters should
//   be in the nodes to exclude.
//
// - Optional store that is trying to shed load, since it is possible that
//   when we consider the context of this constraint expression, the store
//   won't be considered overloaded. This node is not specified if shedding
//   because of failure.
//
// Given these, we need to compute:
//
// - Mean load for constraint expression. We can remember this for the whole
//   rebalancing pass since many ranges will have the same constraint
//   expression. Done via meansMemo.
//
// - Set of nodes that satisfy constraint expression. Again, we can remember this.
//   Done via meansMemo.
//
// - loadSummary for all nodes that satisfy the constraint expression. This
//   may change during rebalancing when changes have been made to node. But
//   can be cached and invalidated. Done via meansMemo.
//
// - Need diversity change for each candidate.
//
// The first 3 bullets are encapsulated in the helper function
// computeCandidatesForRange. It works for both replica additions and
// rebalancing.
//
// For the last bullet (diversity), the caller of computeCandidatesForRange
// needs to populate candidateInfo.diversityScore for each candidate in
// candidateSet. It does so via diversityScoringMemo. Then the (loadSummary,
// diversityScore) pair can be used to order candidates for attempts to add.
//
// TODO(sumeer): caching of information should mostly make the allocator pass
// efficient, but we have one unaddressed problem: When moving a range we are
// having to do O(num-stores) amount of work to iterate over the cached
// information to populate candidateSet. Same to populate the diversity
// information for each candidate (which is also cached and looked up in a
// map). Then this candidateSet needs to be sorted which is O(num-stores*log
// num-stores). Allocator-like components that scale to large clusters try to
// have a constant cost per range that is being examined for rebalancing. If
// cpu profiles of microbenchmarks indicate we have a problem here we could
// try the following optimization:
//
// Assume most ranges in the cluster have exactly the same set of localities
// for their replicas, and the same set of constraints. We have computed the
// candidateSet fully for range R1 that is our current range being examined
// for rebalancing. And say we picked a candidate N1 and changed its adjusted
// load. We can recalculate N1's loadSummary and reposition (or remove) N1
// within this sorted set -- this can be done with cost O(log num-stores) (and
// typically we won't need to reposition). If the next range considered for
// rebalancing, R2, has the same set of replica localities and constraints, we
// can start with this existing computed set.

// loadSheddingStore is only specified if this candidate computation is
// happening because of overload.
func (a *allocatorState) computeCandidatesForRange(
	expr constraintsDisj, storesToExclude storeIDPostingList, loadSheddingStore roachpb.StoreID,
) (_ candidateSet, sheddingSLS storeLoadSummary) {
	means := a.cs.meansMemo.getMeans(expr)
	if loadSheddingStore > 0 {
		sheddingSS := a.cs.stores[loadSheddingStore]
		sheddingSLS = a.cs.meansMemo.getStoreLoadSummary(means, loadSheddingStore, sheddingSS.loadSeqNum)
		if sheddingSLS.sls <= loadNoChange && sheddingSLS.nls <= loadNoChange {
			// In this set of stores, this store no longer looks overloaded.
			return candidateSet{}, sheddingSLS
		}
	}
	// We only filter out stores that are not fdOK. The rest of the filtering
	// happens later.
	var cset candidateSet
	for _, storeID := range means.stores {
		if storesToExclude.contains(storeID) {
			continue
		}
		ss := a.cs.stores[storeID]
		csls := a.cs.meansMemo.getStoreLoadSummary(means, storeID, ss.loadSeqNum)
		if csls.fd != fdOK {
			continue
		}
		cset.candidates = append(cset.candidates, candidateInfo{
			StoreID:          storeID,
			storeLoadSummary: csls,
		})
	}
	cset.means = means
	return cset, sheddingSLS
}

// Diversity scoring is very amenable to caching, since the set of unique
// locality tiers for range replicas is likely to be small. And the cache does
// not need to be cleared after every allocator pass. This caching is done via
// diversityScoringMemo which contains existingReplicaLocalities.

// existingReplicaLocalities is the cache state for a set of replicas with
// locality tiers corresponding to replicasLocalityTiers (which serves as the
// key in diversityScoringMemo). NB: For a range, scoring for non-voters uses
// the set of all replicas, and for voters uses the set of existing voters.
//
// Cases:
type existingReplicaLocalities struct {
	replicasLocalityTiers

	// For a prospective candidate with localityTiers represented by the key,
	// this caches the sum of calls to replicas[i].diversityScore(candidate).
	//
	// If this is an addition, this value can be used directly if the candidate
	// is not already in replicas, else the diversity change is 0.
	//
	// If this is a removal, this is the reduction in diversity. Note that this
	// calculation includes diversityScore with self, but that value is 0.
	scoreSums map[string]float64
}

func (erl *existingReplicaLocalities) clear() {
	erl.replicas = erl.replicas[:0]
	for k := range erl.scoreSums {
		delete(erl.scoreSums, k)
	}
}

var _ mapEntry = &existingReplicaLocalities{}

// replicasLocalityTiers represents the set of localityTiers corresponding to
// a set of replicas (one localityTiers per replica).
type replicasLocalityTiers struct {
	// replicas are sorted in ascending order of localityTiers.str (this is
	// a set, so the order doesn't semantically matter).
	replicas []localityTiers
}

var _ mapKey = replicasLocalityTiers{}

func makeReplicasLocalityTiers(replicas []localityTiers) replicasLocalityTiers {
	slices.SortFunc(replicas, func(a, b localityTiers) int {
		return cmp.Compare(a.str, b.str)
	})
	return replicasLocalityTiers{replicas: replicas}
}

// hash implements the mapKey interface, using the FNV-1a hash algorithm.
func (rlt replicasLocalityTiers) hash() uint64 {
	h := uint64(offset64)
	for i := range rlt.replicas {
		for _, code := range rlt.replicas[i].tiers {
			h ^= uint64(code)
			h *= prime64
		}
		// Separator between different replicas. This is equivalent to
		// encountering the 0 code, which is the empty string. We don't expect to
		// see an empty string in a locality.
		h *= prime64
	}
	return h
}

// isEqual implements the mapKey interface.
func (rlt replicasLocalityTiers) isEqual(b mapKey) bool {
	other := b.(replicasLocalityTiers)
	if len(rlt.replicas) != len(other.replicas) {
		return false
	}
	for i := range rlt.replicas {
		if len(rlt.replicas[i].tiers) != len(other.replicas[i].tiers) {
			return false
		}
		for j := range rlt.replicas[i].tiers {
			if rlt.replicas[i].tiers[j] != other.replicas[i].tiers[j] {
				return false
			}
		}
	}
	return true
}

func (rlt replicasLocalityTiers) clone() replicasLocalityTiers {
	var r replicasLocalityTiers
	r.replicas = append(r.replicas, rlt.replicas...)
	return r
}

var existingReplicaLocalitiesSlicePool = sync.Pool{
	New: func() interface{} {
		return &mapEntrySlice[*existingReplicaLocalities]{}
	},
}

type existingReplicaLocalitiesSlicePoolImpl struct{}

func (p existingReplicaLocalitiesSlicePoolImpl) newEntry() *mapEntrySlice[*existingReplicaLocalities] {
	return existingReplicaLocalitiesSlicePool.New().(*mapEntrySlice[*existingReplicaLocalities])
}

func (p existingReplicaLocalitiesSlicePoolImpl) releaseEntry(
	slice *mapEntrySlice[*existingReplicaLocalities],
) {
	existingReplicaLocalitiesSlicePool.Put(slice)
}

type existingReplicaLocalitiesAllocator struct{}

func (a existingReplicaLocalitiesAllocator) ensureNonNilMapEntry(
	entry *existingReplicaLocalities,
) *existingReplicaLocalities {
	if entry == nil {
		return &existingReplicaLocalities{}
	}
	return entry
}

// diversityScoringMemo provides support for efficiently scoring the diversity
// for a set of replicas, and for adding/removing replicas.
type diversityScoringMemo struct {
	replicasMap *clearableMemoMap[replicasLocalityTiers, *existingReplicaLocalities]
}

func newDiversityScoringMemo() *diversityScoringMemo {
	return &diversityScoringMemo{
		replicasMap: newClearableMapMemo[replicasLocalityTiers, *existingReplicaLocalities](
			existingReplicaLocalitiesAllocator{}, existingReplicaLocalitiesSlicePoolImpl{}),
	}
}

// getExistingReplicaLocalities returns the existingReplicaLocalities object
// for the given replicas. The parameter can be mutated by the callee.
func (dsm *diversityScoringMemo) getExistingReplicaLocalities(
	existingReplicas replicasLocalityTiers,
) *existingReplicaLocalities {
	erl, ok := dsm.replicasMap.get(existingReplicas)
	if ok {
		return erl
	}
	erl.replicasLocalityTiers = existingReplicas.clone()
	erl.scoreSums = map[string]float64{}
	return erl
}

// If this is a NON_VOTER being added, then existingReplicaLocalities must
// represent both VOTER and NON_VOTER replicas, and replicaToAdd must not be
// an existing VOTER. Note that for the case where replicaToAdd is an existing
// VOTER, there is no score change, so this method should not be called.
//
// If this is a VOTER being added, then existingReplicaLocalities must
// represent only VOTER replicas.
func (erl *existingReplicaLocalities) getScoreChangeForNewReplica(
	replicaToAdd localityTiers,
) float64 {
	return erl.getScoreSum(replicaToAdd)
}

// If this is a NON_VOTER being removed, then existingReplicaLocalities must
// represent both VOTER and NON_VOTER replicas.
//
// If this is a VOTER being removed, then existingReplicaLocalities must
// represent only VOTER replicas.
func (erl *existingReplicaLocalities) getScoreChangeForReplicaRemoval(
	replicaToRemove localityTiers,
) float64 {
	// NB: we don't need to compensate for counting the pair-wise score of
	// replicaToRemove with itself, because that score is 0.
	return -erl.getScoreSum(replicaToRemove)
}

// If this is a VOTER being added and removed, replicaToRemove is of course a
// VOTER, and it doesn't matter if the one being added is already a NON_VOTER
// (since the existingReplicaLocalities represents only VOTERs).
//
// If this a NON_VOTER being added and removed, existingReplicaLocalities represents
// all VOTERs and NON_VOTERs. The replicaToAdd must not be an
// existing VOTER. For the case where replicaToAdd is an existing VOTER, there
// is no score change for the addition (to the full set of replicas), and:
//
//   - If replicaToRemove is not becoming a VOTER,
//     getScoreChangeForReplicaRemoval(replicaToRemove) should be called.
//
//   - If replicaToRemove is becoming a VOTER, the score change is 0 for the
//     full set of replicas.
//
// In the preceding cases where the VOTER set is also changing, of course the
// score change to the existingReplicaLocalities that represents only the
// VOTERs must also be considered.
func (erl *existingReplicaLocalities) getScoreChangeForRebalance(
	replicaToRemove localityTiers, replicaToAdd localityTiers,
) float64 {
	score := erl.getScoreSum(replicaToAdd) - erl.getScoreSum(replicaToRemove)
	// We have included the pair-wise diversity of (replicaToRemove,
	// replicaToAdd) above, but since replicaToRemove is being removed, we need
	// to subtract that pair.
	score -= replicaToRemove.diversityScore(replicaToAdd)
	return score
}

func (erl *existingReplicaLocalities) getScoreSum(replica localityTiers) float64 {
	score, ok := erl.scoreSums[replica.str]
	if !ok {
		for i := range erl.replicas {
			score += erl.replicas[i].diversityScore(replica)
		}
		erl.scoreSums[replica.str] = score
	}
	return score
}

func diversityScoresAlmostEqual(score1, score2 float64) bool {
	return math.Abs(score1-score2) < epsilon
}

// This is a somehow arbitrary chosen upper bound on the relative error to be
// used when comparing doubles for equality. The assumption that comes with it
// is that any sequence of operations on doubles won't produce a result with
// accuracy that is worse than this relative error. There is no guarantee
// however that this will be the case. A programmer writing code using
// floating point numbers will still need to be aware of the effect of the
// operations on the results and the possible loss of accuracy.
// More info https://en.wikipedia.org/wiki/Machine_epsilon
// https://en.wikipedia.org/wiki/Floating-point_arithmetic
const epsilon = 1e-10

// Avoid unused lint errors.

var _ = (&existingReplicaLocalities{}).clear
var _ = replicasLocalityTiers{}.hash
var _ = replicasLocalityTiers{}.isEqual
var _ = existingReplicaLocalitiesSlicePoolImpl{}.newEntry
var _ = existingReplicaLocalitiesSlicePoolImpl{}.releaseEntry
var _ = existingReplicaLocalitiesAllocator{}.ensureNonNilMapEntry
var _ = (&diversityScoringMemo{}).getExistingReplicaLocalities
var _ = (&existingReplicaLocalities{}).getScoreChangeForNewReplica
var _ = (&existingReplicaLocalities{}).getScoreChangeForReplicaRemoval
var _ = (&existingReplicaLocalities{}).getScoreChangeForRebalance
var _ = candidateInfo{}.diversityScore
