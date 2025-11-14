// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype/mmaload"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// Misc helper classes for working with range, store and node load.

// A resource can have a capacity, which is also expressed using LoadValue.
// There are some special case capacity values, enumerated here.
const (
	// UnknownCapacity is currenly only used for WriteBandwidth.
	UnknownCapacity mmaload.LoadValue = math.MaxInt64
)

type RangeLoad struct {
	Load mmaload.LoadVector
	// Nanos per second. RaftCPU <= Load[cpu]. Handling this as a special case,
	// rather than trying to (over) generalize, since currently this is the only
	// resource broken down into two components.
	//
	// Load[cpu]-RaftCPU is work being done on this store for evaluating
	// proposals and can vary across replicas. Pessimistically, one can assume
	// that if this replica is the leaseholder and we move the lease to a
	// different existing replica, it will see an addition of Load[cpu]-RaftCPU.
	RaftCPU mmaload.LoadValue
}

// storeLoad is the load information for a store. Roughly, this is the
// information we need each store to provide us periodically, i.e.,
// StoreLoadMsg is the input used to compute this.
type storeLoad struct {
	// Aggregate store load. In general, we don't require this to be a sum of
	// the range loads (since a sharded allocator may only have information
	// about a subset of ranges).
	reportedLoad mmaload.LoadVector

	// Capacity information for this store.
	//
	// Only capacity[WriteBandwidth] is UnknownCapacity. The assumption here is
	// that mean based rebalancing of WriteBandwidth should be sufficient to
	// avoid hotspots, and we don't need to synthesize a capacity. This will
	// need to change when we have heterogeneous stores in terms of capability
	// (disk bandwidth and IOPS; max concurrent compactions).
	//
	// TODO(sumeer): add diskBandwidth, since we will become more aware of
	// provisioned disk bandwidth in the near future.
	capacity mmaload.LoadVector

	reportedSecondaryLoad mmaload.SecondaryLoadVector
}

// NodeLoad is the load information for a node.
type NodeLoad struct {
	NodeID roachpb.NodeID
	// ReportedCPU and CapacityCPU are simply the sum of what we get for all
	// stores on this node.
	ReportedCPU mmaload.LoadValue
	CapacityCPU mmaload.LoadValue
}

// The mean store load for a set of stores.
type meanStoreLoad struct {
	load     mmaload.LoadVector
	capacity mmaload.LoadVector
	// Util is 0 for CPURate, WriteBandwidth. Non-zero for ByteSize.
	util [mmaload.NumLoadDimensions]float64

	secondaryLoad mmaload.SecondaryLoadVector
}

// The mean node load for a set of NodeLoad.
type meanNodeLoad struct {
	loadCPU     mmaload.LoadValue
	capacityCPU mmaload.LoadValue
	utilCPU     float64
}

// The allocator often needs mean load information for a set of stores. This
// set is implied by a constraintsDisj. We also want to know the set of stores
// that satisfy that contraintsDisj. meansForStoreSet encapsulates all of this
// information. It caches the:
// - set of stores.
// - the means for these stores and their corresponding nodes.
//
// Additionally, meansForStoreSet provides the storage to cache the
// storeLoadSummary for some of the stores in this set. This cache is lazily
// populated and is implicitly invalidated if the loadSeqNum of the
// storeLoadSummary no longer matches that of storeState.loadSeqNum.
type meansForStoreSet struct {
	constraintsDisj
	meansLoad
	stores         storeSet
	storeSummaries map[roachpb.StoreID]storeLoadSummary
}

type meansLoad struct {
	storeLoad meanStoreLoad
	nodeLoad  meanNodeLoad
}

var _ mapEntry = &meansForStoreSet{}

func (mss *meansForStoreSet) clear() {
	clear(mss.storeSummaries)
	*mss = meansForStoreSet{
		stores:         mss.stores[:0],
		storeSummaries: mss.storeSummaries,
	}
}

// Helper function for meansMemo.getStoreLoadSummary.
func (mss *meansForStoreSet) tryGetStoreLoadSummary(
	storeID roachpb.StoreID, curLoadSeqNum uint64,
) (storeLoadSummary, bool) {
	l, ok := mss.storeSummaries[storeID]
	if ok {
		ok = l.loadSeqNum >= curLoadSeqNum
	}
	return l, ok
}

// Helper function for meansMemo.getStoreLoadSummary.
func (mss *meansForStoreSet) putStoreLoadSummary(storeID roachpb.StoreID, l storeLoadSummary) {
	if mss.storeSummaries == nil {
		mss.storeSummaries = map[roachpb.StoreID]storeLoadSummary{}
	}
	mss.storeSummaries[storeID] = l
}

// meansMemo should be cleared before every allocator pass. Each allocator
// pass will encounter identical constraints for many ranges under
// consideration, which implies an identical set of stores that satisfy those
// constraints. The set of stores and mean load and utilization for each such
// set is needed to make rebalancing decisions. We compute and store this
// information to avoid computing them repeatedly within an allocator pass.
// The assumption made here is that any change in the means during an
// allocator pass is irrelevant.
type meansMemo struct {
	loadInfoProvider  loadInfoProvider
	constraintMatcher *constraintMatcher
	meansMap          *clearableMemoMap[constraintsDisj, *meansForStoreSet]

	scratchNodes  map[roachpb.NodeID]*NodeLoad
	scratchStores map[roachpb.StoreID]struct{}
}

var meansForStoreSetSlicePool = sync.Pool{
	New: func() interface{} {
		return &mapEntrySlice[*meansForStoreSet]{}
	},
}

// meansForStoreSetSlicePoolImp implements
// mapEntrySlicePool[*meansForStoreSet]. It is just a wrapper around the
// meansForStoreSetSlicePool sync.Pool.
type meansForStoreSetSlicePoolImpl struct{}

func (m meansForStoreSetSlicePoolImpl) newEntry() *mapEntrySlice[*meansForStoreSet] {
	return meansForStoreSetSlicePool.New().(*mapEntrySlice[*meansForStoreSet])
}

func (m meansForStoreSetSlicePoolImpl) releaseEntry(slice *mapEntrySlice[*meansForStoreSet]) {
	meansForStoreSetSlicePool.Put(slice)
}

// meansForStoreSetAllocator implements mapEntryAllocator[*meansForStoreSet].
type meansForStoreSetAllocator struct{}

func (m meansForStoreSetAllocator) ensureNonNilMapEntry(entry *meansForStoreSet) *meansForStoreSet {
	if entry == nil {
		entry = &meansForStoreSet{}
	}
	return entry
}

func newMeansMemo(
	loadInfoProvider loadInfoProvider, constraintMatcher *constraintMatcher,
) *meansMemo {
	return &meansMemo{
		loadInfoProvider:  loadInfoProvider,
		constraintMatcher: constraintMatcher,
		meansMap: newClearableMapMemo[constraintsDisj, *meansForStoreSet](
			meansForStoreSetAllocator{}, meansForStoreSetSlicePoolImpl{}),
		scratchNodes:  map[roachpb.NodeID]*NodeLoad{},
		scratchStores: map[roachpb.StoreID]struct{}{},
	}
}

func (mm *meansMemo) clear() {
	mm.meansMap.clear()
}

type loadInfoProvider interface {
	getStoreReportedLoad(roachpb.StoreID) (roachpb.NodeID, *storeLoad)
	getNodeReportedLoad(roachpb.NodeID) *NodeLoad
	computeLoadSummary(context.Context, roachpb.StoreID, *meanStoreLoad, *meanNodeLoad) storeLoadSummary
}

// getMeans returns the means for an expression.
func (mm *meansMemo) getMeans(expr constraintsDisj) *meansForStoreSet {
	means, ok := mm.meansMap.get(expr)
	if ok {
		return means
	}
	means.constraintsDisj = expr
	mm.constraintMatcher.constrainStoresForExpr(expr, &means.stores)
	means.meansLoad = computeMeansForStoreSet(mm.loadInfoProvider, means.stores, mm.scratchNodes, mm.scratchStores)
	return means
}

// getStoreLoadSummary returns the load summary for a store in the context of
// the given set (encoded in means). It attempts to utilize a cached value if
// curLoadSeqNum permits.
func (mm *meansMemo) getStoreLoadSummary(
	ctx context.Context, means *meansForStoreSet, storeID roachpb.StoreID, curLoadSeqNum uint64,
) storeLoadSummary {
	summary, ok := means.tryGetStoreLoadSummary(storeID, curLoadSeqNum)
	if ok {
		return summary
	}
	summary = mm.loadInfoProvider.computeLoadSummary(ctx, storeID, &means.storeLoad, &means.nodeLoad)
	means.putStoreLoadSummary(storeID, summary)
	return summary
}

// computeMeansForStoreSet computes the mean for the stores in means.stores.
// It does not do any filtering e.g. the stores can include fdDead stores. It
// is up to the caller to adjust means.stores if it wants to do filtering.
//
// stores may contain duplicate storeIDs, in which case computeMeansForStoreSet
// should deduplicate processing of the stores. stores should be immutable.
//
// TODO: fix callers to exclude stores based on node failure detection, from
// the mean.
func computeMeansForStoreSet(
	loadProvider loadInfoProvider,
	stores []roachpb.StoreID,
	scratchNodes map[roachpb.NodeID]*NodeLoad,
	scratchStores map[roachpb.StoreID]struct{},
) (means meansLoad) {
	if len(stores) == 0 {
		panic(fmt.Sprintf("no stores for meansForStoreSet: %v", stores))
	}
	clear(scratchNodes)
	clear(scratchStores)
	n := 0
	for _, storeID := range stores {
		// NB: using reported load and not adjusted load, so cannot be
		// negative.
		nodeID, sload := loadProvider.getStoreReportedLoad(storeID)
		if _, ok := scratchStores[storeID]; ok {
			continue
		}
		n++
		scratchStores[storeID] = struct{}{}
		for j := range sload.reportedLoad {
			means.storeLoad.load[j] += sload.reportedLoad[j]
			if sload.capacity[j] == UnknownCapacity {
				means.storeLoad.capacity[j] = UnknownCapacity
			} else if means.storeLoad.capacity[j] != UnknownCapacity {
				means.storeLoad.capacity[j] += sload.capacity[j]
			}
		}
		for j := range sload.reportedSecondaryLoad {
			means.storeLoad.secondaryLoad[j] += sload.reportedSecondaryLoad[j]
		}
		nLoad := scratchNodes[nodeID]
		if nLoad == nil {
			// NB: using reported load and not adjusted load, so cannot be
			// negative.
			scratchNodes[nodeID] = loadProvider.getNodeReportedLoad(nodeID)
		}
	}
	for i := range means.storeLoad.load {
		if means.storeLoad.capacity[i] != UnknownCapacity {
			means.storeLoad.util[i] =
				float64(means.storeLoad.load[i]) / float64(means.storeLoad.capacity[i])
			means.storeLoad.capacity[i] /= mmaload.LoadValue(n)
		} else {
			means.storeLoad.util[i] = 0
		}
		means.storeLoad.load[i] /= mmaload.LoadValue(n)
	}
	for i := range means.storeLoad.secondaryLoad {
		means.storeLoad.secondaryLoad[i] /= mmaload.LoadValue(n)
	}

	n = len(scratchNodes)
	for _, nl := range scratchNodes {
		means.nodeLoad.loadCPU += nl.ReportedCPU
		means.nodeLoad.capacityCPU += nl.CapacityCPU
	}
	means.nodeLoad.utilCPU =
		float64(means.nodeLoad.loadCPU) / float64(means.nodeLoad.capacityCPU)
	means.nodeLoad.loadCPU /= mmaload.LoadValue(n)
	means.nodeLoad.capacityCPU /= mmaload.LoadValue(n)
	return means
}

// loadSummary aggregates across all load dimensions for a store, or a node.
// This could be a score instead of an enum, but eventually we want to decide
// what scores are roughly equal when deciding on rebalancing priority, and to
// decide how to order the stores we will try to rebalance to. So we simply use
// an enum.
//
// We use a coarse enum since it allows for more randomization when making a
// choice, by making bigger equivalence classes.
type loadSummary uint8

const (
	loadLow loadSummary = iota
	// loadNormal represents that the load is within normal bounds.
	loadNormal
	// loadNoChange represents that no load should be added or removed from this
	// store. This is used when (a) there are enough pending changes at this
	// store that we want to let them finish, (b) we don't want to add load to
	// this store because it is enough above the mean.
	loadNoChange
	// overloadSlow is a state where the store is overloaded, but not so much
	// that it is urgent to shed load.
	overloadSlow
	// overloadUrgent is a state where the store is overloaded, and it is urgent
	// to shed load.
	overloadUrgent
)

func (ls loadSummary) String() string {
	return redact.StringWithoutMarkers(ls)
}

func (ls loadSummary) SafeFormat(w redact.SafePrinter, _ rune) {
	switch ls {
	case loadLow:
		w.Print("loadLow")
	case loadNormal:
		w.Print("loadNormal")
	case loadNoChange:
		w.Print("loadNoChange")
	case overloadSlow:
		w.Print("overloadSlow")
	case overloadUrgent:
		w.Print("overloadUrgent")
	default:
		panic("unknown loadSummary")
	}
}

// Computes the loadSummary for a particular load dimension.
//
// NB: load can be negative since it may be adjusted load.
func loadSummaryForDimension(
	ctx context.Context,
	storeID roachpb.StoreID,
	nodeID roachpb.NodeID,
	dim mmaload.LoadDimension,
	load mmaload.LoadValue,
	capacity mmaload.LoadValue,
	meanLoad mmaload.LoadValue,
	meanUtil float64,
) (summary loadSummary) {
	summ := loadLow
	if dim == mmaload.WriteBandwidth && capacity == UnknownCapacity {
		// Ignore smaller than 1MiB differences in write bandwidth. This 1MiB
		// value is somewhat arbitrary, but is based on EBS gp3 having a default
		// provisioned bandwidth of 125 MiB/s, and assuming that a write amp of
		// ~20, will inflate 1MiB to ~20 MiB/s.
		const minWriteBandwidthGranularity = 128 << 10 // 128 KiB
		load /= minWriteBandwidthGranularity
		meanLoad /= minWriteBandwidthGranularity
	}
	// Heuristics (and very much subject to revision): There are two uses for
	// this loadSummary: to find source stores to shed load and to decide
	// whether the added load on a target store is acceptable (without driving
	// it to overload). This latter use case may be better served by a distance
	// measure since we don't want to get too close to overload since we could
	// overshoot (an alternative to distance would be to slightly over-estimate
	// the load addition due to a range move, and then ask for the load summary
	// -- this is what the caller implements).
	//
	// The load summarization should be even more specialized for each load
	// dimension and secondary load dimension e.g. we would want to do a
	// different summarization for cpu and ByteSize since the consequence of
	// running out-of-disk is much more severe.
	//
	// The capacity may be UnknownCapacity. Even if we have a known capacity, we
	// currently consider how far we are from the mean. But the mean isn't very
	// useful when there are heterogeneous nodes/stores, so this computation
	// will need to be revisited.
	fractionAbove := float64(load)/float64(meanLoad) - 1.0
	var fractionUsed float64
	if capacity != UnknownCapacity {
		fractionUsed = float64(load) / float64(capacity)
	}

	summaryUpperBound := overloadUrgent
	// Be less aggressive about the ByteSize dimension when the fractionUsed is
	// low. Rebalancing along too many dimensions results in more thrashing due
	// to concurrent rebalancing actions by many leaseholders.
	if dim == mmaload.ByteSize && capacity != UnknownCapacity && fractionUsed < 0.5 {
		summaryUpperBound = loadNormal
	}
	// Don't bother equalizing CPURate by shedding, if the utilization is < 5%.
	// The choice of 5% here is arbitrary.
	if dim == mmaload.CPURate && capacity != UnknownCapacity && fractionUsed < 0.05 {
		summaryUpperBound = loadNormal
	}
	// TODO(sumeer): consider adding a summaryUpperBound for small
	// WriteBandwidth values too.
	const (
		meanFractionSlow     = 0.1
		meanFractionLow      = -0.1
		meanFractionNoChange = 0.05
	)
	if fractionAbove > meanFractionSlow {
		summ = overloadSlow
	} else if fractionAbove < meanFractionLow {
		summ = loadLow
	} else if fractionAbove >= meanFractionNoChange {
		summ = loadNoChange
	} else {
		summ = loadNormal
	}
	if capacity != UnknownCapacity && meanUtil*1.1 < fractionUsed {
		// Further tune the summary based on utilization.
		//
		// Currently, we only tune towards overload based on utilization, and not
		// towards underload. The idea is that the former allows us to identify
		// overload due to heterogeneity, while we primarily still want to focus
		// on balancing towards the mean usage.
		if fractionUsed > 0.9 {
			return min(summaryUpperBound, overloadUrgent)
		}
		// INVARIANT: fractionUsed <= 0.9
		if fractionUsed > 0.75 {
			if meanUtil*1.5 < fractionUsed {
				return min(summaryUpperBound, overloadUrgent)
			}
			return min(summaryUpperBound, overloadSlow)
		}
		// INVARIANT: fractionUsed <= 0.75
		if meanUtil*1.75 < fractionUsed {
			return min(summaryUpperBound, overloadSlow)
		}
		return min(summaryUpperBound, max(summ, loadNoChange))
	}
	return min(summaryUpperBound, summ)
}

func highDiskSpaceUtilization(load mmaload.LoadValue, capacity mmaload.LoadValue) bool {
	if capacity == UnknownCapacity {
		log.KvDistribution.Errorf(context.Background(), "disk capacity is unknown")
		return false
	}
	fractionUsed := float64(load) / float64(capacity)
	return fractionUsed > 0.9
}

const loadMultiplierForAddition = 1.1

func loadToAdd(l mmaload.LoadValue) mmaload.LoadValue {
	return mmaload.LoadValue(float64(l) * loadMultiplierForAddition)
}

func loadVectorToAdd(lv mmaload.LoadVector) mmaload.LoadVector {
	var result mmaload.LoadVector
	for i := range lv {
		result[i] = loadToAdd(lv[i])
	}
	return result
}

// Avoid unused lint errors.

var _ = meansForStoreSetSlicePoolImpl{}.newEntry
var _ = meansForStoreSetSlicePoolImpl{}.releaseEntry
var _ = meansForStoreSetAllocator{}.ensureNonNilMapEntry
var _ = (&meansMemo{}).clear
var _ = RangeLoad{}.Load
var _ = RangeLoad{}.RaftCPU
var _ = storeLoad{}.reportedLoad
var _ = storeLoad{}.capacity
var _ = storeLoad{}.reportedSecondaryLoad
var _ = NodeLoad{}.NodeID
var _ = NodeLoad{}.ReportedCPU
var _ = NodeLoad{}.CapacityCPU
var _ = mmaload.CPURate
var _ = mmaload.WriteBandwidth
var _ = mmaload.ByteSize
