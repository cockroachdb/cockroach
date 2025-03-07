// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mma

import (
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// Misc helper classes for working with range, store and node load.

// LoadDimension is an enum of load dimensions corresponding to "real"
// resources. Such resources can sometimes have a capacity. It is generally
// important to rebalance based on these. The code in this package should be
// structured that adding additional resource dimensions is easy.
type LoadDimension uint8

const (
	// CPURate is in nanos per second.
	CPURate LoadDimension = iota
	// WriteBandwidth is the writes in bytes/s.
	WriteBandwidth
	// ByteSize is the size in bytes.
	ByteSize
	NumLoadDimensions
)

// LoadValue is the load on a resource.
type LoadValue int64

// LoadVector represents a vector of loads, with one element for each resource
// dimension.
type LoadVector [NumLoadDimensions]LoadValue

func (lv LoadVector) String() string {
	return redact.StringWithoutMarkers(lv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (lv LoadVector) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[%d,%d,%d]", lv[CPURate], lv[WriteBandwidth], lv[ByteSize])
}

func (lv *LoadVector) add(other LoadVector) {
	for i := range other {
		(*lv)[i] += other[i]
	}
}

func (lv *LoadVector) subtract(other LoadVector) {
	for i := range other {
		(*lv)[i] -= other[i]
	}
}

// A resource can have a capacity, which is also expressed using LoadValue.
// There are some special case capacity values, enumerated here.
const (
	// UnknownCapacity is currenly only used for WriteBandwidth.
	UnknownCapacity LoadValue = math.MaxInt64
)

// Secondary load dimensions should be considered after we are done
// rebalancing using loadDimensions, since these don't represent "real"
// resources. Currently, only lease count is considered here. Lease
// rebalancing will see if there is scope to move some leases between stores
// that do not have any pending changes and are not overloaded (and will not
// get overloaded by the movement). Additionally, real resource rebalancing
// will prefer target nodes (among those with similar real load) that have
// lower lease counts.

// TODO(sumeer): do we need to move replicas too, in order to do lease
// rebalancing, or can we assume that constraints and diversity scores have
// sufficiently achieved enough that we only need to move leases between the
// existing voter replicas. The example in
// https://github.com/cockroachdb/cockroach/issues/93258 suggests we only need
// the latter -- confirmed by kvoli. Also look at
// https://github.com/cockroachdb/cockroach/pull/98893 regarding load means
// and lease means.
type SecondaryLoadDimension uint8

const (
	LeaseCount SecondaryLoadDimension = iota
	NumSecondaryLoadDimensions
)

type SecondaryLoadVector [NumSecondaryLoadDimensions]LoadValue

func (lv *SecondaryLoadVector) add(other SecondaryLoadVector) {
	for i := range other {
		(*lv)[i] += other[i]
	}
}

func (lv *SecondaryLoadVector) subtract(other SecondaryLoadVector) {
	for i := range other {
		(*lv)[i] -= other[i]
	}
}

type RangeLoad struct {
	Load LoadVector
	// Nanos per second. RaftCPU <= Load[cpu]. Handling this as a special case,
	// rather than trying to (over) generalize, since currently this is the only
	// resource broken down into two components.
	//
	// Load[cpu]-RaftCPU is work being done on this store for evaluating
	// proposals and can vary across replicas. Pessimistically, one can assume
	// that if this replica is the leaseholder and we move the lease to a
	// different existing replica, it will see an addition of Load[cpu]-RaftCPU.
	RaftCPU LoadValue
}

// storeLoad is the load information for a store. Roughly, this is the
// information we need each store to provide us periodically, i.e.,
// StoreLoadMsg is the input used to compute this.
type storeLoad struct {
	roachpb.StoreID
	roachpb.StoreDescriptor
	roachpb.NodeID

	// Aggregate store load. In general, we don't require this to be a sum of
	// the range loads (since a sharded allocator may only have information
	// about a subset of ranges).
	reportedLoad LoadVector

	// Capacity information for this store.
	//
	// capacity[WriteBandwidth] is UnknownCapacity.
	//
	// TODO(sumeer): add diskBandwidth, since we will become more aware of
	// provisioned disk bandwidth in the near future.
	//
	// TODO(sumeer): should the LSM getting overloaded result in some capacity
	// signal? We had earlier considered having the store set the capacity to a
	// synthesized value that indicates high utilization, in order to shed some
	// load. However, the code below assumes homogeneity across stores/nodes in
	// terms of whether they use a real capacity or not for a LoadDimension.
	//
	// capacity[ByteSize] is the actual capacity.
	capacity LoadVector

	reportedSecondaryLoad SecondaryLoadVector
}

// NodeLoad is the load information for a node.
type NodeLoad struct {
	NodeID roachpb.NodeID
	// ReportedCPU and CapacityCPU are simply the sum of what we get for all
	// stores on this node.
	ReportedCPU LoadValue
	CapacityCPU LoadValue
}

// The mean store load for a set of stores.
type meanStoreLoad struct {
	load     LoadVector
	capacity LoadVector
	// Util is 0 for CPURate, WriteBandwidth. Non-zero for ByteSize.
	util [NumLoadDimensions]float64

	secondaryLoad SecondaryLoadVector
}

// The mean node load for a set of NodeLoad.
type meanNodeLoad struct {
	loadCPU     LoadValue
	capacityCPU LoadValue
	utilCPU     float64
}

type storeLoadSummary struct {
	sls                      loadSummary
	nls                      loadSummary
	storeCPUSummary          loadSummary
	highDiskSpaceUtilization bool
	fd                       failureDetectionSummary
	maxFractionPending       float64

	loadSeqNum uint64
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
	stores    storeIDPostingList
	storeLoad meanStoreLoad
	nodeLoad  meanNodeLoad

	storeSummaries map[roachpb.StoreID]storeLoadSummary
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

	scratchNodes map[roachpb.NodeID]*NodeLoad
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
		scratchNodes: map[roachpb.NodeID]*NodeLoad{},
	}
}

func (mm *meansMemo) clear() {
	mm.meansMap.clear()
}

type loadInfoProvider interface {
	getStoreReportedLoad(roachpb.StoreID) *storeLoad
	getNodeReportedLoad(roachpb.NodeID) *NodeLoad
	computeLoadSummary(roachpb.StoreID, *meanStoreLoad, *meanNodeLoad) storeLoadSummary
}

// getMeans returns the means for an expression.
func (mm *meansMemo) getMeans(expr constraintsDisj) *meansForStoreSet {
	means, ok := mm.meansMap.get(expr)
	if ok {
		return means
	}
	means.constraintsDisj = expr
	mm.constraintMatcher.constrainStoresForExpr(expr, &means.stores)
	computeMeansForStoreSet(means.stores, mm.loadInfoProvider, means, mm.scratchNodes)
	return means
}

// getStoreLoadSummary returns the load summary for a store in the context of
// the given set (encoded in means). It attempts to utilize a cached value if
// curLoadSeqNum permits.
func (mm *meansMemo) getStoreLoadSummary(
	means *meansForStoreSet, storeID roachpb.StoreID, curLoadSeqNum uint64,
) storeLoadSummary {
	summary, ok := means.tryGetStoreLoadSummary(storeID, curLoadSeqNum)
	if ok {
		return summary
	}
	summary = mm.loadInfoProvider.computeLoadSummary(storeID, &means.storeLoad, &means.nodeLoad)
	means.putStoreLoadSummary(storeID, summary)
	return summary
}

// TODO: Exclude stores which are storeMembershipRemoving,
// storeMembershipRemoved, fdDrain and fdDead. As these are never eligible
// candidate stores and should therefore not be considered in the means.
func computeMeansForStoreSet(
	stores storeIDPostingList,
	loadProvider loadInfoProvider,
	means *meansForStoreSet,
	scratchNodes map[roachpb.NodeID]*NodeLoad,
) {
	n := len(means.stores)
	clear(scratchNodes)
	for _, storeID := range means.stores {
		sload := loadProvider.getStoreReportedLoad(storeID)
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
		nodeID := sload.NodeID
		nLoad := scratchNodes[nodeID]
		if nLoad == nil {
			scratchNodes[sload.NodeID] = loadProvider.getNodeReportedLoad(nodeID)
		}
	}
	for i := range means.storeLoad.load {
		if means.storeLoad.capacity[i] != UnknownCapacity {
			means.storeLoad.util[i] =
				float64(means.storeLoad.load[i]) / float64(means.storeLoad.capacity[i])
			means.storeLoad.capacity[i] /= LoadValue(n)
		} else {
			means.storeLoad.util[i] = 0
		}
		means.storeLoad.load[i] /= LoadValue(n)
	}
	for i := range means.storeLoad.secondaryLoad {
		means.storeLoad.secondaryLoad[i] /= LoadValue(n)
	}

	n = len(scratchNodes)
	for _, nl := range scratchNodes {
		means.nodeLoad.loadCPU += nl.ReportedCPU
		means.nodeLoad.capacityCPU += nl.CapacityCPU
	}
	means.nodeLoad.utilCPU =
		float64(means.nodeLoad.loadCPU) / float64(means.nodeLoad.capacityCPU)
	means.nodeLoad.loadCPU /= LoadValue(n)
	means.nodeLoad.capacityCPU /= LoadValue(n)
}

// loadSummary aggregates across all load dimensions for a store, or a node.
// This could be a score instead of an enum, but eventually we want to decide
// what scores are roughly equal when deciding on rebalancing priority, and to
// decide how to order the stores we will try to rebalance to. So we simply use
// an enum.
type loadSummary uint8

const (
	loadLow loadSummary = iota
	// loadNormal represents that the load is within normal bounds.
	loadNormal
	// loadNoChange represents that no load should be added or removed from this
	// store. This is typically only used when there are enough pending changes
	// at this store that we want to let them finish.
	loadNoChange
	// overloadSlow is a state where the store is overloaded, but not so much
	// that it is urgent to shed load.
	overloadSlow
	// overloadUrgent is a state where the store is overloaded and it is urgent
	// to shed load.
	overloadUrgent
)

// Computes the loadSummary for a particular load dimension.
func loadSummaryForDimension(
	load LoadValue, capacity LoadValue, meanLoad LoadValue, meanUtil float64,
) loadSummary {
	loadSummary := loadLow
	// Heuristics: this is all very rough and subject to revision. There are two
	// uses for this loadSummary: to find source stores to shed load and to
	// decide whether the added load on a target store is acceptable (without
	// driving it to overload). This latter use case may be better served by a
	// distance measure since we don't want to get too close to overload since
	// we could overshoot (an alternative to distance would be to slightly
	// over-estimate the load addition due to a range move, and then ask for the
	// load summary).
	//
	// The load summarization should be specialized for each load dimension and
	// secondary load dimension e.g. we want to do a different summarization for
	// cpu and ByteSize since the consequence of running out-of-disk is much
	// more severe.
	//
	// The capacity may be UnknownCapacity. Even if we have a known capacity, we
	// consider how far we are from the mean. The mean isn't very useful when
	// there are heterogeneous nodes/stores.
	fractionAbove := float64(load)/float64(meanLoad) - 1.0
	if fractionAbove > 0.2 {
		loadSummary = overloadSlow
	} else if fractionAbove < -0.2 {
		loadSummary = loadLow
	} else {
		loadSummary = loadNormal
	}
	if capacity != UnknownCapacity {
		// Further tune the summary based on utilization.
		fractionUsed := float64(load) / float64(capacity)
		if fractionUsed > 0.9 {
			if meanUtil < fractionUsed {
				return overloadUrgent
			}
			return overloadSlow
		}
		// INVARIANT: fractionUsed <= 0.9
		if fractionUsed > 0.75 {
			if meanUtil < fractionUsed {
				return overloadSlow
			} else {
				return loadSummary
			}
		}
		// INVARIANT: fractionUsed <= 0.75
		if fractionUsed < 0.5 && fractionUsed < meanUtil {
			return loadLow
		}
	}
	return loadSummary
}

func highDiskSpaceUtilization(load LoadValue, capacity LoadValue) bool {
	if capacity == UnknownCapacity {
		// TODO(sumeer): log an error.
		return false
	}
	fractionUsed := float64(load) / float64(capacity)
	return fractionUsed > 0.9
}

// Avoid unused lint errors.

var _ = meansForStoreSetSlicePoolImpl{}.newEntry
var _ = meansForStoreSetSlicePoolImpl{}.releaseEntry
var _ = meansForStoreSetAllocator{}.ensureNonNilMapEntry
var _ = (&meansMemo{}).clear
var _ = RangeLoad{}.Load
var _ = RangeLoad{}.RaftCPU
var _ = storeLoad{}.StoreID
var _ = storeLoad{}.StoreDescriptor
var _ = storeLoad{}.NodeID
var _ = storeLoad{}.reportedLoad
var _ = storeLoad{}.capacity
var _ = storeLoad{}.reportedSecondaryLoad
var _ = NodeLoad{}.NodeID
var _ = NodeLoad{}.ReportedCPU
var _ = NodeLoad{}.CapacityCPU
var _ = CPURate
var _ = WriteBandwidth
var _ = ByteSize
