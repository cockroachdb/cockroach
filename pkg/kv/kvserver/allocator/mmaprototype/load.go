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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

// Misc helper classes for working with range, store and node load.

// LoadDimension is an enum of load dimensions corresponding to "real"
// resources. Such resources can sometimes have a capacity. It is generally
// important to rebalance based on these. The code in this package should be
// structured that adding additional resource dimensions is easy.
type LoadDimension uint8

// NumLoadDimensions must be exactly 3. Update SafeFormat methods for
// LoadVector and AmpVector if a dimension is added or removed.
var _ [NumLoadDimensions - 3]struct{}
var _ [3 - NumLoadDimensions]struct{}

const (
	// CPURate is in nanos per second.
	CPURate LoadDimension = iota
	// WriteBandwidth is the writes in bytes/s.
	WriteBandwidth
	// ByteSize is the size in bytes.
	ByteSize
	NumLoadDimensions
)

func (dim LoadDimension) SafeFormat(w redact.SafePrinter, _ rune) {
	switch dim {
	case CPURate:
		w.SafeString("CPURate")
	case WriteBandwidth:
		w.SafeString("WriteBandwidth")
	case ByteSize:
		w.SafeString("ByteSize")
	default:
		panic("unknown LoadDimension")
	}
}

func (dim LoadDimension) String() string {
	return redact.StringWithoutMarkers(dim)
}

// LoadValue is the load on a resource.
type LoadValue int64

func (LoadValue) SafeValue() {}

// LoadVector represents a vector of loads, with one element for each resource
// dimension.
type LoadVector [NumLoadDimensions]LoadValue

func (lv LoadVector) String() string {
	return redact.StringWithoutMarkers(lv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (lv LoadVector) SafeFormat(w redact.SafePrinter, _ rune) {
	formatVal := func(dim LoadDimension) redact.SafeString {
		val := lv[dim]
		if val == UnknownCapacity {
			return "unknown"
		}
		switch dim {
		case CPURate:
			cpuDuration := time.Duration(val)
			if cpuDuration < time.Microsecond {
				// humanizeutil.Duration doesn't handle sub-microsecond durations
				return redact.SafeString(cpuDuration.String())
			}
			return humanizeutil.Duration(cpuDuration)
		case WriteBandwidth, ByteSize:
			isNegative := false
			if val < 0 {
				val = -val
				isNegative = true
			}
			bytesStr := humanize.Bytes(uint64(val))
			if isNegative {
				return redact.SafeString("-" + bytesStr)
			}
			return redact.SafeString(bytesStr)
		default:
			panic(fmt.Sprintf("unknown LoadDimension: %d", dim))
		}
	}
	w.Printf(
		"[cpu:%s/s, write-bandwidth:%s/s, byte-size:%s]",
		formatVal(CPURate),
		formatVal(WriteBandwidth),
		formatVal(ByteSize),
	)
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

// Amp is a per-dimension amplification factor that converts logical per-range
// loads into physical units.
type Amp float64

func (af Amp) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%.2f", redact.SafeFloat(float64(af)))
}

func (af Amp) String() string {
	return redact.StringWithoutMarkers(af)
}

// AmpVector holds per-dimension amplification factors, one for each
// LoadDimension.
type AmpVector [NumLoadDimensions]Amp

// IdentityAmpVector returns an AmpVector where all dimensions have an
// amplification factor of 1.0 (no conversion).
func IdentityAmpVector() AmpVector {
	var av AmpVector
	for i := range av {
		av[i] = 1.0
	}
	return av
}

func (av AmpVector) String() string {
	return redact.StringWithoutMarkers(av)
}

func (av AmpVector) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf(
		"[cpu:%v, write-bandwidth:%v, byte-size:%v]",
		av[CPURate],
		av[WriteBandwidth],
		av[ByteSize],
	)
}

// A resource can have a capacity, which is also expressed using LoadValue.
// There are some special case capacity values, enumerated here.
const (
	// UnknownCapacity is currenly only used for WriteBandwidth.
	UnknownCapacity LoadValue = math.MaxInt64
)

// SecondaryLoadDimension represents secondary load dimensions that should be
// considered after we are done rebalancing using loadDimensions, since these
// don't represent "real" resources. Currently, only lease and replica counts
// are considered here. Lease rebalancing will see if there is scope to move
// some leases between stores that do not have any pending changes and are not
// overloaded (and will not get overloaded by the movement). This will happen
// in a separate pass (i.e., not in clusterState.rebalanceStores) -- the
// current plan is to continue using the leaseQueue and call from it into MMA.
//
// Note that lease rebalancing will only move leases and not replicas. Also,
// the rebalancing will take into account the lease preferences, as discussed
// in https://github.com/cockroachdb/cockroach/issues/93258, and the lease
// counts among the current candidates (see
// https://github.com/cockroachdb/cockroach/pull/98893).
//
// To use MMA for replica count rebalancing, done by the replicateQueue, we
// also have a ReplicaCount load dimension.
//
// These are currently unused, since the initial integration of MMA is to
// replace load-based rebalancing performed by the StoreRebalancer.
type SecondaryLoadDimension uint8

const (
	LeaseCount SecondaryLoadDimension = iota
	ReplicaCount
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

func (lv SecondaryLoadVector) String() string {
	return redact.StringWithoutMarkers(lv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (lv SecondaryLoadVector) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[lease:%d, replica:%d]", lv[LeaseCount], lv[ReplicaCount])
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
	// Aggregate store load. In general, we don't require this to be a sum of
	// the range loads (since a sharded allocator may only have information
	// about a subset of ranges).
	reportedLoad LoadVector

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
	capacity LoadVector

	reportedSecondaryLoad SecondaryLoadVector
}

// NodeLoad is the load information for a node.
type NodeLoad struct {
	NodeID roachpb.NodeID
	// NodeCPULoad and NodeCPUCapacity are the physical node-level CPU values
	// (NodeCPURateUsage and NodeCPURateCapacity), set directly from
	// StoreLoadMsg rather than derived from store sums. These are used for
	// node-level overload detection because the capped multiplier model
	// breaks the invariant sum(store loads)/sum(store capacities) = cpuUtil
	// when background CPU exceeds the cap. See computeCPUCapacityWithCap.
	NodeCPULoad     LoadValue
	NodeCPUCapacity LoadValue
}

// The mean store load for a set of stores.
type meanStoreLoad struct {
	load     LoadVector
	capacity LoadVector
	// util is the capacity-weighted mean utilization, computed as
	// sum(load)/sum(capacity), NOT the average of individual store utilizations.
	//
	// We use capacity-weighted mean because it answers: "Is this store carrying
	// its fair share of load?" rather than "Is this store more utilized than
	// typical stores?". This is the desired behavior for heterogeneous clusters
	// where ideally all stores run at the same utilization regardless of size.
	//
	// Example: 3 stores with (load, capacity) = (10, 10), (10, 10), (10, 100)
	//   - Average of individual utils: (100% + 100% + 10%) / 3 = 70%
	//   - Capacity-weighted (sum/sum): 30 / 120 = 25%
	// The capacity-weighted 25% is the true picture of resource availability,
	// and comparing a store's utilization against this tells us if it's
	// carrying more than its proportional share.
	//
	// Util is 0 for WriteBandwidth (since its Capacity is UnknownCapacity).
	// Non-zero for CPURate, ByteSize.
	util [NumLoadDimensions]float64

	secondaryLoad SecondaryLoadVector
}

// SafeFormat implements the redact.SafeFormatter interface.
func (m meanStoreLoad) SafeFormat(w redact.SafePrinter, _ rune) {
	// Only printing CPU and byte size utilization explicitly since
	// write bandwidth util is necessarily 0, might need to update in the future.
	w.Printf("{%v %v [cpu-util:%.2f byte-util:%.2f] %v", m.load, m.capacity,
		redact.SafeFloat(m.util[CPURate]), redact.SafeFloat(m.util[ByteSize]), m.secondaryLoad)
}

// The mean node load for a set of NodeLoad.
type meanNodeLoad struct {
	loadCPU     LoadValue
	capacityCPU LoadValue
	utilCPU     float64
}

// SafeFormat implements the redact.SafeFormatter interface.
func (m meanNodeLoad) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("{%v %v %v}", m.loadCPU, m.capacityCPU, redact.SafeFloat(m.utilCPU))
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
	computeLoadSummary(context.Context, roachpb.StoreID, *meanStoreLoad, *meanNodeLoad, mmaLogger) storeLoadSummary
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
	summary = mm.loadInfoProvider.computeLoadSummary(ctx, storeID, &means.storeLoad, &means.nodeLoad, makeMMALogger(false /* verboseToInfof */))
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
			// NB: capacity can be 0 for CPURate when nodeCPURateUsage >>
			// nodeCPURateCapacity. Stores with capacity 0 will be classified as
			// overloadUrgent via fractionUsed = +Inf in
			// loadSummaryForDimension.
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
		means.nodeLoad.loadCPU += nl.NodeCPULoad
		means.nodeLoad.capacityCPU += nl.NodeCPUCapacity
	}
	// NB: capacityCPU can be 0 if all nodes report 0 store CPU capacity.
	means.nodeLoad.utilCPU =
		float64(means.nodeLoad.loadCPU) / float64(means.nodeLoad.capacityCPU)
	means.nodeLoad.loadCPU /= LoadValue(n)
	means.nodeLoad.capacityCPU /= LoadValue(n)
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
		w.SafeString("loadLow")
	case loadNormal:
		w.SafeString("loadNormal")
	case loadNoChange:
		w.SafeString("loadNoChange")
	case overloadSlow:
		w.SafeString("overloadSlow")
	case overloadUrgent:
		w.SafeString("overloadUrgent")
	default:
		panic("unknown loadSummary")
	}
}

// Significance floor constants. These are the fallback denominators used when
// capacity is unknown, preventing small mean loads from amplifying small
// absolute differences into large fractions. When capacity is known, the
// floor is capacityFractionForSignificanceFloor * capacity instead.
const (
	// writeBandwidthSignificanceFloor is the fallback floor for
	// WriteBandwidth when capacity is unknown (2 MiB/s).
	writeBandwidthSignificanceFloor LoadValue = 2 << 20

	// cpuRateSignificanceFloor is the defensive fallback floor for CPURate
	// when capacity is unknown (100ms/s, i.e. 5% of 2 vCPU).
	cpuRateSignificanceFloor LoadValue = 100_000_000

	// byteSizeSignificanceFloor is the defensive fallback floor for ByteSize
	// when capacity is unknown (100 MiB).
	byteSizeSignificanceFloor LoadValue = 100 << 20

	// capacityFractionForSignificanceFloor is the fraction of capacity used
	// as the significance floor when capacity is known.
	capacityFractionForSignificanceFloor = 0.05
)

// computeFractionAbove computes how far above (or below) the mean a store's
// load is, as a fraction. For resources with known capacity, this works in
// utilization space (load/capacity vs mean utilization) to handle heterogeneous
// nodes correctly. For resources with unknown capacity (e.g. WriteBandwidth),
// it works in absolute load space.
//
// In both cases, the denominator is clamped to a significance floor to prevent
// small absolute differences from appearing as large fractions when the mean is
// small. For example, 50 KiB/s above the mean is 25% at a 200 KiB/s mean,
// enough to trigger overloadSlow and cause unnecessary replica moves. Clamping
// the denominator to 2 MiB/s for WriteBandwidth makes that same 50 KiB/s only
// ~2.4%. At high loads (mean > floor), the clamp has no effect.
// The returned effectiveDenom is the denominator actually used (after
// clamping), for diagnostic logging.
func computeFractionAbove(
	dim LoadDimension, load, capacity, meanLoad LoadValue, meanUtil float64,
) (fractionAbove, effectiveDenom float64) {
	if capacity != UnknownCapacity {
		util := float64(load) / float64(capacity)
		denom := max(meanUtil, capacityFractionForSignificanceFloor)
		return (util - meanUtil) / denom, denom
	}
	// For dimensions with unknown capacity, fall back to absolute load space.
	denom := float64(max(meanLoad, significanceFloorForUnknownCapacity(dim)))
	return float64(load-meanLoad) / denom, denom
}

// significanceFloorForUnknownCapacity returns the minimum meaningful
// denominator for dimensions that lack a known capacity. This prevents small
// mean loads from amplifying small absolute differences into large fractions.
func significanceFloorForUnknownCapacity(dim LoadDimension) LoadValue {
	if buildutil.CrdbTestBuild && (dim == CPURate || dim == ByteSize) {
		panic(fmt.Sprintf("unexpected call for dimension %v which should have a known capacity", dim))
	}
	switch dim {
	case WriteBandwidth:
		return writeBandwidthSignificanceFloor
	case CPURate:
		return cpuRateSignificanceFloor
	case ByteSize:
		return byteSizeSignificanceFloor
	}
	panic(fmt.Sprintf("unknown dimension: %v", dim))
}

// computeSummaryUpperBound caps the load summary to loadNormal when absolute
// utilization is low, even if the store is above the mean in relative terms.
// This complements the significance floor clamping in computeFractionAbove:
// clamping addresses statistical significance (preventing small absolute
// differences from producing large fractions), while this function addresses
// prioritization (preventing shedding in dimensions that don't matter yet).
//
// At low utilizations there is overlap: significance floor clamping dampens
// fractionAbove, which may already prevent overloadSlow. But the dampening
// alone isn't always sufficient (see Example 2), so the upper bound provides
// a hard backstop based on absolute utilization. Raising the significance
// floor high enough to fully subsume the upper bound is not viable. For
// fractionAbove to stay below the 0.1 overloadSlow threshold, we need
// (util - meanUtil) / floor < 0.1, i.e. floor > 10 * (util - meanUtil). In
// the worst case (one store at threshold X, others near zero, N stores),
// meanUtil ≈ X/N and the required floor approaches 10*X*(N-1)/N. For
// CPURate (X=0.05, N=3): floor > 10 * 0.05 * 2/3 ≈ 33%. A 33% floor would
// cripple detection at moderate utilizations: a store at 30% with a mean of
// 25% would get fractionAbove = 0.05/0.33 ≈ 0.15 — barely above the
// threshold, when it should clearly be shedding.
//
// Example 1 (ByteSize, threshold 50%): Three stores at 40%, 30%, 30%
// utilization. The mean is ~33%, well above the 5% significance floor, so
// clamping has no effect. fractionAbove for the 40% store is
// (0.4-0.333)/0.333 ≈ 0.18, triggering overloadSlow. But at 40% disk usage,
// shedding bytes is wasteful — it causes unnecessary data movement and
// thrashing when multiple leaseholders rebalance concurrently along this
// low-priority dimension. Capping to loadNormal prevents shedding.
//
// Example 2 (CPURate, threshold 5%): Three stores at 4%, 1%, 1% utilization.
// The mean is 2%, below the 5% significance floor, so the denominator is
// clamped to 5%. This dampens fractionAbove to (0.04-0.02)/0.05 = 0.4,
// down from the unclamped (0.04-0.02)/0.02 = 1.0 — but 0.4 still exceeds
// the 0.1 overloadSlow threshold. The upper bound provides the final
// protection: at 4% CPU utilization the cluster is essentially idle, so
// shedding is pointless regardless of relative position.
func computeSummaryUpperBound(
	dim LoadDimension, capacity LoadValue, fractionUsed float64,
) loadSummary {
	switch dim {
	case ByteSize:
		if buildutil.CrdbTestBuild && capacity == UnknownCapacity {
			panic("ByteSize should always have a known capacity")
		}
		// A disk that is less than half full is not considered for shedding to
		// avoid unnecessary data movement that could cause thrashing.
		if capacity != UnknownCapacity && fractionUsed < 0.5 {
			return loadNormal
		}
	case CPURate:
		if buildutil.CrdbTestBuild && capacity == UnknownCapacity {
			panic("CPURate should always have a known capacity")
		}
		// An almost-idle CPU (< 5% utilization) is not considered for shedding.
		if capacity != UnknownCapacity && fractionUsed < 0.05 {
			return loadNormal
		}
	case WriteBandwidth:
		// TODO(sumeer): consider adding a summaryUpperBound for small
		// WriteBandwidth values too.
	default:
		panic(fmt.Sprintf("unknown dimension: %v", dim))
	}
	return overloadUrgent
}

// Computes the loadSummary for a particular load dimension.
//
// NB: load can be negative since it may be adjusted load.
func loadSummaryForDimension(
	ctx context.Context,
	storeID roachpb.StoreID,
	nodeID roachpb.NodeID,
	dim LoadDimension,
	load LoadValue,
	capacity LoadValue,
	meanLoad LoadValue,
	meanUtil float64,
	ml mmaLogger,
) (summary loadSummary) {
	summ := loadLow
	reason := ""

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
	fractionAbove, effectiveDenom := computeFractionAbove(dim, load, capacity, meanLoad, meanUtil)
	var fractionUsed float64
	if capacity != UnknownCapacity {
		// NB: capacity can be 0 if nodeCPURateUsage >> nodeCPURateCapacity.
		fractionUsed = float64(load) / float64(capacity)
	}

	summaryUpperBound := computeSummaryUpperBound(dim, capacity, fractionUsed)
	const (
		meanFractionSlow     = 0.1
		meanFractionLow      = -0.1
		meanFractionNoChange = 0.05
	)
	if fractionAbove > meanFractionSlow {
		summ = overloadSlow
		reason = "load is >10% above mean"
	} else if fractionAbove < meanFractionLow {
		summ = loadLow
		reason = "load is >10% below mean"
	} else if fractionAbove >= meanFractionNoChange {
		summ = loadNoChange
		reason = "load is within 5-10% of mean"
	} else {
		summ = loadNormal
		reason = "load is within 5% of mean"
	}

	defer func() {
		if !ml.V(ctx, 3) {
			return
		}

		var buf redact.StringBuilder
		buf.Printf("load summary for dim=%v (", dim)
		if nodeID > nodeIDForLogging {
			buf.Printf("n%v", nodeID)
		}
		if storeID > storeIDForLogging {
			buf.Printf("s%v", storeID)
		}
		buf.Printf("): %v, reason: %v [load=%v meanLoad=%v", summary, redact.SafeString(reason), load, meanLoad)
		if capacity != UnknownCapacity {
			// Annotate fractionUsed with * when the significance floor clamped
			// the denominator used to compute fractionAbove.
			naturalDenom := meanUtil
			clamped := ""
			if effectiveDenom != naturalDenom {
				clamped = "*"
			}
			buf.Printf(" fractionUsed=%.2f%%%s meanUtil=%.2f%% capacity=%v",
				redact.SafeFloat(fractionUsed*100), redact.SafeString(clamped),
				redact.SafeFloat(meanUtil*100), capacity)
		}
		buf.SafeRune(']')
		ml.logf(ctx, 3, "%s", buf.RedactableString())
	}()

	// Treat nearing full capacity as urgent regardless of relative position.
	// Since fractionAbove is utilization-based (when capacity is known), the
	// relative distance from the mean is already captured above. But absolute
	// utilization > 90% is dangerous enough to always warrant urgency.
	if capacity != UnknownCapacity && fractionUsed > 0.9 {
		reason = "fractionUsed > 90%"
		return min(summaryUpperBound, overloadUrgent)
	}
	return min(summaryUpperBound, summ)
}

// highDiskSpaceUtilization checks if disk utilization >= the given threshold.
// This is called in three places:
//
//  1. updateStoreStatuses (both thresholds):
//     Sets store dispositions based on disk utilization:
//     - >= diskUtilShedThreshold (0.95): ReplicaDispositionShedding
//     - >= diskUtilRefuseThreshold (0.925): ReplicaDispositionRefusing
//
//  2. canShedAndAddLoad (threshold: diskUtilRefuseThreshold, 0.925):
//     Validates post-transfer state by checking if the target would exceed
//     the refuse threshold after receiving the replica. Uses post-transfer
//     load (adjusted + delta) to ensure we don't send replicas to stores
//     that would become too full.
//
//  3. topK dimension selection (threshold: diskUtilShedThreshold, 0.95):
//     Forces ByteSize dimension when selecting ranges to shed, regardless of
//     the load summary. Note that ByteSize can also be selected at lower
//     utilization via the normal load summary logic, if ByteSize is the worst
//     dimension relative to the cluster mean.
//
// Note: Dispositions don't indicate *why* they're set (refusing could be due
// to disk or other reasons), so callers needing explicit disk checks (like
// canShedAndAddLoad and topK) call this function directly.
func highDiskSpaceUtilization(load LoadValue, capacity LoadValue, threshold float64) bool {
	if capacity == UnknownCapacity || capacity == 0 {
		log.KvDistribution.Errorf(context.Background(), "disk capacity is unknown or zero")
		return false
	}
	// load and capacity are both in terms of logical bytes.
	//
	//   load     = LogicalBytes
	//   diskUtil = FractionUsed()  (i.e. Used/(Available+Used), or
	//                               (Capacity-Available)/Capacity as fallback)
	//   capacity = LogicalBytes / diskUtil
	//
	// Therefore:
	//   fractionUsed = load / capacity
	//                = LogicalBytes / (LogicalBytes / diskUtil)
	//                = diskUtil
	//
	// This recovers the actual disk utilization as reported by the store
	// descriptor, expressed in terms of the logical byte load and capacity.
	fractionUsed := float64(load) / float64(capacity)
	return fractionUsed >= threshold
}

const loadMultiplierForAddition = 1.1

func loadToAdd(l LoadValue) LoadValue {
	return LoadValue(float64(l) * loadMultiplierForAddition)
}

func loadVectorToAdd(lv LoadVector) LoadVector {
	var result LoadVector
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
var _ = NodeLoad{}.NodeCPULoad
var _ = NodeLoad{}.NodeCPUCapacity
var _ = CPURate
var _ = WriteBandwidth
var _ = ByteSize
