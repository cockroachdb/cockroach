// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"math"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Misc helper classes for working with range, store and node load.

type loadDimension uint8

const (
	// Nanos per second
	cpu loadDimension = iota
	// Bytes per second.
	writeBandwidth
	// Bytes.
	byteSize
	numLoadDimensions
)

type loadValue int64
type loadVector [numLoadDimensions]loadValue

// Capacity information.
const (
	unknownCapacity loadValue = math.MaxInt64
	// parentCapacity is currently only used for cpu at the store level.
	parentCapacity loadValue = math.MaxInt64 - 1
)

// Secondary load dimensions should be considered after we are done
// rebalancing using loadDimensions, since these don't represent "real" load.
// Currently, only lease count is considered here. Lease rebalancing will see
// if there is scope to move some leases between stores that do not have any
// pending changes and are not overloaded (and will not get overloaded by the
// movement).

// TODO(sumeer): do we need to move replicas too, in order to do lease
// rebalancing, or can we assume that constraints and diversity scores have
// sufficiently achieved enough that we only need to move leases between the
// existing voter replicas. The example in
// https://github.com/cockroachdb/cockroach/issues/93258 suggests we only need
// the latter.
type secondaryLoadDimension uint8

const (
	leaseCount secondaryLoadDimension = iota
	numSecondaryLoadDimensions
)

type secondaryLoadVector [numSecondaryLoadDimensions]loadValue

type rangeLoad struct {
	load loadVector
	// Nanos per second. raftCPU <= load[cpu]. Handling this as a special case,
	// rather than trying to (over) generalize, since currently this is the only
	// resource broken down into two components.
	raftCPU loadValue
}

type storeLoad struct {
	reportedLoad loadVector
	adjustedLoad loadVector
	// Capacity information for this store.
	//
	// capacity[cpu] is parentCapacity.
	//
	// capacity[writeBandwidth] is typically unknownCapacity. However, if the
	// LSM on the store is getting overloaded, whether it is because of disk
	// bandwidth being reached or some other resource bottleneck (compactions
	// not keeping up), the store can set this to a synthesized value that
	// indicates high utilization, in order to shed some load.
	//
	// capacity[byteSize] is the actual capacity.
	capacity loadVector
	nodeLoad *nodeLoad

	reportedSecondaryLoad secondaryLoadVector
	adjustedSecondaryLoad secondaryLoadVector
}

type nodeLoad struct {
	nodeID      roachpb.NodeID
	reportedCPU loadValue
	adjustedCPU loadValue
	capacityCPU loadValue
}

// The mean store load for a set of stores.
type meanStoreLoad struct {
	load     loadVector
	capacity loadVector
	// Util is 0 for cpu, writeBandwidth. Non-zero for byteSize.
	util [numLoadDimensions]float64

	secondaryLoad secondaryLoadVector
}

// The mean node load for a set of nodeLoad.
type meanNodeLoad struct {
	loadCPU     loadValue
	capacityCPU loadValue
	utilCPU     float64
}

// The means for a set of stores.
type meansForStoreSet struct {
	stores    storeIDPostingList
	storeLoad meanStoreLoad
	nodeLoad  meanNodeLoad
}

// meansMemo should be cleared before every allocator pass. Each allocator
// pass will encounter identical constraints for many ranges under
// consideration, which implies an identical set of stores that satisfy those
// constraints. The mean load and utilization for each such set is needed to
// make rebalancing decisions. We compute and store these means to avoid
// computing them repeatedly within an allocator pass. The assumption made
// here is that any change in the means during an allocator pass is
// irrelevant.
type meansMemo struct {
	// Set hash => means
	means map[uint64]*meansForStoreSetSlice

	scratchNodes map[roachpb.NodeID]*nodeLoad
}

type meansForStoreSetSlice struct {
	slice []meansForStoreSet
}

var meansForStoreSetSlicePool = sync.Pool{
	New: func() interface{} {
		return &meansForStoreSetSlice{}
	},
}

func (mm *meansMemo) clear() {
	for k, v := range mm.means {
		for i := range v.slice {
			v.slice[i].stores = v.slice[i].stores[:0]
			v.slice[i] = meansForStoreSet{
				stores: v.slice[i].stores,
			}
		}
		v.slice = v.slice[:0]
		meansForStoreSetSlicePool.Put(v)
		delete(mm.means, k)
	}
}

type storeLoadFunc func(id roachpb.StoreID) *storeLoad

func (mm *meansMemo) getMeans(s storeIDPostingList, storeLoadFunc storeLoadFunc) *meansForStoreSet {
	h := s.hash()
	slice, ok := mm.means[h]
	if ok {
		for _, means := range slice.slice {
			if means.stores.isEqual(s) {
				return &means
			}
		}
	} else {
		slice = meansForStoreSetSlicePool.Get().(*meansForStoreSetSlice)
		mm.means[h] = slice
	}
	index := len(slice.slice)
	if cap(slice.slice) > index {
		slice.slice = slice.slice[:index+1]
	} else {
		slice.slice = append(slice.slice, meansForStoreSet{})
	}
	meansForSet := &slice.slice[index]
	n := len(s)
	if n == 0 {
		return meansForSet
	}
	for k := range mm.scratchNodes {
		delete(mm.scratchNodes, k)
	}
	for _, storeID := range s {
		meansForSet.stores = append(meansForSet.stores, storeID)
		sload := storeLoadFunc(storeID)
		for j := range sload.reportedLoad {
			meansForSet.storeLoad.load[j] += sload.reportedLoad[j]
			if sload.capacity[j] == parentCapacity || sload.capacity[j] == unknownCapacity {
				meansForSet.storeLoad.capacity[j] = parentCapacity
			} else if meansForSet.storeLoad.capacity[j] != parentCapacity {
				meansForSet.storeLoad.capacity[j] += sload.capacity[j]
			}
		}
		for j := range sload.reportedSecondaryLoad {
			meansForSet.storeLoad.secondaryLoad[j] += sload.reportedSecondaryLoad[j]
		}
		mm.scratchNodes[sload.nodeLoad.nodeID] = sload.nodeLoad
	}
	for i := range meansForSet.storeLoad.load {
		if meansForSet.storeLoad.capacity[i] != parentCapacity {
			meansForSet.storeLoad.util[i] =
				float64(meansForSet.storeLoad.load[i]) / float64(meansForSet.storeLoad.capacity[i])
			meansForSet.storeLoad.capacity[i] /= loadValue(n)
		} else {
			meansForSet.storeLoad.util[i] = 0
		}
		meansForSet.storeLoad.load[i] /= loadValue(n)
	}
	for i := range meansForSet.storeLoad.secondaryLoad {
		meansForSet.storeLoad.secondaryLoad[i] /= loadValue(n)
	}

	n = len(mm.scratchNodes)
	for _, nl := range mm.scratchNodes {
		meansForSet.nodeLoad.loadCPU += nl.reportedCPU
		meansForSet.nodeLoad.capacityCPU += nl.capacityCPU
	}
	meansForSet.nodeLoad.utilCPU =
		float64(meansForSet.nodeLoad.loadCPU) / float64(meansForSet.nodeLoad.capacityCPU)
	meansForSet.nodeLoad.loadCPU /= loadValue(n)
	meansForSet.nodeLoad.capacityCPU /= loadValue(n)

	return meansForSet
}

// loadSummary aggregates across all load dimensions for a store, or a node,
// including externally provided failure detection state. This could be a
// score instead of an enum, but eventually we want to decide what scores are
// roughly equal when deciding on rebalancing priority, and to decide how to
// order the stores we will try to rebalance to. So we simply use an enum.
type loadSummary uint8

const (
	failureImminent loadSummary = iota
	suspectShedLeases
	// The two overload states represent how the degree of overload.
	overloadUrgent
	overloadSlow
	// loadNoChange represents that no load should be added or removed from this
	// store. This is typically only used when there are enough pending changes
	// at this store that we want to let them finish.
	loadNoChange
	loadNormal
	loadLow
)

// Computes the loadSummary for a particular load dimension. Will only return
// a loadSummary \in {overloadUrgent, overloadSlow, loadNormal, loadLow}.
func loadSummaryForDimension(
	load loadValue, capacity loadValue, meanLoad loadValue, meanUtil float64,
) loadSummary {
	if capacity == parentCapacity {
		return loadLow
	}
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
	// cpu and byteSize since the consequence of running out-of-disk is much
	// more severe.
	//
	// The capacity may be unknownCapacity. Even if we have a known capacity, we
	// may want to consider how far we are away from mean. The mean isn't very
	// useful when there are heterogeneous nodeLoad. It also does not help when
	// there are constraints only satisfied by a subset of nodeLoad that have much
	// higher utilization. Even though we permit very general constraint and
	// locality specifications, it may be that the set of attributes used in
	// constraints and lease preferences are such that we can partition stores
	// into sets that share the same attributes and have the same locality
	// tiers, and use the means for each set. But even that is not very helpful
	// because some ranges may have fewer constraints than others.
	fractionAbove := float64(load)/float64(meanLoad) - 1.0
	if fractionAbove > 0.2 {
		loadSummary = overloadSlow
	} else if fractionAbove < -0.2 {
		loadSummary = loadLow
	} else {
		loadSummary = loadNormal
	}
	if capacity != unknownCapacity {
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
