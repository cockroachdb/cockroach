// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// MMARebalanceAdvisor contains information that mma needs to determine if a
// candidate is in conflict with its goals. The caller should not access/change
// its internal fields after creation and should use IsInConflictWithMMA
// instead.
//
// MMARebalanceAdvisor uses the meansLoad summary to compute the load summary
// for a provided candidate. Then it compares the candidate load summary against
// the existingStoreSLS to determine if the candidate is more overloaded than
// the existing store. If yes, mma will return true for IsInConflictWithMMA. It
// is up to the caller to decide what to do with this information.
type MMARebalanceAdvisor struct {
	// disabled is true when MMA is disabled. It overrides all decisions with
	// IsInConflictWithMMA returning false.
	disabled bool
	// existingStoreID is the ID of the existing store.
	existingStoreID roachpb.StoreID
	// existingStoreSLS holds the load summary for the existing store. It is
	// initially nil and is computed using existingStoreID and means the first
	// time IsInConflictWithMMA is called. The caller must ensure this advisor is
	// only used with the corresponding existingStoreID.
	existingStoreSLS *storeLoadSummary
	// means is the means for the candidate set.
	means meansLoad
}

// NoopMMARebalanceAdvisor is a no-op MMARebalanceAdvisor that always returns
// false for IsInConflictWithMMA. Used when MMA is disabled or mma does not have
// enough information to determine.
func NoopMMARebalanceAdvisor() *MMARebalanceAdvisor {
	return &MMARebalanceAdvisor{
		disabled: true,
	}
}

// BuildMMARebalanceAdvisor constructs an MMARebalanceAdvisor for the given
// existing store and candidate stores. The advisor can be used to determine if
// a candidate is vetoed by the multi-metric allocator due to running counter to
// its goals. Currently, it is considered counter to its goal if the candidate
// is more overloaded than the existing store with respect to the cands list.
//
// The provided cands list may or may not include the existing store. For the
// replicate queue, the cands list is just the set of candidate stores for the
// equivalence class, excluding the existing store. For the lease queue, the
// cands list includes all stores that satisfy the constraint including the
// existing store.
//
// This method always adds the existing store to the cands list to ensure it is
// included in the mean calculation. This is subject to change, but currently it
// does not make sense to compute the load summary of the existing store with
// respect to the cands list that does not include the existing store itself.
// Alternatively, we could require the cands list to be exactly the same as the
// set of stores used to compute the means for lease / range count. The existing
// store should be included for the lease queue but excluded for the replicate
// queue. Note that since this method always adds the existing store to the
// list, and the provided cands may or may not already include the existing
// store, the cands slice passed to computeMeansForStoreSet may contain
// duplicate storeIDs. It is up to computeMeansForStoreSet to handle
// de-duplication of storeIDs from the cands list.
//
// The returned advisor should be passed to IsInConflictWithMMA as a helper to
// determine if a candidate is vetoed by the multi-metric allocator due to
// running counter to its goals.
func (a *allocatorState) BuildMMARebalanceAdvisor(
	existing roachpb.StoreID, cands []roachpb.StoreID,
) *MMARebalanceAdvisor {
	// TODO(wenyihu6): for simplicity, we create a new scratchNodes every call.
	// We should reuse the scratchNodes instead.
	scratchNodes := map[roachpb.NodeID]*NodeLoad{}
	scratchStores := map[roachpb.StoreID]struct{}{}
	cands = append(cands, existing)
	means := computeMeansForStoreSet(a.cs, cands, scratchNodes, scratchStores)
	return &MMARebalanceAdvisor{
		existingStoreID: existing,
		means:           means,
	}
}

// IsInConflictWithMMA determines if the given candidate is in conflict with the
// existing store using the provided MMARebalanceAdvisor. For simplicity, we
// currently say that this is in conflict if the candidate is more overloaded
// than the existing store. This is subject to change in the future. When
// cpuOnly is true, the candidate store's cpu load summary must not exceed that
// of the existing store, or the operation is considered in conflict. When
// cpuOnly is false, the worst dimension's summary is compared instead.
//
// Caller is responsible for making sure the MMARebalanceAdvisor is for the correct
// existing store and candidate set.
func (a *allocatorState) IsInConflictWithMMA(
	ctx context.Context, cand roachpb.StoreID, advisor *MMARebalanceAdvisor, cpuOnly bool,
) bool {
	if advisor.disabled {
		return false
	}
	// Lazily compute and cache the load summary for the existing store.
	if advisor.existingStoreSLS == nil {
		summary := a.cs.computeLoadSummary(ctx, advisor.existingStoreID, &advisor.means.storeLoad, &advisor.means.nodeLoad)
		advisor.existingStoreSLS = &summary
	}
	existingSLS := advisor.existingStoreSLS
	// Always compute the candidate's load summary.
	candSLS := a.cs.computeLoadSummary(ctx, cand, &advisor.means.storeLoad, &advisor.means.nodeLoad)

	// TODO(wenyihu6): unify the branches below by assigning based on sls.worstDim and cpuOnly.
	var conflict bool
	if cpuOnly {
		conflict = candSLS.dimSummary[CPURate] > existingSLS.dimSummary[CPURate]
		if conflict {
			log.KvDistribution.VEventf(
				ctx, 2,
				"mma rejected candidate s%d(cpu-only) as a replacement for s%d: candidate=%v(cpu) > existing=%v(cpu)",
				cand, advisor.existingStoreID, candSLS.dimSummary[CPURate], existingSLS.dimSummary[CPURate],
			)
		}
	} else {
		conflict = candSLS.sls > existingSLS.sls
		if conflict {
			log.KvDistribution.VEventf(
				ctx, 2,
				"mma rejected candidate s%d as a replacement for s%d: candidate=%v(%v) > existing=%v(%v)",
				cand, advisor.existingStoreID, candSLS.sls, candSLS.worstDim, existingSLS.sls, existingSLS.worstDim,
			)
		}
	}
	return conflict
}
