// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// The replicate and lease queues are unaware of load (such as cpu or write
// bandwidth), so their decisions can conflict with mma’s goals and cause
// thrashing. For example, a store may be underfull in replica count but
// overloaded on write bandwidth; the replicate queue may try to move replicas
// to it while MMA simultaneously tries to shed load from it.
//
// Our long-term goal is to extend mma to also rebalance based on range and
// lease counts, which would eliminate the need for replicate and lease queues
// to perform count-based rebalancing. At the time of this comment, mma only
// handles load, so we still rely on the queues for count-based balancing.
//
// To reduce thrashing, the approach is that when the replicate/lease queue
// considers a rebalance, if the action repairs a bad state (such as constraint
// violation, disk fullness, or diversity for the replicate queue, or lease
// preference and io-overload repair for the lease queue), we let it proceed
// without consulting mma. Otherwise, we pass the chosen source and target
// stores to mma to confirm that the move does not conflict with its goals. The
// methods in this file provide helper functions for the replicate and lease
// queues to consult MMA, allowing them to check whether a proposed rebalance
// aligns with MMA’s goals before proceeding.
//
// There are a couple of ways to achieve this. We are aiming for the least
// intrusive approach since this is gated by a cluster setting and meant to be
// iterated on. Most of the discussion focused on the replicate queue, since the
// lease queue changes are less complex and follow the same principle.
//
// Two main design questions came up.
// 1. The first was when to exclude overloaded stores (early before mean
// calculation or late only at target selection).
// - We decided to include them in the mean calculation but exclude them at the
// final target selection step. This minimizes code churn, avoids plumbing new
// fields into candidate structs, and reduces number of mma calls by checking
// only the final target instead of on every candidate. It does not eliminate
// thrashing, since a store may look like a good candidate during scoring but be
// rejected later, picking a not-so-good but still better than existing
// candidate. The lease queue follows the same rule, filtering overloaded stores
// only at final target selection.
// - Alternatives considered: 1. mma participates in the allocator's scoring
// options either by jittering balance score or by introducing a new field in
// the candidate struct. 2. exclude the store right before or right after the
// equivalence class construction.
//
// 2. The second question was which set of stores to use when computing load
// summaries with respect to.
// - The principle we followed is to use the same set of stores that is used to
// compute the mean for range or lease count. For the replicate queue, this
// means we use all stores that satisfy constraints to compute mean. The
// principle we are following here is that we want this set or the mean to be
// stable and not fluctuate as store load changes. This also aligns with mma’s
// range rebalancing behavior, which computes load summaries with respect to all
// constraint-satisfying stores. For the lease queue, this means we use all
// stores that satisfy the constraint to compute the lease count mean as well.
// This approach differs from how mma computes load summary for lease transfers
// - mma computes load summary over stores that the existing replicas are on.
//
// Alternatives considered:
// 1. Another option was to let mma choose from a set of candidates, but this was
// considered too intrusive.
// 2. Instead of having the lease or replicate queue pass a candidate set for
// mma to compute the load summary over with, we could simply pass the rangeID
// along with the source and target stores, and let mma look up its range state
// and compute load summary as if they are performing a lease transfer / range
// rebalance. This is something to consider in the future. I am somewhat
// concerned that the external view could diverge from MMA’s internal state,
// potentially not reflecting the correct set of stores in the replica set. At
// this point, it’s unclear which approach is better.

// BuildMMARebalanceAdvisor constructs an MMARebalanceAdvisor that can be later
// passed to IsInConflictWithMMA to determine if a candidate conflicts with
// MMA's goals.
//
// If MMA is disabled, a no-op advisor is returned, which always returns false
// for IsInConflictWithMMA. If MMA is enabled, the advisor is created by
// computing the load summary for the provided existing store and candidate set.
//
// Note that MMA continues to use this candidate set to compute load summaries,
// so it is safe for the caller to modify the candidate set after calling this
// function. The caller is responsible for keeping track of the returned advisor
// and associating it with the corresponding existing store.
func (as *AllocatorSync) BuildMMARebalanceAdvisor(
	existing roachpb.StoreID, cands []roachpb.StoreID,
) mmaprototype.MMARebalanceAdvisor {
	if kvserverbase.LoadBasedRebalancingMode.Get(&as.st.SV) != kvserverbase.LBRebalancingMultiMetricAndCount {
		return mmaprototype.NoopMMARebalanceAdvisor()
	}
	return as.mmaAllocator.BuildMMARebalanceAdvisor(existing, cands)
}

// IsInConflictWithMMA determines if a candidate conflicts with MMA's goals.
func (as *AllocatorSync) IsInConflictWithMMA(
	cand roachpb.StoreID, advisor mmaprototype.MMARebalanceAdvisor, cpuOnly bool,
) bool {
	return as.mmaAllocator.IsInConflictWithMMA(cand, advisor, cpuOnly)
}
