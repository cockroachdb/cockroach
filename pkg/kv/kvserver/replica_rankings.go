// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"go.etcd.io/raft/v3"
)

const (
	// TODO(aayush): Scale this up based on the number of replicas on a store?
	numTopReplicasToTrack = 128
)

// CandidateReplica is a replica that is being tracked as a potential candidate
// for rebalancing activities. It maintains a set of methods that enable
// querying it's state and processing a rebalancing action if taken.
type CandidateReplica interface {
	// OwnsValidLease returns whether this replica is the current valid
	// leaseholder.
	OwnsValidLease(context.Context, hlc.ClockTimestamp) bool
	// StoreID returns the Replica's StoreID.
	StoreID() roachpb.StoreID
	// GetRangeID returns the Range ID.
	GetRangeID() roachpb.RangeID
	// RaftStatus returns the current raft status of the replica. It returns
	// nil if the Raft group has not been initialized yet.
	RaftStatus() *raft.Status
	// GetFirstIndex returns the index of the first entry in the replica's Raft
	// log.
	GetFirstIndex() kvpb.RaftIndex
	// SpanConfig returns the span config for the replica or an error if it can't
	// be determined.
	SpanConfig() (roachpb.SpanConfig, error)
	// Desc returns the authoritative range descriptor.
	Desc() *roachpb.RangeDescriptor
	// RangeUsageInfo returns usage information (sizes and traffic) needed by
	// the allocator to make rebalancing decisions for a given range.
	RangeUsageInfo() allocator.RangeUsageInfo
	// AdminTransferLease transfers the LeaderLease to another replica.
	AdminTransferLease(ctx context.Context, target roachpb.StoreID, bypassSafetyChecks bool) error
	// Repl returns the underlying replica for this CandidateReplica. It is
	// only used for determining timeouts in production code and not the
	// simulator.
	//
	// TODO(kvoli): Remove this method. Refactor the timeout calculation to
	// avoid needing the replica ref.
	Repl() *Replica
	// String implements the string interface.
	String() string
}

type candidateReplica struct {
	*Replica
	usage allocator.RangeUsageInfo
}

// RangeUsageInfo returns usage information (sizes and traffic) needed by
// the allocator to make rebalancing decisions for a given range.
func (cr candidateReplica) RangeUsageInfo() allocator.RangeUsageInfo {
	return cr.usage
}

// Replica returns the underlying replica for this CandidateReplica. It is
// only used for determining timeouts in production code and not the
// simulator.
func (cr candidateReplica) Repl() *Replica {
	return cr.Replica
}

// ReplicaRankings maintains top-k orderings of the replicas in a store by QPS.
type ReplicaRankings struct {
	mu struct {
		syncutil.Mutex
		dimAccumulator *RRAccumulator
		byDim          []CandidateReplica
	}
}

// NewReplicaRankings returns a new ReplicaRankings struct.
func NewReplicaRankings() *ReplicaRankings {
	return &ReplicaRankings{}
}

// NewReplicaAccumulator returns a new rrAccumulator.
//
// TODO(kvoli): When adding another load dimension to be balanced upon, it will
// be necessary to clarify the semantics of this API. This is especially true
// since the UI is coupled to this function.
func NewReplicaAccumulator(dims ...load.Dimension) *RRAccumulator {
	res := &RRAccumulator{
		dims: map[load.Dimension]*rrPriorityQueue{},
	}
	for _, dim := range dims {
		// Reassign dim variable to ensure correct values is captured down below in val() func.
		dim := dim
		res.dims[dim] = &rrPriorityQueue{}
		res.dims[dim].val = func(r CandidateReplica) float64 {
			return r.RangeUsageInfo().Load().Dim(dim)
		}
	}
	return res
}

// Update sets the accumulator for replica tracking to be the passed in value.
func (rr *ReplicaRankings) Update(acc *RRAccumulator) {
	rr.mu.Lock()
	rr.mu.dimAccumulator = acc
	rr.mu.Unlock()
}

// TopLoad returns the highest load CandidateReplicas that are tracked.
func (rr *ReplicaRankings) TopLoad(dimension load.Dimension) []CandidateReplica {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	// If we have a new set of data, consume it. Otherwise, just return the most
	// recently consumed data.
	if rr.mu.dimAccumulator != nil && rr.mu.dimAccumulator.dims[dimension].Len() > 0 {
		rr.mu.byDim = consumeAccumulator(rr.mu.dimAccumulator.dims[dimension])
	}
	return rr.mu.byDim
}

// RRAccumulator is used to update the replicas tracked by ReplicaRankings.
// The typical pattern should be to call NewAccumulator, add
// all the replicas you care about to the accumulator using addReplica, then
// pass the accumulator back to the ReplicaRankings using the update method.
// This method of loading the new rankings all at once avoids interfering with
// any consumers that are concurrently reading from the rankings, and also
// prevents concurrent loaders of data from messing with each other -- the last
// `update`d accumulator will win.
type RRAccumulator struct {
	dims map[load.Dimension]*rrPriorityQueue
}

// AddReplica adds a replica to the replica accumulator.
func (a *RRAccumulator) AddReplica(repl CandidateReplica) {
	for dim := range a.dims {
		a.addReplicaForDimension(repl, dim)
	}
}

func (a *RRAccumulator) addReplicaForDimension(repl CandidateReplica, dim load.Dimension) {
	rr := a.dims[dim]
	// If the heap isn't full, just push the new replica and return.
	if rr.Len() < numTopReplicasToTrack {

		heap.Push(a.dims[dim], repl)
		return
	}

	// Otherwise, conditionally push if the new replica is more deserving than
	// the current tip of the heap.
	if rr.val(repl) > rr.val(rr.entries[0]) {
		heap.Pop(rr)
		heap.Push(rr, repl)
	}

}

func consumeAccumulator(pq *rrPriorityQueue) []CandidateReplica {
	length := pq.Len()
	sorted := make([]CandidateReplica, length)
	for i := 1; i <= length; i++ {
		sorted[length-i] = heap.Pop(pq).(CandidateReplica)
	}
	return sorted
}

type rrPriorityQueue struct {
	entries []CandidateReplica
	val     func(CandidateReplica) float64
}

func (pq rrPriorityQueue) Len() int { return len(pq.entries) }

func (pq rrPriorityQueue) Less(i, j int) bool {
	return pq.val(pq.entries[i]) < pq.val(pq.entries[j])
}

func (pq rrPriorityQueue) Swap(i, j int) {
	pq.entries[i], pq.entries[j] = pq.entries[j], pq.entries[i]
}

func (pq *rrPriorityQueue) Push(x interface{}) {
	item := x.(CandidateReplica)
	pq.entries = append(pq.entries, item)
}

func (pq *rrPriorityQueue) Pop() interface{} {
	old := pq.entries
	n := len(old)
	item := old[n-1]
	pq.entries = old[0 : n-1]
	return item
}

// ReplicaRankingMap maintains top-k orderings of the replicas per tenant in a
// store by QPS.
//
// TODO(kvoli): The separation between this struct and ReplicaRankings is
// problematic in that a separation now exists between what the cluster cares
// about w.r.t "hotness" and maintaining compatibility with the UI which
// expects QPS ranked replicas. The per-tenant rankings is logical for tenant
// UI parity, however useless in the current form for being used with cluster
// rebalancing or new signals. We should clarify what a good end state is, if
// different signals than QPS may be used by the system to rebalance in some
// but not all cases.
type ReplicaRankingMap struct {
	mu struct {
		syncutil.Mutex
		dimAccumulators *RRAccumulatorByTenant
		// byDims map keeps last computed values per tenant.
		byDims map[roachpb.TenantID][]CandidateReplica
	}
}

// NewReplicaRankingsMap returns a new ReplicaRankingMap struct.
func NewReplicaRankingsMap() *ReplicaRankingMap {
	rr := &ReplicaRankingMap{}
	rr.mu.byDims = map[roachpb.TenantID][]CandidateReplica{}
	rr.mu.dimAccumulators = NewTenantReplicaAccumulator()
	return rr
}

// NewTenantReplicaAccumulator returns a new RRAccumulatorByTenant.
func NewTenantReplicaAccumulator(dims ...load.Dimension) *RRAccumulatorByTenant {
	return &RRAccumulatorByTenant{
		dims:   dims,
		accums: map[roachpb.TenantID]*RRAccumulator{},
	}
}

// Update sets the accumulator for replica tracking to be the passed in value.
func (rr *ReplicaRankingMap) Update(acc *RRAccumulatorByTenant) {
	rr.mu.Lock()
	rr.mu.dimAccumulators = acc
	rr.mu.Unlock()
}

// TopLoad returns the highest load CandidateReplicas that are tracked.
func (rr *ReplicaRankingMap) TopLoad(
	tenantID roachpb.TenantID, dimension load.Dimension,
) []CandidateReplica {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	r, ok := rr.mu.dimAccumulators.accums[tenantID]
	if !ok {
		return []CandidateReplica{}
	}
	if dim, ok := r.dims[dimension]; ok && dim.Len() > 0 {
		rr.mu.byDims[tenantID] = consumeAccumulator(dim)
	}
	return rr.mu.byDims[tenantID]
}

// RRAccumulatorByTenant accumulates replicas per tenant to update the replicas tracked by ReplicaRankingMap.
// It should be used in the same way as RRAccumulator (see doc string).
type RRAccumulatorByTenant struct {
	accums map[roachpb.TenantID]*RRAccumulator
	dims   []load.Dimension
}

// AddReplica adds a replica to the replica accumulator.
func (ra RRAccumulatorByTenant) AddReplica(repl CandidateReplica) {
	tID, ok := repl.Repl().TenantID()
	if !ok {
		return
	}
	acc, ok := ra.accums[tID]
	if !ok {
		acc = NewReplicaAccumulator(ra.dims...)
	}
	acc.AddReplica(repl)
	ra.accums[tID] = acc
}
