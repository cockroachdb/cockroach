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
	GetFirstIndex() uint64
	// DescAndSpanConfig returns the authoritative range descriptor as well
	// as the span config for the replica.
	DescAndSpanConfig() (*roachpb.RangeDescriptor, roachpb.SpanConfig)
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

// NewAccumulator returns a new rrAccumulator.
//
// TODO(kvoli): When adding another load dimension to be balanced upon, it will
// be necessary to clairfy the semantics of this API. This is especially true
// since the UI is coupled to this function.
func (rr *ReplicaRankings) NewAccumulator(dimension load.Dimension) *RRAccumulator {
	res := &RRAccumulator{}
	res.dim.val = func(r CandidateReplica) float64 {
		return r.RangeUsageInfo().Load().Dim(dimension)
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
func (rr *ReplicaRankings) TopLoad() []CandidateReplica {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	// If we have a new set of data, consume it. Otherwise, just return the most
	// recently consumed data.
	if rr.mu.dimAccumulator != nil && rr.mu.dimAccumulator.dim.Len() > 0 {
		rr.mu.byDim = consumeAccumulator(&rr.mu.dimAccumulator.dim)
	}
	return rr.mu.byDim
}

// RRAccumulator is used to update the replicas tracked by ReplicaRankings.
// The typical pattern should be to call ReplicaRankings.newAccumulator, add
// all the replicas you care about to the accumulator using addReplica, then
// pass the accumulator back to the ReplicaRankings using the update method.
// This method of loading the new rankings all at once avoids interfering with
// any consumers that are concurrently reading from the rankings, and also
// prevents concurrent loaders of data from messing with each other -- the last
// `update`d accumulator will win.
type RRAccumulator struct {
	dim rrPriorityQueue
}

// AddReplica adds a replica to the replica accumulator.
func (a *RRAccumulator) AddReplica(repl CandidateReplica) {
	// If the heap isn't full, just push the new replica and return.
	if a.dim.Len() < numTopReplicasToTrack {
		heap.Push(&a.dim, repl)
		return
	}

	// Otherwise, conditionally push if the new replica is more deserving than
	// the current tip of the heap.
	if a.dim.val(repl) > a.dim.val(a.dim.entries[0]) {
		heap.Pop(&a.dim)
		heap.Push(&a.dim, repl)
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

// ReplicaRankingMap maintains top-k orderings of the replicas per tenant in a store by QPS.
type ReplicaRankingMap struct {
	mu struct {
		syncutil.Mutex
		items RRAccumulatorByTenant
	}
}

// NewReplicaRankingsMap returns a new ReplicaRankingMap struct.
func NewReplicaRankingsMap() *ReplicaRankingMap {
	return &ReplicaRankingMap{}
}

// NewAccumulator returns a new rrAccumulator.
func (rr *ReplicaRankingMap) NewAccumulator() *RRAccumulatorByTenant {
	return &RRAccumulatorByTenant{}
}

// Update sets the accumulator for replica tracking to be the passed in value.
func (rr *ReplicaRankingMap) Update(acc *RRAccumulatorByTenant) {
	rr.mu.Lock()
	rr.mu.items = *acc
	rr.mu.Unlock()
}

// TopQPS returns the highest QPS CandidateReplicas that are tracked.
func (rr *ReplicaRankingMap) TopQPS(tenantID roachpb.TenantID) []CandidateReplica {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	r, ok := rr.mu.items[tenantID]
	if !ok {
		return []CandidateReplica{}
	}
	if r.Len() > 0 {
		r.entries = consumeAccumulator(&r)
		rr.mu.items[tenantID] = r
	}
	return r.entries
}

// RRAccumulatorByTenant accumulates replicas per tenant to update the replicas tracked by ReplicaRankingMap.
// It should be used in the same way as RRAccumulator (see doc string).
type RRAccumulatorByTenant map[roachpb.TenantID]rrPriorityQueue

// AddReplica adds a replica to the replica accumulator.
func (a RRAccumulatorByTenant) AddReplica(repl CandidateReplica) {
	// Do not consider ranges as hot when they are accessed once or less times.
	if repl.RangeUsageInfo().QueriesPerSecond <= 1 {
		return
	}

	tID, ok := repl.Repl().TenantID()
	if !ok {
		return
	}

	r, ok := a[tID]
	if !ok {
		q := rrPriorityQueue{
			val: func(r CandidateReplica) float64 { return r.RangeUsageInfo().QueriesPerSecond },
		}
		heap.Push(&q, repl)
		a[tID] = q
		return
	}

	if r.Len() < numTopReplicasToTrack {
		heap.Push(&r, repl)
		a[tID] = r
		return
	}

	if repl.RangeUsageInfo().QueriesPerSecond > r.entries[0].RangeUsageInfo().QueriesPerSecond {
		heap.Pop(&r)
		heap.Push(&r, repl)
		a[tID] = r
	}
}
