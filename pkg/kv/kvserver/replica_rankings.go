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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	// TODO(aayush): Scale this up based on the number of replicas on a store?
	numTopReplicasToTrack = 128
)

type replicaWithStats struct {
	repl *Replica
	qps  float64
	// TODO(aayush): Include writes-per-second and logicalBytes of storage?
}

// replicaRankings maintains top-k orderings of the replicas in a store by QPS.
type replicaRankings struct {
	mu struct {
		syncutil.Mutex
		qpsAccumulator *rrAccumulator
		byQPS          []replicaWithStats
	}
}

func newReplicaRankings() *replicaRankings {
	return &replicaRankings{}
}

func (rr *replicaRankings) newAccumulator() *rrAccumulator {
	res := &rrAccumulator{}
	res.qps.val = func(r replicaWithStats) float64 { return r.qps }
	return res
}

func (rr *replicaRankings) update(acc *rrAccumulator) {
	rr.mu.Lock()
	rr.mu.qpsAccumulator = acc
	rr.mu.Unlock()
}

func (rr *replicaRankings) topQPS() []replicaWithStats {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	// If we have a new set of data, consume it. Otherwise, just return the most
	// recently consumed data.
	if rr.mu.qpsAccumulator.qps.Len() > 0 {
		rr.mu.byQPS = consumeAccumulator(&rr.mu.qpsAccumulator.qps)
	}
	return rr.mu.byQPS
}

// rrAccumulator is used to update the replicas tracked by replicaRankings.
// The typical pattern should be to call replicaRankings.newAccumulator, add
// all the replicas you care about to the accumulator using addReplica, then
// pass the accumulator back to the replicaRankings using the update method.
// This method of loading the new rankings all at once avoids interfering with
// any consumers that are concurrently reading from the rankings, and also
// prevents concurrent loaders of data from messing with each other -- the last
// `update`d accumulator will win.
type rrAccumulator struct {
	qps rrPriorityQueue
}

func (a *rrAccumulator) addReplica(repl replicaWithStats) {
	// If the heap isn't full, just push the new replica and return.
	if a.qps.Len() < numTopReplicasToTrack {
		heap.Push(&a.qps, repl)
		return
	}

	// Otherwise, conditionally push if the new replica is more deserving than
	// the current tip of the heap.
	if repl.qps > a.qps.entries[0].qps {
		heap.Pop(&a.qps)
		heap.Push(&a.qps, repl)
	}
}

func consumeAccumulator(pq *rrPriorityQueue) []replicaWithStats {
	length := pq.Len()
	sorted := make([]replicaWithStats, length)
	for i := 1; i <= length; i++ {
		sorted[length-i] = heap.Pop(pq).(replicaWithStats)
	}
	return sorted
}

type rrPriorityQueue struct {
	entries []replicaWithStats
	val     func(replicaWithStats) float64
}

func (pq rrPriorityQueue) Len() int { return len(pq.entries) }

func (pq rrPriorityQueue) Less(i, j int) bool {
	return pq.val(pq.entries[i]) < pq.val(pq.entries[j])
}

func (pq rrPriorityQueue) Swap(i, j int) {
	pq.entries[i], pq.entries[j] = pq.entries[j], pq.entries[i]
}

func (pq *rrPriorityQueue) Push(x interface{}) {
	item := x.(replicaWithStats)
	pq.entries = append(pq.entries, item)
}

func (pq *rrPriorityQueue) Pop() interface{} {
	old := pq.entries
	n := len(old)
	item := old[n-1]
	pq.entries = old[0 : n-1]
	return item
}
