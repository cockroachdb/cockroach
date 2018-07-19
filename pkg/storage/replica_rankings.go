// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	numTopReplicasToTrack = 128
)

type replicaWithStats struct {
	repl *Replica
	qps  float64
}

// replicaRankings maintains top-k orderings of the replicas in a store along
// different dimensions of concern, such as QPS, keys written per second, and
// disk used.
type replicaRankings struct {
	mu struct {
		syncutil.Mutex
		accumulator *rrAccumulator
		byQPS       []replicaWithStats
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
	rr.mu.accumulator = acc
	rr.mu.Unlock()
}

func (rr *replicaRankings) topQPS() replicaWithStats {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if len(rr.mu.byQPS) == 0 && rr.mu.accumulator.qps.Len() > 0 {
		consumeAccumulator(&rr.mu.accumulator.qps, &rr.mu.byQPS)
	}
	if len(rr.mu.byQPS) > 0 {
		retval := rr.mu.byQPS[0]
		rr.mu.byQPS = rr.mu.byQPS[1:]
		return retval
	}
	return replicaWithStats{}
}

func (a *rrAccumulator) addReplica(repl replicaWithStats) {
	heap.Push(&a.qps, repl)
	if a.qps.Len() > numTopReplicasToTrack {
		heap.Pop(&a.qps)
	}
}

type rrAccumulator struct {
	qps rrPriorityQueue
}

func consumeAccumulator(pq *rrPriorityQueue, sorted *[]replicaWithStats) {
	length := pq.Len()
	if cap(*sorted) < length {
		*sorted = make([]replicaWithStats, length)
	} else {
		*sorted = (*sorted)[:length]
	}
	for i := 1; i <= length; i++ {
		(*sorted)[length-i] = heap.Pop(pq).(replicaWithStats)
	}
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
