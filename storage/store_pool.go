// Copyright 2015 The Cockroach Authors.
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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"container/heap"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// TestTimeUntilStoreDead is the test value for TimeUntilStoreDead to
	// quickly mark stores as dead.
	TestTimeUntilStoreDead = 5 * time.Millisecond

	// TestTimeUntilStoreDeadOff is the test value for TimeUntilStoreDead that
	// prevents the store pool from marking stores as dead.
	TestTimeUntilStoreDeadOff = 24 * time.Hour
)

type storeDetail struct {
	desc            *roachpb.StoreDescriptor
	dead            bool
	timesDied       int
	foundDeadOn     roachpb.Timestamp
	lastUpdatedTime roachpb.Timestamp // This is also the priority for the queue.
	index           int               // index of the item in the heap, required for heap.Interface
}

// markDead sets the storeDetail to dead(inactive).
func (sd *storeDetail) markDead(foundDeadOn roachpb.Timestamp) {
	sd.dead = true
	sd.foundDeadOn = foundDeadOn
	sd.timesDied++
	if sd.desc != nil {
		// sd.desc can still be nil if it was markedAlive and enqueued in getStoreDetailLocked
		// and never markedAlive again.
		log.Warningf("store %s on node %s is now considered offline", sd.desc.StoreID, sd.desc.Node.NodeID)
	}
}

// markAlive sets the storeDetail to alive(active) and saves the updated time
// and descriptor.
func (sd *storeDetail) markAlive(foundAliveOn roachpb.Timestamp, storeDesc *roachpb.StoreDescriptor) {
	sd.desc = storeDesc
	sd.dead = false
	sd.lastUpdatedTime = foundAliveOn
}

// storePoolPQ implements the heap.Interface (which includes sort.Interface)
// and holds storeDetail. storePoolPQ is not threadsafe.
type storePoolPQ []*storeDetail

// Len implements the sort.Interface.
func (pq storePoolPQ) Len() int {
	return len(pq)
}

// Less implements the sort.Interface.
func (pq storePoolPQ) Less(i, j int) bool {
	return pq[i].lastUpdatedTime.Less(pq[j].lastUpdatedTime)
}

// Swap implements the sort.Interface.
func (pq storePoolPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index, pq[j].index = i, j
}

// Push implements the heap.Interface.
func (pq *storePoolPQ) Push(x interface{}) {
	n := len(*pq)
	item := x.(*storeDetail)
	item.index = n
	*pq = append(*pq, item)
}

// Pop implements the heap.Interface.
func (pq *storePoolPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// peek returns the next value in the priority queue without dequeuing it.
func (pq storePoolPQ) peek() *storeDetail {
	if len(pq) == 0 {
		return nil
	}
	return (pq)[0]
}

// enqueue either adds the detail to the queue or updates its location in the
// priority queue.
func (pq *storePoolPQ) enqueue(detail *storeDetail) {
	if detail.index < 0 {
		heap.Push(pq, detail)
	} else {
		heap.Fix(pq, detail.index)
	}
}

// dequeue removes the next detail from the priority queue.
func (pq *storePoolPQ) dequeue() *storeDetail {
	if len(*pq) == 0 {
		return nil
	}
	return heap.Pop(pq).(*storeDetail)
}

// StorePool maintains a list of all known stores in the cluster and
// information on their health.
type StorePool struct {
	clock              *hlc.Clock
	timeUntilStoreDead time.Duration

	// Each storeDetail is contained in both a map and a priorityQueue; pointers
	// are used so that data can be kept in sync.
	mu     sync.RWMutex // Protects stores and queue.
	stores map[roachpb.StoreID]*storeDetail
	queue  storePoolPQ
}

// NewStorePool creates a StorePool and registers the store updating callback
// with gossip.
func NewStorePool(g *gossip.Gossip, clock *hlc.Clock, timeUntilStoreDead time.Duration, stopper *stop.Stopper) *StorePool {
	sp := &StorePool{
		clock:              clock,
		timeUntilStoreDead: timeUntilStoreDead,
		stores:             make(map[roachpb.StoreID]*storeDetail),
	}
	heap.Init(&sp.queue)

	storeRegex := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	g.RegisterCallback(storeRegex, sp.storeGossipUpdate)

	sp.start(stopper)

	return sp
}

// storeGossipUpdate is the gossip callback used to keep the StorePool up to date.
func (sp *StorePool) storeGossipUpdate(_ string, content roachpb.Value) {
	var storeDesc roachpb.StoreDescriptor
	if err := content.GetProto(&storeDesc); err != nil {
		log.Error(err)
		return
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()
	// Does this storeDetail exist yet?
	detail, ok := sp.stores[storeDesc.StoreID]
	if !ok {
		// Setting index to -1 ensures this gets added to the queue.
		detail = &storeDetail{index: -1}
		sp.stores[storeDesc.StoreID] = detail
	}
	detail.markAlive(sp.clock.Now(), &storeDesc)
	sp.queue.enqueue(detail)
}

// start will run continuously and mark stores as offline if they haven't been
// heard from in longer than timeUntilStoreDead.
func (sp *StorePool) start(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		var timeoutTimer util.Timer
		defer timeoutTimer.Stop()
		for {
			var timeout time.Duration
			sp.mu.Lock()
			detail := sp.queue.peek()
			if detail == nil {
				// No stores yet, wait the full timeout.
				timeout = sp.timeUntilStoreDead
			} else {
				// Check to see if the store should be marked as dead.
				deadAsOf := detail.lastUpdatedTime.GoTime().Add(sp.timeUntilStoreDead)
				now := sp.clock.Now()
				if now.GoTime().After(deadAsOf) {
					deadDetail := sp.queue.dequeue()
					deadDetail.markDead(now)
					// The next store might be dead as well, set the timeout to
					// 0 to process it immediately.
					timeout = 0
				} else {
					// Store is still alive, schedule the next check for when
					// it should timeout.
					timeout = deadAsOf.Sub(now.GoTime())
				}
			}
			sp.mu.Unlock()
			timeoutTimer.Reset(timeout)
			select {
			case <-timeoutTimer.C:
				timeoutTimer.Read = true
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// getStoreDetailLocked returns the store detail for the given storeID.
// The lock must be held *in write mode* even though this looks like a
// read-only method.
func (sp *StorePool) getStoreDetailLocked(storeID roachpb.StoreID) storeDetail {
	detail, ok := sp.stores[storeID]
	if !ok {
		// We don't have this store yet (this is normal when we're
		// starting up and don't have full information from the gossip
		// network). The first time this occurs, presume the store is
		// alive, but start the clock so it will become dead if enough
		// time passes without updates from gossip.
		detail = &storeDetail{index: -1}
		sp.stores[storeID] = detail
		detail.markAlive(sp.clock.Now(), nil)
		sp.queue.enqueue(detail)
	}

	return *detail
}

// getStoreDescriptor returns the latest store descriptor for the given
// storeID.
func (sp *StorePool) getStoreDescriptor(storeID roachpb.StoreID) *roachpb.StoreDescriptor {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	detail, ok := sp.stores[storeID]
	if !ok {
		return nil
	}

	return detail.desc
}

// deadReplicas returns any replicas from the supplied slice that are
// located on dead stores.
func (sp *StorePool) deadReplicas(repls []roachpb.ReplicaDescriptor) []roachpb.ReplicaDescriptor {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var deadReplicas []roachpb.ReplicaDescriptor
	for _, repl := range repls {
		if sp.getStoreDetailLocked(repl.StoreID).dead {
			deadReplicas = append(deadReplicas, repl)
		}
	}
	return deadReplicas
}

// stat provides a running sample size and mean.
type stat struct {
	n, mean float64
}

// Update adds the specified value to the stat, augmenting the sample
// size & mean.
func (s *stat) update(x float64) {
	s.n++
	s.mean += (x - s.mean) / s.n
}

// StoreList holds a list of store descriptors and associated count and used
// stats for those stores.
type StoreList struct {
	stores      []*roachpb.StoreDescriptor
	count, used stat
}

// add includes the store descriptor to the list of stores and updates
// maintained statistics.
func (sl *StoreList) add(s *roachpb.StoreDescriptor) {
	sl.stores = append(sl.stores, s)
	sl.count.update(float64(s.Capacity.RangeCount))
	sl.used.update(s.Capacity.FractionUsed())
}

// GetStoreList returns a storeList that contains all active stores that
// contain the required attributes and their associated stats. It also returns
// the number of total alive stores.
// TODO(embark, spencer): consider using a reverse index map from
// Attr->stores, for efficiency. Ensure that entries in this map still
// have an opportunity to be garbage collected.
func (sp *StorePool) getStoreList(required roachpb.Attributes, deterministic bool) (StoreList, int) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	var storeIDs roachpb.StoreIDSlice
	for storeID := range sp.stores {
		storeIDs = append(storeIDs, storeID)
	}
	// Sort the stores by key if deterministic is requested. This is only for
	// unit testing.
	if deterministic {
		sort.Sort(storeIDs)
	}
	sl := StoreList{}
	var aliveStoreCount int
	for _, storeID := range storeIDs {
		detail := sp.stores[roachpb.StoreID(storeID)]
		if !detail.dead && detail.desc != nil {
			aliveStoreCount++
			if required.IsSubset(*detail.desc.CombinedAttrs()) {
				sl.add(detail.desc)
			}
		}
	}
	return sl, aliveStoreCount
}
