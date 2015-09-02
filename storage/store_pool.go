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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"container/heap"
	"sort"
	"sync"
	"time"

	gogoproto "github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

const (
	// testTimeUntilStoreDead is the the test value for TimeUntilStoreDead to
	// quickly mark stores as dead.
	testTimeUntilStoreDead = 5 * time.Millisecond

	// TestTimeUntilStoreDeadOff is the test value for TimeUntilStoreDead that
	// prevents the store pool from marking stores as dead.
	TestTimeUntilStoreDeadOff = 24 * time.Hour
)

type storeDetail struct {
	desc            proto.StoreDescriptor
	dead            bool
	gossiped        bool // Was this store updated via gossip?
	timesDied       int
	foundDeadOn     time.Time
	lastUpdatedTime time.Time // This is also the priority for the queue.
	index           int       // index of the item in the heap, required for heap.Interface
}

// markDead sets the storeDetail to dead(inactive).
func (sd *storeDetail) markDead(foundDeadOn time.Time) {
	sd.dead = true
	sd.foundDeadOn = foundDeadOn
	sd.timesDied++
	log.Warningf("store %s on node %s is now considered offline", sd.desc.StoreID, sd.desc.Node.NodeID)
}

// markAlive sets the storeDetail to alive(active) and saves the updated time
// and descriptor.
func (sd *storeDetail) markAlive(foundAliveOn time.Time, storeDesc proto.StoreDescriptor, gossiped bool) {
	sd.desc = storeDesc
	sd.dead = false
	sd.gossiped = gossiped
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
	return pq[i].lastUpdatedTime.Before(pq[j].lastUpdatedTime)
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
	gossip             *gossip.Gossip
	clock              *hlc.Clock
	timeUntilStoreDead time.Duration

	mu     sync.RWMutex // Protects stores and queue.
	stores map[proto.StoreID]*storeDetail
	queue  storePoolPQ
}

// NewStorePool creates a StorePool and registers the store updating callback
// with gossip.
func NewStorePool(g *gossip.Gossip, timeUntilStoreDead time.Duration, stopper *stop.Stopper) *StorePool {
	sp := &StorePool{
		timeUntilStoreDead: timeUntilStoreDead,
		stores:             make(map[proto.StoreID]*storeDetail),
	}
	heap.Init(&sp.queue)

	storeRegex := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	g.RegisterCallback(storeRegex, sp.storeGossipUpdate)

	sp.start(stopper)

	return sp
}

// storeGossipUpdate The gossip callback used to keep the StorePool up to date.
func (sp *StorePool) storeGossipUpdate(_ string, content []byte) {
	var storeDesc proto.StoreDescriptor
	if err := gogoproto.Unmarshal(content, &storeDesc); err != nil {
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
	detail.markAlive(time.Now(), storeDesc, true)
	sp.queue.enqueue(detail)
}

// start will run continuously and mark stores as offline if they haven't been
// heard from in longer than timeUntilStoreDead.
func (sp *StorePool) start(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		for {
			var timeout time.Duration
			sp.mu.Lock()
			detail := sp.queue.peek()
			if detail == nil {
				// No stores yet, wait the full timeout.
				timeout = sp.timeUntilStoreDead
			} else {
				// Check to see if the store should be marked as dead.
				deadAsOf := detail.lastUpdatedTime.Add(sp.timeUntilStoreDead)
				now := time.Now()
				if now.After(deadAsOf) {
					deadDetail := sp.queue.dequeue()
					deadDetail.markDead(now)
					// The next store might be dead as well, set the timeout to
					// 0 to process it immediately.
					timeout = 0
				} else {
					// Store is still alive, schedule the next check for when
					// it should timeout.
					timeout = deadAsOf.Sub(now)
				}
			}
			sp.mu.Unlock()
			select {
			case <-time.After(timeout):
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// GetStoreDescriptor returns the store detail for the given storeID.
func (sp *StorePool) getStoreDetail(storeID proto.StoreID) storeDetail {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	detail, ok := sp.stores[storeID]
	if !ok {
		// We don't seem to have that store yet, create a new detail and add
		// it to the queue. This will give it the full timeout before it is
		// considered dead.
		detail = &storeDetail{index: -1}
		sp.stores[storeID] = detail
		detail.markAlive(time.Now(), proto.StoreDescriptor{StoreID: storeID}, false)
		sp.queue.enqueue(detail)
	}

	return *detail
}

// GetStoreDescriptor returns the latest store descriptor for the given
// storeID.
func (sp *StorePool) getStoreDescriptor(storeID proto.StoreID) *proto.StoreDescriptor {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	detail, ok := sp.stores[storeID]
	if !ok {
		return nil
	}

	// Only return gossiped stores.
	if !detail.gossiped {
		return nil
	}

	return &detail.desc
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
	stores      []*proto.StoreDescriptor
	count, used stat
}

// add includes the store descriptor to the list of stores and updates
// maintained statistics.
func (sl *StoreList) add(s *proto.StoreDescriptor) {
	sl.stores = append(sl.stores, s)
	sl.count.update(float64(s.Capacity.RangeCount))
	sl.used.update(s.Capacity.FractionUsed())
}

// GetStoreList returns a storeList that contains all active stores that
// contain the required attributes and their associated stats.
// TODO(embark, spencer): consider using a reverse index map from
// Attr->stores, for efficiency. Ensure that entries in this map still
// have an opportunity to be garbage collected.
func (sp *StorePool) getStoreList(required proto.Attributes, deterministic bool) *StoreList {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	// TODO(bram): Consider adding the sort interface to proto.StoreID.
	var storeIDs []int
	for storeID := range sp.stores {
		storeIDs = append(storeIDs, int(storeID))
	}
	// Sort the stores by key if deterministic is requested. This is only for
	// unit testing.
	if deterministic {
		sort.Ints(storeIDs)
	}
	sl := new(StoreList)
	for _, storeID := range storeIDs {
		detail := sp.stores[proto.StoreID(storeID)]
		if !detail.dead && required.IsSubset(*detail.desc.CombinedAttrs()) {
			sl.add(&detail.desc)
		}
	}
	return sl
}
