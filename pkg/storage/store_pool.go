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
	"bytes"
	"container/heap"
	"fmt"
	"sort"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// TestTimeUntilStoreDead is the test value for TimeUntilStoreDead to
	// quickly mark stores as dead.
	TestTimeUntilStoreDead = 5 * time.Millisecond

	// TestTimeUntilStoreDeadOff is the test value for TimeUntilStoreDead that
	// prevents the store pool from marking stores as dead.
	TestTimeUntilStoreDeadOff = 24 * time.Hour

	// defaultFailedReservationsTimeout is the amount of time to consider the
	// store throttled for up-replication after a failed reservation call.
	defaultFailedReservationsTimeout = 5 * time.Second

	// defaultDeclinedReservationsTimeout is the amount of time to consider the
	// store throttled for up-replication after a reservation was declined.
	defaultDeclinedReservationsTimeout = 0 * time.Second
)

type storeDetail struct {
	ctx         context.Context
	desc        *roachpb.StoreDescriptor
	dead        bool
	timesDied   int
	foundDeadOn hlc.Timestamp
	// throttledUntil is when an throttled store can be considered available
	// again due to a failed or declined Reserve RPC.
	throttledUntil  time.Time
	lastUpdatedTime hlc.Timestamp // This is also the priority for the queue.
	index           int           // index of the item in the heap, required for heap.Interface
	deadReplicas    map[roachpb.RangeID][]roachpb.ReplicaDescriptor
}

// markDead sets the storeDetail to dead(inactive).
func (sd *storeDetail) markDead(foundDeadOn hlc.Timestamp) {
	sd.dead = true
	sd.foundDeadOn = foundDeadOn
	sd.timesDied++
	if sd.desc != nil {
		// sd.desc can still be nil if it was markedAlive and enqueued in getStoreDetailLocked
		// and never markedAlive again.
		log.Warningf(
			sd.ctx, "store %s on node %s is now considered offline", sd.desc.StoreID, sd.desc.Node.NodeID,
		)
	}
}

// markAlive sets the storeDetail to alive(active) and saves the updated time
// and descriptor.
func (sd *storeDetail) markAlive(foundAliveOn hlc.Timestamp, storeDesc *roachpb.StoreDescriptor) {
	sd.desc = storeDesc
	sd.dead = false
	sd.lastUpdatedTime = foundAliveOn
}

// isThrottled returns whether the store is currently throttled.
func (sd storeDetail) isThrottled(now time.Time) bool {
	return sd.throttledUntil.After(now)
}

// storeStatus is the current status of a store.
type storeStatus int

// These are the possible values for a storeStatus.
const (
	// The store is not yet available or has been timed out.
	storeStatusDead storeStatus = iota
	// The store is alive but it is throttled.
	storeStatusThrottled
	// The store is alive but a replica for the same rangeID was recently
	// discovered to be corrupt.
	storeStatusReplicaCorrupted
	// The store is alive and available.
	storeStatusAvailable
)

// status returns the current status of the store.
func (sd *storeDetail) status(now time.Time, rangeID roachpb.RangeID) storeStatus {
	// The store must be alive and it must have a descriptor to be considered
	// alive.
	if sd.dead || sd.desc == nil {
		return storeStatusDead
	}

	// The store must not have a recent declined reservation to be available.
	if sd.isThrottled(now) {
		return storeStatusThrottled
	}

	// The store must not have a corrupt replica on it.
	if len(sd.deadReplicas[rangeID]) > 0 {
		return storeStatusReplicaCorrupted
	}

	return storeStatusAvailable
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
	log.AmbientContext

	clock                       *hlc.Clock
	timeUntilStoreDead          time.Duration
	rpcContext                  *rpc.Context
	failedReservationsTimeout   time.Duration
	declinedReservationsTimeout time.Duration
	resolver                    NodeAddressResolver
	deterministic               bool
	mu                          struct {
		syncutil.RWMutex
		// Each storeDetail is contained in both a map and a priorityQueue;
		// pointers are used so that data can be kept in sync.
		storeDetails map[roachpb.StoreID]*storeDetail
		queue        storePoolPQ
	}
}

// NewStorePool creates a StorePool and registers the store updating callback
// with gossip.
func NewStorePool(
	ambient log.AmbientContext,
	g *gossip.Gossip,
	clock *hlc.Clock,
	rpcContext *rpc.Context,
	timeUntilStoreDead time.Duration,
	stopper *stop.Stopper,
	deterministic bool,
) *StorePool {
	sp := &StorePool{
		AmbientContext:     ambient,
		clock:              clock,
		timeUntilStoreDead: timeUntilStoreDead,
		rpcContext:         rpcContext,
		failedReservationsTimeout: envutil.EnvOrDefaultDuration("COCKROACH_FAILED_RESERVATION_TIMEOUT",
			defaultFailedReservationsTimeout),
		declinedReservationsTimeout: envutil.EnvOrDefaultDuration("COCKROACH_DECLINED_RESERVATION_TIMEOUT",
			defaultDeclinedReservationsTimeout),
		resolver:      GossipAddressResolver(g),
		deterministic: deterministic,
	}
	sp.mu.storeDetails = make(map[roachpb.StoreID]*storeDetail)
	heap.Init(&sp.mu.queue)
	storeRegex := gossip.MakePrefixPattern(gossip.KeyStorePrefix)
	g.RegisterCallback(storeRegex, sp.storeGossipUpdate)
	deadReplicasRegex := gossip.MakePrefixPattern(gossip.KeyDeadReplicasPrefix)
	g.RegisterCallback(deadReplicasRegex, sp.deadReplicasGossipUpdate)
	sp.start(stopper)

	return sp
}

func (sp *StorePool) String() string {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	ids := make(roachpb.StoreIDSlice, 0, len(sp.mu.storeDetails))
	for id := range sp.mu.storeDetails {
		ids = append(ids, id)
	}
	sort.Sort(ids)

	var buf bytes.Buffer
	now := timeutil.Now()

	for _, id := range ids {
		detail := sp.mu.storeDetails[id]
		fmt.Fprintf(&buf, "%d", id)
		if detail.dead {
			_, _ = buf.WriteString("*")
		}
		fmt.Fprintf(&buf, ": range-count=%d fraction-used=%.2f",
			detail.desc.Capacity.RangeCount, detail.desc.Capacity.FractionUsed())
		throttled := detail.throttledUntil.Sub(now)
		if throttled > 0 {
			fmt.Fprintf(&buf, " [throttled=%.1fs]", throttled.Seconds())
		}
		_, _ = buf.WriteString("\n")
	}
	return buf.String()
}

// storeGossipUpdate is the gossip callback used to keep the StorePool up to date.
func (sp *StorePool) storeGossipUpdate(_ string, content roachpb.Value) {
	var storeDesc roachpb.StoreDescriptor
	if err := content.GetProto(&storeDesc); err != nil {
		ctx := sp.AnnotateCtx(context.TODO())
		log.Error(ctx, err)
		return
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()
	// Does this storeDetail exist yet?
	detail := sp.getStoreDetailLocked(storeDesc.StoreID)
	detail.markAlive(sp.clock.Now(), &storeDesc)
	sp.mu.queue.enqueue(detail)
}

// deadReplicasGossipUpdate is the gossip callback used to keep the StorePool up to date.
func (sp *StorePool) deadReplicasGossipUpdate(_ string, content roachpb.Value) {
	var replicas roachpb.StoreDeadReplicas
	if err := content.GetProto(&replicas); err != nil {
		ctx := sp.AnnotateCtx(context.TODO())
		log.Error(ctx, err)
		return
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()
	detail := sp.getStoreDetailLocked(replicas.StoreID)
	deadReplicas := make(map[roachpb.RangeID][]roachpb.ReplicaDescriptor)
	for _, r := range replicas.Replicas {
		deadReplicas[r.RangeID] = append(deadReplicas[r.RangeID], r.Replica)
	}
	detail.deadReplicas = deadReplicas
}

// start will run continuously and mark stores as offline if they haven't been
// heard from in longer than timeUntilStoreDead.
func (sp *StorePool) start(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		var timeoutTimer timeutil.Timer
		defer timeoutTimer.Stop()
		for {
			var timeout time.Duration
			sp.mu.Lock()
			detail := sp.mu.queue.peek()
			if detail == nil {
				// No stores yet, wait the full timeout.
				timeout = sp.timeUntilStoreDead
			} else {
				// Check to see if the store should be marked as dead.
				deadAsOf := detail.lastUpdatedTime.GoTime().Add(sp.timeUntilStoreDead)
				now := sp.clock.Now()
				if now.GoTime().After(deadAsOf) {
					deadDetail := sp.mu.queue.dequeue()
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

// newStoreDetail makes a new storeDetail struct. It sets index to be -1 to
// ensure that it will be processed by a queue immediately.
func newStoreDetail(ctx context.Context) *storeDetail {
	return &storeDetail{
		ctx:          ctx,
		index:        -1,
		deadReplicas: make(map[roachpb.RangeID][]roachpb.ReplicaDescriptor),
	}
}

// getStoreDetailLocked returns the store detail for the given storeID.
// The lock must be held *in write mode* even though this looks like a
// read-only method.
func (sp *StorePool) getStoreDetailLocked(storeID roachpb.StoreID) *storeDetail {
	detail, ok := sp.mu.storeDetails[storeID]
	if !ok {
		// We don't have this store yet (this is normal when we're
		// starting up and don't have full information from the gossip
		// network). The first time this occurs, presume the store is
		// alive, but start the clock so it will become dead if enough
		// time passes without updates from gossip.
		ctx := sp.AnnotateCtx(context.TODO())
		detail = newStoreDetail(ctx)
		sp.mu.storeDetails[storeID] = detail
		detail.markAlive(sp.clock.Now(), nil)
		sp.mu.queue.enqueue(detail)
	}

	return detail
}

// getStoreDescriptor returns the latest store descriptor for the given
// storeID.
func (sp *StorePool) getStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if detail, ok := sp.mu.storeDetails[storeID]; ok && detail.desc != nil {
		return *detail.desc, true
	}
	return roachpb.StoreDescriptor{}, false
}

// deadReplicas returns any replicas from the supplied slice that are
// located on dead stores or dead replicas for the provided rangeID.
func (sp *StorePool) deadReplicas(
	rangeID roachpb.RangeID, repls []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var deadReplicas []roachpb.ReplicaDescriptor
outer:
	for _, repl := range repls {
		detail := sp.getStoreDetailLocked(repl.StoreID)
		// Mark replica as dead if store is dead.
		if detail.dead {
			deadReplicas = append(deadReplicas, repl)
			continue
		}

		for _, deadRepl := range detail.deadReplicas[rangeID] {
			if deadRepl.ReplicaID == repl.ReplicaID {
				deadReplicas = append(deadReplicas, repl)
				continue outer
			}
		}
	}
	return deadReplicas
}

// stat provides a running sample size and running stats.
type stat struct {
	n, mean, s float64
}

// Update adds the specified value to the stat, augmenting the running stats.
func (s *stat) update(x float64) {
	s.n++
	oldMean := s.mean
	s.mean += (x - s.mean) / s.n

	// Update variable used to calculate running standard deviation. See: Knuth
	// TAOCP, vol 2, 3rd ed, page 232.
	s.s = s.s + (x-oldMean)*(x-s.mean)
}

// StoreList holds a list of store descriptors and associated count and used
// stats for those stores.
type StoreList struct {
	stores      []roachpb.StoreDescriptor
	count, used stat

	// candidateCount tracks range count stats for stores that are eligible to
	// be rebalance targets (their used capacity percentage must be lower than
	// maxFractionUsedThreshold).
	candidateCount stat
}

// Generates a new store list based on the passed in descriptors. It will
// maintain the order of those descriptors.
func makeStoreList(descriptors []roachpb.StoreDescriptor) StoreList {
	sl := StoreList{stores: descriptors}
	for _, desc := range descriptors {
		sl.count.update(float64(desc.Capacity.RangeCount))
		sl.used.update(desc.Capacity.FractionUsed())
		if desc.Capacity.FractionUsed() <= maxFractionUsedThreshold {
			sl.candidateCount.update(float64(desc.Capacity.RangeCount))
		}
	}
	return sl
}

func (sl StoreList) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "  candidate-count: mean=%v\n", sl.candidateCount.mean)
	for _, desc := range sl.stores {
		fmt.Fprintf(&buf, "  %d: range-count=%d fraction-used=%.2f\n",
			desc.StoreID, desc.Capacity.RangeCount, desc.Capacity.FractionUsed())
	}
	return buf.String()
}

// filter takes a store list and filters it using the passed in constraints. It
// maintains the original order of the passed in store list.
func (sl StoreList) filter(constraints config.Constraints) StoreList {
	if len(constraints.Constraints) == 0 {
		return sl
	}
	var filteredDescs []roachpb.StoreDescriptor
storeLoop:
	for _, store := range sl.stores {
		m := map[string]struct{}{}
		for _, s := range store.CombinedAttrs().Attrs {
			m[s] = struct{}{}
		}
		for _, c := range constraints.Constraints {
			if _, ok := m[c.Value]; !ok {
				continue storeLoop
			}
		}
		filteredDescs = append(filteredDescs, store)
	}

	return makeStoreList(filteredDescs)
}

// getStoreList returns a storeList that contains all active stores that
// contain the required attributes and their associated stats. It also returns
// the total number of alive and throttled stores.
func (sp *StorePool) getStoreList(rangeID roachpb.RangeID) (StoreList, int, int) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	var storeIDs roachpb.StoreIDSlice
	for storeID := range sp.mu.storeDetails {
		storeIDs = append(storeIDs, storeID)
	}

	if sp.deterministic {
		sort.Sort(storeIDs)
	}

	var aliveStoreCount int
	var throttledStoreCount int
	var storeDescriptors []roachpb.StoreDescriptor

	now := sp.clock.PhysicalTime()
	for _, storeID := range storeIDs {
		detail := sp.mu.storeDetails[storeID]
		switch detail.status(now, rangeID) {
		case storeStatusThrottled:
			aliveStoreCount++
			throttledStoreCount++
		case storeStatusReplicaCorrupted:
			aliveStoreCount++
		case storeStatusAvailable:
			aliveStoreCount++
			storeDescriptors = append(storeDescriptors, *detail.desc)
		}
	}

	return makeStoreList(storeDescriptors), aliveStoreCount, throttledStoreCount
}

type throttleReason int

const (
	_ throttleReason = iota
	throttleDeclined
	throttleFailed
)

// throttle informs the store pool that the given remote store declined a
// snapshot or failed to apply one, ensuring that it will not be considered
// for up-replication or rebalancing until after the configured timeout period
// has elapsed. Declined being true indicates that the remote store explicitly
// declined a snapshot.
func (sp *StorePool) throttle(reason throttleReason, toStoreID roachpb.StoreID) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	detail := sp.getStoreDetailLocked(toStoreID)
	ctx := sp.AnnotateCtx(context.TODO())

	// If a snapshot is declined, be it due to an error or because it was
	// rejected, we mark the store detail as having been declined so it won't
	// be considered as a candidate for new replicas until after the configured
	// timeout period has passed.
	switch reason {
	case throttleDeclined:
		detail.throttledUntil = sp.clock.Now().GoTime().Add(sp.declinedReservationsTimeout)
		if log.V(2) {
			log.Infof(ctx, "snapshot declined, store:%s will be throttled for %s until %s",
				toStoreID, sp.declinedReservationsTimeout, detail.throttledUntil)
		}
	case throttleFailed:
		detail.throttledUntil = sp.clock.Now().GoTime().Add(sp.failedReservationsTimeout)
		if log.V(2) {
			log.Infof(ctx, "snapshot failed, store:%s will be throttled for %s until %s",
				toStoreID, sp.failedReservationsTimeout, detail.throttledUntil)
		}
	}
}

// updateRemoteCapacityEstimate updates the StorePool's estimate of the given
// remote store's capacity.
func (sp *StorePool) updateRemoteCapacityEstimate(
	toStoreID roachpb.StoreID, capacity roachpb.StoreCapacity,
) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	// During tests, we might not have received a gossip update before trying to
	// send a snapshot. In that case, desc could be nil here.
	desc := sp.getStoreDetailLocked(toStoreID).desc
	if desc != nil {
		// TODO(jordan,bram): Consider updating the full capacity here.
		desc.Capacity.RangeCount = capacity.RangeCount
	}
}
