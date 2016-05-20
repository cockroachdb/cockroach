// Copyright 2016 The Cockroach Authors.
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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// reservation contains all the info required to reserve a replica spot
// on a store.
// TODO(bram): move this to a proto that can be sent via rpc.
type reservation struct {
	rangeID roachpb.RangeID // The range looking to be rebalanced.
	storeID roachpb.StoreID // The store that requested the reservation.
	nodeID  roachpb.NodeID  // The node that requested the reservation.
	size    int64           // Approximate maximum size to reserve.
}

// booking is an item in both the bookingPQ and the used in bookie's
// reservation map.
type booking struct {
	reservation reservation
	expireAt    roachpb.Timestamp // This is also the priority for the queue.
	filled      bool              // Has this booking been filled?
	// index is required to maintain the priority queue and is updated by the
	// heap.Interface methods.
	index int
}

// bookingPQ is a priority queue for bookings (maintained by using a heap)
// sorted by the earliest expireAt time.
// bookingPQ implements the heap.Interface (which includes the sort.Interface).
// It is not threadsafe.
type bookingPQ []*booking

// Len implements the sort.Interface.
func (pq bookingPQ) Len() int {
	return len(pq)
}

// Less implements the sort.Interface.
func (pq bookingPQ) Less(i, j int) bool {
	return pq[i].expireAt.Less(pq[j].expireAt)
}

// Swap implements the sort.Interface.
func (pq bookingPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index, pq[j].index = i, j
}

// Push implements the heap.Interface.
func (pq *bookingPQ) Push(x interface{}) {
	n := len(*pq)
	item := x.(*booking)
	item.index = n
	*pq = append(*pq, item)
}

// Pop implements the heap.Interface.
func (pq *bookingPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// peek returns the next value in the priority queue without dequeuing it.
func (pq bookingPQ) peek() *booking {
	if len(pq) == 0 {
		return nil
	}
	return pq[0]
}

// enqueue adds the booking to the queue.
func (pq *bookingPQ) enqueue(book *booking) {
	heap.Push(pq, book)
}

// dequeue removes the next booking from the priority queue.
func (pq *bookingPQ) dequeue() *booking {
	if len(*pq) == 0 {
		return nil
	}
	return heap.Pop(pq).(*booking)
}

// bookie contains a store's replica reservations.
type bookie struct {
	clock              *hlc.Clock
	reservationTimeout time.Duration // How long each reservation is held.
	mu                 struct {
		sync.RWMutex                              // Protects all values within the mu struct.
		queue        bookingPQ                    // Priority Queue used to handle expiring of reservations.
		resByRangeID map[roachpb.RangeID]*booking // All active reservations
		size         int64                        // Total bytes required for all reservations.
	}
}

// newBookie creates a reservations system and starts its timeout queue.
func newBookie(clock *hlc.Clock, reservationTimeout time.Duration, stopper *stop.Stopper) *bookie {
	b := &bookie{
		clock:              clock,
		reservationTimeout: reservationTimeout,
	}
	b.mu.resByRangeID = make(map[roachpb.RangeID]*booking)
	heap.Init(&b.mu.queue)
	b.start(stopper)
	return b
}

// Outstanding returns the total number of outstanding reservations.
func (b *bookie) Outstanding() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.mu.resByRangeID)
}

// ReservedBytes returns the total bytes currently reserved.
func (b *bookie) ReservedBytes() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.mu.size
}

// Reserve reserves a new replica.
func (b *bookie) Reserve(res reservation) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if oldRes, ok := b.mu.resByRangeID[res.rangeID]; ok {
		if log.V(2) {
			log.Infof("there is already an existing reservation for rangeID:%d, %v", res.rangeID,
				oldRes)
		}
		return false
	}

	newBooking := &booking{
		reservation: res,
		expireAt:    b.clock.Now().Add(b.reservationTimeout.Nanoseconds(), 0),
	}

	b.mu.resByRangeID[res.rangeID] = newBooking
	b.mu.queue.enqueue(newBooking)
	b.mu.size += res.size
	return true
}

// Fill removes a reservation.
func (b *bookie) Fill(rangeID roachpb.RangeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Lookup the reservation.
	res, ok := b.mu.resByRangeID[rangeID]
	if !ok {
		if log.V(2) {
			log.Infof("there is no reservation for rangeID:%d", rangeID)
		}
		return false
	}

	return b.fillBookingLocked(res)
}

// fillBookingLocked fills a booking. It requires that the reservation lock is
// held. This should only be called internally.
func (b *bookie) fillBookingLocked(res *booking) bool {
	if res.filled {
		if log.V(2) {
			log.Infof("the resrvation for rangeID %d has already been filled.", res.reservation.rangeID)
		}
		return false
	}

	// Mark the reservation as filled.
	res.filled = true

	// Remove it from resByRangeID. Note that we don't remove it from the queue
	// since it will expire and remove itself.
	delete(b.mu.resByRangeID, res.reservation.rangeID)

	// Adjust the total reserved size.
	b.mu.size -= res.reservation.size
	return true
}

// start will run continuously and expire old reservations.
func (b *bookie) start(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		var timeoutTimer timeutil.Timer
		defer timeoutTimer.Stop()
		for {
			var timeout time.Duration
			b.mu.Lock()
			nextExpiration := b.mu.queue.peek()
			if nextExpiration == nil {
				// No reservations to expire.
				timeout = b.reservationTimeout
			} else {
				now := b.clock.Now()
				if now.GoTime().After(nextExpiration.expireAt.GoTime()) {
					// We have a reservation expiration, remove it.
					expiredBooking := b.mu.queue.dequeue()
					_ = b.fillBookingLocked(expiredBooking)
					// Set the timeout to 0 to force another peek.
					timeout = 0
				} else {
					timeout = nextExpiration.expireAt.GoTime().Sub(now.GoTime())
				}
			}
			b.mu.Unlock()
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
