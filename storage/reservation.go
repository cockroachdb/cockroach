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

// booking is an item in both the bookingQ and the used in bookie's
// reservation map.
type booking struct {
	reservation
	expireAt roachpb.Timestamp
}

// bookingQ is a queue for bookings. Since all new bookings have the same
// time until expireAt, a simple FIFO queue is sufficient.
// It is not threadsafe.
type bookingQ []*booking

// peek returns the next value in the queue without dequeuing it.
func (pq bookingQ) peek() *booking {
	if len(pq) == 0 {
		return nil
	}
	return pq[0]
}

// enqueue adds the booking to the queue.
func (pq *bookingQ) enqueue(book *booking) {
	*pq = append(*pq, book)
}

// dequeue removes the next booking from the queue.
func (pq *bookingQ) dequeue() *booking {
	if len(*pq) == 0 {
		return nil
	}
	book := (*pq)[0]
	// Remove the pointer to the removed element for more efficient gc.
	(*pq)[0] = nil
	*pq = (*pq)[1:]
	return book
}

// bookie contains a store's replica reservations.
type bookie struct {
	clock              *hlc.Clock
	reservationTimeout time.Duration // How long each reservation is held.
	metrics            *storeMetrics
	mu                 struct {
		sync.Mutex                                // Protects all values within the mu struct.
		queue        bookingQ                     // Queue used to handle expiring of reservations.
		resByRangeID map[roachpb.RangeID]*booking // All active reservations
		size         int64                        // Total bytes required for all reservations.
	}
}

// newBookie creates a reservations system and starts its timeout queue.
func newBookie(
	clock *hlc.Clock,
	reservationTimeout time.Duration,
	stopper *stop.Stopper,
	metrics *storeMetrics) *bookie {
	b := &bookie{
		clock:              clock,
		reservationTimeout: reservationTimeout,
		metrics:            metrics,
	}
	b.mu.resByRangeID = make(map[roachpb.RangeID]*booking)
	b.start(stopper)
	return b
}

// Reserve a new replica. Returns true on a successful reservation.
// TODO(bram): either here or in the store, prevent taking too many
// reservations at once.
func (b *bookie) Reserve(res reservation) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if oldRes, ok := b.mu.resByRangeID[res.rangeID]; ok {
		// If the reservation is a repeat of an already existing one, just
		// update it. Thie can occur when an RPC repeats.
		if oldRes.nodeID == res.nodeID && oldRes.storeID == res.storeID {
			// To update the reservation, fill the original one and add the
			// new one.
			if log.V(2) {
				log.Infof("updating existing reservation for rangeID:%d, %v", res.rangeID, oldRes)
			}
			b.fillBookingLocked(oldRes)
		} else {
			if log.V(2) {
				log.Infof("there is pre-existing reservation %v, can't update with %v", oldRes, res)
			}
			return false
		}
	}

	newBooking := &booking{
		reservation: res,
		expireAt:    b.clock.Now().Add(b.reservationTimeout.Nanoseconds(), 0),
	}

	b.mu.resByRangeID[res.rangeID] = newBooking
	b.mu.queue.enqueue(newBooking)
	b.mu.size += res.size

	// Update the store metrics.
	b.metrics.reservedReplicaCount.Inc(1)
	b.metrics.reserved.Inc(res.size)
	return true
}

// Fill removes a reservation. Returns true when the reservation has been
// successfully removed.
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

	b.fillBookingLocked(res)
	return true
}

// fillBookingLocked fills a booking. It requires that the reservation lock is
// held. This should only be called internally.
func (b *bookie) fillBookingLocked(res *booking) {
	// Remove it from resByRangeID. Note that we don't remove it from the queue
	// since it will expire and remove itself.
	delete(b.mu.resByRangeID, res.rangeID)

	// Adjust the total reserved size.
	b.mu.size -= res.size

	// Update the store metrics.
	b.metrics.reservedReplicaCount.Dec(1)
	b.metrics.reserved.Dec(res.size)
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
					// Is it an active reservation?
					if b.mu.resByRangeID[expiredBooking.rangeID] == expiredBooking {
						b.fillBookingLocked(expiredBooking)
					} else if log.V(2) {
						log.Infof("the resrvation for rangeID %d has already been filled.", expiredBooking.rangeID)
					}
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
