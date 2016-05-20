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

// booking is an item in both the bookingPQ and the used in bookie's
// reservation map.
type booking struct {
	reservation reservation
	expireAt    roachpb.Timestamp // This is also the priority for the queue.
	filled      bool              // Has this booking been filled?
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
	mu                 struct {
		sync.Mutex                                // Protects all values within the mu struct.
		queue        bookingQ                     // Queue used to handle expiring of reservations.
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
	b.mu.queue = []*booking{}
	b.start(stopper)
	return b
}

// Outstanding returns the total number of outstanding reservations.
func (b *bookie) Outstanding() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.mu.resByRangeID)
}

// ReservedBytes returns the total bytes currently reserved.
func (b *bookie) ReservedBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.size
}

// Reserve a new replica. Returns true on a successful reservation.
func (b *bookie) Reserve(res reservation) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if oldRes, ok := b.mu.resByRangeID[res.rangeID]; ok {
		// If the reservation is a repeat of an already existing one, just
		// update it. Thie can occur when an RPC repeats.
		if oldRes.reservation.nodeID == res.nodeID &&
			oldRes.reservation.storeID == res.storeID {
			// To update the reservation, fill the original one and add the
			// new one.
			if !b.fillBookingLocked(oldRes) {
				if log.V(2) {
					log.Infof("could not update reservation %d, %v", res.rangeID, oldRes)
				}
				return false
			}
			if log.V(2) {
				log.Infof("updating existing reservation for rangeID:%d, %v", res.rangeID, oldRes)
			}
		} else {
			if log.V(2) {
				log.Infof("there is pre-existing reservation for rangeID:%d, %v", res.rangeID, oldRes)
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
