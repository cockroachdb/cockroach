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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// defaultMaxReservations is the number of concurrent reservations allowed.
	defaultMaxReservations = 1
	// defaultMaxReservedBytes is the total number of bytes that can be
	// reserved, by all active reservations, at any time.
	defaultMaxReservedBytes = 250 << 20 // 250 MiB
)

// ReservationRequest represents a request for a replica reservation.
type ReservationRequest struct {
	StoreRequestHeader
	RangeID   roachpb.RangeID
	RangeSize int64
}

// ReservationResponse represents a response from the reservation system.
type ReservationResponse struct {
	Reserved bool
}

// reservation is an item in both the reservationQ and the used in bookie's
// reservation map.
type reservation struct {
	ReservationRequest
	expireAt hlc.Timestamp
}

// reservationQ is a queue for reservations. Since all new reservations have the
// same time until expireAt, a simple FIFO queue is sufficient.
// It is not threadsafe.
type reservationQ []*reservation

// peek returns the next value in the queue without dequeuing it.
func (pq reservationQ) peek() *reservation {
	if len(pq) == 0 {
		return nil
	}
	return pq[0]
}

// enqueue adds the reservation to the queue.
func (pq *reservationQ) enqueue(book *reservation) {
	*pq = append(*pq, book)
}

// dequeue removes the next reservation from the queue.
func (pq *reservationQ) dequeue() *reservation {
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
	metrics            *StoreMetrics
	reservationTimeout time.Duration // How long each reservation is held.
	maxReservations    int           // Maximum number of allowed reservations.
	maxReservedBytes   int64         // Maximum bytes allowed for all reservations combined.
	mu                 struct {
		syncutil.Mutex                                         // Protects all values within the mu struct.
		queue                 reservationQ                     // Queue used to handle expiring of reservations.
		reservationsByRangeID map[roachpb.RangeID]*reservation // All active reservations
		size                  int64                            // Total bytes required for all reservations.
	}
}

// newBookie creates a reservations system and starts its timeout queue.
func newBookie(
	clock *hlc.Clock, stopper *stop.Stopper, metrics *StoreMetrics, reservationTimeout time.Duration,
) *bookie {
	b := &bookie{
		clock:              clock,
		metrics:            metrics,
		reservationTimeout: reservationTimeout,
		maxReservations:    envutil.EnvOrDefaultInt("COCKROACH_MAX_RESERVATIONS", defaultMaxReservations),
		maxReservedBytes:   envutil.EnvOrDefaultBytes("COCKROACH_MAX_RESERVED_BYTES", defaultMaxReservedBytes),
	}
	b.mu.reservationsByRangeID = make(map[roachpb.RangeID]*reservation)
	b.start(stopper)
	return b
}

// Reserve a new replica. Reservations can be rejected due to having too many
// outstanding reservations already or not having enough free disk space.
// Accepted reservations return a ReservationResponse with Reserved set to true.
func (b *bookie) Reserve(
	ctx context.Context, req ReservationRequest, deadReplicas []roachpb.ReplicaIdent,
) ReservationResponse {
	b.mu.Lock()
	defer b.mu.Unlock()

	resp := ReservationResponse{
		Reserved: false,
	}

	if olderReservation, ok := b.mu.reservationsByRangeID[req.RangeID]; ok {
		// If the reservation is a repeat of an already existing one, just
		// update it. This can occur when an RPC repeats.
		if olderReservation.NodeID == req.NodeID && olderReservation.StoreID == req.StoreID {
			// To update the reservation, fill the original one and add the
			// new one.
			if log.V(2) {
				log.Infof(ctx, "[r%d], updating existing reservation", req.RangeID)
			}
			b.fillReservationLocked(ctx, olderReservation)
		} else {
			if log.V(2) {
				log.Infof(ctx, "[r%d] unable to update due to pre-existing reservation", req.RangeID)
			}
			return resp
		}
	}

	// Do we have too many current reservations?
	if len(b.mu.reservationsByRangeID) >= b.maxReservations {
		if log.V(1) {
			log.Infof(ctx, "[r%d] unable to book reservation, too many reservations (current:%d, max:%d)",
				req.RangeID, len(b.mu.reservationsByRangeID), b.maxReservations)
		}
		return resp
	}

	// Can we accommodate the requested number of bytes (doubled for safety) on
	// the hard drive?
	// TODO(bram): Explore if doubling the requested size enough?
	// Store `available` in case it changes between if and log.
	available := b.metrics.Available.Value()
	if b.mu.size+(req.RangeSize*2) > available {
		if log.V(1) {
			log.Infof(ctx, "[r%d] unable to book reservation, not enough available disk space (requested:%d*2, reserved:%d, available:%d)",
				req.RangeID, req.RangeSize, b.mu.size, available)
		}
		return resp
	}

	// Do we have enough reserved space free for the reservation?
	if b.mu.size+req.RangeSize > b.maxReservedBytes {
		if log.V(1) {
			log.Infof(ctx, "[r%d] unable to book reservation, not enough available reservation space (requested:%d, reserved:%d, maxReserved:%d)",
				req.RangeID, req.RangeSize, b.mu.size, b.maxReservedBytes)
		}
		return resp
	}

	// Make sure that we don't add back a destroyed replica.
	for _, rep := range deadReplicas {
		if req.RangeID == rep.RangeID {
			if log.V(1) {
				log.Infof(ctx, "[r%d] unable to book reservation, the replica has been destroyed",
					req.RangeID)
			}
			return ReservationResponse{Reserved: false}
		}
	}

	newReservation := &reservation{
		ReservationRequest: req,
		expireAt:           b.clock.Now().Add(b.reservationTimeout.Nanoseconds(), 0),
	}

	b.mu.reservationsByRangeID[req.RangeID] = newReservation
	b.mu.queue.enqueue(newReservation)
	b.mu.size += req.RangeSize

	// Update the store metrics.
	b.metrics.ReservedReplicaCount.Inc(1)
	b.metrics.Reserved.Inc(req.RangeSize)

	if log.V(1) {
		log.Infof(ctx, "[r%s] new reservation, size=%d",
			newReservation.RangeID, newReservation.RangeSize)
	}

	resp.Reserved = true
	return resp
}

// Fill removes a reservation. Returns true when the reservation has been
// successfully removed.
func (b *bookie) Fill(ctx context.Context, rangeID roachpb.RangeID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Lookup the reservation.
	res, ok := b.mu.reservationsByRangeID[rangeID]
	if !ok {
		if log.V(2) {
			log.Infof(ctx, "[r%d] reservation not found", rangeID)
		}
		return false
	}

	b.fillReservationLocked(ctx, res)
	return true
}

// fillReservationLocked fills a reservation. It requires that the bookie's
// lock is held. This should only be called internally.
func (b *bookie) fillReservationLocked(ctx context.Context, res *reservation) {
	if log.V(2) {
		log.Infof(ctx, "[r%d] filling reservation", res.RangeID)
	}

	// Remove it from reservationsByRangeID. Note that we don't remove it from the
	// queue since it will expire and remove itself.
	delete(b.mu.reservationsByRangeID, res.RangeID)

	// Adjust the total reserved size.
	b.mu.size -= res.RangeSize

	// Update the store metrics.
	b.metrics.ReservedReplicaCount.Dec(1)
	b.metrics.Reserved.Dec(res.RangeSize)
}

// start will run continuously and expire old reservations.
func (b *bookie) start(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		var timeoutTimer timeutil.Timer
		defer timeoutTimer.Stop()
		ctx := context.TODO()
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
					expiredReservation := b.mu.queue.dequeue()
					// Is it an active reservation?
					if b.mu.reservationsByRangeID[expiredReservation.RangeID] == expiredReservation {
						b.fillReservationLocked(ctx, expiredReservation)
					} else if log.V(2) {
						log.Infof(ctx, "[r%d] expired reservation has already been filled",
							expiredReservation.RangeID)
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
