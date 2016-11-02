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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	// defaultMaxReservations is the number of concurrent reservations allowed.
	defaultMaxReservations = 1
	// defaultMaxReservedBytes is the total number of bytes that can be
	// reserved, by all active reservations, at any time.
	defaultMaxReservedBytes = 250 << 20 // 250 MiB
)

// reservationRequest represents a request for a replica reservation.
type reservationRequest struct {
	StoreRequestHeader
	RangeID   roachpb.RangeID
	RangeSize int64
}

// reservationResponse represents a response from the reservation system.
type reservationResponse struct {
	Reserved bool
}

// bookie contains a store's replica reservations.
type bookie struct {
	metrics          *StoreMetrics
	maxReservations  int   // Maximum number of allowed reservations.
	maxReservedBytes int64 // Maximum bytes allowed for all reservations combined.
	mu               struct {
		syncutil.Mutex                                               // Protects all values within the mu struct.
		reservationsByRangeID map[roachpb.RangeID]reservationRequest // All active reservations
		size                  int64                                  // Total bytes required for all reservations.
	}
}

// newBookie creates a reservations system.
func newBookie(metrics *StoreMetrics) *bookie {
	b := &bookie{
		metrics:          metrics,
		maxReservations:  envutil.EnvOrDefaultInt("COCKROACH_MAX_RESERVATIONS", defaultMaxReservations),
		maxReservedBytes: envutil.EnvOrDefaultBytes("COCKROACH_MAX_RESERVED_BYTES", defaultMaxReservedBytes),
	}
	b.mu.reservationsByRangeID = make(map[roachpb.RangeID]reservationRequest)
	return b
}

// Reserve a new replica. Reservations can be rejected due to having too many
// outstanding reservations already or not having enough free disk space.
// Accepted reservations return a ReservationResponse with Reserved set to true.
func (b *bookie) Reserve(
	ctx context.Context, req reservationRequest, deadReplicas []roachpb.ReplicaIdent,
) reservationResponse {
	b.mu.Lock()
	defer b.mu.Unlock()

	resp := reservationResponse{
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
			return reservationResponse{Reserved: false}
		}
	}

	b.mu.reservationsByRangeID[req.RangeID] = req
	b.mu.size += req.RangeSize

	// Update the store metrics.
	b.metrics.ReservedReplicaCount.Inc(1)
	b.metrics.Reserved.Inc(req.RangeSize)

	if log.V(1) {
		log.Infof(ctx, "[r%s] new reservation, size=%d", req.RangeID, req.RangeSize)
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
func (b *bookie) fillReservationLocked(ctx context.Context, res reservationRequest) {
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
