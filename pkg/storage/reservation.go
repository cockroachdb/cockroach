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

// bookie contains a store's replica reservations.
type bookie struct {
	metrics          *StoreMetrics
	maxReservations  int   // Maximum number of allowed reservations.
	maxReservedBytes int64 // Maximum bytes allowed for all reservations combined.
	mu               struct {
		syncutil.Mutex
		reservations  int
		reservedBytes int64
	}
}

// newBookie creates a reservations system.
func newBookie(metrics *StoreMetrics) *bookie {
	return &bookie{
		metrics:          metrics,
		maxReservations:  envutil.EnvOrDefaultInt("COCKROACH_MAX_RESERVATIONS", defaultMaxReservations),
		maxReservedBytes: envutil.EnvOrDefaultBytes("COCKROACH_MAX_RESERVED_BYTES", defaultMaxReservedBytes),
	}
}

// Reserve attempts to reserve a replica and returns a boolean indicating
// success. If reservation is successful, the returned function must be called
// when the reservation is no longer needed.
func (b *bookie) Reserve(ctx context.Context, req reservationRequest) (func(), bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Do we have too many current reservations?
	if b.mu.reservations >= b.maxReservations {
		if log.V(1) {
			log.Infof(ctx, "[r%d] unable to book reservation, too many reservations (current:%d, max:%d)",
				req.RangeID, b.mu.reservations, b.maxReservations)
		}
		return nil, false
	}

	// Can we accommodate the requested number of bytes (doubled for safety) on
	// the hard drive?
	// TODO(bram): Explore if doubling the requested size enough?
	// Store `available` in case it changes between if and log.
	available := b.metrics.Available.Value()
	if b.mu.reservedBytes+(req.RangeSize*2) > available {
		if log.V(1) {
			log.Infof(ctx, "[r%d] unable to book reservation, not enough available disk space (requested:%d*2, reserved:%d, available:%d)",
				req.RangeID, req.RangeSize, b.mu.reservedBytes, available)
		}
		return nil, false
	}

	// Do we have enough reserved space free for the reservation?
	if b.mu.reservedBytes+req.RangeSize > b.maxReservedBytes {
		if log.V(1) {
			log.Infof(ctx, "[r%d] unable to book reservation, not enough available reservation space (requested:%d, reserved:%d, maxReserved:%d)",
				req.RangeID, req.RangeSize, b.mu.reservedBytes, b.maxReservedBytes)
		}
		return nil, false
	}

	b.mu.reservations++
	b.mu.reservedBytes += req.RangeSize

	// Update the store metrics.
	b.metrics.ReservedReplicaCount.Inc(1)
	b.metrics.Reserved.Inc(req.RangeSize)

	if log.V(1) {
		log.Infof(ctx, "[r%s] new reservation, size=%d", req.RangeID, req.RangeSize)
	}

	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if log.V(2) {
			log.Infof(ctx, "[r%d] filling reservation", req.RangeID)
		}

		b.mu.reservations--
		b.mu.reservedBytes -= req.RangeSize

		// Update the store metrics.
		b.metrics.ReservedReplicaCount.Dec(1)
		b.metrics.Reserved.Dec(req.RangeSize)
	}, true
}
