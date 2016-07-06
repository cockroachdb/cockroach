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

package storage

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

// createTestBookie creates a new bookie, stopper and manual clock for testing.
func createTestBookie(
	reservationTimeout time.Duration,
	maxReservations int,
	maxReservedBytes int64,
) (*stop.Stopper, *hlc.ManualClock, *bookie) {
	stopper := stop.NewStopper()
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	b := newBookie(clock, stopper, newStoreMetrics(), reservationTimeout)
	// Lock the bookie to prevent the main loop from running as we change some
	// of the bookie's state.
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maxReservations = maxReservations
	b.maxReservedBytes = maxReservedBytes
	// Set a high number for a mocked total available space.
	b.metrics.available.Update(defaultMaxReservedBytes * 10)
	return stopper, mc, b
}

// verifyBookie ensures that the correct number of reservations, reserved bytes,
// and that the expirationQueue's length are correct.
func verifyBookie(t *testing.T, b *bookie, reservations, queueLen int, reservedBytes int64) {
	if e, a := reservedBytes, b.metrics.reserved.Count(); e != a {
		t.Error(errors.Errorf("expected total bytes reserved to be %d, got %d", e, a))
	}
	if e, a := reservations, int(b.metrics.reservedReplicaCount.Count()); e != a {
		t.Error(errors.Errorf("expected total reservations to be %d, got %d", e, a))
	}
	if e, a := queueLen, bookieQueueLen(b); e != a {
		t.Error(errors.Errorf("expected total queue length to be %d, got %d", e, a))
	}
}

// bookieQueueLen returns the length of the bookie queue.
func bookieQueueLen(b *bookie) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.mu.queue)
}

// TestBookieReserve ensures that you can never have more than one reservation
// for a specific rangeID at a time, and that both `Reserve` and `Fill` function
// correctly.
func TestBookieReserve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, b := createTestBookie(time.Hour, defaultMaxReservations, defaultMaxReservedBytes)
	defer stopper.Stop()

	testCases := []struct {
		rangeID         int
		reserve         bool                   // true for reserve, false for fill
		expSuc          bool                   // is the operation expected to succeed
		expOut          int                    // expected number of reserved replicas
		expBytes        int64                  // expected number of bytes being reserved
		expReservations int                    // expected number of reservations held in the bookie's queue
		deadReplicas    []roachpb.ReplicaIdent // dead replicas that we should not reserve over
	}{
		{rangeID: 1, reserve: true, expSuc: true, expOut: 1, expBytes: 1, expReservations: 1},
		{rangeID: 1, reserve: true, expSuc: false, expOut: 1, expBytes: 1, expReservations: 1},
		{rangeID: 1, reserve: false, expSuc: true, expOut: 0, expBytes: 0, expReservations: 1},
		{rangeID: 1, reserve: false, expSuc: false, expOut: 0, expBytes: 0, expReservations: 1},
		{rangeID: 2, reserve: true, expSuc: true, expOut: 1, expBytes: 2, expReservations: 2},
		{rangeID: 3, reserve: true, expSuc: true, expOut: 2, expBytes: 5, expReservations: 3},
		{rangeID: 1, reserve: true, expSuc: true, expOut: 3, expBytes: 6, expReservations: 4},
		{rangeID: 2, reserve: true, expSuc: false, expOut: 3, expBytes: 6, expReservations: 4},
		{rangeID: 2, reserve: false, expSuc: true, expOut: 2, expBytes: 4, expReservations: 4},
		{rangeID: 2, reserve: false, expSuc: false, expOut: 2, expBytes: 4, expReservations: 4},
		{rangeID: 3, reserve: false, expSuc: true, expOut: 1, expBytes: 1, expReservations: 4},
		{rangeID: 1, reserve: false, expSuc: true, expOut: 0, expBytes: 0, expReservations: 4},
		{rangeID: 2, reserve: false, expSuc: false, expOut: 0, expBytes: 0, expReservations: 4},
		{rangeID: 0, reserve: true, expSuc: false, expOut: 0, expBytes: 0, expReservations: 4, deadReplicas: []roachpb.ReplicaIdent{{RangeID: 0}}},
		{rangeID: 0, reserve: true, expSuc: true, expOut: 1, expBytes: 0, expReservations: 5, deadReplicas: []roachpb.ReplicaIdent{{RangeID: 1}}},
		{rangeID: 0, reserve: false, expSuc: true, expOut: 0, expBytes: 0, expReservations: 5},
	}

	for i, testCase := range testCases {
		if testCase.reserve {
			// Try to reserve the range.
			req := roachpb.ReservationRequest{
				StoreRequestHeader: roachpb.StoreRequestHeader{
					StoreID: roachpb.StoreID(i),
					NodeID:  roachpb.NodeID(i),
				},
				RangeID:   roachpb.RangeID(testCase.rangeID),
				RangeSize: int64(testCase.rangeID),
			}
			if resp := b.Reserve(context.Background(), req, testCase.deadReplicas); resp.Reserved != testCase.expSuc {
				if testCase.expSuc {
					t.Errorf("%d: expected a successful reservation, was rejected", i)
				} else {
					t.Errorf("%d: expected no reservation, but it was accepted", i)
				}
			}
		} else {
			// Fill the reservation.
			if filled := b.Fill(roachpb.RangeID(testCase.rangeID)); filled != testCase.expSuc {
				if testCase.expSuc {
					t.Errorf("%d: expected a successful filled reservation, was rejected", i)
				} else {
					t.Errorf("%d: expected no reservation to be filled, but it was accepted", i)
				}
			}
		}

		verifyBookie(t, b, testCase.expOut, testCase.expReservations, testCase.expBytes)
	}

	// Test that repeated requests with the same store and node number extend
	// the timeout of the pre-existing reservation.
	repeatReq := roachpb.ReservationRequest{
		StoreRequestHeader: roachpb.StoreRequestHeader{
			StoreID: 100,
			NodeID:  100,
		},
		RangeID:   100,
		RangeSize: 100,
	}
	queueLen := bookieQueueLen(b)
	for i := 1; i < 10; i++ {
		if !b.Reserve(context.Background(), repeatReq, nil).Reserved {
			t.Errorf("%d: could not add repeated reservation", i)
		}
		verifyBookie(t, b, 1, queueLen+i, 100)
	}
	queueLen = bookieQueueLen(b)

	// Test rejecting a reservation due to disk space constraints.
	overfilledReq := roachpb.ReservationRequest{
		StoreRequestHeader: roachpb.StoreRequestHeader{
			StoreID: 200,
			NodeID:  200,
		},
		RangeID:   200,
		RangeSize: 200,
	}

	b.mu.Lock()
	// Set the bytes have 1 less byte free than needed by the reservation.
	b.metrics.available.Update(b.mu.size + (2 * overfilledReq.RangeSize) - 1)
	b.mu.Unlock()

	if b.Reserve(context.Background(), overfilledReq, nil).Reserved {
		t.Errorf("expected reservation to fail due to disk space constraints, but it succeeded")
	}
	verifyBookie(t, b, 1, queueLen, 100) // The same numbers from the last call to verifyBookie.
}

// TestBookieReserveMaxRanges ensures that over-booking doesn't occur when there
// are already maxReservations.
func TestBookieReserveMaxRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	previousReserved := 10

	stopper, _, b := createTestBookie(time.Hour, previousReserved, defaultMaxReservedBytes)
	defer stopper.Stop()

	// Load up reservations.
	for i := 1; i <= previousReserved; i++ {
		req := roachpb.ReservationRequest{
			StoreRequestHeader: roachpb.StoreRequestHeader{
				StoreID: roachpb.StoreID(i),
				NodeID:  roachpb.NodeID(i),
			},
			RangeID:   roachpb.RangeID(i),
			RangeSize: 1,
		}
		if !b.Reserve(context.Background(), req, nil).Reserved {
			t.Errorf("%d: could not add reservation", i)
		}
		verifyBookie(t, b, i, i, int64(i))
	}

	overbookedReq := roachpb.ReservationRequest{
		StoreRequestHeader: roachpb.StoreRequestHeader{
			StoreID: roachpb.StoreID(previousReserved + 1),
			NodeID:  roachpb.NodeID(previousReserved + 1),
		},
		RangeID:   roachpb.RangeID(previousReserved + 1),
		RangeSize: 1,
	}
	if b.Reserve(context.Background(), overbookedReq, nil).Reserved {
		t.Errorf("expected reservation to fail due to too many already existing reservations, but it succeeded")
	}
	// The same numbers from the last call to verifyBookie.
	verifyBookie(t, b, previousReserved, previousReserved, int64(previousReserved))
}

// TestBookieReserveMaxBytes ensures that over-booking doesn't occur when trying
// to reserve more bytes than maxReservedBytes.
func TestBookieReserveMaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	previousReservedBytes := 10

	stopper, _, b := createTestBookie(time.Hour, previousReservedBytes*2, int64(previousReservedBytes))
	defer stopper.Stop()

	// Load up reservations with a size of 1 each.
	for i := 1; i <= previousReservedBytes; i++ {
		req := roachpb.ReservationRequest{
			StoreRequestHeader: roachpb.StoreRequestHeader{
				StoreID: roachpb.StoreID(i),
				NodeID:  roachpb.NodeID(i),
			},
			RangeID:   roachpb.RangeID(i),
			RangeSize: 1,
		}
		if !b.Reserve(context.Background(), req, nil).Reserved {
			t.Errorf("%d: could not add reservation", i)
		}
		verifyBookie(t, b, i, i, int64(i))
	}

	overbookedReq := roachpb.ReservationRequest{
		StoreRequestHeader: roachpb.StoreRequestHeader{
			StoreID: roachpb.StoreID(previousReservedBytes + 1),
			NodeID:  roachpb.NodeID(previousReservedBytes + 1),
		},
		RangeID:   roachpb.RangeID(previousReservedBytes + 1),
		RangeSize: 1,
	}
	if b.Reserve(context.Background(), overbookedReq, nil).Reserved {
		t.Errorf("expected reservation to fail due to too many already existing reservations, but it succeeded")
	}
	// The same numbers from the last call to verifyBookie.
	verifyBookie(t, b, previousReservedBytes, previousReservedBytes, int64(previousReservedBytes))
}

// expireNextReservation advances the manual clock to one nanosecond passed the
// next expiring reservation and waits until exactly one reservation has expired.
func expireNextReservation(t *testing.T, mc *hlc.ManualClock, b *bookie) {
	b.mu.Lock()
	nextExpiredReservation := b.mu.queue.peek()
	if nextExpiredReservation == nil {
		t.Fatalf("expected at least one reservation, but there are none")
	}
	expectedExpires := len(b.mu.queue) - 1
	// Set the clock to after next timeout.
	mc.Set(nextExpiredReservation.expireAt.WallTime + 1)
	b.mu.Unlock()

	util.SucceedsSoon(t, func() error {
		b.mu.Lock()
		defer b.mu.Unlock()
		if expectedExpires != len(b.mu.queue) {
			nextExpiredReservation := b.mu.queue.peek()
			return fmt.Errorf("expiration has not yet occurred, next expiration in %s for rangeID:%d",
				nextExpiredReservation.expireAt, nextExpiredReservation.RangeID)
		}
		return nil
	})
}

// TestReservationQueue checks to ensure that the expiration loop functions
// correctly expiring any unfilled reservations in a number of different cases.
func TestReservationQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test loads up 7 reservations at once, so set the queue higher to
	// accommodate them.
	stopper, mc, b := createTestBookie(time.Microsecond, 20, defaultMaxReservedBytes)
	defer stopper.Stop()

	bytesPerReservation := int64(100)

	// Load a collection of reservations into the bookie.
	for i := 1; i <= 10; i++ {
		// Ensure all the reservations expire 100 nanoseconds apart.
		mc.Increment(100)
		if !b.Reserve(context.Background(), roachpb.ReservationRequest{
			StoreRequestHeader: roachpb.StoreRequestHeader{
				StoreID: roachpb.StoreID(i),
				NodeID:  roachpb.NodeID(i),
			},
			RangeID:   roachpb.RangeID(i),
			RangeSize: bytesPerReservation,
		}, nil).Reserved {
			t.Fatalf("could not book a reservation for reservation number %d", i)
		}
	}
	verifyBookie(t, b, 10 /*reservations*/, 10 /*queue*/, 10*bytesPerReservation /*bytes*/)

	// Fill reservation 2.
	if !b.Fill(2) {
		t.Fatalf("Could not fill reservation 2")
	}
	// After filling a reservation, wait a full cycle so that it can be timed
	// out.
	verifyBookie(t, b, 9 /*reservations*/, 10 /*queue*/, 9*bytesPerReservation /*bytes*/)

	// Expire reservation 1.
	expireNextReservation(t, mc, b)
	verifyBookie(t, b, 8 /*reservations*/, 9 /*queue*/, 8*bytesPerReservation /*bytes*/)

	// Fill reservations 4 and 6.
	if !b.Fill(4) {
		t.Fatalf("Could not fill reservation 4")
	}
	if !b.Fill(6) {
		t.Fatalf("Could not fill reservation 6")
	}
	verifyBookie(t, b, 6 /*reservations*/, 9 /*queue*/, 6*bytesPerReservation /*bytes*/)

	expireNextReservation(t, mc, b) // Expire 2 (already filled)
	verifyBookie(t, b, 6 /*reservations*/, 8 /*queue*/, 6*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 3
	verifyBookie(t, b, 5 /*reservations*/, 7 /*queue*/, 5*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 4 (already filled)
	verifyBookie(t, b, 5 /*reservations*/, 6 /*queue*/, 5*bytesPerReservation /*bytes*/)

	// Add three new reservations, 1 and 2, which have already been filled and
	// timed out, and 6, which has been filled by not timed out. Only increment
	// by 10 here to ensure we don't expire any of the other reservations.
	mc.Increment(10)
	if !b.Reserve(context.Background(), roachpb.ReservationRequest{
		StoreRequestHeader: roachpb.StoreRequestHeader{
			StoreID: roachpb.StoreID(11),
			NodeID:  roachpb.NodeID(11),
		},
		RangeID:   roachpb.RangeID(1),
		RangeSize: bytesPerReservation,
	}, nil).Reserved {
		t.Fatalf("could not book a reservation for reservation number 1 (second pass)")
	}
	verifyBookie(t, b, 6 /*reservations*/, 7 /*queue*/, 6*bytesPerReservation /*bytes*/)

	mc.Increment(10)
	if !b.Reserve(context.Background(), roachpb.ReservationRequest{
		StoreRequestHeader: roachpb.StoreRequestHeader{
			StoreID: roachpb.StoreID(12),
			NodeID:  roachpb.NodeID(12),
		},
		RangeID:   roachpb.RangeID(2),
		RangeSize: bytesPerReservation,
	}, nil).Reserved {
		t.Fatalf("could not book a reservation for reservation number 2 (second pass)")
	}
	verifyBookie(t, b, 7 /*reservations*/, 8 /*queue*/, 7*bytesPerReservation /*bytes*/)

	mc.Increment(10)
	if !b.Reserve(context.Background(), roachpb.ReservationRequest{
		StoreRequestHeader: roachpb.StoreRequestHeader{
			StoreID: roachpb.StoreID(13),
			NodeID:  roachpb.NodeID(13),
		},
		RangeID:   roachpb.RangeID(6),
		RangeSize: bytesPerReservation,
	}, nil).Reserved {
		t.Fatalf("could not book a reservation for reservation number 6 (second pass)")
	}
	verifyBookie(t, b, 8 /*reservations*/, 9 /*queue*/, 8*bytesPerReservation /*bytes*/)

	// Fill 1 a second time.
	if !b.Fill(1) {
		t.Fatalf("Could not fill reservation 1 (second pass)")
	}
	verifyBookie(t, b, 7 /*reservations*/, 9 /*queue*/, 7*bytesPerReservation /*bytes*/)

	// Expire all the remaining reservations one at a time.
	expireNextReservation(t, mc, b) // Expire 5
	verifyBookie(t, b, 6 /*reservations*/, 8 /*queue*/, 6*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 6(1) - already filled
	verifyBookie(t, b, 6 /*reservations*/, 7 /*queue*/, 6*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 7
	verifyBookie(t, b, 5 /*reservations*/, 6 /*queue*/, 5*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 8
	verifyBookie(t, b, 4 /*reservations*/, 5 /*queue*/, 4*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 9
	verifyBookie(t, b, 3 /*reservations*/, 4 /*queue*/, 3*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 10
	verifyBookie(t, b, 2 /*reservations*/, 3 /*queue*/, 2*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 1(2) - already filled
	verifyBookie(t, b, 2 /*reservations*/, 2 /*queue*/, 2*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 2(2)
	verifyBookie(t, b, 1 /*reservations*/, 1 /*queue*/, 1*bytesPerReservation /*bytes*/)
	expireNextReservation(t, mc, b) // Expire 6(2)
	verifyBookie(t, b, 0 /*reservations*/, 0 /*queue*/, 0 /*bytes*/)
}
