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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

// createTestBookie creates a new bookie, stopp and manual clock for testing.
func createTestBookie(reservationTimeout time.Duration) (*stop.Stopper, *hlc.ManualClock, *bookie) {
	stopper := stop.NewStopper()
	mc := hlc.NewManualClock(0)
	clock := hlc.NewClock(mc.UnixNano)
	b := newBookie(clock, reservationTimeout, stopper)
	return stopper, mc, b
}

// TestReserve ensures that you can never have more than one reservation for a
// perticual rangeID at a time, and that both `reserve` and `fill` function
// correctly.
func TestReserve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, _, b := createTestBookie(time.Hour)
	defer stopper.Stop()

	testCases := []struct {
		rangeID          int
		reserve          bool // true for reserve, false for fill
		expected         bool
		expectedLength   int
		expectedBytes    int64
		expectedTimeouts int
	}{
		{1, true, true, 1, 1, 1},
		{1, true, false, 1, 1, 1},
		{1, false, true, 0, 0, 1},
		{1, false, false, 0, 0, 1},
		{2, true, true, 1, 2, 2},
		{3, true, true, 2, 5, 3},
		{1, true, true, 3, 6, 4},
		{2, true, false, 3, 6, 4},
		{2, false, true, 2, 4, 4},
		{2, false, false, 2, 4, 4},
		{3, false, true, 1, 1, 4},
		{1, false, true, 0, 0, 4},
		{2, false, false, 0, 0, 4},
	}

	for i, testCase := range testCases {
		if testCase.reserve {
			// Try to reserve the range.
			res := reservation{
				rangeID: roachpb.RangeID(testCase.rangeID),
				storeID: roachpb.StoreID(i),
				nodeID:  roachpb.NodeID(i),
				size:    int64(testCase.rangeID),
			}
			if reserved := b.reserve(res); reserved != testCase.expected {
				if testCase.expected {
					t.Errorf("%d: expected a successful reservation, was rejected", i)
				} else {
					t.Errorf("%d: expected no reservation, but it wasn't rejected", i)
				}
			}
		} else {
			// Fill the booking.
			if filled := b.fill(roachpb.RangeID(testCase.rangeID)); filled != testCase.expected {
				if testCase.expected {
					t.Errorf("%d: expected a successful filled reservation, was rejected", i)
				} else {
					t.Errorf("%d: expected no reservation to be filled, but it wasn't rejected", i)
				}
			}
		}

		// Check the length of the active reservations.
		if e, a := testCase.expectedLength, b.len(); e != a {
			t.Errorf("%d: expected %d active reservations, actual %d", i, e, a)
		}

		// Check the reserved bytes.
		if e, a := testCase.expectedBytes, b.size(); e != a {
			t.Errorf("%d: expected %d bytes reserved, actual %d", i, e, a)
		}

		// Check the length of the active timeouts.
		b.mu.RLock()
		if e, a := testCase.expectedTimeouts, b.mu.queue.Len(); e != a {
			t.Errorf("%d: expected %d active timeouts, actual %d", i, e, a)
		}
		b.mu.RUnlock()
	}
}

// expireNextBooking advances the manual clock to one nanosecond passed the
// next expiring booking and waits until exactly the number of expired bookings
// is equal to expireCount.
func expireNextBooking(t *testing.T, mc *hlc.ManualClock, b *bookie, expireCount int) {
	b.mu.RLock()
	nextExpiredBooking := b.mu.queue.peek()
	expectedExpires := b.mu.queue.Len() - expireCount
	b.mu.RUnlock()
	if nextExpiredBooking == nil {
		return
	}
	// Set the clock to after next timeout.
	mc.Set(nextExpiredBooking.expireAt.WallTime + 1)

	util.SucceedsSoon(t, func() error {
		b.mu.RLock()
		defer b.mu.RUnlock()
		if expectedExpires != b.mu.queue.Len() {
			nextExpiredBooking := b.mu.queue.peek()
			return fmt.Errorf("expiration has not occured yet, next expiration in %s for rangeID:%d",
				nextExpiredBooking.expireAt, nextExpiredBooking.reservation.rangeID)
		}
		return nil
	})
}

// verifyBookie ensure that the correct number of reservation, byte size and
// expirationQueue length are correct. It assumes that each reservation has
// reserved 100 bytes.
func verifyBookie(t *testing.T, b *bookie, reservations, queueLen int) {
	if e, a := int64(reservations*100), b.size(); e != a {
		t.Error(util.ErrorfSkipFrames(1, "expected total bytes reserved to be %d, got %d", e, a))
	}
	if e, a := reservations, b.len(); e != a {
		t.Error(util.ErrorfSkipFrames(1, "expected total reservations to be %d, got %d", e, a))
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if e, a := queueLen, b.mu.queue.Len(); e != a {
		t.Error(util.ErrorfSkipFrames(1, "expected total queue length to be %d, got %d", e, a))
	}
}

// TestReservationQueue checks to ensure that the expiration loop functions
// correctly expiring any unfilled reservations in a number of different cases.
func TestReservationQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper, mc, b := createTestBookie(time.Microsecond)
	defer stopper.Stop()

	// Load a collection of reservations into the bookie.
	for i := 1; i <= 10; i++ {
		// Ensure all the timesouts are 10 nanoseconds apart.
		mc.Increment(100)
		if !b.reserve(reservation{
			rangeID: roachpb.RangeID(i),
			storeID: roachpb.StoreID(i),
			nodeID:  roachpb.NodeID(i),
			size:    100,
		}) {
			t.Fatalf("could not book a reservation for reservation number %d", i)
		}
	}
	verifyBookie(t, b, 10, 10)

	// Fill reservation 2.
	if !b.fill(2) {
		t.Fatalf("Could not fill reservation 2")
	}
	// After filling a reservation, wait a full cycle so that it can be timed
	// out.
	verifyBookie(t, b, 9, 10)

	// Expire reservation 1.
	expireNextBooking(t, mc, b, 1)
	verifyBookie(t, b, 8, 9)

	// Fill reservations 4 and 6.
	if !b.fill(4) {
		t.Fatalf("Could not fill reservation 4")
	}
	if !b.fill(6) {
		t.Fatalf("Could not fill reservation 6")
	}
	verifyBookie(t, b, 6, 9)

	expireNextBooking(t, mc, b, 1) // Expire 2 (already filled)
	verifyBookie(t, b, 6, 8)
	expireNextBooking(t, mc, b, 1) // Expire 3
	verifyBookie(t, b, 5, 7)
	expireNextBooking(t, mc, b, 1) // Expire 4 (already filled)
	verifyBookie(t, b, 5, 6)

	// Add three new reservations, 1 and 2, which have already been filled and
	// timed out, and 6, which has been filled by not timed out. Only increment
	// by 10 here to ensure we don't expire any of the other reservations.
	mc.Increment(10)
	if !b.reserve(reservation{
		rangeID: roachpb.RangeID(1),
		storeID: roachpb.StoreID(11),
		nodeID:  roachpb.NodeID(11),
		size:    100,
	}) {
		t.Fatalf("could not book a reservation for reservation number 1 (second pass)")
	}
	verifyBookie(t, b, 6, 7)

	mc.Increment(10)
	if !b.reserve(reservation{
		rangeID: roachpb.RangeID(2),
		storeID: roachpb.StoreID(12),
		nodeID:  roachpb.NodeID(12),
		size:    100,
	}) {
		t.Fatalf("could not book a reservation for reservation number 2 (second pass)")
	}
	verifyBookie(t, b, 7, 8)

	mc.Increment(10)
	if !b.reserve(reservation{
		rangeID: roachpb.RangeID(6),
		storeID: roachpb.StoreID(13),
		nodeID:  roachpb.NodeID(13),
		size:    100,
	}) {
		t.Fatalf("could not book a reservation for reservation number 6 (second pass)")
	}
	verifyBookie(t, b, 8, 9)

	// Fill 1 a second time.
	if !b.fill(1) {
		t.Fatalf("Could not fill reservation 1 (second pass)")
	}
	verifyBookie(t, b, 7, 9)

	// Expire all the remaining reservations one at a time.
	expireNextBooking(t, mc, b, 1) // Expire 5
	verifyBookie(t, b, 6, 8)
	expireNextBooking(t, mc, b, 1) // Expire 6(1) - already filled
	verifyBookie(t, b, 6, 7)
	expireNextBooking(t, mc, b, 1) // Expire 7
	verifyBookie(t, b, 5, 6)
	expireNextBooking(t, mc, b, 1) // Expire 8
	verifyBookie(t, b, 4, 5)
	expireNextBooking(t, mc, b, 1) // Expire 9
	verifyBookie(t, b, 3, 4)
	expireNextBooking(t, mc, b, 1) // Expire 10
	verifyBookie(t, b, 2, 3)
	expireNextBooking(t, mc, b, 1) // Expire 1(2) - already filled
	verifyBookie(t, b, 2, 2)
	expireNextBooking(t, mc, b, 1) // Expire 2(2)
	verifyBookie(t, b, 1, 1)
	expireNextBooking(t, mc, b, 1) // Expire 6(2)
	verifyBookie(t, b, 0, 0)
}
