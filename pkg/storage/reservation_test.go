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
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// createTestBookie creates a new bookie, stopper and manual clock for testing.
func createTestBookie(
	reservationTimeout time.Duration, maxReservations int, maxReservedBytes int64,
) *bookie {
	b := newBookie(newStoreMetrics(time.Hour))
	// Lock the bookie to prevent the main loop from running as we change some
	// of the bookie's state.
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maxReservations = maxReservations
	b.maxReservedBytes = maxReservedBytes
	// Set a high number for a mocked total available space.
	b.metrics.Available.Update(defaultMaxReservedBytes * 10)
	return b
}

// verifyBookie ensures that there are the correct number of reservations and
// reserved bytes.
func verifyBookie(t *testing.T, b *bookie, reservations int, reservedBytes int64) {
	if e, a := reservedBytes, b.metrics.Reserved.Count(); e != a {
		t.Error(errors.Errorf("expected total bytes reserved to be %d, got %d", e, a))
	}
	if e, a := reservations, int(b.metrics.ReservedReplicaCount.Count()); e != a {
		t.Error(errors.Errorf("expected total reservations to be %d, got %d", e, a))
	}
}

// TestBookieReserve ensures that you can never have more than one reservation
// for a specific rangeID at a time, and that both `Reserve` and `Fill` function
// correctly.
func TestBookieReserve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	b := createTestBookie(time.Hour, 5, defaultMaxReservedBytes)

	testCases := []struct {
		rangeID      int
		reserve      bool                   // true for reserve, false for fill
		expSuc       bool                   // is the operation expected to succeed
		expOut       int                    // expected number of reserved replicas
		expBytes     int64                  // expected number of bytes being reserved
		deadReplicas []roachpb.ReplicaIdent // dead replicas that we should not reserve over
	}{
		{rangeID: 1, reserve: true, expSuc: true, expOut: 1, expBytes: 1},
		{rangeID: 1, reserve: true, expSuc: false, expOut: 1, expBytes: 1},
		{rangeID: 1, reserve: false, expSuc: true, expOut: 0, expBytes: 0},
		{rangeID: 1, reserve: false, expSuc: false, expOut: 0, expBytes: 0},
		{rangeID: 2, reserve: true, expSuc: true, expOut: 1, expBytes: 2},
		{rangeID: 3, reserve: true, expSuc: true, expOut: 2, expBytes: 5},
		{rangeID: 1, reserve: true, expSuc: true, expOut: 3, expBytes: 6},
		{rangeID: 2, reserve: true, expSuc: false, expOut: 3, expBytes: 6},
		{rangeID: 2, reserve: false, expSuc: true, expOut: 2, expBytes: 4},
		{rangeID: 2, reserve: false, expSuc: false, expOut: 2, expBytes: 4},
		{rangeID: 3, reserve: false, expSuc: true, expOut: 1, expBytes: 1},
		{rangeID: 1, reserve: false, expSuc: true, expOut: 0, expBytes: 0},
		{rangeID: 2, reserve: false, expSuc: false, expOut: 0, expBytes: 0},
		{rangeID: 0, reserve: true, expSuc: false, expOut: 0, expBytes: 0, deadReplicas: []roachpb.ReplicaIdent{{RangeID: 0}}},
		{rangeID: 0, reserve: true, expSuc: true, expOut: 1, expBytes: 0, deadReplicas: []roachpb.ReplicaIdent{{RangeID: 1}}},
		{rangeID: 0, reserve: false, expSuc: true, expOut: 0, expBytes: 0},
	}

	ctx := context.Background()
	for i, testCase := range testCases {
		if testCase.reserve {
			// Try to reserve the range.
			req := reservationRequest{
				StoreRequestHeader: StoreRequestHeader{
					StoreID: roachpb.StoreID(i),
					NodeID:  roachpb.NodeID(i),
				},
				RangeID:   roachpb.RangeID(testCase.rangeID),
				RangeSize: int64(testCase.rangeID),
			}
			if resp := b.Reserve(ctx, req, testCase.deadReplicas); resp.Reserved != testCase.expSuc {
				if testCase.expSuc {
					t.Errorf("%d: expected a successful reservation, was rejected", i)
				} else {
					t.Errorf("%d: expected no reservation, but it was accepted", i)
				}
			}
		} else {
			// Fill the reservation.
			if filled := b.Fill(ctx, roachpb.RangeID(testCase.rangeID)); filled != testCase.expSuc {
				if testCase.expSuc {
					t.Errorf("%d: expected a successful filled reservation, was rejected", i)
				} else {
					t.Errorf("%d: expected no reservation to be filled, but it was accepted", i)
				}
			}
		}

		verifyBookie(t, b, testCase.expOut, testCase.expBytes)
	}

	// Test that repeated requests with the same store and node number extend
	// the timeout of the pre-existing reservation.
	repeatReq := reservationRequest{
		StoreRequestHeader: StoreRequestHeader{
			StoreID: 100,
			NodeID:  100,
		},
		RangeID:   100,
		RangeSize: 100,
	}
	for i := 1; i < 10; i++ {
		if !b.Reserve(context.Background(), repeatReq, nil).Reserved {
			t.Errorf("%d: could not add repeated reservation", i)
		}
		verifyBookie(t, b, 1, 100)
	}

	// Test rejecting a reservation due to disk space constraints.
	overfilledReq := reservationRequest{
		StoreRequestHeader: StoreRequestHeader{
			StoreID: 200,
			NodeID:  200,
		},
		RangeID:   200,
		RangeSize: 200,
	}

	b.mu.Lock()
	// Set the bytes have 1 less byte free than needed by the reservation.
	b.metrics.Available.Update(b.mu.size + (2 * overfilledReq.RangeSize) - 1)
	b.mu.Unlock()

	if b.Reserve(context.Background(), overfilledReq, nil).Reserved {
		t.Errorf("expected reservation to fail due to disk space constraints, but it succeeded")
	}
	verifyBookie(t, b, 1, 100) // The same numbers from the last call to verifyBookie.
}

// TestBookieReserveMaxRanges ensures that over-booking doesn't occur when there
// are already maxReservations.
func TestBookieReserveMaxRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	previousReserved := 10

	b := createTestBookie(time.Hour, previousReserved, defaultMaxReservedBytes)

	// Load up reservations.
	for i := 1; i <= previousReserved; i++ {
		req := reservationRequest{
			StoreRequestHeader: StoreRequestHeader{
				StoreID: roachpb.StoreID(i),
				NodeID:  roachpb.NodeID(i),
			},
			RangeID:   roachpb.RangeID(i),
			RangeSize: 1,
		}
		if !b.Reserve(context.Background(), req, nil).Reserved {
			t.Errorf("%d: could not add reservation", i)
		}
		verifyBookie(t, b, i, int64(i))
	}

	overbookedReq := reservationRequest{
		StoreRequestHeader: StoreRequestHeader{
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
	verifyBookie(t, b, previousReserved, int64(previousReserved))
}

// TestBookieReserveMaxBytes ensures that over-booking doesn't occur when trying
// to reserve more bytes than maxReservedBytes.
func TestBookieReserveMaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	previousReservedBytes := 10

	b := createTestBookie(time.Hour, previousReservedBytes*2, int64(previousReservedBytes))

	// Load up reservations with a size of 1 each.
	for i := 1; i <= previousReservedBytes; i++ {
		req := reservationRequest{
			StoreRequestHeader: StoreRequestHeader{
				StoreID: roachpb.StoreID(i),
				NodeID:  roachpb.NodeID(i),
			},
			RangeID:   roachpb.RangeID(i),
			RangeSize: 1,
		}
		if !b.Reserve(context.Background(), req, nil).Reserved {
			t.Errorf("%d: could not add reservation", i)
		}
		verifyBookie(t, b, i, int64(i))
	}

	overbookedReq := reservationRequest{
		StoreRequestHeader: StoreRequestHeader{
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
	verifyBookie(t, b, previousReservedBytes, int64(previousReservedBytes))
}
