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
	"math"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func createTestBookie(maxReservations int, maxReservedBytes int64) *bookie {
	b := newBookie(newStoreMetrics(math.MaxInt32))
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

// TestBookieReserveMaxRanges ensures that over-booking doesn't occur when there
// are already maxReservations.
func TestBookieReserveMaxRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	previousReserved := 10

	b := createTestBookie(previousReserved, defaultMaxReservedBytes)

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
		if _, reserved := b.Reserve(context.Background(), req); !reserved {
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
	if _, reserved := b.Reserve(context.Background(), overbookedReq); reserved {
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

	b := createTestBookie(previousReservedBytes*2, int64(previousReservedBytes))

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
		if _, reserved := b.Reserve(context.Background(), req); !reserved {
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
	if _, reserved := b.Reserve(context.Background(), overbookedReq); reserved {
		t.Errorf("expected reservation to fail due to too many already existing reservations, but it succeeded")
	}
	// The same numbers from the last call to verifyBookie.
	verifyBookie(t, b, previousReservedBytes, int64(previousReservedBytes))
}
