// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestLeaseHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.UnixNano, 0)

	// Lower the max number of entires to ensure the test functions correctly.
	leaseHistoryMaxEntries = 10

	const rangeIDEpoch = roachpb.RangeID(1)
	const rangeIDExpiration = roachpb.RangeID(2)
	const rangeIDOther = roachpb.RangeID(3)

	history := newLeaseHistory()

	for i := 0; i < leaseHistoryMaxEntries; i++ {
		if e, a := i, history.get(rangeIDEpoch); e != len(a) {
			t.Errorf("%d: expected r%d history len to be %d , actual %d:\n%+v", i, rangeIDEpoch, e, len(a), a)
		}
		if e, a := i, history.get(rangeIDExpiration); e != len(a) {
			t.Errorf("%d: expected r%d history len to be %d , actual %d:\n%+v", i, rangeIDExpiration, e, len(a), a)
		}
		if e, a := 0, history.get(rangeIDOther); e != len(a) {
			t.Errorf("%d: expected r%d history len to be %d , actual %d:\n%+v", i, rangeIDOther, e, len(a), a)
		}

		epoch := int64(i)
		history.add(rangeIDEpoch, roachpb.Lease{
			Start: clock.Now(),
			Epoch: &epoch,
		})
		history.add(rangeIDExpiration, roachpb.Lease{
			Start:      clock.Now(),
			Expiration: clock.Now().Add(time.Minute.Nanoseconds(), 0),
		})
	}

	// Now overflow the circular buffers.
	for i := 0; i < leaseHistoryMaxEntries; i++ {
		if e, a := 10, history.get(rangeIDEpoch); e != len(a) {
			t.Errorf("%d: expected r%d history len to be %d , actual %d:\n%+v", i, rangeIDEpoch, e, len(a), a)
		}
		if e, a := 10, history.get(rangeIDExpiration); e != len(a) {
			t.Errorf("%d: expected r%d history len to be %d , actual %d:\n%+v", i, rangeIDExpiration, e, len(a), a)
		}
		if e, a := 0, history.get(rangeIDOther); e != len(a) {
			t.Errorf("%d: expected r%d history len to be %d , actual %d:\n%+v", i, rangeIDOther, e, len(a), a)
		}

		epoch := int64(i + leaseHistoryMaxEntries)
		history.add(rangeIDEpoch, roachpb.Lease{
			Start: clock.Now(),
			Epoch: &epoch,
		})
		history.add(rangeIDExpiration, roachpb.Lease{
			Start:      clock.Now(),
			Expiration: clock.Now().Add(time.Minute.Nanoseconds(), 0),
		})
	}
}
