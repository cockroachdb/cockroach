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

package tscache

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestTimestampCacheEviction verifies the eviction of
// timestamp cache entries after MinTSCacheWindow interval.
func TestTimestampCacheImplEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := newCacheImpl(clock)
	defer tc.clear(clock.Now())

	tc.maxBytes = 0

	// Increment time to the low water mark + 1.
	manual.Increment(1)
	aTS := clock.Now()
	tc.add(roachpb.Key("a"), nil, aTS, uuid.UUID{}, true)

	// Increment time by the MinTSCacheWindow and add another key.
	manual.Increment(MinTSCacheWindow.Nanoseconds())
	tc.add(roachpb.Key("b"), nil, clock.Now(), uuid.UUID{}, true)

	// Verify looking up key "c" returns the new low water mark ("a"'s timestamp).
	if rTS, _, ok := tc.GetMaxRead(roachpb.Key("c"), nil); rTS != aTS || ok {
		t.Errorf("expected low water mark %s, got %s; ok=%t", aTS, rTS, ok)
	}
}

// TestTimestampCacheNoEviction verifies that even after
// the MinTSCacheWindow interval, if the cache has not hit
// its size threshold, it will not evict entries.
func TestTimestampCacheImplNoEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := newCacheImpl(clock)
	defer tc.clear(clock.Now())

	// Increment time to the low water mark + 1.
	manual.Increment(1)
	aTS := clock.Now()
	tc.add(roachpb.Key("a"), nil, aTS, uuid.UUID{}, true)
	tc.AddRequest(&Request{
		Reads:     []roachpb.Span{{Key: roachpb.Key("c")}},
		Timestamp: aTS,
	})

	// Increment time by the MinTSCacheWindow and add another key.
	manual.Increment(MinTSCacheWindow.Nanoseconds())
	tc.add(roachpb.Key("b"), nil, clock.Now(), uuid.UUID{}, true)
	tc.AddRequest(&Request{
		Reads:     []roachpb.Span{{Key: roachpb.Key("d")}},
		Timestamp: clock.Now(),
	})

	// Verify that the cache still has 4 entries in it
	if l, want := tc.len(), 4; l != want {
		t.Errorf("expected %d entries to remain, got %d", want, l)
	}
}

func TestTimestampCacheImplExpandRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	tc := newCacheImpl(clock)
	defer tc.clear(clock.Now())

	ab := roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("b")}
	bc := roachpb.RSpan{Key: roachpb.RKey("b"), EndKey: roachpb.RKey("c")}

	// Increment time to the low water mark + 1.
	start := clock.Now()
	manual.Increment(1)
	tc.AddRequest(&Request{
		Span:      ab,
		Reads:     []roachpb.Span{{Key: roachpb.Key("a")}},
		Timestamp: clock.Now(),
	})

	tc.ExpandRequests(bc, start)
	if tc.requests.Len() != 1 {
		t.Fatalf("expected 1 cached request, but found %d", tc.requests.Len())
	}

	tc.ExpandRequests(ab, start)
	if tc.requests.Len() != 0 {
		t.Fatalf("expected 0 cached requests, but found %d", tc.requests.Len())
	}
}
