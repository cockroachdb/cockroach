// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/hlc"
)

const (
	maxClockSkew = 250 * time.Millisecond
)

func TestReadTimestampCache(t *testing.T) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewHLClock(manual.UnixNano)
	clock.SetMaxDrift(maxClockSkew)
	rtc := NewReadTimestampCache(clock)

	// First simulate a read of just "a" at time 0.
	rtc.Add(Key("a"), nil, clock.Now())
	// Verify GetMax returns the highWater mark which is maxClockSkew.
	if rtc.GetMax(Key("a"), nil).WallTime != maxClockSkew.Nanoseconds() {
		t.Error("expected maxClockSkew for key \"a\"")
	}
	if rtc.GetMax(Key("notincache"), nil).WallTime != maxClockSkew.Nanoseconds() {
		t.Error("expected maxClockSkew for key \"notincache\"")
	}

	// Advance the clock and verify same high water mark.
	manual = hlc.ManualClock(maxClockSkew.Nanoseconds() + 1)
	if rtc.GetMax(Key("a"), nil).WallTime != maxClockSkew.Nanoseconds() {
		t.Error("expected maxClockSkew for key \"a\"")
	}
	if rtc.GetMax(Key("notincache"), nil).WallTime != maxClockSkew.Nanoseconds() {
		t.Error("expected maxClockSkew for key \"notincache\"")
	}

	// Sim a read of "b"-"c" at time maxClockSkew + 1.
	ts := clock.Now()
	rtc.Add(Key("b"), Key("c"), ts)

	// Verify all permutations of direct and range access.
	if rtc.GetMax(Key("b"), nil) != ts {
		t.Error("expected current time for key \"b\"; got %+v", rtc.GetMax(Key("b"), nil))
	}
	if rtc.GetMax(Key("bb"), nil) != ts {
		t.Error("expected current time for key \"bb\"")
	}
	if rtc.GetMax(Key("c"), nil).WallTime != maxClockSkew.Nanoseconds() {
		t.Error("expected maxClockSkew for key \"c\"")
	}
	if rtc.GetMax(Key("b"), Key("c")) != ts {
		t.Error("expected current time for key \"b\"-\"c\"")
	}
	if rtc.GetMax(Key("bb"), Key("bz")) != ts {
		t.Error("expected current time for key \"bb\"-\"bz\"")
	}
	if rtc.GetMax(Key("a"), Key("b")).WallTime != maxClockSkew.Nanoseconds() {
		t.Error("expected maxClockSkew for key \"a\"-\"b\"")
	}
	if rtc.GetMax(Key("a"), Key("bb")) != ts {
		t.Error("expected current time for key \"a\"-\"bb\"")
	}
	if rtc.GetMax(Key("a"), Key("d")) != ts {
		t.Error("expected current time for key \"a\"-\"d\"")
	}
	if rtc.GetMax(Key("bz"), Key("c")) != ts {
		t.Error("expected current time for key \"bz\"-\"c\"")
	}
	if rtc.GetMax(Key("bz"), Key("d")) != ts {
		t.Error("expected current time for key \"bz\"-\"d\"")
	}
	if rtc.GetMax(Key("c"), Key("d")).WallTime != maxClockSkew.Nanoseconds() {
		t.Error("expected maxClockSkew for key \"c\"-\"d\"")
	}
}

// TestReadTimestampCacheEviction verifies the eviction of
// read timestamp cache entries after minCacheWindow interval.
func TestReadTimestampCacheEviction(t *testing.T) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewHLClock(manual.UnixNano)
	clock.SetMaxDrift(maxClockSkew)
	rtc := NewReadTimestampCache(clock)

	// Increment time to the maxClockSkew high water mark + 1.
	manual = hlc.ManualClock(maxClockSkew.Nanoseconds() + 1)
	aTS := clock.Now()
	rtc.Add(Key("a"), nil, aTS)

	// Increment time by the minCacheWindow and add another key.
	manual = hlc.ManualClock(int64(manual) + minCacheWindow.Nanoseconds())
	rtc.Add(Key("b"), nil, clock.Now())

	// Verify looking up key "c" returns the new high water mark ("a"'s timestamp).
	if rtc.GetMax(Key("c"), nil) != aTS {
		t.Error("expected high water mark %+v, got %+v", aTS, rtc.GetMax(Key("c"), nil))
	}
}

// TestReadTimestampCacheLayeredIntervals verifies the maximum
// timestamp is chosen if previous reads have ranges which are
// layered over each other.
func TestReadTimestampCacheLayeredIntervals(t *testing.T) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewHLClock(manual.UnixNano)
	clock.SetMaxDrift(maxClockSkew)
	rtc := NewReadTimestampCache(clock)
	manual = hlc.ManualClock(maxClockSkew.Nanoseconds() + 1)

	adTS := clock.Now()
	rtc.Add(Key("a"), Key("d"), adTS)

	beTS := clock.Now()
	rtc.Add(Key("b"), Key("e"), beTS)

	cTS := clock.Now()
	rtc.Add(Key("c"), nil, cTS)

	// Try different sub ranges.
	if rtc.GetMax(Key("a"), nil) != adTS {
		t.Error("expected \"a\" to have adTS timestamp")
	}
	if rtc.GetMax(Key("b"), nil) != beTS {
		t.Error("expected \"b\" to have beTS timestamp")
	}
	if rtc.GetMax(Key("c"), nil) != cTS {
		t.Error("expected \"b\" to have cTS timestamp")
	}
	if rtc.GetMax(Key("d"), nil) != beTS {
		t.Error("expected \"d\" to have beTS timestamp")
	}
	if rtc.GetMax(Key("a"), Key("b")) != adTS {
		t.Error("expected \"a\"-\"b\" to have adTS timestamp")
	}
	if rtc.GetMax(Key("a"), Key("c")) != beTS {
		t.Error("expected \"a\"-\"c\" to have beTS timestamp")
	}
	if rtc.GetMax(Key("a"), Key("d")) != cTS {
		t.Error("expected \"a\"-\"d\" to have cTS timestamp")
	}
	if rtc.GetMax(Key("b"), Key("d")) != cTS {
		t.Error("expected \"b\"-\"d\" to have cTS timestamp")
	}
	if rtc.GetMax(Key("c"), Key("d")) != cTS {
		t.Error("expected \"c\"-\"d\" to have cTS timestamp")
	}
	if rtc.GetMax(Key("c0"), Key("d")) != beTS {
		t.Error("expected \"c0\"-\"d\" to have beTS timestamp")
	}
}

func TestReadTimestampCacheClear(t *testing.T) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewHLClock(manual.UnixNano)
	clock.SetMaxDrift(maxClockSkew)
	rtc := NewReadTimestampCache(clock)

	// Increment time to the maxClockSkew high water mark + 1.
	manual = hlc.ManualClock(maxClockSkew.Nanoseconds() + 1)
	ts := clock.Now()
	rtc.Add(Key("a"), nil, ts)

	// Clear the cache, which will reset the high water mark to
	// the current time + maxClockSkew.
	rtc.Clear()

	// Fetching any keys should give current time + maxClockSkew
	expTS := clock.Timestamp()
	expTS.WallTime += maxClockSkew.Nanoseconds()
	if rtc.GetMax(Key("a"), nil) != expTS {
		t.Error("expected \"a\" to have cleared timestamp")
	}
}
