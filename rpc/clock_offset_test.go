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
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package rpc

import (
	"math"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

// TestUpdateOffset tests the three cases that UpdateOffset should or should
// not update the offset for an addr.
func TestUpdateOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	monitor := newRemoteClockMonitor(hlc.NewClock(hlc.NewManualClock(123).UnixNano), time.Hour)

	const key = "addr"

	// Case 1: There is no prior offset for the address.
	offset1 := RemoteOffset{
		Offset:      0,
		Uncertainty: 20,
		MeasuredAt:  monitor.clock.PhysicalTime().Add(-(monitor.offsetTTL + 1)).UnixNano(),
	}
	monitor.UpdateOffset(key, offset1)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %s to be set in %v, but it was not", key, monitor.mu.offsets)
	} else if o != offset1 {
		t.Errorf("expected offset %v, instead %v", offset1, o)
	}
	monitor.mu.Unlock()

	// Case 2: The old offset for addr is stale.
	offset2 := RemoteOffset{
		Offset:      0,
		Uncertainty: 20,
		MeasuredAt:  monitor.clock.PhysicalTime().Add(-(monitor.offsetTTL + 1)).UnixNano(),
	}
	monitor.UpdateOffset(key, offset2)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %s to be set in %v, but it was not", key, monitor.mu.offsets)
	} else if o != offset2 {
		t.Errorf("expected offset %v, instead %v", offset2, o)
	}
	monitor.mu.Unlock()

	// Case 3: The new offset's error is smaller.
	offset3 := RemoteOffset{
		Offset:      0,
		Uncertainty: 10,
		MeasuredAt:  offset2.MeasuredAt + 1,
	}
	monitor.UpdateOffset(key, offset3)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %s to be set in %v, but it was not", key, monitor.mu.offsets)
	} else if o != offset3 {
		t.Errorf("expected offset %v, instead %v", offset3, o)
	}
	monitor.mu.Unlock()

	// Larger error and offset3 is not stale, so no update.
	monitor.UpdateOffset(key, offset2)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %s to be set in %v, but it was not", key, monitor.mu.offsets)
	} else if o != offset3 {
		t.Errorf("expected offset %v, instead %v", offset3, o)
	}
	monitor.mu.Unlock()
}

// TestEndpointListSort tests the sort interface for endpointLists.
func TestEndpointListSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	list := endpointList{
		endpoint{offset: 5, endType: +1},
		endpoint{offset: 3, endType: -1},
		endpoint{offset: 3, endType: +1},
		endpoint{offset: 1, endType: -1},
		endpoint{offset: 4, endType: +1},
	}

	sortedList := endpointList{
		endpoint{offset: 1, endType: -1},
		endpoint{offset: 3, endType: -1},
		endpoint{offset: 3, endType: +1},
		endpoint{offset: 4, endType: +1},
		endpoint{offset: 5, endType: +1},
	}

	sort.Sort(list)
	for i := range sortedList {
		if list[i] != sortedList[i] {
			t.Errorf("expected index %d of sorted list to be %v, instead %v",
				i, sortedList[i], list[i])
		}
	}

	if len(list) != len(sortedList) {
		t.Errorf("exptected endpoint list to be size %d, instead %d",
			len(sortedList), len(list))
	}
}

// TestBuildEndpointList tests that the map of RemoteOffsets is correctly
// manipulated into a list of endpoints used in Marzullo's algorithm.
func TestBuildEndpointList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Build the offsets we will turn into an endpoint list.
	offsets := map[string]RemoteOffset{
		"0": {Offset: 0, Uncertainty: 10},
		"1": {Offset: 1, Uncertainty: 10},
		"2": {Offset: 2, Uncertainty: 10},
		"3": {Offset: 3, Uncertainty: 10},
	}
	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	clock.SetMaxOffset(5 * time.Nanosecond)
	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.mu.offsets = offsets

	expectedList := endpointList{
		endpoint{offset: -10, endType: -1},
		endpoint{offset: -9, endType: -1},
		endpoint{offset: -8, endType: -1},
		endpoint{offset: -7, endType: -1},
		endpoint{offset: 10, endType: 1},
		endpoint{offset: 11, endType: 1},
		endpoint{offset: 12, endType: 1},
		endpoint{offset: 13, endType: 1},
	}

	list := monitor.buildEndpointList()
	sort.Sort(list)

	for i := range expectedList {
		if list[i] != expectedList[i] {
			t.Errorf("expected index %d of list to be %v, instead %v",
				i, expectedList[i], list[i])
		}
	}

	if len(list) != len(expectedList) {
		t.Errorf("exptected endpoint list to be size %d, instead %d",
			len(expectedList), len(list))
	}
}

// TestBuildEndpointListRemoveStagnantClocks tests the side effect of removing
// older offsets when we build an endpoint list.
func TestBuildEndpointListRemoveStagnantClocks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	clock.SetMaxOffset(5 * time.Nanosecond)
	monitor := newRemoteClockMonitor(clock, time.Hour)

	offsets := map[string]RemoteOffset{
		"0":         {Offset: 0, Uncertainty: 10, MeasuredAt: monitor.clock.PhysicalTime().Add(-(monitor.offsetTTL - 1)).UnixNano()},
		"stagnant0": {Offset: 1, Uncertainty: 10, MeasuredAt: monitor.clock.PhysicalTime().Add(-(monitor.offsetTTL + 10)).UnixNano()},
		"1":         {Offset: 2, Uncertainty: 10, MeasuredAt: monitor.clock.PhysicalTime().Add(-(monitor.offsetTTL - 10)).UnixNano()},
		"stagnant1": {Offset: 3, Uncertainty: 10, MeasuredAt: monitor.clock.PhysicalTime().Add(-(monitor.offsetTTL + 1)).UnixNano()},
	}

	monitor.mu.offsets = offsets

	monitor.buildEndpointList()

	if stagnant, ok := offsets["stagnant0"]; ok {
		t.Errorf("expected stagant offset to have been removed, instead found: %v", stagnant)
	}
	if stagnant, ok := offsets["stagnant1"]; ok {
		t.Errorf("expected stagant offset to have been removed, instead found: %v", stagnant)
	}
}

// TestFindOffsetInterval tests that we correctly determine the interval that
// a majority of remote offsets overlap.
func TestFindOffsetInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Build the offsets. We will return the interval that the maximum number
	// of remote clocks overlap.
	offsets := map[string]RemoteOffset{
		"0": {Offset: 20, Uncertainty: 10},
		"1": {Offset: 58, Uncertainty: 20},
		"2": {Offset: 71, Uncertainty: 25},
		"3": {Offset: 91, Uncertainty: 31},
	}

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	clock.SetMaxOffset(0 * time.Nanosecond)
	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.mu.offsets = offsets
	expectedInterval := clusterOffsetInterval{lowerbound: 60, upperbound: 78}
	assertClusterOffset(monitor, expectedInterval, t)
}

// TestFindOffsetIntervalNoMajorityOverlap tests that, if a majority of offsets
// do not overlap, an error is returned.
func TestFindOffsetIntervalNoMajorityOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Build the offsets. We will return the interval that the maximum number
	// of remote clocks overlap.
	offsets := map[string]RemoteOffset{
		"0": {Offset: 0, Uncertainty: 1},
		"1": {Offset: 1, Uncertainty: 1},
		"2": {Offset: 3, Uncertainty: 1},
		"3": {Offset: 4, Uncertainty: 1},
	}

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	clock.SetMaxOffset(0 * time.Nanosecond)
	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.mu.offsets = offsets
	assertMajorityIntervalError(monitor, t)
}

// TestFindOffsetIntervalNoRemotes tests that we measure 0 offset if there are
// no recent remote clock readings.
func TestFindOffsetIntervalNoRemotes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	offsets := map[string]RemoteOffset{}
	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	clock.SetMaxOffset(10 * time.Nanosecond)
	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.mu.offsets = offsets
	expectedInterval := clusterOffsetInterval{lowerbound: 0, upperbound: 0}
	assertClusterOffset(monitor, expectedInterval, t)
}

// TestFindOffsetIntervalOneClock tests that we return the entire remote offset
// of the single remote clock.
func TestFindOffsetIntervalOneClock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	offset := RemoteOffset{Offset: 1, Uncertainty: 2}

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	// The clock interval will be:
	// [offset - error, offset + error]
	// MaxOffset does not matter.
	clock.SetMaxOffset(10 * time.Hour)
	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.mu.offsets["0"] = offset
	expectedInterval := clusterOffsetInterval{lowerbound: -1, upperbound: 3}
	assertClusterOffset(monitor, expectedInterval, t)
}

// TestFindOffsetIntervalTwoClocks tests the edge case of two remote clocks.
func TestFindOffsetIntervalTwoClocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	offsets := map[string]RemoteOffset{}

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	clock.SetMaxOffset(0 * time.Nanosecond)
	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.mu.offsets = offsets

	// Two intervals overlap.
	offsets["0"] = RemoteOffset{Offset: 0, Uncertainty: 10}
	offsets["1"] = RemoteOffset{Offset: 20, Uncertainty: 10}
	expectedInterval := clusterOffsetInterval{lowerbound: 10, upperbound: 10}
	assertClusterOffset(monitor, expectedInterval, t)

	// Two intervals don't overlap.
	offsets["0"] = RemoteOffset{Offset: 0, Uncertainty: 10}
	offsets["1"] = RemoteOffset{Offset: 30, Uncertainty: 10}
	assertMajorityIntervalError(monitor, t)
}

// TestFindOffsetWithLargeError tests a case where offset errors are
// bigger than the max offset (e.g., a case where heartbeat messages
// to the node are having high latency).
func TestFindOffsetWithLargeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	maxOffset := 100 * time.Nanosecond

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	clock.SetMaxOffset(maxOffset)
	offsets := map[string]RemoteOffset{}
	// Offsets are bigger than maxOffset, but Errors are also bigger than Offset.
	offsets["0"] = RemoteOffset{Offset: 110, Uncertainty: 300}
	offsets["1"] = RemoteOffset{Offset: 120, Uncertainty: 300}
	offsets["2"] = RemoteOffset{Offset: 130, Uncertainty: 300}

	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.mu.offsets = offsets

	interval, err := monitor.findOffsetInterval()
	if err != nil {
		t.Fatal(err)
	}
	expectedInterval := clusterOffsetInterval{lowerbound: -170, upperbound: 410}
	if interval != expectedInterval {
		t.Errorf("expected interval %v, instead %v", expectedInterval, interval)
	}
	// The interval is still considered healthy.
	assertIntervalHealth(true, interval, maxOffset, t)
}

// TestIsHealthyOffsetInterval tests if we correctly determine if
// a clusterOffsetInterval is healthy or not i.e. if it indicates that the
// local clock has too great an offset or not.
func TestIsHealthyOffsetInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	maxOffset := 10 * time.Nanosecond

	interval := clusterOffsetInterval{
		lowerbound: 0,
		upperbound: 0,
	}
	assertIntervalHealth(true, interval, maxOffset, t)

	interval = clusterOffsetInterval{
		lowerbound: -11,
		upperbound: 11,
	}
	assertIntervalHealth(true, interval, maxOffset, t)

	interval = clusterOffsetInterval{
		lowerbound: -20,
		upperbound: -11,
	}
	assertIntervalHealth(false, interval, maxOffset, t)

	interval = clusterOffsetInterval{
		lowerbound: 11,
		upperbound: 20,
	}
	assertIntervalHealth(false, interval, maxOffset, t)

	interval = clusterOffsetInterval{
		lowerbound: math.MaxInt64,
		upperbound: math.MaxInt64,
	}
	assertIntervalHealth(false, interval, maxOffset, t)
}

func TestClockOffsetMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Create a RemoteClockMonitor with a hand-picked offset.
	offset := RemoteOffset{
		Offset:      13,
		Uncertainty: 7,
		MeasuredAt:  6,
	}
	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano)
	monitor := newRemoteClockMonitor(clock, time.Hour)
	monitor.UpdateOffset("0", offset)

	if err := monitor.VerifyClockOffset(); err != nil {
		t.Fatal(err)
	}

	reg := monitor.Registry()
	expLower := offset.Offset - offset.Uncertainty
	if a, e := reg.GetGauge("lower-bound-nanos").Value(), expLower; a != e {
		t.Errorf("lower bound %d != expected %d", a, e)
	}
	expHigher := offset.Offset + offset.Uncertainty
	if a, e := reg.GetGauge("upper-bound-nanos").Value(), expHigher; a != e {
		t.Errorf("upper bound %d != expected %d", a, e)
	}
}

func assertMajorityIntervalError(clocks *RemoteClockMonitor, t *testing.T) {
	_, err := clocks.findOffsetInterval()
	if _, ok := err.(*majorityIntervalNotFoundError); !ok {
		t.Errorf("expected a %T, got: %+v", &majorityIntervalNotFoundError{}, err)
	}
}

func assertClusterOffset(clocks *RemoteClockMonitor, expected clusterOffsetInterval, t *testing.T) {
	interval, err := clocks.findOffsetInterval()
	if err != nil {
		t.Errorf("expected interval %v, instead err: %s",
			expected, err)
	} else if interval != expected {
		t.Errorf("expected interval %v, instead %v", expected, interval)
	}
}

func assertIntervalHealth(expectedHealthy bool, i clusterOffsetInterval, maxOffset time.Duration, t *testing.T) {
	if expectedHealthy {
		if !isHealthyOffsetInterval(i, maxOffset) {
			t.Errorf("expected interval %v for offset %d nanoseconds to be healthy",
				i, maxOffset.Nanoseconds())
		}
	} else {
		if isHealthyOffsetInterval(i, maxOffset) {
			t.Errorf("expected interval %v for offset %d nanoseconds to be unhealthy",
				i, maxOffset.Nanoseconds())
		}
	}
}
