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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Kathy Spradlin (kathyspradlin@gmail.com)

package rpc

import (
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/hlc"
)

// TestUpdateOffset tests the three cases that UpdateOffset should or should
// not update the offset for an addr.
func TestUpdateOffset(t *testing.T) {
	monitor := newRemoteClockMonitor(hlc.NewClock(hlc.UnixNano))

	// Case 1: There is no prior offset for the address.
	offset1 := RemoteOffset{}
	monitor.UpdateOffset("addr", offset1)
	if o := monitor.offsets["addr"]; o != offset1 {
		t.Errorf("expected offset %v, instead %v", offset1, o)
	}

	// Case 2: The old offset for addr was measured before lastMonitoredAt.
	monitor.lastMonitoredAt = 5
	offset2 := RemoteOffset{
		Offset:     0,
		Error:      20,
		MeasuredAt: 6,
	}
	monitor.UpdateOffset("addr", offset2)
	if o := monitor.offsets["addr"]; o != offset2 {
		t.Errorf("expected offset %v, instead %v", offset2, o)
	}

	// Case 3: The new offset's error is smaller.
	offset3 := RemoteOffset{
		Offset:     0,
		Error:      10,
		MeasuredAt: 8,
	}
	monitor.UpdateOffset("addr", offset3)
	if o := monitor.offsets["addr"]; o != offset3 {
		t.Errorf("expected offset %v, instead %v", offset3, o)
	}

	// Larger error and offset3.MeasuredAt > lastMonitoredAt, so no update.
	monitor.UpdateOffset("addr", offset2)
	if o := monitor.offsets["addr"]; o != offset3 {
		t.Errorf("expected offset %v, instead %v", offset3, o)
	}

	// InfiniteOffset, shouldn't update because it has larger error.
	monitor.UpdateOffset("addr", InfiniteOffset)
	if o := monitor.offsets["addr"]; o != offset3 {
		t.Errorf("expected offset %v, instead %v", offset3, o)
	}

	// LastMonitoredAt moved up, so InfiniteOffset can be added now.
	monitor.lastMonitoredAt = 10
	monitor.UpdateOffset("addr", InfiniteOffset)
	if o := monitor.offsets["addr"]; o != InfiniteOffset {
		t.Errorf("expected offset %v, instead %v", InfiniteOffset, o)
	}
}

// TestEndpointListSort tests the sort interface for endpointLists.
func TestEndpointListSort(t *testing.T) {
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
	// Build the offsets we will turn into an endpoint list.
	offsets := map[string]RemoteOffset{
		"0": RemoteOffset{Offset: 0, Error: 10},
		"1": RemoteOffset{Offset: 1, Error: 10},
		"2": RemoteOffset{Offset: 2, Error: 10},
		"3": RemoteOffset{Offset: 3, Error: 10},
	}
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(5 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets: offsets,
		lClock:  clock,
	}

	expectedList := endpointList{
		endpoint{offset: -15, endType: -1},
		endpoint{offset: -14, endType: -1},
		endpoint{offset: -13, endType: -1},
		endpoint{offset: -12, endType: -1},
		endpoint{offset: 15, endType: 1},
		endpoint{offset: 16, endType: 1},
		endpoint{offset: 17, endType: 1},
		endpoint{offset: 18, endType: 1},
	}

	list := remoteClocks.buildEndpointList()
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
	offsets := map[string]RemoteOffset{
		"0":         RemoteOffset{Offset: 0, Error: 10, MeasuredAt: 11},
		"stagnant0": RemoteOffset{Offset: 1, Error: 10, MeasuredAt: 0},
		"1":         RemoteOffset{Offset: 2, Error: 10, MeasuredAt: 20},
		"stagnant1": RemoteOffset{Offset: 3, Error: 10, MeasuredAt: 9},
	}

	// The stagnant offsets older than 10ns ago will be removed.
	monitorInterval = 10 * time.Nanosecond

	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(5 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets:         offsets,
		lClock:          clock,
		lastMonitoredAt: 10, // offsets measured before this will be removed.
	}

	remoteClocks.buildEndpointList()

	_, ok0 := offsets["stagnant0"]
	_, ok1 := offsets["stagnant1"]

	if ok0 || ok1 {
		t.Errorf("expected stagant offsets removed, instead offsets: %v", offsets)
	}
}

// TestFindOffsetInterval tests that we correctly determine the interval that
// a majority of remote offsets overlap.
func TestFindOffsetInterval(t *testing.T) {
	// Build the offsets. We will return the interval that the maximum number
	// of remote clocks overlap.
	offsets := map[string]RemoteOffset{
		"0": RemoteOffset{Offset: 20, Error: 10},
		"1": RemoteOffset{Offset: 58, Error: 20},
		"2": RemoteOffset{Offset: 71, Error: 25},
		"3": RemoteOffset{Offset: 91, Error: 31},
	}

	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(0 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets: offsets,
		lClock:  clock,
	}
	expectedInterval := ClusterOffsetInterval{Lowerbound: 60, Upperbound: 78}
	assertClusterOffset(remoteClocks, expectedInterval, t)
}

// TestFindOffsetIntervalNoMajorityOverlap tests that, if a majority of offsets
// do not overlap, an error is returned.
func TestFindOffsetIntervalNoMajorityOverlap(t *testing.T) {
	// Build the offsets. We will return the interval that the maximum number
	// of remote clocks overlap.
	offsets := map[string]RemoteOffset{
		"0": RemoteOffset{Offset: 0, Error: 1},
		"1": RemoteOffset{Offset: 1, Error: 1},
		"2": RemoteOffset{Offset: 3, Error: 1},
		"3": RemoteOffset{Offset: 4, Error: 1},
	}

	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(0 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets: offsets,
		lClock:  clock,
	}
	assertMajorityIntervalError(remoteClocks, t)
}

// TestFindOffsetIntervalWithInfinites tests that, if most of the clocks have
// an InfiniteOffset (they missed their last heartbeat), then we get an
// interval for an infinite offset.
func TestFindOffsetIntervalWithInfinites(t *testing.T) {
	offsets := map[string]RemoteOffset{
		"0": RemoteOffset{Offset: 0, Error: 1},
		"1": InfiniteOffset,
		"2": InfiniteOffset,
		"3": InfiniteOffset,
	}

	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(0 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets: offsets,
		lClock:  clock,
	}
	expectedInterval := ClusterOffsetInterval{
		Lowerbound: InfiniteOffset.Offset,
		Upperbound: InfiniteOffset.Offset,
	}
	assertClusterOffset(remoteClocks, expectedInterval, t)
}

// TestFindOffsetIntervalNoRemotes tests that we measure 0 offset if there are
// no recent remote clock readings.
func TestFindOffsetIntervalNoRemotes(t *testing.T) {
	offsets := map[string]RemoteOffset{}
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(10 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets: offsets,
		lClock:  clock,
	}
	expectedInterval := ClusterOffsetInterval{Lowerbound: 0, Upperbound: 0}
	assertClusterOffset(remoteClocks, expectedInterval, t)
}

// TestFindOffsetIntervalOneClock tests that we return the entire remote offset
// of the single remote clock.
func TestFindOffsetIntervalOneClock(t *testing.T) {
	offsets := map[string]RemoteOffset{
		"0": RemoteOffset{Offset: 0, Error: 10},
	}

	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	// The clock interval will be:
	// [offset - error - maxOffset, offset + error + maxOffset]
	clock.SetMaxOffset(10 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets: offsets,
		lClock:  clock,
	}
	expectedInterval := ClusterOffsetInterval{Lowerbound: -20, Upperbound: 20}
	assertClusterOffset(remoteClocks, expectedInterval, t)
}

// TestFindOffsetIntervalTwoClocks tests the edge case of two remote clocks.
func TestFindOffsetIntervalTwoClocks(t *testing.T) {
	offsets := map[string]RemoteOffset{}

	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(0 * time.Nanosecond)
	remoteClocks := &RemoteClockMonitor{
		offsets: offsets,
		lClock:  clock,
	}

	// Two intervals overlap.
	offsets["0"] = RemoteOffset{Offset: 0, Error: 10}
	offsets["1"] = RemoteOffset{Offset: 20, Error: 10}
	expectedInterval := ClusterOffsetInterval{Lowerbound: 10, Upperbound: 10}
	assertClusterOffset(remoteClocks, expectedInterval, t)

	// Two intervals don't overlap.
	offsets["0"] = RemoteOffset{Offset: 0, Error: 10}
	offsets["1"] = RemoteOffset{Offset: 30, Error: 10}
	assertMajorityIntervalError(remoteClocks, t)
}

// TestIsHealthyOffsetInterval tests if we correctly determine if
// a ClusterOffsetInterval is healthy or not i.e. if it indicates that the
// local clock has too great an offset or not.
func TestIsHealthyOffsetInterval(t *testing.T) {
	maxOffset := 10 * time.Nanosecond

	interval := ClusterOffsetInterval{
		Lowerbound: 0,
		Upperbound: 0,
	}
	assertIntervalHealth(true, interval, maxOffset, t)

	interval = ClusterOffsetInterval{
		Lowerbound: -11,
		Upperbound: 11,
	}
	assertIntervalHealth(true, interval, maxOffset, t)

	interval = ClusterOffsetInterval{
		Lowerbound: -20,
		Upperbound: -11,
	}
	assertIntervalHealth(false, interval, maxOffset, t)

	interval = ClusterOffsetInterval{
		Lowerbound: 11,
		Upperbound: 20,
	}
	assertIntervalHealth(false, interval, maxOffset, t)

	interval = ClusterOffsetInterval{
		Lowerbound: InfiniteOffset.Offset,
		Upperbound: InfiniteOffset.Offset,
	}
	assertIntervalHealth(false, interval, maxOffset, t)
}

func assertMajorityIntervalError(clocks *RemoteClockMonitor, t *testing.T) {
	interval, err := clocks.findOffsetInterval()
	expectedErr := MajorityIntervalNotFoundError{}
	if err == nil {
		t.Errorf("expected MajorityIntervalNotFoundError, instead %v", interval)
	} else if err != expectedErr {
		t.Errorf("expected MajorityIntervalNotFoundError, instead: %s", err)
	}
}

func assertClusterOffset(clocks *RemoteClockMonitor,
	expected ClusterOffsetInterval, t *testing.T) {
	interval, err := clocks.findOffsetInterval()
	if err != nil {
		t.Errorf("expected interval %v, instead err: %s",
			expected, err)
	} else if interval != expected {
		t.Errorf("expected interval %v, instead %v", expected, interval)
	}
}

func assertIntervalHealth(expectedHealthy bool, i ClusterOffsetInterval,
	maxOffset time.Duration, t *testing.T) {
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
