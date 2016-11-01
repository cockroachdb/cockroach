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
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const errOffsetGreaterThanMaxOffset = "fewer than half the known nodes are within the maximum offset"

// TestUpdateOffset tests the three cases that UpdateOffset should or should
// not update the offset for an addr.
func TestUpdateOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano, time.Nanosecond)
	monitor := newRemoteClockMonitor(context.TODO(), clock, time.Hour)

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

func TestVerifyClockOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano, 50*time.Nanosecond)
	monitor := newRemoteClockMonitor(context.TODO(), clock, time.Hour)

	for idx, tc := range []struct {
		offsets       []RemoteOffset
		expectedError bool
	}{
		// no error if no offsets.
		{[]RemoteOffset{}, false},
		// no error when a majority of offsets are under the maximum tolerated offset.
		{[]RemoteOffset{{Offset: 20, Uncertainty: 10}, {Offset: 48, Uncertainty: 20}, {Offset: 61, Uncertainty: 25}, {Offset: 91, Uncertainty: 31}}, false},
		// error when less than a majority of offsets are under the maximum tolerated offset.
		{[]RemoteOffset{{Offset: 20, Uncertainty: 10}, {Offset: 58, Uncertainty: 20}, {Offset: 85, Uncertainty: 25}, {Offset: 91, Uncertainty: 31}}, true},
	} {
		monitor.mu.offsets = make(map[string]RemoteOffset)
		for i, offset := range tc.offsets {
			monitor.mu.offsets[strconv.Itoa(i)] = offset
		}

		if tc.expectedError {
			if err := monitor.VerifyClockOffset(); !testutils.IsError(err, errOffsetGreaterThanMaxOffset) {
				t.Errorf("%d: unexpected error %v", idx, err)
			}
		} else {
			if err := monitor.VerifyClockOffset(); err != nil {
				t.Errorf("%d: unexpected error %s", idx, err)
			}
		}
	}
}

// TestIsHealthyOffsetInterval tests if we correctly determine if
// a clusterOffsetInterval is healthy or not i.e. if it indicates that the
// local clock has too great an offset or not.
func TestIsHealthyOffsetInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	maxOffset := 10 * time.Nanosecond

	for i, tc := range []struct {
		offset          RemoteOffset
		expectedHealthy bool
	}{
		{RemoteOffset{}, true},
		{RemoteOffset{Offset: 0, Uncertainty: 5}, true},
		{RemoteOffset{Offset: -15, Uncertainty: 4}, false},
		{RemoteOffset{Offset: 15, Uncertainty: 4}, false},
		{RemoteOffset{Offset: math.MaxInt64, Uncertainty: 0}, false},
	} {
		if isHealthy := tc.offset.isHealthy(context.TODO(), maxOffset); tc.expectedHealthy {
			if !isHealthy {
				t.Errorf("%d: expected remote offset %s for maximum offset %s to be healthy", i, tc.offset, maxOffset)
			}
		} else {
			if isHealthy {
				t.Errorf("%d: expected remote offset %s for maximum offset %s to be unhealthy", i, tc.offset, maxOffset)
			}
		}
	}
}

func TestClockOffsetMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()

	clock := hlc.NewClock(hlc.NewManualClock(123).UnixNano, 20*time.Nanosecond)
	monitor := newRemoteClockMonitor(context.TODO(), clock, time.Hour)
	monitor.mu.offsets = map[string]RemoteOffset{
		"0": {
			Offset:      13,
			Uncertainty: 7,
			MeasuredAt:  6,
		},
	}

	if err := monitor.VerifyClockOffset(); err != nil {
		t.Fatal(err)
	}

	if a, e := monitor.Metrics().ClockOffsetMeanNanos.Value(), int64(13); a != e {
		t.Errorf("mean %d != expected %d", a, e)
	}
	if a, e := monitor.Metrics().ClockOffsetStdDevNanos.Value(), int64(7); a != e {
		t.Errorf("stdDev %d != expected %d", a, e)
	}
}
