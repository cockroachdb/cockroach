// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const errOffsetGreaterThanMaxOffset = "clock synchronization error: this node is more than .+ away from at least half of the known nodes"

// TestUpdateOffset tests the three cases that UpdateOffset should or should
// not update the offset for an addr.
func TestUpdateOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	maxOffset := time.Nanosecond
	monitor := newRemoteClockMonitor(clock, maxOffset, time.Hour, 0)

	const key = 2
	const latency = 10 * time.Millisecond

	// Case 1: There is no prior offset for the address.
	offset1 := RemoteOffset{
		Offset:      0,
		Uncertainty: 20,
		MeasuredAt:  monitor.clock.Now().Add(-(monitor.offsetTTL + 1)).UnixNano(),
	}
	monitor.UpdateOffset(context.Background(), key, offset1, latency)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %d to be set in %v, but it was not", key, monitor.mu.offsets)
	} else if o != offset1 {
		t.Errorf("expected offset %v, instead %v", offset1, o)
	}
	monitor.mu.Unlock()

	// Case 2: The old offset for addr is stale.
	offset2 := RemoteOffset{
		Offset:      0,
		Uncertainty: 20,
		MeasuredAt:  monitor.clock.Now().Add(-(monitor.offsetTTL + 1)).UnixNano(),
	}
	monitor.UpdateOffset(context.Background(), key, offset2, latency)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %d to be set in %v, but it was not", key, monitor.mu.offsets)
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
	monitor.UpdateOffset(context.Background(), key, offset3, latency)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %d to be set in %v, but it was not", key, monitor.mu.offsets)
	} else if o != offset3 {
		t.Errorf("expected offset %v, instead %v", offset3, o)
	}
	monitor.mu.Unlock()

	// Larger error and offset3 is not stale, so no update.
	monitor.UpdateOffset(context.Background(), key, offset2, latency)
	monitor.mu.Lock()
	if o, ok := monitor.mu.offsets[key]; !ok {
		t.Errorf("expected key %d to be set in %v, but it was not", key, monitor.mu.offsets)
	} else if o != offset3 {
		t.Errorf("expected offset %v, instead %v", offset3, o)
	}
	monitor.mu.Unlock()
}

func TestVerifyClockOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	maxOffset := 50 * time.Nanosecond
	monitor := newRemoteClockMonitor(clock, maxOffset, time.Hour, 0)

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
		monitor.mu.offsets = make(map[roachpb.NodeID]RemoteOffset)
		for i, offset := range tc.offsets {
			monitor.mu.offsets[roachpb.NodeID(i)] = offset
		}

		if tc.expectedError {
			if err := monitor.VerifyClockOffset(context.Background()); !testutils.IsError(err, errOffsetGreaterThanMaxOffset) {
				t.Errorf("%d: unexpected error %v", idx, err)
			}
		} else {
			if err := monitor.VerifyClockOffset(context.Background()); err != nil {
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
		if isHealthy := tc.offset.isHealthy(context.Background(), maxOffset); tc.expectedHealthy {
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
	defer stopper.Stop(context.Background())

	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	maxOffset := 20 * time.Nanosecond
	monitor := newRemoteClockMonitor(clock, maxOffset, time.Hour, 0)
	monitor.mu.offsets = map[roachpb.NodeID]RemoteOffset{
		0: {
			Offset:      13,
			Uncertainty: 7,
			MeasuredAt:  6,
		},
	}

	if err := monitor.VerifyClockOffset(context.Background()); err != nil {
		t.Fatal(err)
	}

	if a, e := monitor.Metrics().ClockOffsetMeanNanos.Value(), int64(13); a != e {
		t.Errorf("mean %d != expected %d", a, e)
	}
	if a, e := monitor.Metrics().ClockOffsetStdDevNanos.Value(), int64(7); a != e {
		t.Errorf("stdDev %d != expected %d", a, e)
	}
	if a, e := monitor.Metrics().ClockOffsetMedianNanos.Value(), int64(13); a != e {
		t.Errorf("median %d != expected %d", a, e)
	}
	if a, e := monitor.Metrics().ClockOffsetMedianAbsDevNanos.Value(), int64(7); a != e {
		t.Errorf("MAD %d != expected %d", a, e)
	}
}

// TestLatencies tests the tracking of round-trip latency between nodes.
func TestLatencies(t *testing.T) {
	defer leaktest.AfterTest(t)()

	clock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	maxOffset := time.Nanosecond
	monitor := newRemoteClockMonitor(clock, maxOffset, time.Hour, 0)

	// All test cases have to have at least 11 measurement values in order for
	// the exponentially-weighted moving average to work properly. See the
	// comment on the WARMUP_SAMPLES const in the ewma package for details.
	const emptyKey = 1
	for i := 0; i < 11; i++ {
		monitor.UpdateOffset(context.Background(), emptyKey, RemoteOffset{}, 0)
	}
	if l, ok := monitor.mu.latencyInfos[emptyKey]; ok {
		t.Errorf("expected no latency measurement for %q, got %v", emptyKey, l.avgNanos.Value())
	}

	testCases := []struct {
		measurements []time.Duration
		expectedAvg  time.Duration
	}{
		{[]time.Duration{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, 10},
		{[]time.Duration{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 0}, 10},
		{[]time.Duration{0, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10}, 10},
		{[]time.Duration{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 99}, 18},
		{[]time.Duration{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 10}, 90},
		{[]time.Duration{10, 10, 10, 10, 10, 10, 99, 99, 99, 99, 99}, 50},
		{[]time.Duration{99, 99, 99, 99, 99, 99, 10, 10, 10, 10, 10}, 58},
		{[]time.Duration{10, 10, 10, 10, 10, 99, 99, 99, 99, 99, 99}, 58},
		{[]time.Duration{99, 99, 99, 99, 99, 10, 10, 10, 10, 10, 10}, 50},
	}
	for i, tc := range testCases {
		// Start counting from node 1 since a 0 node id is special cased.
		key := roachpb.NodeID(i + 1)
		for _, measurement := range tc.measurements {
			monitor.UpdateOffset(context.Background(), key, RemoteOffset{}, measurement)
		}
		if val, ok := monitor.Latency(key); !ok || val != tc.expectedAvg {
			t.Errorf("%q: expected latency %d, got %d", key, tc.expectedAvg, val)
		}
	}
}

func TestResettingMaxTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var tr resettingMaxTrigger
	testdata := []struct {
		expected         bool
		value            float64
		resetThreshold   float64
		triggerThreshold float64
	}{
		{false, 5, 10, 20},
		{false, 15, 10, 20},
		{true, 25, 10, 20},
		{false, 25, 10, 20},
		{false, 15, 10, 20},
		{false, 25, 10, 20},
		{false, 5, 10, 20},
		{true, 25, 10, 20},
	}
	for i, td := range testdata {
		if tr.triggers(td.value, td.resetThreshold, td.triggerThreshold) != td.expected {
			t.Errorf("Failed in iteration %v: %v", i, td)
		}
	}
}
