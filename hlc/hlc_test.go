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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package hlc

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/glog"
)

type Event uint8

const (
	SEND = iota
	RECV
)

// ExampleNewHLClock shows how to create a new
// hybrid logical clock based on the local machine's
// physical clock. The sanity checks in this example
// will, of course, not fail and the output will be
// the age of the Unix epoch in nanoseconds.
func ExampleNewHLClock() {
	// Initialize a new clock, using the local
	// physical clock.
	c := NewHLClock(UnixNano)
	// Update the state of the hybrid clock.
	s := c.Now()
	time.Sleep(50 * time.Nanosecond)
	t := HLTimestamp{WallTime: UnixNano()}
	// The sanity checks below will usually never be triggered.

	// HLTimestamp implements the util.Ordered interface.
	if s.Less(t) || !t.Less(s) {
		glog.Fatalf("The later timestamp is smaller than the earlier one")
	}

	if t.WallTime-s.WallTime > 0 {
		glog.Fatalf("HLC timestamp %d deviates from physical clock %d", s, t)
	}

	if s.Logical > 0 {
		glog.Fatalf("Trivial timestamp has logical component")
	}

	fmt.Printf("The Unix Epoch is now approximately %dns old.\n", t.WallTime)
}

func TestLess(t *testing.T) {
	var m ManualClock
	c := NewHLClock(m.UnixNano)
	a := c.Timestamp()
	b := c.Timestamp()
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m = ManualClock(1)
	b = c.Now()
	if !a.Less(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if !b.Less(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

// TestHLClock performs a complete test of all basic phenomena,
// including backward jumps in local physical time and clock drift.
func TestHLClock(t *testing.T) {
	var m ManualClock
	c := NewHLClock(m.UnixNano)
	c.SetMaxDrift(1000)
	expectedHistory := []struct {
		// The physical time that this event should take place at.
		wallClock int64
		event     Event
		// If this is a receive event, this holds the "input" timestamp.
		input *HLTimestamp
		// The expected timestamp generated from the input.
		expected HLTimestamp
	}{
		// A few valid steps to warm up.
		{5, SEND, nil, HLTimestamp{5, 0}},
		{6, SEND, nil, HLTimestamp{6, 0}},
		{10, RECV, &HLTimestamp{10, 5}, HLTimestamp{10, 6}},
		// Our clock mysteriously jumps back.
		{7, SEND, nil, HLTimestamp{10, 7}},
		// Wall clocks coincide, but the local logical clock wins.
		{8, RECV, &HLTimestamp{10, 4}, HLTimestamp{10, 8}},
		// The next message comes from a faulty clock and should
		// be discarded.
		{9, RECV, &HLTimestamp{1100, 888}, HLTimestamp{10, 8}},
		// Wall clocks coincide, but the remote logical clock wins.
		{10, RECV, &HLTimestamp{10, 99}, HLTimestamp{10, 100}},
		// The physical clock has caught up and takes over.
		{11, RECV, &HLTimestamp{10, 31}, HLTimestamp{11, 0}},
		{11, SEND, nil, HLTimestamp{11, 1}},
	}

	var current HLTimestamp
	var err error
	for i, step := range expectedHistory {
		m = ManualClock(step.wallClock)
		switch step.event {
		case SEND:
			current = c.Now()
		case RECV:
			fallthrough
		default:
			previous := c.Timestamp()
			current, err = c.Update(*step.input)
			if current == previous && err == nil {
				t.Errorf("%d: clock not updated even though no error occurred", i)
			}
		}
		if current != step.expected {
			t.Fatalf("HLC error: %d expected %v, got %v", i, step.expected, current)
		}
	}
	c.Now()
}

// TestSetMaxDrift ensures that checking received timestamps
// for excessive drifts works correctly.
func TestSetMaxDrift(t *testing.T) {
	var m ManualClock = 123456789
	skewedTime := int64(123456789 + 51)
	c := NewHLClock(m.UnixNano)
	if c.MaxDrift() != 0 {
		t.Fatalf("unexpected drift setting")
	}
	c.SetMaxDrift(50)
	if c.MaxDrift() != 50 {
		t.Fatalf("unexpected drift setting")
	}
	c.Now()
	if c.Timestamp().WallTime != int64(m) {
		t.Fatalf("unexpected clock value")
	}
	_, err := c.Update(HLTimestamp{WallTime: skewedTime})
	if err == nil {
		t.Fatalf("clock drift not recognized")
	}
	// Disable drift checking.
	c.SetMaxDrift(0)
	_, err = c.Update(HLTimestamp{WallTime: skewedTime})
	if err != nil || c.Timestamp().WallTime != skewedTime {
		t.Fatalf("failed to disable drift checking")
	}
}

// ExampleManualClock shows how a manual clock can be
// used as a physical clock. This is useful for testing.
func ExampleManualClock() {
	var m ManualClock = 10
	c := NewHLClock(m.UnixNano)
	c.Now()
	if c.Timestamp().WallTime != 10 {
		glog.Fatalf("manual clock error")
	}
	m = 20
	c.Now()
	if c.Timestamp().WallTime != 20 {
		glog.Fatalf("manual clock error")
	}
}
