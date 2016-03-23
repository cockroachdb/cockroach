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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package hlc

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
)

type Event uint8

const (
	SEND = iota
	RECV
)

// ExampleNewClock shows how to create a new
// hybrid logical clock based on the local machine's
// physical clock. The sanity checks in this example
// will, of course, not fail and the output will be
// the age of the Unix epoch in nanoseconds.
func ExampleNewClock() {
	// Initialize a new clock, using the local
	// physical clock.
	c := NewClock(UnixNano)
	// Update the state of the hybrid clock.
	s := c.Now()
	time.Sleep(50 * time.Nanosecond)
	t := roachpb.Timestamp{WallTime: UnixNano()}
	// The sanity checks below will usually never be triggered.

	if s.Less(t) || !t.Less(s) {
		log.Fatalf("The later timestamp is smaller than the earlier one")
	}

	if t.WallTime-s.WallTime > 0 {
		log.Fatalf("HLC timestamp %d deviates from physical clock %d", s, t)
	}

	if s.Logical > 0 {
		log.Fatalf("Trivial timestamp has logical component")
	}

	fmt.Printf("The Unix Epoch is now approximately %dns old.\n", t.WallTime)
}

func TestLess(t *testing.T) {
	m := NewManualClock(0)
	c := NewClock(m.UnixNano)
	a := c.Timestamp()
	b := c.Timestamp()
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m.Set(1)
	b = c.Now()
	if !a.Less(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if !b.Less(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestEqual(t *testing.T) {
	m := NewManualClock(0)
	c := NewClock(m.UnixNano)
	a := c.Timestamp()
	b := c.Timestamp()
	if !a.Equal(b) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m.Set(1)
	b = c.Now()
	if a.Equal(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if b.Equal(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

// TestClock performs a complete test of all basic phenomena,
// including backward jumps in local physical time and clock offset.
func TestClock(t *testing.T) {
	m := NewManualClock(0)
	c := NewClock(m.UnixNano)
	c.SetMaxOffset(1000)
	expectedHistory := []struct {
		// The physical time that this event should take place at.
		wallClock int64
		event     Event
		// If this is a receive event, this holds the "input" timestamp.
		input *roachpb.Timestamp
		// The expected timestamp generated from the input.
		expected roachpb.Timestamp
	}{
		// A few valid steps to warm up.
		{5, SEND, nil, roachpb.Timestamp{WallTime: 5, Logical: 0}},
		{6, SEND, nil, roachpb.Timestamp{WallTime: 6, Logical: 0}},
		{10, RECV, &roachpb.Timestamp{WallTime: 10, Logical: 5}, roachpb.Timestamp{WallTime: 10, Logical: 6}},
		// Our clock mysteriously jumps back.
		{7, SEND, nil, roachpb.Timestamp{WallTime: 10, Logical: 7}},
		// Wall clocks coincide, but the local logical clock wins.
		{8, RECV, &roachpb.Timestamp{WallTime: 10, Logical: 4}, roachpb.Timestamp{WallTime: 10, Logical: 8}},
		// Wall clocks coincide, but the remote logical clock wins.
		{10, RECV, &roachpb.Timestamp{WallTime: 10, Logical: 99}, roachpb.Timestamp{WallTime: 10, Logical: 100}},
		// The physical clock has caught up and takes over.
		{11, RECV, &roachpb.Timestamp{WallTime: 10, Logical: 31}, roachpb.Timestamp{WallTime: 11, Logical: 0}},
		{11, SEND, nil, roachpb.Timestamp{WallTime: 11, Logical: 1}},
	}

	var current roachpb.Timestamp
	for i, step := range expectedHistory {
		m.Set(step.wallClock)
		switch step.event {
		case SEND:
			current = c.Now()
		case RECV:
			fallthrough
		default:
			previous := c.Timestamp()
			current = c.Update(*step.input)
			if current.Equal(previous) {
				t.Errorf("%d: clock not updated", i)
			}
		}
		if !current.Equal(step.expected) {
			t.Fatalf("HLC error: %d expected %v, got %v", i, step.expected, current)
		}
	}
	c.Now()
}

// TestExampleManualClock shows how a manual clock can be
// used as a physical clock. This is useful for testing.
func TestExampleManualClock(t *testing.T) {
	m := NewManualClock(10)
	c := NewClock(m.UnixNano)
	c.Now()
	if c.Timestamp().WallTime != 10 {
		t.Fatalf("manual clock error")
	}
	m.Set(20)
	c.Now()
	if c.Timestamp().WallTime != 20 {
		t.Fatalf("manual clock error")
	}
}

func TestMonotonicityCheck(t *testing.T) {
	m := NewManualClock(100000)
	c := NewClock(m.UnixNano)

	// Update the state of the hybrid clock.
	firstTime := c.Now()

	m.Increment((-10 * time.Minute).Nanoseconds())

	secondTime := c.Now()
	if c.monotonicityErrorsCount != 1 {
		t.Fatalf("clock backward jump was not detected by the monotonicity checker (from %s to %s)", firstTime, secondTime)
	}

	c.SetMaxOffset(10 * time.Hour)

	m.Increment((-10 * time.Minute).Nanoseconds())

	thirdTime := c.Now()
	if c.monotonicityErrorsCount != 1 {
		t.Fatalf("clock backward jump below threshold was incorrectly detected by the monotonicity checker (from %s to %s)", secondTime, thirdTime)
	}

}
