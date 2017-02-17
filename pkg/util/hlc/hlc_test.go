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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	c := NewClock(UnixNano, time.Nanosecond)
	// Update the state of the hybrid clock.
	s := c.Now()
	time.Sleep(50 * time.Nanosecond)
	t := Timestamp{WallTime: UnixNano()}
	// The sanity checks below will usually never be triggered.

	if s.Less(t) || !t.Less(s) {
		log.Fatalf(context.Background(), "The later timestamp is smaller than the earlier one")
	}

	if t.WallTime-s.WallTime > 0 {
		log.Fatalf(context.Background(), "HLC timestamp %d deviates from physical clock %d", s, t)
	}

	if s.Logical > 0 {
		log.Fatalf(context.Background(), "Trivial timestamp has logical component")
	}

	fmt.Printf("The Unix Epoch is now approximately %dns old.\n", t.WallTime)
}

func TestHLCLess(t *testing.T) {
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, time.Nanosecond)
	a := c.Now()
	b := a
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m.Increment(1)
	b = c.Now()
	if !a.Less(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if !b.Less(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestHLCEqual(t *testing.T) {
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, time.Nanosecond)
	a := c.Now()
	b := a
	if a != b {
		t.Errorf("expected %+v == %+v", a, b)
	}
	m.Increment(1)
	b = c.Now()
	if a == b {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = c.Now() // add one to logical clock from b
	if b == a {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

// TestHLCClock performs a complete test of all basic phenomena,
// including backward jumps in local physical time and clock offset.
func TestHLCClock(t *testing.T) {
	m := NewManualClock(1)
	c := NewClock(m.UnixNano, 1000*time.Nanosecond)
	expectedHistory := []struct {
		// The physical time that this event should take place at.
		wallClock int64
		event     Event
		// If this is a receive event, this holds the "input" timestamp.
		input *Timestamp
		// The expected timestamp generated from the input.
		expected Timestamp
	}{
		// A few valid steps to warm up.
		{5, SEND, nil, Timestamp{WallTime: 5, Logical: 0}},
		{6, SEND, nil, Timestamp{WallTime: 6, Logical: 0}},
		{10, RECV, &Timestamp{WallTime: 10, Logical: 5}, Timestamp{WallTime: 10, Logical: 6}},
		// Our clock mysteriously jumps back.
		{7, SEND, nil, Timestamp{WallTime: 10, Logical: 7}},
		// Wall clocks coincide, but the local logical clock wins.
		{8, RECV, &Timestamp{WallTime: 10, Logical: 4}, Timestamp{WallTime: 10, Logical: 9}},
		// Wall clocks coincide, but the remote logical clock wins.
		{10, RECV, &Timestamp{WallTime: 10, Logical: 99}, Timestamp{WallTime: 10, Logical: 100}},
		// The physical clock has caught up and takes over.
		{11, RECV, &Timestamp{WallTime: 10, Logical: 31}, Timestamp{WallTime: 11, Logical: 1}},
		{11, SEND, nil, Timestamp{WallTime: 11, Logical: 2}},
	}

	var current Timestamp
	for i, step := range expectedHistory {
		m.Set(step.wallClock)
		switch step.event {
		case SEND:
			current = c.Now()
		case RECV:
			fallthrough
		default:
			previous := c.Now()
			current = c.Update(*step.input)
			if current == previous {
				t.Errorf("%d: clock not updated", i)
			}
		}
		if current != step.expected {
			t.Fatalf("HLC error: %d expected %v, got %v", i, step.expected, current)
		}
	}
}

// TestExampleManualClock shows how a manual clock can be
// used as a physical clock. This is useful for testing.
func TestExampleManualClock(t *testing.T) {
	m := NewManualClock(10)
	c := NewClock(m.UnixNano, time.Nanosecond)
	if wallNanos := c.Now().WallTime; wallNanos != 10 {
		t.Fatalf("unexpected wall time: %d", wallNanos)
	}
	m.Increment(10)
	if wallNanos := c.Now().WallTime; wallNanos != 20 {
		t.Fatalf("unexpected wall time: %d", wallNanos)
	}
}

func TestHLCMonotonicityCheck(t *testing.T) {
	m := NewManualClock(100000)
	c := NewClock(m.UnixNano, 100*time.Nanosecond)

	// Update the state of the hybrid clock.
	firstTime := c.Now()
	m.Increment((-110 * time.Nanosecond).Nanoseconds())
	secondTime := c.Now()

	{
		c.mu.Lock()
		errCount := c.mu.monotonicityErrorsCount
		c.mu.Unlock()

		if errCount != 1 {
			t.Fatalf("clock backward jump was not detected by the monotonicity checker (from %s to %s)", firstTime, secondTime)
		}
	}

	m.Increment((-10 * time.Nanosecond).Nanoseconds())
	thirdTime := c.Now()

	{
		c.mu.Lock()
		errCount := c.mu.monotonicityErrorsCount
		c.mu.Unlock()

		if errCount != 1 {
			t.Fatalf("clock backward jump below threshold was incorrectly detected by the monotonicity checker (from %s to %s)", secondTime, thirdTime)
		}
	}
}
