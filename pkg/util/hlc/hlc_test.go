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

package hlc

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
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

// isErrSimilar returns true of the expected error is similar to the
// actual error
func isErrSimilar(expected *regexp.Regexp, actual error) bool {
	if actual == nil {
		return expected == nil
	}
	// actual != nil
	return expected != nil && expected.FindString(actual.Error()) != ""
}

func TestHLCPhysicalClockJump(t *testing.T) {
	var fatal bool
	defer log.SetExitFunc(os.Exit)
	log.SetExitFunc(func(r int) {
		if r != 0 {
			fatal = true
		}
	})

	testCases := []struct {
		name       string
		actualJump time.Duration
		maxJump    time.Duration
		isFatal    bool
	}{
		{
			name:       "small forward jump",
			actualJump: 5 * time.Second,
			maxJump:    50 * time.Second,
			isFatal:    false,
		},
		{
			name:       "large forward jump",
			actualJump: 51 * time.Second,
			maxJump:    50 * time.Second,
			isFatal:    true,
		},
		{
			name:       "large forward jump large thresh",
			actualJump: 51 * time.Second,
			maxJump:    70 * time.Second,
			isFatal:    false,
		},
		{
			name:       "small backward jump",
			actualJump: -4 * time.Second,
			maxJump:    50 * time.Second,
			isFatal:    false,
		},
		{
			name:       "large backward jump",
			actualJump: -7 * time.Second,
			maxJump:    50 * time.Second,
			isFatal:    false,
		},
		{
			name:       "large backward jump large thresh",
			actualJump: -7 * time.Second,
			maxJump:    90 * time.Second,
			isFatal:    false,
		},
	}

	maxJumpCh := make(chan time.Duration, 1)
	defer close(maxJumpCh)

	m := NewManualClock(1)
	c := NewClock(m.UnixNano, time.Nanosecond)
	var tickerDuration time.Duration
	tickerCh := make(chan time.Time)
	tickProcessedCh := make(chan struct{})
	if err := c.StartMonitoringClockJumps(
		maxJumpCh,
		func(d time.Duration) *time.Ticker {
			tickerDuration = d
			ticker := time.NewTicker(d)
			ticker.Stop()
			ticker.C = tickerCh
			return ticker
		},
		func() {
			tickProcessedCh <- struct{}{}
		},
	); err != nil {
		t.Error(err)
		return
	}

	if err := c.StartMonitoringClockJumps(
		maxJumpCh,
		time.NewTicker,
		nil, /* tick callback */
	); !isErrSimilar(regexp.MustCompile("already being monitored"), err) {
		t.Error("expected an error when starting monitor goroutine twice")
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)

			maxJumpCh <- test.maxJump
			<-tickProcessedCh

			fatal = false
			t1 := c.Now()
			a.Equal(false, fatal)

			m.Increment(int64(test.actualJump))
			tickerCh <- timeutil.Now()
			<-tickProcessedCh

			fatal = false
			t2 := c.Now()
			a.True(t1.Less(t2), fmt.Sprintf("expected %+v < %+v", t1, t2))
			// This should not fatal as tickerCh has ticked
			a.Equal(false, fatal)
			// After ticker ticks, last physical time should be equal to physical now
			lastPhysicalTime := c.lastPhysicalTime()
			physicalNow := c.PhysicalNow()
			a.Equal(lastPhysicalTime, physicalNow)

			// Potentially a fatal jump depending on the test case
			fatal = false
			m.Increment(int64(test.actualJump))
			t3 := c.Now()
			a.True(t2.Less(t3), fmt.Sprintf("expected %+v < %+v", t2, t3))
			a.Equal(test.isFatal, fatal)

			a.True(
				tickerDuration <= test.maxJump,
				fmt.Sprintf(
					"ticker duration %+v should be less than max jump %+v",
					tickerDuration,
					test.maxJump,
				),
			)
		})
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
