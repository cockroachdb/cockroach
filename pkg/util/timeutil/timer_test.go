// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const timeStep = 10 * time.Millisecond

func TestTimerTimeout(t *testing.T) {
	var timer Timer
	defer func() {
		if stopped := timer.Stop(); stopped {
			t.Errorf("expected Stop to return false, got true")
		}
	}()
	timer.Reset(timeStep)

	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * timeStep):
	}
}

func TestTimerStop(t *testing.T) {
	for sleepMult := time.Duration(0); sleepMult < 3; sleepMult++ {
		sleepDur := sleepMult * timeStep
		t.Run(fmt.Sprintf("sleepDur=%d*timeStep", sleepMult), func(t *testing.T) {
			var timer Timer
			timer.Reset(timeStep)
			time.Sleep(sleepDur)

			// Get a handle to the timer channel before calling Stop, because Stop
			// clears the struct.
			c := timer.C

			// Even though we sleep for a certain duration which we know to be more
			// or less than the timer's duration, we can't assert whether the timer
			// fires before calling timer.Stop because we have no control over the
			// scheduler. Instead, we handle both cases to avoid flakiness and assert
			// that Stop returns the correct status.
			stopped := timer.Stop()
			select {
			case <-c:
				if stopped {
					t.Errorf("timer unexpectedly fired after stopping")
				}
			case <-time.After(5 * timeStep):
				if !stopped {
					t.Errorf("timer did not fire after failing to stop")
				}
			}
		})
	}

}

func TestTimerUninitializedStopNoop(t *testing.T) {
	var timer Timer
	if stopped := timer.Stop(); stopped {
		t.Errorf("expected Stop to return false when the timer was never reset, got true")
	}
}

func TestTimerResetBeforeTimeout(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	timer.Reset(timeStep)

	timer.Reset(timeStep)
	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * timeStep):
	}
}

func TestTimerResetAfterTimeoutAndNoRead(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	timer.Reset(timeStep)

	time.Sleep(2 * timeStep)

	timer.Reset(timeStep)
	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * timeStep):
	}
}

func TestTimerResetAfterTimeoutAndRead(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	timer.Reset(timeStep)

	<-timer.C
	timer.Read = true

	timer.Reset(timeStep)
	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * timeStep):
	}
}

func TestTimerMakesProgressInLoop(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	for i := 0; i < 5; i++ {
		timer.Reset(timeStep)
		<-timer.C
		timer.Read = true
	}
}

// TestIllegalTimerShare is a regression test for sharing the same Timer between
// multiple users when it was originally allocated on the stack of one of them
// but then later was put into timerPool on Stop() (see #119593).
func TestIllegalTimerShare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resetTimer := func(t *Timer, rng *rand.Rand) {
		t.Reset(time.Duration(rng.Intn(100)+1) * time.Nanosecond)
	}

	var wg sync.WaitGroup
	// Simulate a pattern of usage of the stack-allocated Timer that is being
	// stopped each time when the Timer fires.
	fromStack := func() {
		defer wg.Done()
		rng, _ := randutil.NewTestRand()
		var t Timer
		defer t.Stop()
		resetTimer(&t, rng)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				t.Read = true
				t.Stop()
				resetTimer(&t, rng)
			}
		}
	}
	// Simulate the most common pattern where the Timer is taken from the
	// timerPool, fires repeatedly, and then is stopped in a defer.
	fromPool := func() {
		defer wg.Done()
		rng, _ := randutil.NewTestRand()
		t := NewTimer()
		defer t.Stop()
		resetTimer(t, rng)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				t.Read = true
				resetTimer(t, rng)
			}
		}
	}
	// Spin up a few goroutines per each access pattern.
	wg.Add(6)
	for i := 0; i < 3; i++ {
		go fromStack()
		go fromPool()
	}
	wg.Wait()
}
