// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"sync"
	"time"
)

var timeTimerPool sync.Pool

// The Timer type represents a single event. When the Timer expires,
// the current time will be sent on Timer.C.
//
// This timer implementation is an abstraction around the standard library's
// time.Timer that uses a pool of stopped timers to reduce allocations.
//
// Note that unlike the standard library's Timer type, this Timer will
// not begin counting down until Reset is called for the first time, as
// there is no constructor function. The zero value for Timer is ready
// to use.
type Timer struct {
	timer *time.Timer
	// C is a local "copy" of timer.C that can be used in a select case before
	// the timer has been initialized (via Reset).
	C    <-chan time.Time
	Read bool
}

// AsTimerI returns the Timer as a TimerI. This is helpful
// to write code that accepts a Timer in production and a manual
// timer in tests.
func (t *Timer) AsTimerI() TimerI {
	return (*timer)(t)
}

// Reset changes the timer to expire after duration d and returns
// the new value of the timer.
func (t *Timer) Reset(d time.Duration) {
	if t.timer == nil {
		switch timer := timeTimerPool.Get(); timer {
		case nil:
			t.timer = time.NewTimer(d)
		default:
			t.timer = timer.(*time.Timer)
			t.timer.Reset(d)
		}
		t.C = t.timer.C
		return
	}
	t.timer.Reset(d)
}

// Stop prevents the Timer from firing. It returns true if the call stops
// the timer, false if the timer has already expired, been stopped previously,
// or had never been initialized with a call to Timer.Reset. Stop does not
// close the channel, to prevent a read from succeeding incorrectly.
func (t *Timer) Stop() bool {
	var res bool
	if t.timer != nil {
		res = t.timer.Stop()
		timeTimerPool.Put(t.timer)
	}
	*t = Timer{}
	return res
}
