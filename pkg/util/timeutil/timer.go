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
// This timer implementation is an abstraction around the standard
// library's time.Timer that provides a temporary workaround for the
// issue described in https://github.com/golang/go/issues/14038. As
// such, this timer should only be used when Reset is planned to
// be called continually in a loop. For this Reset pattern to work,
// Timer.Read must be set to true whenever a timestamp is read from
// the Timer.C channel. If Timer.Read is not set to true when the
// channel is read from, the next call to Timer.Reset will deadlock.
// This pattern looks something like:
//
//	var timer timeutil.Timer
//	defer timer.Stop()
//	for {
//	    timer.Reset(wait)
//	    select {
//	    case <-timer.C:
//	        timer.Read = true
//	        ...
//	    }
//	}
//
// Note that unlike the standard library's Timer type, this Timer will
// not begin counting down until Reset is called for the first time, as
// there is no constructor function. The zero value for Timer is ready
// to use.
//
// TODO(nvanbenschoten): follow https://github.com/golang/go/issues/37196
// and remove this abstraction once it's no longer needed. There's some
// recent progress in https://go-review.googlesource.com/c/go/+/568341.
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
// the new value of the timer. This method includes the fix proposed
// in https://github.com/golang/go/issues/11513#issuecomment-157062583,
// but requires users of Timer to set Timer.Read to true whenever
// they successfully read from the Timer's channel.
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
	t.stopAndDrain()
	t.timer.Reset(d)
	t.Read = false
}

// Stop prevents the Timer from firing. It returns true if the call stops
// the timer, false if the timer has already expired, been stopped previously,
// or had never been initialized with a call to Timer.Reset. Stop does not
// close the channel, to prevent a read from succeeding incorrectly.
func (t *Timer) Stop() bool {
	var res bool
	if t.timer != nil {
		res = t.stopAndDrain()
		timeTimerPool.Put(t.timer)
	}
	*t = Timer{}
	return res
}

// stopAndDrain stops the underlying *time.Timer and drains the channel if the
// timer has already expired but the channel has not been read from. It returns
// true if the call stops the timer and false if the timer has already expired.
// t.timer must not be nil and must not have already been stopped.
func (t *Timer) stopAndDrain() bool {
	res := t.timer.Stop()
	if !res && !t.Read {
		// The timer expired, but the channel has not been read from. Drain it.
		<-t.C
		// Even though we did not stop the timer before it expired, the channel was
		// never read from and we had to drain it ourselves, so we consider the stop
		// attempt successful. For any caller consulting this return value, this is
		// an indication that after the call to Stop, the timer channel will remain
		// empty until the next call to Reset.
		res = true
	}
	return res
}
