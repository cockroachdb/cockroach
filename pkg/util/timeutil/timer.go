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
	"sync"
	"time"
)

var timerPool = sync.Pool{
	New: func() interface{} {
		return &Timer{}
	},
}
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
//  var timer timeutil.Timer
//  defer timer.Stop()
//  for {
//      timer.Reset(wait)
//      switch {
//      case <-timer.C:
//          timer.Read = true
//          ...
//      }
//  }
//
// Note that unlike the standard library's Timer type, this Timer will
// not begin counting down until Reset is called for the first time, as
// there is no constructor function.
type Timer struct {
	timer *time.Timer
	// C is a local "copy" of timer.C that can be used in a select case before
	// the timer has been initialized (via Reset).
	C    <-chan time.Time
	Read bool
}

// NewTimer allocates a new timer.
func NewTimer() *Timer {
	return timerPool.Get().(*Timer)
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
	if !t.timer.Stop() && !t.Read {
		<-t.C
	}
	t.timer.Reset(d)
	t.Read = false
}

// Stop prevents the Timer from firing. It returns true if the call stops
// the timer, false if the timer has already expired, been stopped previously,
// or had never been initialized with a call to Timer.Reset. Stop does not
// close the channel, to prevent a read from succeeding incorrectly.
// Note that a Timer must never be used again after calls to Stop as the timer
// object will be put into an object pool for reuse.
func (t *Timer) Stop() bool {
	var res bool
	if t.timer != nil {
		res = t.timer.Stop()
		if res {
			// Only place the timer back in the pool if we successfully stopped
			// it. Otherwise, we'd have to read from the channel if !t.Read.
			timeTimerPool.Put(t.timer)
		}
	}
	*t = Timer{}
	timerPool.Put(t)
	return res
}
