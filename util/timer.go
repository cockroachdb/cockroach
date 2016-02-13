// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package util

import "time"

// The Timer type represents a single event. When the Timer expires,
// the current time will be sent on C.
//
// This timer implementation is an abstraction around the standard
// library's time.Timer that provides a temporary workaround for the
// issue described in https://github.com/golang/go/issues/14038. As
// such, this timer should only be used when Reset is planned to
// be called continually in a loop. For this Reset pattern to work,
// Timer.Read must be set to true whenever the timer goes off and a
// timestamp is read from the Timer.C channel. If Timer.Read is not
// set to true when the channel is read from, the timer will deadlock
// next time Timer.Reset is called. This pattern would then look
// something like:
//
//  var timer util.Timer
//  for {
//      timer = timer.Reset(wait)
//      switch {
//      case <-timer.C:
//          timer.Read = true
//          ...
//      }
//  }
//
// Note that unlike the standard library's Timer type, this Timer
// may be stack allocated if Timer.Reset is called immediately on
// the instance before use.
type Timer struct {
	*time.Timer
	Read bool
}

// Reset changes the timer to expire after duration d and returns
// the new value of the timer. This method includes the fix proposed
// in https://github.com/golang/go/issues/11513#issuecomment-157062583,
// but requires users of Timer to set Timer.Read to true whenever
// they successfully read from the Timer's channel. Reset operates on
// and returns a value so that Timer can be stack allocated.
func (t Timer) Reset(d time.Duration) Timer {
	if t.Timer == nil {
		t.Timer = time.NewTimer(d)
		return t
	}
	if !t.Timer.Reset(d) && !t.Read {
		<-t.C
	}
	t.Read = false
	return t
}

// Stop prevents the Timer from firing. It returns true if the call stops
// the timer, false if the timer has already expired, been stopped previously,
// or had never been initialized with a call to Timer.Reset. Stop does not
// close the channel, to prevent a read from the channel succeeding incorrectly.
func (t Timer) Stop() bool {
	if t.Timer == nil {
		return false
	}
	return t.Timer.Stop()
}
