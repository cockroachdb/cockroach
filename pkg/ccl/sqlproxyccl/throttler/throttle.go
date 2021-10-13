// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package throttler

import "time"

const (
	// throttleDisabled is a sentinal value used to disable the throttle.
	throttleDisabled = time.Duration(0)
)

type throttle struct {
	// The next time an operation blocked by this throttle can proceed.
	nextTime time.Time
	// The amount of backoff to introduce the next time the throttle
	// is triggered. Setting nextBackoff to zero disables the throttle.
	nextBackoff time.Duration
}

func newThrottle(initialBackoff time.Duration) *throttle {
	return &throttle{
		nextTime:    time.Time{},
		nextBackoff: initialBackoff,
	}
}

func (l *throttle) triggerThrottle(now time.Time, maxBackoff time.Duration) {
	l.nextTime = now.Add(l.nextBackoff)
	l.nextBackoff *= 2
	if maxBackoff < l.nextBackoff {
		l.nextBackoff = maxBackoff
	}
}

func (l *throttle) isThrottled(throttleTime time.Time) bool {
	return l.nextBackoff != throttleDisabled && throttleTime.Before(l.nextTime)
}

func (l *throttle) disable() {
	l.nextBackoff = throttleDisabled
}
