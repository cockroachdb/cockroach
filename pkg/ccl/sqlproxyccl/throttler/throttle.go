// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package throttler

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// throttleDisabled is a sentinel value used to disable the throttle.
	throttleDisabled = time.Duration(0)
	// throttleLogErrorDuration indicates how frequent the throttle error should
	// be logged for a given throttle instance.
	throttleLogErrorDuration = 5 * time.Minute
)

type throttle struct {
	// The next time an operation blocked by this throttle can proceed.
	nextTime time.Time
	// The amount of backoff to introduce the next time the throttle
	// is triggered. Setting nextBackoff to zero disables the throttle.
	nextBackoff time.Duration
	// everyLog controls how frequent the throttle error should be logged.
	everyLog log.EveryN
}

func newThrottle(initialBackoff time.Duration) *throttle {
	return &throttle{
		nextTime:    time.Time{},
		nextBackoff: initialBackoff,
		everyLog:    log.Every(throttleLogErrorDuration),
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
