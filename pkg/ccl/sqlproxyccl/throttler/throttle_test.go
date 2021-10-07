// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package throttler

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestTriggerThrottle(t *testing.T) {
	type testCase struct {
		backoffBefore time.Duration
		backoffAfter  time.Duration
		maxBackoff    time.Duration
		timeAfter     time.Time
	}

	now := timeutil.Now()

	tests := []testCase{
		{
			backoffBefore: time.Second,
			backoffAfter:  time.Second * 2,
			maxBackoff:    time.Hour,
			timeAfter:     now.Add(time.Second),
		},
		{
			backoffBefore: time.Minute,
			backoffAfter:  time.Minute * 2,
			maxBackoff:    time.Hour,
			timeAfter:     now.Add(time.Minute),
		},
		{
			backoffBefore: time.Minute * 30,
			backoffAfter:  time.Hour,
			maxBackoff:    time.Hour,
			timeAfter:     now.Add(time.Minute * 30),
		},
		{
			backoffBefore: time.Minute * 45,
			backoffAfter:  time.Hour,
			maxBackoff:    time.Hour,
			timeAfter:     now.Add(time.Minute * 45),
		},
		{
			backoffBefore: time.Hour,
			backoffAfter:  time.Hour,
			maxBackoff:    time.Hour,
			timeAfter:     now.Add(time.Hour),
		},
	}

	for _, test := range tests {
		l := newThrottle(test.backoffBefore)
		require.Equal(t, l.nextBackoff, test.backoffBefore)
		l.triggerThrottle(now, test.maxBackoff)
		require.Equal(t, l.nextBackoff, test.backoffAfter)
		require.Equal(t, l.nextTime, test.timeAfter)
	}
}

func TestIsThrottled(t *testing.T) {
	now := timeutil.Now()

	l := newThrottle(time.Second)

	// Limiters are initialized with the limit disabled.
	require.False(t, l.isThrottled(now))
	require.False(t, l.isThrottled(now.Add(-time.Hour)))

	// Throttle for the next second
	l.triggerThrottle(now, time.Hour)

	require.True(t, l.isThrottled(now))
	require.True(t, l.isThrottled(now.Add(-time.Hour)))
	require.True(t, l.isThrottled(now.Add(time.Millisecond)))
	require.True(t, l.isThrottled(now.Add(999*time.Millisecond)))
	require.False(t, l.isThrottled(now.Add(1000*time.Millisecond)))
	require.False(t, l.isThrottled(now.Add(1500*time.Millisecond)))
	require.False(t, l.isThrottled(now.Add(1999*time.Millisecond)))
	require.False(t, l.isThrottled(now.Add(2000*time.Millisecond)))
	require.False(t, l.isThrottled(now.Add(time.Hour)))

	// Throttle for the next two seconds
	l.triggerThrottle(now, time.Hour)

	require.True(t, l.isThrottled(now))
	require.True(t, l.isThrottled(now.Add(-time.Hour)))
	require.True(t, l.isThrottled(now.Add(time.Millisecond)))
	require.True(t, l.isThrottled(now.Add(999*time.Millisecond)))
	require.True(t, l.isThrottled(now.Add(1000*time.Millisecond)))
	require.True(t, l.isThrottled(now.Add(1500*time.Millisecond)))
	require.True(t, l.isThrottled(now.Add(1999*time.Millisecond)))
	require.False(t, l.isThrottled(now.Add(2000*time.Millisecond)))
	require.False(t, l.isThrottled(now.Add(time.Hour)))
}

func TestDisableThrottle(t *testing.T) {
	now := timeutil.Now()
	l := newThrottle(time.Second)

	l.triggerThrottle(now, time.Hour)
	require.True(t, l.isThrottled(now))

	l.disable()
	require.False(t, l.isThrottled(now))
	require.Equal(t, l.nextBackoff, throttleDisabled)

	// Triggering the throttle does not renable the limiter.
	l.triggerThrottle(now, time.Hour)
	require.False(t, l.isThrottled(now))
	require.Equal(t, l.nextBackoff, throttleDisabled)
}
