// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/crlib/testutils/require"
)

func TestThrottle(t *testing.T) {
	skip.UnderDuress(t, "test is flaky under duress due to timing issues")
	const interval = 10 * time.Millisecond
	const unit = time.Millisecond

	type event struct {
		name string
		time time.Duration
	}
	testcases := []struct {
		name     string
		leading  bool
		trailing bool
		events   []event
		expected []string
	}{
		// Leading: true, Trailing: true
		{
			name:    "suppressed events should fire last event after interval",
			leading: true, trailing: true,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9},
			},
			expected: []string{"A", "D"},
		},
		{
			name:    "trailing event should block next event from firing immediately",
			leading: true, trailing: true,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9}, {"E", 11}, {"F", 17},
			},
			expected: []string{"A", "D", "F"},
		},
		{
			name:    "event fired after long interval should fire immediately",
			leading: true, trailing: true,
			events: []event{
				{"A", 0}, {"B", 21}, {"C", 22}, {"D", 23},
			},
			expected: []string{"A", "B", "D"},
		},
		// Leading: true, Trailing: false
		{
			name:    "suppressed event should not fire last event after interval",
			leading: true, trailing: false,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9},
			},
			expected: []string{"A"},
		},
		{
			name:    "next event immediately after first interval should fire",
			leading: true, trailing: false,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9}, {"E", 13},
			},
			expected: []string{"A", "E"},
		},
		{
			name:    "next event immediately after first interval should block next event",
			leading: true, trailing: false,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9}, {"E", 13}, {"F", 22},
			},
			expected: []string{"A", "E"},
		},
		{
			name:    "event fired after long interval should fire immediately",
			leading: true, trailing: false,
			events: []event{
				{"A", 0}, {"B", 21}, {"C", 22}, {"D", 23},
			},
			expected: []string{"A", "B"},
		},
		// Leading: false, Trailing: true
		{
			name:    "first event should be blocked in suppressed sequence",
			leading: false, trailing: true,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9},
			},
			expected: []string{"D"},
		},
		{
			name:    "next event immediately after first interval can fire if no follow up events",
			leading: false, trailing: true,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9}, {"E", 13},
			},
			expected: []string{"D", "E"},
		},
		{
			name:    "next event immediately after first interval should be blocked by follow up event",
			leading: false, trailing: true,
			events: []event{
				{"A", 0}, {"B", 5}, {"C", 7}, {"D", 9}, {"E", 13}, {"F", 18},
			},
			expected: []string{"D", "F"},
		},
		{
			name:    "suppressed events after long interval should result in two fired events",
			leading: false, trailing: true,
			events: []event{
				{"A", 0}, {"B", 21}, {"C", 22}, {"D", 23},
			},
			expected: []string{"A", "D"},
		},
	}
	for _, tc := range testcases {
		if !tc.leading && !tc.trailing {
			t.Fatalf("test case %q must have at least one of leading or trailing set to true", tc.name)
		}

		t.Run(
			fmt.Sprintf("leading=%t/trailing=%t/%s", tc.leading, tc.trailing, tc.name),
			func(t *testing.T) {
				start := timeutil.UnixEpoch
				timeSource := timeutil.NewManualTime(start)
				opts := Options{
					Interval: interval,
					Leading:  tc.leading,
					Trailing: tc.trailing,
					Clock:    timeSource,
				}
				throttler := NewThrottler(context.Background(), opts)

				var results []string
				for _, event := range tc.events {
					eventTime := start.Add(event.time * unit)

					for timeSource.Now().Before(eventTime) {
						timeSource.Advance(unit)
					}
					// While we do manually control the clock, the throttler itself is
					// listening on the timer in a separate goroutine, so we need to add
					// a small delay to ensure the timer has a chance to process timer
					// firings before we move on to the next event.
					time.Sleep(time.Millisecond)

					throttler.Call(func() {
						results = append(results, event.name)
					})
				}
				// We advance the time source by one interval to ensure that any
				// trailing calls are made, and then sleep for a short time to allow the
				// throttler to process the timer events.
				timeSource.Advance(interval)
				time.Sleep(time.Millisecond)

				// We advance the time source by another interval to allow for the
				// cooldown to complete.
				timeSource.Advance(interval * 2)
				waitCh := make(chan struct{})
				throttler.WaitForCooldown(waitCh)
				<-waitCh

				require.Equal(t, tc.expected, results)
			})
	}
}
