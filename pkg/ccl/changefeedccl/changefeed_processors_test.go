// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestSaveRateLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	intervals := map[string]time.Duration{
		"positive interval":   30 * time.Second,
		"negative interval":   -30 * time.Second,
		"zero interval":       0,
		"very small interval": time.Nanosecond,
	}

	jitters := map[string]float64{
		"positive jitter":   0.1,
		"negative jitter":   -0.1,
		"zero jitter":       0,
		"very small jitter": 1e-12,
	}

	for intervalName, interval := range intervals {
		for jitterName, jitter := range jitters {
			t.Run(fmt.Sprintf("%s with %s", intervalName, jitterName), func(t *testing.T) {
				// Set up the mock clock for testing.
				now := timeutil.Now()
				clock := timeutil.NewManualTime(now)

				// Create the save rate limiter.
				l, err := newSaveRateLimiter(saveRateConfig{
					name: "test",
					intervalName: func() redact.SafeValue {
						return redact.SafeString(intervalName)
					},
					interval: func() time.Duration {
						return interval
					},
					jitter: func() float64 {
						return jitter
					},
				}, clock)
				require.NoError(t, err)

				// A non-positive interval indicates that saving is disabled so we only
				// need to test that we can't save at all.
				if interval <= 0 {
					require.False(t, l.canSave(ctx))
					clock.Advance(24 * time.Hour)
					require.False(t, l.canSave(ctx))
					return
				}

				// We can do one save right away if the interval.
				require.True(t, l.canSave(ctx))
				l.doneSave(0 /* saveDuration */)

				// Can't immediately save again.
				require.False(t, l.canSave(ctx))

				// Make sure interval and jitter works correctly.
				var maxJitter time.Duration
				if jitter > 0 {
					maxJitter = time.Duration(jitter * float64(interval))
				}
				clock.Advance(interval + maxJitter)
				require.True(t, l.canSave(ctx))

				// Set the save duration to something high to make sure we can't save
				// due to high average save duration.
				l.doneSave(time.Hour)
				clock.Advance(interval + maxJitter)
				require.False(t, l.canSave(ctx))
				clock.Advance(time.Hour - (interval + maxJitter))
				require.True(t, l.canSave(ctx))

				// Set the save duration to something even higher to make sure the
				// average algorithm works.
				l.doneSave(2 * time.Hour)
				clock.Advance(time.Hour)
				require.False(t, l.canSave(ctx))
				clock.Advance(time.Hour)
				require.True(t, l.canSave(ctx))
				l.doneSave(0 /* saveDuration */)
			})
		}
	}
}

func TestSaveRateLimiterError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	name := redact.SafeString("test")
	intervalName := func() redact.SafeValue {
		return redact.SafeString("interval")
	}
	interval := func() time.Duration {
		return 30 * time.Second
	}

	for testName, tc := range map[string]struct {
		config      saveRateConfig
		expectedErr string
	}{
		"missing name": {
			config: saveRateConfig{
				intervalName: intervalName,
				interval:     interval,
			},
			expectedErr: "name is required",
		},
		"missing interval name": {
			config: saveRateConfig{
				name:     name,
				interval: interval,
			},
			expectedErr: "interval name is required",
		},
		"missing interval": {
			config: saveRateConfig{
				name:         name,
				intervalName: intervalName,
			},
			expectedErr: "interval is required",
		},
	} {
		t.Run(testName, func(t *testing.T) {
			if tc.expectedErr == "" {
				t.Fatal("missing expected error")
			}
			_, err := newSaveRateLimiter(tc.config, timeutil.DefaultTimeSource{})
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
