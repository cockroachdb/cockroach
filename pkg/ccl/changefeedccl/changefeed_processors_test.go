package changefeedccl

import (
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
				config := saveRateConfig{
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
				}

				now := timeutil.Now()
				clock := timeutil.NewManualTime(now)
				l, err := newSaveRateLimiter(config, clock)
				require.NoError(t, err)
				_ = l
			})
		}
	}
}

func TestSaveRateLimiterError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for name, tc := range map[string]struct {
		config      saveRateConfig
		expectedErr string
	}{
		"missing name": {
			config: saveRateConfig{
				intervalName: func() redact.SafeValue {
					return redact.SafeString("interval")
				},
				interval: func() time.Duration {
					return 30 * time.Second
				},
			},
			expectedErr: "name is required",
		},
		"missing interval name": {
			config: saveRateConfig{
				name: "test",
				interval: func() time.Duration {
					return 30 * time.Second
				},
			},
			expectedErr: "interval name is required",
		},
		"missing interval": {
			config: saveRateConfig{
				name: "test",
				intervalName: func() redact.SafeValue {
					return redact.SafeString("interval")
				},
			},
			expectedErr: "interval is required",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.expectedErr == "" {
				t.Fatal("missing expected error")
			}
			_, err := newSaveRateLimiter(tc.config, timeutil.DefaultTimeSource{})
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
