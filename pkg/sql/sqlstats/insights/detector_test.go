// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/stretchr/testify/require"
)

func TestAnyDetector(t *testing.T) {
	t.Run("enabled is false without any detectors", func(t *testing.T) {
		detector := &compositeDetector{}
		require.False(t, detector.enabled())
	})

	t.Run("enabled is false with all disabled detectors", func(t *testing.T) {
		detector := &compositeDetector{[]detector{&fakeDetector{}, &fakeDetector{}}}
		require.False(t, detector.enabled())
	})

	t.Run("enabled is true with at least one enabled detector", func(t *testing.T) {
		detector := &compositeDetector{[]detector{&fakeDetector{stubEnabled: true}, &fakeDetector{}}}
		require.True(t, detector.enabled())
	})

	t.Run("isSlow is false without any detectors", func(t *testing.T) {
		detector := &compositeDetector{}
		require.False(t, detector.isSlow(&Statement{}))
	})

	t.Run("isSlow is false without any concerned detectors", func(t *testing.T) {
		detector := &compositeDetector{[]detector{&fakeDetector{}, &fakeDetector{}}}
		require.False(t, detector.isSlow(&Statement{}))
	})

	t.Run("isSlow is true with at least one concerned detector", func(t *testing.T) {
		detector := &compositeDetector{[]detector{&fakeDetector{stubIsSlow: true}, &fakeDetector{}}}
		require.True(t, detector.isSlow(&Statement{}))
	})

	t.Run("isSlow consults all detectors without short-circuiting", func(t *testing.T) {
		// Detector implementations may wish to observe all statements, to
		// build up their baseline sense of what "usual" is. To short-circuit
		// would deny them that chance.
		d1 := &fakeDetector{stubIsSlow: true}
		d2 := &fakeDetector{stubIsSlow: true}

		detector := &compositeDetector{[]detector{d1, d2}}
		detector.isSlow(&Statement{})
		require.True(t, d1.isSlowCalled, "the first detector should be consulted")
		require.True(t, d2.isSlowCalled, "the second detector should be consulted")
	})
}

func TestLatencyQuantileDetector(t *testing.T) {
	t.Run("enabled false by cluster setting", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		d := newAnomalyDetector(st, NewMetrics())
		AnomalyDetectionEnabled.Override(context.Background(), &st.SV, false)
		require.False(t, d.enabled())
	})

	t.Run("enabled true by cluster setting", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		d := newAnomalyDetector(st, NewMetrics())
		AnomalyDetectionEnabled.Override(context.Background(), &st.SV, true)
		require.True(t, d.enabled())
	})

	t.Run("isSlow", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		AnomalyDetectionEnabled.Override(ctx, &st.SV, true)
		AnomalyDetectionLatencyThreshold.Override(ctx, &st.SV, 100*time.Millisecond)

		tests := []struct {
			name             string
			seedLatency      time.Duration
			candidateLatency time.Duration
			isSlow           bool
		}{{
			name:             "false with normal latency",
			seedLatency:      100 * time.Millisecond,
			candidateLatency: 100 * time.Millisecond,
			isSlow:           false,
		}, {
			name:             "true with higher latency",
			seedLatency:      100 * time.Millisecond,
			candidateLatency: 200 * time.Millisecond,
			isSlow:           true,
		}, {
			name:             "false with higher latency under interesting threshold",
			seedLatency:      10 * time.Millisecond,
			candidateLatency: 20 * time.Millisecond,
			isSlow:           false,
		}}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				d := newAnomalyDetector(st, NewMetrics())
				for i := 0; i < 1000; i++ {
					d.isSlow(&Statement{LatencyInSeconds: test.seedLatency.Seconds()})
				}
				require.Equal(t, test.isSlow, d.isSlow(&Statement{LatencyInSeconds: test.candidateLatency.Seconds()}))
			})
		}
	})

	// Testing the slow and failure detectors at the same time.
	t.Run("isSlow and isFailed", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		AnomalyDetectionEnabled.Override(ctx, &st.SV, true)
		AnomalyDetectionLatencyThreshold.Override(ctx, &st.SV, 100*time.Millisecond)

		tests := []struct {
			name             string
			seedLatency      time.Duration
			candidateLatency time.Duration
			status           Statement_Status
			isSlow           bool
			isFailed         bool
		}{{
			name:             "slow and failed statement",
			seedLatency:      100 * time.Millisecond,
			candidateLatency: 200 * time.Millisecond,
			status:           Statement_Failed,
			isSlow:           true,
			isFailed:         true,
		}, {
			name:             "slow and non-failed statement",
			seedLatency:      100 * time.Millisecond,
			candidateLatency: 200 * time.Millisecond,
			status:           Statement_Completed,
			isSlow:           true,
			isFailed:         false,
		}, {
			name:             "fast and non-failed statement",
			seedLatency:      100 * time.Millisecond,
			candidateLatency: 50 * time.Millisecond,
			status:           Statement_Completed,
			isSlow:           false,
			isFailed:         false,
		}, {
			name:             "fast and failed statement",
			seedLatency:      100 * time.Millisecond,
			candidateLatency: 50 * time.Millisecond,
			status:           Statement_Failed,
			isSlow:           false,
			isFailed:         true,
		}}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				d := newAnomalyDetector(st, NewMetrics())
				for i := 0; i < 1000; i++ {
					d.isSlow(&Statement{LatencyInSeconds: test.seedLatency.Seconds()})
				}
				stmt := &Statement{LatencyInSeconds: test.candidateLatency.Seconds(), Status: test.status}
				require.Equal(t, test.isSlow, d.isSlow(stmt))
				require.Equal(t, test.isFailed, isFailed(stmt))
			})
		}
	})

	t.Run("metrics", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		AnomalyDetectionEnabled.Override(ctx, &st.SV, true)
		AnomalyDetectionMemoryLimit.Override(ctx, &st.SV, 1024)

		tests := []struct {
			name         string
			fingerprints int
			assertion    func(*testing.T, Metrics)
		}{{
			name:         "reports distinct fingerprints",
			fingerprints: 1,
			assertion: func(t *testing.T, metrics Metrics) {
				require.Equal(t, int64(1), metrics.Fingerprints.Value())
			},
		}, {
			// Each Stream with one observation requires ~104 bytes,
			// so the 20 Streams here will consume ~2 kilobytes of memory,
			// surpassing the above 1 kilobyte cap and triggering some evictions.
			// We don't assume a precise number of evictions because platform
			// differences may lead to different memory usage calculations.
			name:         "reports distinct fingerprints, taking eviction into account",
			fingerprints: 20,
			assertion: func(t *testing.T, metrics Metrics) {
				require.Less(t, metrics.Fingerprints.Value(), int64(20))
			},
		}, {
			name:         "reports memory usage",
			fingerprints: 1,
			assertion: func(t *testing.T, metrics Metrics) {
				require.Greater(t, metrics.Memory.Value(), int64(0))
			},
		}, {
			name:         "reports memory usage, taking eviction into account",
			fingerprints: 20,
			assertion: func(t *testing.T, metrics Metrics) {
				require.LessOrEqual(t, metrics.Memory.Value(), int64(1024))
			},
		}, {
			name:         "reports evictions",
			fingerprints: 20,
			assertion: func(t *testing.T, metrics Metrics) {
				require.Greater(t, metrics.Evictions.Count(), int64(0))
			},
		}}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				metrics := NewMetrics()
				d := newAnomalyDetector(st, metrics)
				// Show the detector `test.fingerprints` distinct fingerprints.
				for i := 0; i < test.fingerprints; i++ {
					d.isSlow(&Statement{
						LatencyInSeconds: AnomalyDetectionLatencyThreshold.Get(&st.SV).Seconds(),
						FingerprintID:    appstatspb.StmtFingerprintID(i),
					})
				}
				test.assertion(t, metrics)
			})
		}
	})
}

// dev bench pkg/sql/sqlstats/insights  --bench-mem --verbose
// BenchmarkLatencyQuantileDetector-16    	 1589583	       701.1 ns/op	      24 B/op	       1 allocs/op
func BenchmarkLatencyQuantileDetector(b *testing.B) {
	random := rand.New(rand.NewSource(42))
	settings := cluster.MakeTestingClusterSettings()
	AnomalyDetectionEnabled.Override(context.Background(), &settings.SV, true)
	d := newAnomalyDetector(settings, NewMetrics())
	for i := 0; i < b.N; i++ {
		d.isSlow(&Statement{
			LatencyInSeconds: random.Float64(),
		})
	}
}

func TestLatencyThresholdDetector(t *testing.T) {
	t.Run("enabled false with zero threshold", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 0)
		detector := latencyThresholdDetector{st: st}
		require.False(t, detector.enabled())
	})

	t.Run("enabled true with nonzero threshold", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.True(t, detector.enabled())
	})

	t.Run("isSlow false when disabled", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 0)
		detector := latencyThresholdDetector{st: st}
		require.False(t, detector.isSlow(&Statement{LatencyInSeconds: 1}))
	})

	t.Run("isSlow false when fast enough", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.False(t, detector.isSlow(&Statement{LatencyInSeconds: 0.5}))
	})

	t.Run("isSlow true beyond threshold", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.True(t, detector.isSlow(&Statement{LatencyInSeconds: 1}))
	})
}

type fakeDetector struct {
	stubEnabled  bool
	stubIsSlow   bool
	isSlowCalled bool
}

func (f *fakeDetector) enabled() bool {
	return f.stubEnabled
}

func (f *fakeDetector) isSlow(*Statement) bool {
	f.isSlowCalled = true
	return f.stubIsSlow
}

var _ detector = &fakeDetector{}
