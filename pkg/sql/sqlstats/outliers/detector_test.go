// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package outliers

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/require"
)

func TestAnyDetector(t *testing.T) {
	t.Run("enabled is false without any detectors", func(t *testing.T) {
		detector := &anyDetector{}
		require.False(t, detector.enabled())
	})

	t.Run("enabled is false with all disabled detectors", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{}, &fakeDetector{}}}
		require.False(t, detector.enabled())
	})

	t.Run("enabled is true with at least one enabled detector", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{stubEnabled: true}, &fakeDetector{}}}
		require.True(t, detector.enabled())
	})

	t.Run("isOutlier is false without any detectors", func(t *testing.T) {
		detector := &anyDetector{}
		require.False(t, detector.isOutlier(&Outlier_Statement{}))
	})

	t.Run("isOutlier is false without any concerned detectors", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{}, &fakeDetector{}}}
		require.False(t, detector.isOutlier(&Outlier_Statement{}))
	})

	t.Run("isOutlier is true with at least one concerned detector", func(t *testing.T) {
		detector := &anyDetector{[]detector{&fakeDetector{stubIsOutlier: true}, &fakeDetector{}}}
		require.True(t, detector.isOutlier(&Outlier_Statement{}))
	})

	t.Run("isOutlier consults all detectors without short-circuiting", func(t *testing.T) {
		// Detector implementations may wish to observe all statements, to
		// build up their baseline sense of what "usual" is. To short-circuit
		// would deny them that chance.
		d1 := &fakeDetector{stubIsOutlier: true}
		d2 := &fakeDetector{stubIsOutlier: true}
		detector := &anyDetector{[]detector{d1, d2}}
		detector.isOutlier(&Outlier_Statement{})
		require.True(t, d1.isOutlierCalled, "the first detector should be consulted")
		require.True(t, d2.isOutlierCalled, "the second detector should be consulted")
	})
}

func TestLatencyQuantileDetector(t *testing.T) {
	t.Run("enabled false by default", func(t *testing.T) {
		d := newLatencyQuantileDetector(cluster.MakeTestingClusterSettings(), NewMetrics())
		require.False(t, d.enabled())
	})

	t.Run("enabled true by cluster setting", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(context.Background(), &st.SV, true)
		d := newLatencyQuantileDetector(st, NewMetrics())
		require.True(t, d.enabled())
	})

	t.Run("isOutlier false with normal latency", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(ctx, &st.SV, true)
		LatencyQuantileDetectorInterestingThreshold.Override(ctx, &st.SV, 0)
		d := newLatencyQuantileDetector(st, NewMetrics())
		for i := 0; i < 1000; i++ { // Seed the detector with many instances of this statement running in 100ms.
			d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1})
		}
		// Another occurrence with the same latency should not be an outlier.
		require.False(t, d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1}))
	})

	t.Run("isOutlier true with higher latency", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(ctx, &st.SV, true)
		LatencyQuantileDetectorInterestingThreshold.Override(ctx, &st.SV, 0)
		d := newLatencyQuantileDetector(st, NewMetrics())
		for i := 0; i < 1000; i++ { // Seed the detector with many instances of this statement running in 100ms.
			d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1})
		}
		// Another occurrence with a higher latency should be an outlier.
		require.True(t, d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.2}))
	})

	t.Run("isOutlier false with higher latency under 100ms", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(ctx, &st.SV, true)
		LatencyQuantileDetectorInterestingThreshold.Override(ctx, &st.SV, 100*time.Millisecond)
		d := newLatencyQuantileDetector(st, NewMetrics())
		for i := 0; i < 1000; i++ { // Seed the detector with many instances of this statement running in 10ms.
			d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.01})
		}
		// Another occurrence with a higher latency, but less than 100ms, should not be an outlier.
		require.False(t, d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.02}))
	})

	t.Run("reports distinct fingerprints", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(ctx, &st.SV, true)
		LatencyQuantileDetectorInterestingThreshold.Override(ctx, &st.SV, 100*time.Millisecond)
		metrics := NewMetrics()
		d := newLatencyQuantileDetector(st, metrics)
		d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1})
		require.Equal(t, int64(1), metrics.Fingerprints.Value())
	})

	t.Run("reports distinct fingerprints, taking eviction into account", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(context.Background(), &st.SV, true)
		metrics := NewMetrics()
		d := newLatencyQuantileDetector(st, metrics)
		// Observe 20k distinct fingerprints. Since each Stream with one observation requires ~100 bytes,
		// without any eviction that would put us around 2 megabytes of memory used, surpassing our
		// 1 megabyte cap and triggering some evictions.
		for i := 0; i < 20_000; i++ {
			d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1, FingerprintID: roachpb.StmtFingerprintID(i)})
		}
		require.Less(t, metrics.Fingerprints.Value(), int64(20_000))
	})

	t.Run("reports memory usage", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(context.Background(), &st.SV, true)
		metrics := NewMetrics()
		d := newLatencyQuantileDetector(st, metrics)
		d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1})
		require.Greater(t, metrics.Memory.Value(), int64(0))
	})

	t.Run("uses at most 1MB of memory", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(context.Background(), &st.SV, true)
		metrics := NewMetrics()
		d := newLatencyQuantileDetector(st, metrics)
		// Observe 20k distinct fingerprints. Since each Stream with one observation requires ~100 bytes,
		// without any eviction that would put us around 2 megabytes of memory used, surpassing our
		// 1 megabyte cap and triggering some evictions.
		for i := 0; i < 20_000; i++ {
			d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1, FingerprintID: roachpb.StmtFingerprintID(i)})
		}
		require.Less(t, metrics.Memory.Value(), int64(1024*1024))
	})

	t.Run("reports cache evictions", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(context.Background(), &st.SV, true)
		metrics := NewMetrics()
		d := newLatencyQuantileDetector(st, metrics)
		// Observe 20k distinct fingerprints. Since each Stream with one observation requires ~100 bytes,
		// without any eviction that would put us around 2 megabytes of memory used, surpassing our
		// 1 megabyte cap and triggering some evictions.
		for i := 0; i < 20_000; i++ {
			d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.1, FingerprintID: roachpb.StmtFingerprintID(i)})
		}
		require.Greater(t, metrics.Evictions.Count(), int64(0))
	})

	t.Run("does not track a fingerprint until an execution exceeds 100ms, to conserve memory", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyQuantileDetectionEnabled.Override(context.Background(), &st.SV, true)
		metrics := NewMetrics()
		d := newLatencyQuantileDetector(st, metrics)
		d.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.01})
		require.Equal(t, int64(0), metrics.Memory.Value())
	})
}

// dev bench pkg/sql/sqlstats/outliers  --bench-mem --verbose
// BenchmarkQuantileDetector-16    	 1705492	       688.5 ns/op	      72 B/op	       2 allocs/op
func BenchmarkLatencyQuantileDetector(b *testing.B) {
	random := rand.New(rand.NewSource(42))
	settings := cluster.MakeTestingClusterSettings()
	LatencyQuantileDetectionEnabled.Override(context.Background(), &settings.SV, true)
	d := newLatencyQuantileDetector(settings, NewMetrics())
	for i := 0; i < b.N; i++ {
		d.isOutlier(&Outlier_Statement{
			LatencyInSeconds: random.Float64(),
		})
	}
}

func TestLatencyThresholdDetector(t *testing.T) {
	t.Run("enabled false by default", func(t *testing.T) {
		detector := latencyThresholdDetector{st: cluster.MakeTestingClusterSettings()}
		require.False(t, detector.enabled())
	})

	t.Run("enabled true with nonzero threshold", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.True(t, detector.enabled())
	})

	t.Run("isOutlier false when disabled", func(t *testing.T) {
		detector := latencyThresholdDetector{st: cluster.MakeTestingClusterSettings()}
		require.False(t, detector.isOutlier(&Outlier_Statement{LatencyInSeconds: 1}))
	})

	t.Run("isOutlier false when fast enough", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.False(t, detector.isOutlier(&Outlier_Statement{LatencyInSeconds: 0.5}))
	})

	t.Run("isOutlier true beyond threshold", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LatencyThreshold.Override(context.Background(), &st.SV, 1*time.Second)
		detector := latencyThresholdDetector{st: st}
		require.True(t, detector.isOutlier(&Outlier_Statement{LatencyInSeconds: 1}))
	})
}

type fakeDetector struct {
	stubEnabled     bool
	stubIsOutlier   bool
	isOutlierCalled bool
}

func (f fakeDetector) enabled() bool {
	return f.stubEnabled
}

func (f *fakeDetector) isOutlier(_ *Outlier_Statement) bool {
	f.isOutlierCalled = true
	return f.stubIsOutlier
}

var _ detector = &fakeDetector{}
