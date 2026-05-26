// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timers

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/prometheus/client_golang/prometheus"
)

func BenchmarkTimerHandle(b *testing.B) {
	histOpts := metric.HistogramOptions{
		Metadata: metric.Metadata{
			Name:        "test.timer",
			Help:        "Test timer",
			Unit:        metric.Unit_NANOSECONDS,
			Measurement: "Latency",
		},
		Duration: time.Hour,
		Buckets:  prometheus.ExponentialBucketsRange(float64(1*time.Microsecond), float64(1*time.Hour), 60),
		Mode:     metric.HistogramModePrometheus,
	}
	builder := aggmetric.MakeBuilder("scope")
	hist := builder.Histogram(histOpts).AddChild("test")
	timer := &timer{hist: hist}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		handle := timer.Start()
		_ = handle.End()
	}

}
