// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package slinstance_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// BenchmarkHeartbeatThroughput measures the throughput and concurrency
// characteristics of the SQL liveness heartbeat loop with and without jitter.
//
// This benchmark creates many concurrent SQL liveness instances sharing a
// single instrumented storage backend. It measures:
// - Operations per second
// - Peak concurrent storage operations (to detect thundering herd effects)
//
// With jitter enabled, we expect lower peak concurrency due to heartbeats
// being spread out over time rather than synchronized.
func BenchmarkHeartbeatThroughput(b *testing.B) {
	const numInstances = 300

	for _, tc := range []struct {
		name       string
		jitterFrac float64
	}{
		{"NoJitter", 0},
		{"Jitter5", 0.05},
		{"Jitter10", 0.1},
		{"Jitter15", 0.15},
		{"Jitter20", 0.2},
		{"Jitter50", 0.5},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			stopper := stop.NewStopper()

			var ambientCtx log.AmbientContext
			clock := hlc.NewClockForTesting(timeutil.DefaultTimeSource{})
			settings := cluster.MakeTestingClusterSettingsWithVersions(
				clusterversion.Latest.Version(),
				clusterversion.MinSupported.Version(),
				true, /* initializeVersion */
			)
			slbase.DefaultTTL.Override(ctx, &settings.SV, 10*time.Millisecond)
			slbase.DefaultHeartBeat.Override(ctx, &settings.SV, 5*time.Millisecond)
			// Override the jitter setting to test different configurations.
			slbase.HeartbeatJitter.Override(ctx, &settings.SV, tc.jitterFrac)

			storage := slstorage.NewInstrumentedStorage()
			defaultHB := slbase.DefaultHeartBeat.Get(&settings.SV)

			instances := make([]*slinstance.Instance, numInstances)
			for i := 0; i < numInstances; i++ {
				instances[i] = slinstance.NewSQLInstance(
					ambientCtx, stopper, clock, storage, settings, nil, nil,
				)
			}
			for i := 0; i < numInstances; i++ {
				instances[i].Start(ctx, nil)
			}
			var wg sync.WaitGroup
			for i := 0; i < numInstances; i++ {
				wg.Add(1)
				go func(inst *slinstance.Instance) {
					defer wg.Done()
					for {
						if _, err := inst.Session(ctx); err == nil {
							return
						}
						time.Sleep(100 * time.Microsecond)
					}
				}(instances[i])
			}
			wg.Wait()

			// Reset metrics after initial session creation.
			storage.ResetMetrics()

			// Reset the timer to exclude setup time.
			b.ResetTimer()

			// Run the benchmark for the specified number of heartbeat cycles.
			// We run b.N heartbeat intervals worth of time.
			runDuration := time.Duration(b.N) * defaultHB
			time.Sleep(runDuration)

			b.StopTimer()

			// Stop all heartbeat loops before reading metrics to avoid
			// post-window activity affecting the measurements.
			stopper.Stop(ctx)

			// Report metrics.
			totalOps := storage.TotalUpdates()
			peakConc := storage.PeakConcurrency()

			// Report ops per second.
			if runDuration > 0 {
				opsPerSec := float64(totalOps) / runDuration.Seconds()
				b.ReportMetric(opsPerSec, "ops/sec")
			}
			b.ReportMetric(float64(peakConc), "peak_conc")
			b.ReportMetric(float64(totalOps), "total_ops")
		})
	}
}
