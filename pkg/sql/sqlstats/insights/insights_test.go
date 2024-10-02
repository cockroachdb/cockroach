// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/insights"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

// Here we benchmark the entire insights stack, so that we can include in our
// measurements the effects of any backpressure on the ingester applied by
// the registry.
func BenchmarkInsights(b *testing.B) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Enable the insights detectors, so that we can get some meaningful
	// backpressure from the registry.
	settings := cluster.MakeTestingClusterSettings()
	insights.LatencyThreshold.Override(ctx, &settings.SV, 100*time.Millisecond)
	insights.AnomalyDetectionEnabled.Override(ctx, &settings.SV, true)

	// Run these benchmarks with an increasing number of concurrent (simulated)
	// SQL sessions, to gauge where our runtime performance starts to break
	// down, guiding us as we tune buffer sizes, etc.
	for _, numSessions := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("numSessions=%d", numSessions), func(b *testing.B) {
			provider := insights.New(settings, insights.NewMetrics(), nil)
			provider.Start(ctx, stopper)

			// Spread the b.N work across the simulated SQL sessions, so that we
			// can make apples-to-apples comparisons in the benchmark reports:
			// each N is an "op," which for our measurement purposes is a
			// statement in an implicit transaction.
			numTransactionsPerSession := b.N / numSessions
			var sessions sync.WaitGroup
			sessions.Add(numSessions)
			writer := provider.Writer()
			statements := make([]insights.Statement, b.N)
			transactions := make([]insights.Transaction, b.N)
			for i := 0; i < numSessions; i++ {
				for j := 0; j < numTransactionsPerSession; j++ {
					statements[numTransactionsPerSession*i+j] = insights.Statement{
						// Spread across 6 different statement fingerprints.
						FingerprintID: appstatspb.StmtFingerprintID(j % 6),
						// Choose latencies in 20ms, 40ms, 60ms, 80ms, 100ms, 120ms, 140ms.
						// As configured above, only latencies >=100ms are noteworthy.
						// Since 7 is relatively prime to 6, we'll spread these across all fingerprints.
						LatencyInSeconds: float64(j%7+1) * 0.02,
					}
				}
			}

			b.ResetTimer()
			for i := 0; i < numSessions; i++ {
				sessionID := clusterunique.ID{Uint128: uint128.FromInts(rand.Uint64(), uint64(i))}
				go func(i int) {
					defer sessions.Done()
					for j := 0; j < numTransactionsPerSession; j++ {
						idx := numTransactionsPerSession*i + j
						writer.ObserveStatement(sessionID, &statements[idx])
						writer.ObserveTransaction(sessionID, &transactions[idx])
					}
				}(i)
			}

			sessions.Wait()
		})
	}
}
