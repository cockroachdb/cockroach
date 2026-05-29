// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/obs/ash/enrichment"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// BenchmarkPutExecutionDisabled measures the cost of a single
// PutExecution call when the enrichment subsystem is disabled by
// cluster setting. The Attributes literal construction is gated behind
// Cache.Enabled() at the call site, so this measures just the gate
// check itself (two atomic loads).
func BenchmarkPutExecutionDisabled(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	enrichment.Enabled.Override(ctx, &st.SV, false)
	enrichment.CacheLimit.Override(ctx, &st.SV, 1024)
	metrics := enrichment.NewMetrics()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	c := enrichment.NewCache(st, &metrics)
	c.Start(ctx, stopper)

	id := makeID(1)
	attrs := enrichment.Attributes{AppName: "myapp"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if c.Enabled() {
			c.PutExecution(id, attrs)
		}
	}
}

// BenchmarkPutExecutionEnabled measures the cost of a single
// PutExecution call when the enrichment subsystem is enabled. This is
// the worst-case per-statement gateway cost: sharded hash, atomic
// reservation, block slot write, and ConcurrentBufferGuard read-lock
// hold.
func BenchmarkPutExecutionEnabled(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	enrichment.Enabled.Override(ctx, &st.SV, true)
	enrichment.CacheLimit.Override(ctx, &st.SV, 1<<20)
	metrics := enrichment.NewMetrics()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	c := enrichment.NewCache(st, &metrics)
	c.Start(ctx, stopper)

	attrs := enrichment.Attributes{
		AppName:   "myapp",
		Database:  "mydb",
		User:      "alice",
		SessionID: makeID(99),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.PutExecution(makeID(uint64(i+1)), attrs)
	}
}
