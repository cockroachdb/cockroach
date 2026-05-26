// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// BenchmarkSetWorkState measures the per-operation cost of a
// SetWorkState + clearWorkState cycle — the pair of calls every
// registered goroutine makes on the ASH hot path.
//
// activeWorkStates is a global syncutil.Map. On high-core machines,
// concurrent Stores from different cores cause cache-line
// invalidations on the map's internal mutex and dirty-map pointer,
// degrading throughput as core count grows. This benchmark
// quantifies that degradation by varying GOMAXPROCS via -test.cpu.
// RunParallel spawns one goroutine per core, which is the right
// model: contention scales with cores executing simultaneously,
// not goroutines queued in the scheduler.
//
// See #168289 for the sharded-map proposal to address this.
//
// Example:
//
//	./dev bench pkg/obs/ash -f BenchmarkSetWorkState --test-args='-test.cpu=1,4,16,64'
func BenchmarkSetWorkState(b *testing.B) {
	enabled.Store(true)
	defer enabled.Store(false)

	tenantID := roachpb.MustMakeTenantID(5)
	info := WorkloadInfo{WorkloadID: 12345}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			clear := SetWorkState(tenantID, info, WorkCPU, "BenchmarkOp")
			clear()
		}
	})

	// Drain retired states so subsequent benchmarks start clean.
	reclaimRetiredWorkStates()
}
