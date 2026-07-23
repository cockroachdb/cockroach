// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// BenchmarkSetWorkState measures the per-operation cost of a
// SetWorkState + clearWorkState cycle — the pair of calls every
// registered goroutine makes on the ASH hot path.
//
// Use this benchmark with varying GOMAXPROCS (test.cpu). This excercises
// the performance of concurrent access to the underlying activeWorkStates
// data structure.
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

// BenchmarkSetWorkStateWithRange measures Set+Clear throughput under
// continuous rangeWorkStates pressure, using fresh goroutines per
// iteration to simulate CockroachDB's real access pattern: each
// incoming BatchRequest spawns a new goroutine with a unique,
// never-reused goroutine ID.
//
// Example:
//
//	./dev bench pkg/obs/ash -f BenchmarkSetWorkStateWithRange --test-args='-test.benchtime=5000x -test.count=10'
func BenchmarkSetWorkStateWithRange(b *testing.B) {
	enabled.Store(true)
	defer enabled.Store(false)

	tenantID := roachpb.MustMakeTenantID(5)
	info := WorkloadInfo{WorkloadID: 12345}

	var stop atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			rangeWorkStates(func(gid int64, state WorkState) bool {
				return true
			})
		}
	}()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan struct{})
			go func() {
				clear := SetWorkState(tenantID, info, WorkCPU, "BenchmarkOp")
				clear()
				close(ch)
			}()
			<-ch
		}
	})

	stop.Store(true)
	<-done
	reclaimRetiredWorkStates()
}
