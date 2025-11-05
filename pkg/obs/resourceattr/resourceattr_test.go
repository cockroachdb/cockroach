// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resourceattr

import (
	"testing"
)

func BenchmarkResourceAttrRecord(b *testing.B) {
	// Create ResourceAttr directly without background task for clean benchmarking
	ra := &ResourceAttr{
		workloads: make([]uint64, 1000),
		cpuTimes:  make([]float64, 1000),
	}

	workloadID := uint64(12345)
	cpuTime := float64(1000000)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ra.Record(workloadID, cpuTime)
	}
}
