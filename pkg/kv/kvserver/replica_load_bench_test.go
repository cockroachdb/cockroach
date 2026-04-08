// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// BenchmarkReplicaRangeUsageInfo benchmarks Replica.RangeUsageInfo() which is
// called per-replica during Store.Capacity scans.
func BenchmarkReplicaRangeUsageInfo(b *testing.B) {
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	locality := func(_ roachpb.NodeID) string { return "us-east-1" }

	r := &Replica{}
	r.loadStats = load.NewReplicaLoad(clock, locality)
	r.shMu.state.Stats = &enginepb.MVCCStats{
		LiveBytes: 1024 * 1024,
		KeyBytes:  512 * 1024,
		ValBytes:  512 * 1024,
	}

	// Record some load so stats are non-zero.
	for i := 0; i < 100; i++ {
		r.loadStats.RecordBatchRequests(1, 1)
		r.loadStats.RecordRequests(5)
		r.loadStats.RecordWriteKeys(3)
		r.loadStats.RecordReadKeys(10)
		r.loadStats.RecordWriteBytes(1024)
		r.loadStats.RecordReadBytes(4096)
		r.loadStats.RecordReqCPUNanos(5000)
		r.loadStats.RecordRaftCPUNanos(2000)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.RangeUsageInfo()
	}
}
