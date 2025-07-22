// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestReplicaMMARangeLoad tests the Replica.MMARangeLoad() by verifying
// that it correctly converts ReplicaLoadStats to mmaprototype.RangeLoad.
func TestReplicaMMARangeLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	r := &Replica{}
	r.loadStats = load.NewReplicaLoad(hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123))), nil)

	expectedRequestCPU := 1000.0
	expectedRaftCPU := 500.0
	expectedWriteBytes := 2000.0
	expectedByteSize := int64(5000)

	r.loadStats.TestingSetStat(load.ReqCPUNanos, expectedRequestCPU)
	r.loadStats.TestingSetStat(load.RaftCPUNanos, expectedRaftCPU)
	r.loadStats.TestingSetStat(load.WriteBytes, expectedWriteBytes)
	r.SetMVCCStatsForTesting(&enginepb.MVCCStats{
		KeyBytes: expectedByteSize,
	})

	expectedTotalCPU := mmaprototype.LoadValue(expectedRequestCPU + expectedRaftCPU)
	expectedRaftCPULoad := mmaprototype.LoadValue(expectedRaftCPU)
	expectedWriteBandwidth := mmaprototype.LoadValue(expectedWriteBytes)
	expectedByteSizeLoad := mmaprototype.LoadValue(expectedByteSize)

	actual := r.MMARangeLoad()
	require.Equal(t, expectedTotalCPU, actual.Load[mmaprototype.CPURate])
	require.Equal(t, expectedRaftCPULoad, actual.RaftCPU)
	require.Equal(t, expectedWriteBandwidth, actual.Load[mmaprototype.WriteBandwidth])
	require.Equal(t, expectedByteSizeLoad, actual.Load[mmaprototype.ByteSize])
}
