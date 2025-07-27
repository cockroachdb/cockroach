// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

func TestTryConstructMMARangeMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeTestingClusterSettings()

	t.Run("not_initialized", func(t *testing.T) {
		ctx := context.Background()
		repl := &Replica{}
		isLeaseholder, shouldBeSkipped, msg := repl.TryConstructMMARangeMsg(ctx, map[roachpb.StoreID]struct{}{})
		require.False(t, isLeaseholder)
		require.False(t, shouldBeSkipped)
		require.Equal(t, mmaprototype.RangeMsg{}, msg)
		require.False(t, msg.Populated)
	})

	// Override to disable mma store rebalancer and ensure we are the only
	// one calling into TryConstructMMARangeMsg.
	ctx := context.Background()
	LoadBasedRebalancingMode.Override(ctx, &st.SV, LBRebalancingOff)

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
		},
	})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, scratchKey, tc.Target(1), tc.Target(2))
	knownStores := map[roachpb.StoreID]struct{}{
		roachpb.StoreID(1): {},
		roachpb.StoreID(2): {},
		roachpb.StoreID(3): {},
	}
	store1, err := tc.Server(0).GetStores().(*Stores).GetStore(tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)
	store2, err := tc.Server(1).GetStores().(*Stores).GetStore(tc.Server(1).GetFirstStoreID())
	require.NoError(t, err)
	repl1, err := store1.GetReplica(desc.RangeID)
	require.NoError(t, err)
	repl2, err := store2.GetReplica(desc.RangeID)
	require.NoError(t, err)

	t.Run("missing_known_stores", func(t *testing.T) {
		isLeaseholder, shouldBeSkipped, msg := repl1.TryConstructMMARangeMsg(ctx, map[roachpb.StoreID]struct{}{
			roachpb.StoreID(1): {},
			roachpb.StoreID(2): {},
			// missing store 3
		})
		require.True(t, isLeaseholder)
		require.True(t, shouldBeSkipped)
		require.False(t, msg.Populated)
		require.True(t, repl1.mmaRangeMessageNeeded.wasLeaseholder)
	})

	t.Run("leaseholder", func(t *testing.T) {
		isLeaseholder, shouldBeSkipped, msg := repl1.TryConstructMMARangeMsg(ctx, knownStores)
		require.True(t, isLeaseholder)
		require.False(t, shouldBeSkipped)
		require.True(t, msg.Populated)
		require.True(t, repl1.mmaRangeMessageNeeded.wasLeaseholder)
	})

	t.Run("not_leaseholder", func(t *testing.T) {
		isLeaseholder, shouldBeSkipped, msg := repl2.TryConstructMMARangeMsg(ctx, knownStores)
		require.False(t, isLeaseholder)
		require.False(t, shouldBeSkipped)
		require.Equal(t, mmaprototype.RangeMsg{}, msg)
		require.False(t, msg.Populated)
		require.False(t, repl2.mmaRangeMessageNeeded.wasLeaseholder)
	})

	t.Run("transfer_lease", func(t *testing.T) {
		require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))
		testutils.SucceedsSoon(t, func() error {
			if !repl2.mmaRangeMessageNeeded.stateChangeTriggered.Load() {
				return errors.New("stateChangeTriggered is not set")
			}
			isLeaseholder, shouldBeSkipped, msg := repl2.TryConstructMMARangeMsg(ctx, knownStores)
			if !isLeaseholder || shouldBeSkipped || !msg.Populated {
				return errors.Newf("expected leaseholder=true, shouldBeSkipped=false, msg.Populated=true, but got %v, %v, %v",
					isLeaseholder, shouldBeSkipped, msg)
			}
			return nil
		})

		isLeaseholder, shouldBeSkipped, msg := repl1.TryConstructMMARangeMsg(ctx, knownStores)
		require.False(t, isLeaseholder)
		require.False(t, shouldBeSkipped)
		require.Equal(t, mmaprototype.RangeMsg{}, msg)
		require.False(t, msg.Populated)
		require.False(t, repl1.mmaRangeMessageNeeded.wasLeaseholder)
		require.Zero(t, repl1.mmaRangeMessageNeeded.lastRangeLoad)
	})

	t.Run("set_span_config", func(t *testing.T) {
		spanConfig, err := repl2.LoadSpanConfig(ctx)
		require.NoError(t, err)
		require.NotNil(t, spanConfig)
		repl2.SetSpanConfig(*spanConfig, roachpb.Span{Key: scratchKey})
		require.True(t, repl2.mmaRangeMessageNeeded.stateChangeTriggered.Load())
		isLeaseholder, shouldBeSkipped, msg := repl2.TryConstructMMARangeMsg(ctx, knownStores)
		require.True(t, isLeaseholder)
		require.False(t, shouldBeSkipped)
		require.True(t, msg.Populated)
		require.True(t, repl2.mmaRangeMessageNeeded.wasLeaseholder)
	})

	t.Run("significant_load_change", func(t *testing.T) {
		load1 := repl2.GetLoadStatsForTesting()
		setReqCPULoad := max(10, load1.TestingGetSum(load.ReqCPUNanos))
		load1.TestingSetStat(load.ReqCPUNanos, setReqCPULoad)
		isLeaseholder, shouldBeSkipped, msg := repl2.TryConstructMMARangeMsg(context.Background(), knownStores)
		require.True(t, isLeaseholder)
		require.False(t, shouldBeSkipped)
		require.True(t, msg.Populated)
		require.True(t, repl2.mmaRangeMessageNeeded.wasLeaseholder)
		require.NotZero(t, repl2.mmaRangeMessageNeeded.lastRangeLoad)
	})

	t.Run("leaseholder_again", func(t *testing.T) {
		require.NoError(t, tc.TransferRangeLease(desc, tc.Target(0)))
		testutils.SucceedsSoon(t, func() error {
			if !repl1.mmaRangeMessageNeeded.stateChangeTriggered.Load() {
				return errors.New("stateChangeTriggered is not set")
			}
			isLeaseholder, shouldBeSkipped, msg := repl1.TryConstructMMARangeMsg(ctx, knownStores)
			if !isLeaseholder || shouldBeSkipped || !msg.Populated {
				return errors.Newf("expected leaseholder=true, shouldBeSkipped=false, msg.Populated=true, but got %v, %v, %v",
					isLeaseholder, shouldBeSkipped, msg)
			}
			return nil
		})
		require.True(t, repl1.mmaRangeMessageNeeded.wasLeaseholder)
	})
}
