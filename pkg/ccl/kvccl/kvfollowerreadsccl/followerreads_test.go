// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvfollowerreadsccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvtestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	expectedFollowerReadOffset time.Duration = -4200 * time.Millisecond
)

func TestEvalFollowerReadOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilccl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	if offset, err := evalFollowerReadOffset(st); err != nil {
		t.Fatal(err)
	} else if offset != expectedFollowerReadOffset {
		t.Fatalf("expected %v, got %v", expectedFollowerReadOffset, offset)
	}
	disableEnterprise()
	_, err := evalFollowerReadOffset(st)
	require.NoError(t, err)
}

func TestZeroDurationDisablesFollowerReadOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()
	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	closedts.TargetDuration.Override(ctx, &st.SV, 0)
	if offset, err := evalFollowerReadOffset(st); err != nil {
		t.Fatal(err)
	} else if offset != math.MinInt64 {
		t.Fatalf("expected %v, got %v", math.MinInt64, offset)
	}
}

func TestCanSendToFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t, "test is flaky under deadlock+stress")

	ctx := context.Background()
	clock := hlc.NewClockWithSystemTimeSource(base.DefaultMaxClockOffset, base.DefaultMaxClockOffset, hlc.PanicLogger)
	stale := clock.Now().Add(2*expectedFollowerReadOffset.Nanoseconds(), 0)
	current := clock.Now()
	future := clock.Now().Add(2*clock.MaxOffset().Nanoseconds(), 0)

	txn := func(ts hlc.Timestamp) *roachpb.Transaction {
		txn := roachpb.MakeTransaction("txn", nil, 0, 0, ts, 0, 1, 0, false /* omitInRangefeeds */)
		return &txn
	}
	withWriteTimestamp := func(txn *roachpb.Transaction, ts hlc.Timestamp) *roachpb.Transaction {
		txn.WriteTimestamp = ts
		return txn
	}
	withUncertaintyLimit := func(txn *roachpb.Transaction, ts hlc.Timestamp) *roachpb.Transaction {
		txn.GlobalUncertaintyLimit = ts
		return txn
	}
	batch := func(txn *roachpb.Transaction, reqs ...kvpb.Request) *kvpb.BatchRequest {
		ba := &kvpb.BatchRequest{}
		ba.Txn = txn
		for _, req := range reqs {
			ba.Add(req)
		}
		return ba
	}
	withBatchTimestamp := func(ba *kvpb.BatchRequest, ts hlc.Timestamp) *kvpb.BatchRequest {
		ba.Timestamp = ts
		return ba
	}
	withServerSideBatchTimestamp := func(ba *kvpb.BatchRequest, ts hlc.Timestamp) *kvpb.BatchRequest {
		ba = withBatchTimestamp(ba, ts)
		ba.TimestampFromServerClock = (*hlc.ClockTimestamp)(&ts)
		return ba
	}

	testCases := []struct {
		name                  string
		ba                    *kvpb.BatchRequest
		ctPolicy              roachpb.RangeClosedTimestampPolicy
		disabledFollowerReads bool
		zeroTargetDuration    bool
		exp                   bool
	}{
		{
			name: "non-txn batch, without ts",
			ba:   batch(nil, &kvpb.GetRequest{}),
			exp:  false,
		},
		{
			name: "stale non-txn batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), stale),
			exp:  true,
		},
		{
			name: "stale non-txn export batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}), stale),
			exp:  true,
		},
		{
			name: "stale non-txn multiple exports batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}, &kvpb.ExportRequest{}), stale),
			exp:  true,
		},
		{
			name: "stale non-txn mixed export-scan batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}, &kvpb.ScanRequest{}), stale),
			exp:  true,
		},
		{
			name: "current-time non-txn batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), current),
			exp:  false,
		},
		{
			name: "current-time non-txn export batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}), current),
			exp:  false,
		},
		{
			name: "current-time non-txn multiple exports batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}, &kvpb.ExportRequest{}), current),
			exp:  false,
		},
		{
			name: "future non-txn batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), future),
			exp:  false,
		},
		{
			name: "future non-txn export batch",
			ba:   withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}), future),
			exp:  false,
		},
		{
			name: "stale non-txn batch, server-side ts",
			ba:   withServerSideBatchTimestamp(batch(nil, &kvpb.GetRequest{}), stale),
			exp:  false,
		},
		{
			name: "current-time non-txn batch, server-side ts",
			ba:   withServerSideBatchTimestamp(batch(nil, &kvpb.GetRequest{}), current),
			exp:  false,
		},
		{
			name: "future non-txn batch, server-side ts",
			ba:   withServerSideBatchTimestamp(batch(nil, &kvpb.GetRequest{}), future),
			exp:  false,
		},
		{
			name: "stale read",
			ba:   batch(txn(stale), &kvpb.GetRequest{}),
			exp:  true,
		},
		{
			name: "stale locking read",
			ba:   batch(txn(stale), &kvpb.GetRequest{KeyLockingStrength: lock.Exclusive}),
			exp:  false,
		},
		{
			name: "stale scan",
			ba:   batch(txn(stale), &kvpb.ScanRequest{}),
			exp:  true,
		},
		{
			name: "stale reverse scan",
			ba:   batch(txn(stale), &kvpb.ReverseScanRequest{}),
			exp:  true,
		},
		{
			name: "stale refresh",
			ba:   batch(txn(stale), &kvpb.RefreshRequest{}),
			exp:  true,
		},
		{
			name: "stale refresh range",
			ba:   batch(txn(stale), &kvpb.RefreshRangeRequest{}),
			exp:  true,
		},
		{
			name: "stale write",
			ba:   batch(txn(stale), &kvpb.PutRequest{}),
			exp:  false,
		},
		{
			name: "stale heartbeat txn",
			ba:   batch(txn(stale), &kvpb.HeartbeatTxnRequest{}),
			exp:  false,
		},
		{
			name: "stale end txn",
			ba:   batch(txn(stale), &kvpb.EndTxnRequest{}),
			exp:  false,
		},
		{
			name: "stale non-txn request",
			ba:   batch(txn(stale), &kvpb.QueryTxnRequest{}),
			exp:  false,
		},
		{
			name: "stale read with current-time writes",
			ba:   batch(withWriteTimestamp(txn(stale), current), &kvpb.GetRequest{}),
			exp:  false,
		},
		{
			name: "stale read with current-time uncertainty limit",
			ba:   batch(withUncertaintyLimit(txn(stale), current), &kvpb.GetRequest{}),
			exp:  false,
		},
		{
			name:               "stale read when zero target_duration",
			ba:                 batch(txn(stale), &kvpb.GetRequest{}),
			zeroTargetDuration: true,
			exp:                false,
		},
		{
			name: "current-time read",
			ba:   batch(txn(current), &kvpb.GetRequest{}),
			exp:  false,
		},
		{
			name: "future read",
			ba:   batch(txn(future), &kvpb.GetRequest{}),
			exp:  false,
		},
		{
			name:     "non-txn batch, without ts, global reads policy",
			ba:       batch(nil, &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale non-txn batch, global reads policy",
			ba:       withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), stale),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "stale non-txn export batch, global reads policy",
			ba:       withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}), stale),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time non-txn batch, global reads policy",
			ba:       withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), current),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time non-txn export batch, global reads policy",
			ba:       withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}), current),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "future non-txn batch, global reads policy",
			ba:       withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), future),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "future non-txn export batch, global reads policy",
			ba:       withBatchTimestamp(batch(nil, &kvpb.ExportRequest{}), future),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale non-txn batch, server-side ts, global reads policy",
			ba:       withServerSideBatchTimestamp(batch(nil, &kvpb.GetRequest{}), stale),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "current-time non-txn batch, server-side ts, global reads policy",
			ba:       withServerSideBatchTimestamp(batch(nil, &kvpb.GetRequest{}), current),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "future non-txn batch, server-side ts, global reads policy",
			ba:       withServerSideBatchTimestamp(batch(nil, &kvpb.GetRequest{}), future),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale read, global reads policy",
			ba:       batch(txn(stale), &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "stale locking read, global reads policy",
			ba:       batch(txn(stale), &kvpb.GetRequest{KeyLockingStrength: lock.Exclusive}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale scan, global reads policy",
			ba:       batch(txn(stale), &kvpb.ScanRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "stale reverse scan, global reads policy",
			ba:       batch(txn(stale), &kvpb.ReverseScanRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "stale refresh, global reads policy",
			ba:       batch(txn(stale), &kvpb.RefreshRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "stale refresh range, global reads policy",
			ba:       batch(txn(stale), &kvpb.RefreshRangeRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "stale write, global reads policy",
			ba:       batch(txn(stale), &kvpb.PutRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale heartbeat txn, global reads policy",
			ba:       batch(txn(stale), &kvpb.HeartbeatTxnRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale end txn, global reads policy",
			ba:       batch(txn(stale), &kvpb.EndTxnRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale non-txn request, global reads policy",
			ba:       batch(txn(stale), &kvpb.QueryTxnRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "stale read with current-time writes, global reads policy",
			ba:       batch(withWriteTimestamp(txn(stale), current), &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "stale read with current-time uncertainty limit, global reads policy",
			ba:       batch(withUncertaintyLimit(txn(stale), current), &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time read, global reads policy",
			ba:       batch(txn(current), &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time scan, global reads policy",
			ba:       batch(txn(current), &kvpb.ScanRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time reverse scan, global reads policy",
			ba:       batch(txn(current), &kvpb.ReverseScanRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time refresh, global reads policy",
			ba:       batch(txn(current), &kvpb.RefreshRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time refresh range, global reads policy",
			ba:       batch(txn(current), &kvpb.RefreshRangeRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      true,
		},
		{
			name:     "current-time read with future writes, global reads policy",
			ba:       batch(withWriteTimestamp(txn(current), future), &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "current-time read with future uncertainty limit, global reads policy",
			ba:       batch(withUncertaintyLimit(txn(current), future), &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:     "future read, global reads policy",
			ba:       batch(txn(future), &kvpb.GetRequest{}),
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      false,
		},
		{
			name:                  "follower reads disabled",
			ba:                    withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), stale),
			disabledFollowerReads: true,
			exp:                   false,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			kvserver.FollowerReadsEnabled.Override(ctx, &st.SV, !c.disabledFollowerReads)
			if c.zeroTargetDuration {
				closedts.TargetDuration.Override(ctx, &st.SV, 0)
			}

			can := canSendToFollower(ctx, st, clock, c.ctPolicy, c.ba)
			require.Equal(t, c.exp, can)
		})
	}
}

// mockNodeStore implements the kvclient.NodeDescStore interface.
type mockNodeStore []roachpb.NodeDescriptor

func (s mockNodeStore) GetNodeDescriptor(id roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	for i := range s {
		desc := &s[i]
		if desc.NodeID == id {
			return desc, nil
		}
	}
	return nil, kvpb.NewNodeDescNotFoundError(id)
}

func (s mockNodeStore) GetNodeDescriptorCount() int {
	return len(s)
}

func (s mockNodeStore) GetStoreDescriptor(id roachpb.StoreID) (*roachpb.StoreDescriptor, error) {
	return nil, kvpb.NewStoreDescNotFoundError(id)
}

// TestOracle tests the Oracle exposed by this package.
func TestOracle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClockWithSystemTimeSource(base.DefaultMaxClockOffset, base.DefaultMaxClockOffset, hlc.PanicLogger)
	stale := clock.Now().Add(2*expectedFollowerReadOffset.Nanoseconds(), 0)
	current := clock.Now()
	future := clock.Now().Add(2*clock.MaxOffset().Nanoseconds(), 0)

	c := kv.NewDB(log.MakeTestingAmbientCtxWithNewTracer(), kv.MockTxnSenderFactory{}, clock, stopper)
	staleTxn := kv.NewTxn(ctx, c, 0)
	require.NoError(t, staleTxn.SetFixedTimestamp(ctx, stale))
	currentTxn := kv.NewTxn(ctx, c, 0)
	require.NoError(t, currentTxn.SetFixedTimestamp(ctx, current))
	futureTxn := kv.NewTxn(ctx, c, 0)
	require.NoError(t, futureTxn.SetFixedTimestamp(ctx, future))

	region := func(name string) (l roachpb.Locality) {
		return roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: name}}}
	}
	nodes := mockNodeStore{
		{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1"), Locality: region("a")},
		{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2"), Locality: region("b")},
		{NodeID: 3, Address: util.MakeUnresolvedAddr("tcp", "3"), Locality: region("c")},
	}
	replicas := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 2, StoreID: 2, ReplicaID: 2},
		{NodeID: 3, StoreID: 3, ReplicaID: 3},
	}
	desc := &roachpb.RangeDescriptor{
		InternalReplicas: replicas,
	}
	closestFollower := replicas[1]
	leaseholder := replicas[2]

	rpcContext := rpc.NewInsecureTestingContext(ctx, clock, stopper)
	setLatency := func(id roachpb.NodeID, latency time.Duration) {
		// All test cases have to have at least 11 measurement values in order for
		// the exponentially-weighted moving average to work properly. See the
		// comment on the WARMUP_SAMPLES const in the ewma package for details.
		for i := 0; i < 11; i++ {
			rpcContext.RemoteClocks.UpdateOffset(ctx, id, rpc.RemoteOffset{}, latency)
		}
	}
	setLatency(1, 100*time.Millisecond)
	setLatency(2, 2*time.Millisecond)
	setLatency(3, 80*time.Millisecond)

	testCases := []struct {
		name                  string
		txn                   *kv.Txn
		lh                    *roachpb.ReplicaDescriptor
		ctPolicy              roachpb.RangeClosedTimestampPolicy
		disabledFollowerReads bool
		exp                   roachpb.ReplicaDescriptor
	}{
		{
			name: "non-txn, known leaseholder",
			txn:  nil,
			lh:   &leaseholder,
			exp:  leaseholder,
		},
		{
			name: "non-txn, unknown leaseholder",
			txn:  nil,
			exp:  closestFollower,
		},
		{
			name: "stale txn, known leaseholder",
			txn:  staleTxn,
			lh:   &leaseholder,
			exp:  closestFollower,
		},
		{
			name: "stale txn, unknown leaseholder",
			txn:  staleTxn,
			exp:  closestFollower,
		},
		{
			name: "current txn, known leaseholder",
			txn:  currentTxn,
			lh:   &leaseholder,
			exp:  leaseholder,
		},
		{
			name: "current txn, unknown leaseholder",
			txn:  currentTxn,
			exp:  closestFollower,
		},
		{
			name: "future txn, known leaseholder",
			txn:  futureTxn,
			lh:   &leaseholder,
			exp:  leaseholder,
		},
		{
			name: "future txn, unknown leaseholder",
			txn:  futureTxn,
			exp:  closestFollower,
		},
		{
			name:     "stale txn, known leaseholder, global reads policy",
			txn:      staleTxn,
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			lh:       &leaseholder,
			exp:      closestFollower,
		},
		{
			name:     "stale txn, unknown leaseholder, global reads policy",
			txn:      staleTxn,
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      closestFollower,
		},
		{
			name:     "current txn, known leaseholder, global reads policy",
			txn:      currentTxn,
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			lh:       &leaseholder,
			exp:      closestFollower,
		},
		{
			name:     "current txn, unknown leaseholder, global reads policy",
			txn:      currentTxn,
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      closestFollower,
		},
		{
			name:     "future txn, known leaseholder, global reads policy",
			txn:      futureTxn,
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			lh:       &leaseholder,
			exp:      leaseholder,
		},
		{
			name:     "future txn, unknown leaseholder, global reads policy",
			txn:      futureTxn,
			ctPolicy: roachpb.LEAD_FOR_GLOBAL_READS,
			exp:      closestFollower,
		},
		{
			name:                  "stale txn, follower reads disabled",
			txn:                   staleTxn,
			lh:                    &leaseholder,
			disabledFollowerReads: true,
			exp:                   leaseholder,
		},
	}
	cfg := func(st *cluster.Settings) replicaoracle.Config {
		return replicaoracle.Config{
			NodeDescs:  nodes,
			Settings:   st,
			RPCContext: rpcContext,
			Clock:      clock,
			HealthFunc: func(roachpb.NodeID) bool { return true },
		}
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			kvserver.FollowerReadsEnabled.Override(ctx, &st.SV, !c.disabledFollowerReads)

			o := replicaoracle.NewOracle(followerReadOraclePolicy, cfg(st))

			res, _, err := o.ChoosePreferredReplica(ctx, c.txn, desc, c.lh, c.ctPolicy, replicaoracle.QueryState{})
			require.NoError(t, err)
			require.Equal(t, c.exp, res)
		})
	}

	t.Run("bulk", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		stNoFollowers := cluster.MakeTestingClusterSettings()
		kvserver.FollowerReadsEnabled.Override(ctx, &stNoFollowers.SV, false)

		ctx := context.Background()
		var noTxn *kv.Txn
		var noLeaseholder *roachpb.ReplicaDescriptor
		var noCTPolicy roachpb.RangeClosedTimestampPolicy
		var noQueryState replicaoracle.QueryState
		sk := StreakConfig{Min: 10, SmallPlanMin: 3, SmallPlanThreshold: 3, MaxSkew: 0.95}
		// intMap(k1, v1, k2, v2, ...) is a FastIntMaps constructor shorthand.
		intMap := func(pairs ...int) util.FastIntMap {
			f := util.FastIntMap{}
			for i := 0; i < len(pairs); i += 2 {
				f.Set(pairs[i], pairs[i+1])
			}
			return f
		}

		t.Run("no-followers", func(t *testing.T) {
			br := NewBulkOracle(cfg(stNoFollowers), roachpb.Locality{}, sk)
			leaseholder := &roachpb.ReplicaDescriptor{NodeID: 99}
			picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, leaseholder, noCTPolicy, noQueryState)
			require.NoError(t, err)
			require.Equal(t, leaseholder.NodeID, picked.NodeID, "no follower reads means we pick the leaseholder")
		})
		t.Run("no-filter", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), roachpb.Locality{}, sk)
			picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, noQueryState)
			require.NoError(t, err)
			require.NotNil(t, picked, "no filter picks some node but could be any node")
		})
		t.Run("filter", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), region("b"), sk)
			picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, noQueryState)
			require.NoError(t, err)
			require.Equal(t, roachpb.NodeID(2), picked.NodeID, "filter means we pick the node that matches the filter")
		})
		t.Run("filter-no-match", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), region("z"), sk)
			picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, noQueryState)
			require.NoError(t, err)
			require.NotNil(t, picked, "no match still picks some non-zero node")
		})
		t.Run("streak-short", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), roachpb.Locality{}, sk)
			for _, r := range replicas { // Check for each to show it isn't random.
				picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, replicaoracle.QueryState{
					NodeStreak:     1,
					LastAssignment: r.NodeID,
				})
				require.NoError(t, err)
				require.Equal(t, r.NodeID, picked.NodeID)
			}
		})
		t.Run("streak-medium", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), roachpb.Locality{}, sk)
			for _, r := range replicas { // Check for each to show it isn't random.
				picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, replicaoracle.QueryState{
					NodeStreak:     9,
					RangesPerNode:  intMap(1, 3, 2, 3, 3, 3),
					LastAssignment: r.NodeID,
				})
				require.NoError(t, err)
				require.Equal(t, r.NodeID, picked.NodeID)
			}
		})
		t.Run("streak-long-even", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), roachpb.Locality{}, sk)
			for _, r := range replicas { // Check for each to show it isn't random.
				picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, replicaoracle.QueryState{
					NodeStreak:     50,
					RangesPerNode:  intMap(1, 1000, 2, 1002, 3, 1005),
					LastAssignment: r.NodeID,
				})
				require.NoError(t, err)
				require.Equal(t, r.NodeID, picked.NodeID)
			}
		})
		t.Run("streak-long-skewed-to-other", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), roachpb.Locality{}, sk)
			for i := 0; i < 10; i++ { // Prove it isn't just randomly picking n2.
				qs := replicaoracle.QueryState{
					NodeStreak:     50,
					RangesPerNode:  intMap(1, 10, 2, 10, 3, 1005),
					LastAssignment: 2,
				}
				picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, qs)
				require.NoError(t, err)
				require.Equal(t, roachpb.NodeID(2), picked.NodeID)
			}
		})
		t.Run("streak-long-skewed-randomizes", func(t *testing.T) {
			br := NewBulkOracle(cfg(st), roachpb.Locality{}, sk)
			qs := replicaoracle.QueryState{
				NodeStreak:     50,
				RangesPerNode:  intMap(1, 10, 2, 10, 3, 1005),
				LastAssignment: 3,
			}
			randomized := false
			for i := 0; i < 100; i++ { // .33^100 is close enough to zero that this shouldn't flake.
				picked, _, err := br.ChoosePreferredReplica(ctx, noTxn, desc, noLeaseholder, noCTPolicy, qs)
				require.NoError(t, err)
				if picked.NodeID != qs.LastAssignment {
					randomized = true
					break
				}
			}
			require.True(t, randomized)
		})
	})
}

// Test that follower reads recover from a situation where a gateway node has
// the right leaseholder cached, but stale followers. This is an integration
// test checking that the cache on the gateway gets updated by the first request
// encountering this situation, and then follower reads work.
func TestFollowerReadsWithStaleDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// The test uses follower_read_timestamp().
	defer utilccl.TestingEnableEnterprise()()

	var historicalQuery atomic.Value
	historicalQuery.Store(`SELECT * FROM test AS OF SYSTEM TIME follower_read_timestamp() WHERE k=2`)
	recCh := make(chan tracingpb.Recording, 1)

	settings := cluster.MakeClusterSettings()
	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings:          settings,
				DefaultTestTenant: base.TODOTestTenantDisabled,
				UseDatabase:       "t",
			},
			// n4 pretends to have low latency to n2 and n3, so that it tries to use
			// them for follower reads.
			// Also, we're going to collect a trace of the test's final query.
			ServerArgsPerNode: map[int]base.TestServerArgs{
				3: {
					UseDatabase: "t",
					Knobs: base.TestingKnobs{
						KVClient: &kvcoord.ClientTestingKnobs{
							// Inhibit the checking of connection health done by the
							// GRPCTransport. This test wants to control what replica (which
							// follower) a request is sent to and, depending on timing, the
							// connection from n4 to the respective follower might not be
							// heartbeated by the time the test wants to use it. Without this
							// knob, that would cause the transport to reorder replicas.
							DontConsiderConnHealth: true,
							LatencyFunc: func(id roachpb.NodeID) (time.Duration, bool) {
								if (id == 2) || (id == 3) {
									return time.Millisecond, true
								}
								return 100 * time.Millisecond, true
							},
						},
						SQLExecutor: &sql.ExecutorTestingKnobs{
							WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
								if stmt == historicalQuery.Load().(string) {
									recCh <- trace
								}
							},
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	// Further down, we'll set up the test to pin the lease to store 1. Turn off
	// load based rebalancing to make sure it doesn't move.
	kvserver.LoadBasedRebalancingMode.Override(ctx, &settings.SV, kvserver.LBRebalancingOff)

	n1 := sqlutils.MakeSQLRunner(tc.Conns[0])
	n1.Exec(t, `CREATE DATABASE t`)
	n1.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY)`)
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE VOTERS VALUES (ARRAY[1,2], 1)`)
	// Speed up closing of timestamps, in order to sleep less below before we can
	// use follower_read_timestamp(). follower_read_timestamp() uses the sum of
	// the following settings.
	n1.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '0.1s'`)
	n1.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '0.1s'`)

	// Sleep so that we can perform follower reads. The read timestamp needs to be
	// above the timestamp when the table was created.
	log.Infof(ctx, "test sleeping for the follower read timestamps to pass the table creation timestamp...")
	n1.Exec(t, `SELECT pg_sleep((now() - follower_read_timestamp())::FLOAT)`)
	log.Infof(ctx, "test sleeping... done")

	// Run a query on n4 to populate its cache.
	n4 := sqlutils.MakeSQLRunner(tc.Conns[3])
	n4.Exec(t, "SELECT * from test WHERE k=1")
	// Check that the cache was indeed populated.
	var tableID uint32
	n1.QueryRow(t, `SELECT id from system.namespace WHERE name='test'`).Scan(&tableID)
	tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
	n4Cache := tc.Server(3).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
	entry, err := n4Cache.TestingGetCached(ctx, tablePrefix, false, roachpb.LAG_BY_CLUSTER_SETTING)
	require.NoError(t, err)
	require.False(t, entry.Lease.Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease.Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 2, StoreID: 2, ReplicaID: 2},
	}, entry.Desc.Replicas().Descriptors())

	// Remove the follower and add a new non-voter to n3. n2 will no longer have a
	// replica.
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE VOTERS VALUES (ARRAY[1], 1)`)
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE NONVOTERS VALUES (ARRAY[3], 1)`)

	// Execute the query again and assert the cache is updated. This query will
	// not be executed as a follower read since it attempts to use n2 which
	// doesn't have a replica any more and then it tries n1 which returns an
	// updated descriptor.
	n4.Exec(t, historicalQuery.Load().(string))
	// As a sanity check, verify that this was not a follower read.
	rec := <-recCh
	require.False(t, kvtestutils.OnlyFollowerReads(rec), "query was served through follower reads: %s", rec)
	// Check that the cache was properly updated.
	entry, err = n4Cache.TestingGetCached(ctx, tablePrefix, false, roachpb.LAG_BY_CLUSTER_SETTING)
	require.NoError(t, err)
	require.False(t, entry.Lease.Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease.Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 3, StoreID: 3, ReplicaID: 3, Type: roachpb.NON_VOTER},
	}, entry.Desc.Replicas().Descriptors())

	// Make a note of the follower reads metric on n3. We'll check that it was
	// incremented.
	var followerReadsCountBefore int64
	err = tc.Servers[2].GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
		followerReadsCountBefore = s.Metrics().FollowerReadsCount.Count()
		return nil
	})
	require.NoError(t, err)

	// Run a historical query and assert that it's served from the follower (n3).
	// n4 should attempt to route to n3 because we pretend n3 has a lower latency
	// (see testing knob).
	n4.Exec(t, historicalQuery.Load().(string))
	rec = <-recCh

	// Look at the trace and check that we've served a follower read.
	require.True(t, kvtestutils.OnlyFollowerReads(rec), "query was not served through follower reads: %s", rec)

	// Check that the follower read metric was incremented.
	var followerReadsCountAfter int64
	err = tc.Servers[2].GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
		followerReadsCountAfter = s.Metrics().FollowerReadsCount.Count()
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, followerReadsCountAfter, followerReadsCountBefore)

	// Now verify that follower reads aren't mistakenly counted as "misplanned
	// ranges" (#61313).

	// First, run a query on n3 to populate its cache.
	n3 := sqlutils.MakeSQLRunner(tc.Conns[2])
	n3.Exec(t, "SELECT * from test WHERE k=1")
	n3Cache := tc.Server(2).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
	entry, err = n3Cache.TestingGetCached(ctx, tablePrefix, false, roachpb.LAG_BY_CLUSTER_SETTING)
	require.NoError(t, err)
	require.False(t, entry.Lease.Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease.Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 3, StoreID: 3, ReplicaID: 3, Type: roachpb.NON_VOTER},
	}, entry.Desc.Replicas().Descriptors())

	// Enable DistSQL so that we have a distributed plan with a single flow on
	// n3 (local plans ignore the misplanned ranges).
	n4.Exec(t, "SET distsql=on")

	// Run a historical query and assert that it's served from the follower (n3).
	// n4 should choose n3 to plan the TableReader on because we pretend n3 has
	// a lower latency (see testing knob).

	// Note that this query is such that the physical planning needs to fetch
	// the ReplicaInfo twice for the same range. This allows us to verify that
	// the cached - in the spanResolverIterator - information is correctly
	// preserved.
	historicalQuery.Store(`SELECT * FROM [SELECT * FROM test WHERE k=2 UNION ALL SELECT * FROM test WHERE k=3] AS OF SYSTEM TIME follower_read_timestamp()`)
	n4.Exec(t, historicalQuery.Load().(string))
	rec = <-recCh

	// Sanity check that the plan was distributed.
	require.True(t, strings.Contains(rec.String(), "creating DistSQL plan with isLocal=false"))
	// Look at the trace and check that we've served a follower read.
	require.True(t, kvtestutils.OnlyFollowerReads(rec), "query was not served through follower reads: %s", rec)
	// Verify that we didn't produce the "misplanned ranges" metadata that would
	// purge the non-stale entries from the range cache on n4.
	require.False(t, strings.Contains(rec.String(), "clearing entries overlapping"))
	// Also confirm that we didn't even check for the "misplanned ranges"
	// metadata on n3.
	require.False(t, strings.Contains(rec.String(), "checking range cache to see if range info updates should be communicated to the gateway"))
}

// TestSecondaryTenantFollowerReadsRouting ensures that secondary tenants route
// their requests to the nearest replica. The test exercises three
// configurations:
//   - shared-process multi-tenancy
//   - separate-process multi-tenancy, with accurate latency information between
//     nodes available
//   - separate-process multi-tenancy, with accurate latency information between
//     nodes unavailable which requires the fallback to estimates using node
//     localities.
//
// For the shared-process multi-tenancy we set up a single region cluster so
// that locality information didn't come into play when choosing the replica. We
// use n2 as the gateway for the query and expect that its replica will serve
// the follower read.
//
// For the separate-process multi-tenancy we set up a three region cluster where
// n2 and n4 are in the same region, and we use n4 as the gateway and expect
// that n2 serves the follower read.
func TestSecondaryTenantFollowerReadsRouting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	skip.UnderRace(t, "times out")
	skip.UnderDeadlock(t)

	rng, _ := randutil.NewTestRand()
	for _, testCase := range []struct {
		name             string
		sharedProcess    bool
		validLatencyFunc bool
	}{
		{name: "shared-process", sharedProcess: true},
		{name: "latency-based", sharedProcess: false, validLatencyFunc: true},
		{name: "locality-based", sharedProcess: false, validLatencyFunc: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			const numNodes = 4
			gatewayNode := 3
			if testCase.sharedProcess {
				gatewayNode = 1
			}

			serverArgs := make(map[int]base.TestServerArgs)
			localities := make(map[int]roachpb.Locality)
			for i := 0; i < numNodes; i++ {
				regionName := fmt.Sprintf("region_%d", i)
				if i == gatewayNode {
					// Make it such that n4 and n2 are in the same region.
					// Below, we'll expect a follower read from n4 to be served
					// by n2 because they're in the same locality (when
					// validLatencyFunc is false).
					regionName = fmt.Sprintf("region_%d", 1)
				}
				if testCase.sharedProcess {
					// In shared-process config we want all nodes to be in the
					// same region so that other considerations (like latency
					// function and locality matching don't come into play).
					regionName = "test_region"
				}
				locality := roachpb.Locality{
					Tiers: []roachpb.Tier{{Key: "region", Value: regionName}},
				}
				localities[i] = locality
				serverArgs[i] = base.TestServerArgs{
					Locality: localities[i],
				}
			}
			tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
				ReplicationMode:   base.ReplicationManual,
				ServerArgsPerNode: serverArgs,
				ServerArgs: base.TestServerArgs{
					DefaultTestTenant: base.TestControlsTenantsExplicitly,
				},
			})
			ctx := context.Background()
			defer tc.Stopper().Stop(ctx)

			// Speed up closing of timestamps in order to sleep less below
			// before we can use follower_read_timestamp(). Note that we need to
			// override the setting for the tenant as well, because the builtin
			// is run in the tenant's sql pod.
			systemSQL := sqlutils.MakeSQLRunner(tc.Conns[0])
			systemSQL.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '0.1s'`)
			systemSQL.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '0.1s'`)
			// Disable the store rebalancer to make sure leases stay where they are;
			// the test cares about this.
			systemSQL.Exec(t, `SET CLUSTER SETTING kv.allocator.load_based_rebalancing = off`)

			historicalQuery := `SELECT * FROM t.test AS OF SYSTEM TIME follower_read_timestamp() WHERE k=2`
			useExplainAnalyze := rng.Float64() < 0.5
			if useExplainAnalyze {
				historicalQuery = "EXPLAIN ANALYZE " + historicalQuery
			}
			recCh := make(chan tracingpb.Recording, 1)

			var tenants [numNodes]serverutils.ApplicationLayerInterface
			dbs := make([]*gosql.DB, numNodes)
			// In shared-process multi-tenancy we must initialize the tenant
			// server on the gateway first in order to guarantee that the knobs
			// are used (otherwise we have a race between the tenant server
			// being started by the server controller and us explicitly starting
			// the shared-process tenant server).
			initOrder := []int{gatewayNode}
			for i := 0; i < numNodes; i++ {
				if i != gatewayNode {
					initOrder = append(initOrder, i)
				}
			}
			for _, i := range initOrder {
				knobs := base.TestingKnobs{}
				if i == gatewayNode {
					knobs = base.TestingKnobs{
						KVClient: &kvcoord.ClientTestingKnobs{
							DontConsiderConnHealth: true,
							// For the validLatencyFunc=true version of the
							// test, the client pretends to have a low latency
							// connection to n2. As a result, we expect n2 to be
							// used for follower reads originating from n4.
							//
							// For the variant where no latency information is
							// available, we expect n2 to serve follower reads
							// as well, but because it is in the same locality
							// as the client.
							//
							// Note that this latency func doesn't matter in the
							// shared-process config.
							LatencyFunc: func(id roachpb.NodeID) (time.Duration, bool) {
								if !testCase.validLatencyFunc {
									return 0, false
								}
								if id == 2 {
									return time.Millisecond, true
								}
								return 100 * time.Millisecond, true
							},
						},
						SQLExecutor: &sql.ExecutorTestingKnobs{
							WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
								if stmt == historicalQuery {
									recCh <- trace
								}
							},
						},
					}
				}
				var err error
				if testCase.sharedProcess {
					tenants[i], dbs[i], err = tc.Server(i).TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
						TenantName: "test",
						TenantID:   serverutils.TestTenantID(),
						Knobs:      knobs,
					})
				} else {
					tenants[i], err = tc.Server(i).TenantController().StartTenant(ctx, base.TestTenantArgs{
						TenantID:     serverutils.TestTenantID(),
						Locality:     localities[i],
						TestingKnobs: knobs,
					})
					dbs[i] = tenants[i].SQLConn(t)
				}
				require.NoError(t, err)
			}

			// Wait until all tenant servers are aware of the setting override.
			testutils.SucceedsSoon(t, func() error {
				settingNames := []string{
					"kv.closed_timestamp.target_duration", "kv.closed_timestamp.side_transport_interval",
				}
				for _, settingName := range settingNames {
					for i := 0; i < numNodes; i++ {
						db := dbs[i]

						var val string
						err := db.QueryRow(
							fmt.Sprintf("SHOW CLUSTER SETTING %s", settingName),
						).Scan(&val)
						require.NoError(t, err)
						if val != "00:00:00.1" {
							return errors.Errorf("tenant server %d is still waiting for %s update: currently %s",
								i,
								settingName,
								val,
							)
						}
					}
				}
				return nil
			})

			tenantSQLDB := dbs[gatewayNode]
			tenantSQL := sqlutils.MakeSQLRunner(tenantSQLDB)

			tenantSQL.Exec(t, `CREATE DATABASE t`)
			tenantSQL.Exec(t, `CREATE TABLE t.test (k INT PRIMARY KEY)`)

			codec := tenants[gatewayNode].Codec()
			startKey := codec.TenantPrefix()
			tc.AddVotersOrFatal(t, startKey, tc.Target(1), tc.Target(2))
			tc.WaitForVotersOrFatal(t, startKey, tc.Target(1), tc.Target(2))
			desc := tc.LookupRangeOrFatal(t, startKey)
			require.Equal(t, []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
			}, desc.Replicas().Descriptors())

			// Sleep so that we can perform follower reads. The read timestamp
			// needs to be above the timestamp when the table was created.
			log.Infof(ctx, "test sleeping for the follower read timestamps to pass the table creation timestamp...")
			tenantSQL.Exec(t, `SELECT pg_sleep((now() - follower_read_timestamp())::FLOAT)`)
			log.Infof(ctx, "test sleeping... done")

			// Check that the cache was indeed populated.
			tenantSQL.Exec(t, `SELECT * FROM t.test WHERE k = 1`)
			tablePrefix := keys.MustAddr(codec.TenantPrefix())
			cache := tenants[gatewayNode].DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
			entry, err := cache.TestingGetCached(ctx, tablePrefix, false, roachpb.LAG_BY_CLUSTER_SETTING)
			require.NoError(t, err)
			require.False(t, entry.Lease.Empty())
			require.Equal(t, roachpb.StoreID(1), entry.Lease.Replica.StoreID)
			require.Equal(t, []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
			}, entry.Desc.Replicas().Descriptors())

			rows := tenantSQL.QueryStr(t, historicalQuery)
			rec := <-recCh

			// Look at the trace and check that the follower read was served by
			// n2.
			var numFRs, numN2FRs int
			for _, sp := range rec {
				for _, l := range sp.Logs {
					if msg := l.Message.StripMarkers(); strings.Contains(msg, kvbase.FollowerReadServingMsg) {
						numFRs++
						if strings.Contains(msg, "n2") {
							numN2FRs++
						}
					}
				}
			}
			require.Equal(t, numFRs, 1, "query wasn't served through follower reads: %s", rec)
			require.Equal(t, numN2FRs, 1, "follower read wasn't served by n2: %s", rec)

			if useExplainAnalyze {
				frMessage, historicalMessage := "used follower read", "historical"
				var foundFRMessage, foundHistoricalMessage bool
				for _, row := range rows {
					if strings.TrimSpace(row[0]) == frMessage {
						foundFRMessage = true
					} else if strings.HasPrefix(strings.TrimSpace(row[0]), historicalMessage) {
						foundHistoricalMessage = true
					}
				}
				require.True(t, foundFRMessage, "didn't see %q message in EXPLAIN ANALYZE: %v", frMessage, rows)
				require.True(t, foundHistoricalMessage, "didn't see %q message in EXPLAIN ANALYZE: %v", historicalMessage, rows)
			}
		})
	}
}

// Test draining a node stops any follower reads to that node. This is important
// because a drained node is about to shut down and a follower read prior to a
// shutdown may need to wait for a gRPC timeout.
func TestDrainStopsFollowerReads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	sv := &settings.SV
	// TODO(baptist): Remove this if we make this the default.
	kvcoord.FollowerReadsUnhealthy.Override(context.Background(), sv, false)

	// Turn down these durations to allow follower reads to happen faster.
	closeTime := 10 * time.Millisecond
	closedts.TargetDuration.Override(ctx, sv, closeTime)
	closedts.SideTransportCloseInterval.Override(ctx, sv, closeTime)
	ClosedTimestampPropagationSlack.Override(ctx, sv, closeTime)

	// Configure localities so n3 and n4 are in the same locality.
	// SQL runs on n4 (west).
	// Drain n3 (west).
	numNodes := 4
	locality := func(region string) roachpb.Locality {
		return roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: region},
			},
		}
	}
	localities := []roachpb.Locality{
		locality("us-east"),
		locality("us-east"),
		locality("us-west"),
		locality("us-west"),
	}
	manualClock := hlc.NewHybridManualClock()

	// Record which store processed the read request for our key.
	var lastReader atomic.Int32
	recordDestStore := func(args kvserverbase.FilterArgs) *kvpb.Error {
		getArg, ok := args.Req.(*kvpb.GetRequest)
		if !ok || !keys.ScratchRangeMin.Equal(getArg.Key) {
			return nil
		}
		lastReader.Store(int32(args.Sid))
		return nil
	}

	// Set up the nodes in different locality and use the LatencyFunc to
	// simulate latency.
	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numNodes; i++ {
		i := i
		serverArgs[i] = base.TestServerArgs{
			Settings: settings,
			Locality: localities[i],
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: recordDestStore,
					},
				},
				// Currently we use latency as the "primary" signal and use
				// locality only if the latency is unavailable. Simulate
				// locality based on whether the nodes are in the same locality.
				// TODO(baptist): Remove this if we sort replicas by region (#112993).
				KVClient: &kvcoord.ClientTestingKnobs{
					LatencyFunc: func(id roachpb.NodeID) (time.Duration, bool) {
						if localities[id-1].Equal(localities[i]) {
							return time.Millisecond, true
						}
						return 100 * time.Millisecond, true
					},
				},
			},
		}
	}

	// Set ReplicationManual as we don't want any leases to move around and affect
	// the results of this test.
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
			ServerArgsPerNode: serverArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// Put the scratch range on nodes 1, 2, 3 and leave the lease on 1.
	// We want all follower read request to come from 4 and go to node 3 due to
	// the way latency and localities are set up.
	scratchKey := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, scratchKey, tc.Targets(1, 2)...)
	server := tc.Server(3).ApplicationLayer()
	db := server.DB()

	// Keep the read time the same as a time in the recent past. Once the
	// readTime is closed, we expect all future reads to go to n3.
	readTime := server.Clock().Now()

	testutils.SucceedsSoon(t, func() error {
		sendFollowerRead(t, db, scratchKey, readTime)
		reader := lastReader.Load()
		if reader != 3 {
			return errors.Newf("expected read to n3 not n%d", reader)
		}
		return nil
	})

	// Send a drain request to n3 and wait until the drain is completed. Other
	// nodes find out about the drain asynchronously through gossip.
	req := serverpb.DrainRequest{Shutdown: false, DoDrain: true, NodeId: "3"}
	drainStream, err := tc.Server(0).GetAdminClient(t).Drain(ctx, &req)
	require.NoError(t, err)
	// When we get a response the drain is complete.
	drainResp, err := drainStream.Recv()
	require.NoError(t, err)
	require.True(t, drainResp.IsDraining)

	// Follower reads should stop going to n3 once other nodes notice it
	// draining.
	testutils.SucceedsSoon(t, func() error {
		sendFollowerRead(t, db, scratchKey, readTime)
		reader := lastReader.Load()
		if reader == 3 {
			return errors.New("expected to not read from n3")
		}
		return nil
	})
}

func sendFollowerRead(t *testing.T, db *kv.DB, scratchKey roachpb.Key, readTime hlc.Timestamp) {
	// Manually construct the BatchRequest to set the Timestamp.
	b := db.NewBatch()
	b.Get(scratchKey)
	b.Header.Timestamp = readTime
	require.NoError(t, db.Run(context.Background(), b))
}
