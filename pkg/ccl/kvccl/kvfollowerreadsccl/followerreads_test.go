// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfollowerreadsccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	// Blank import kvtenantccl so that we can create a tenant.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	if offset, err := evalFollowerReadOffset(uuid.MakeV4(), st); err != nil {
		t.Fatal(err)
	} else if offset != expectedFollowerReadOffset {
		t.Fatalf("expected %v, got %v", expectedFollowerReadOffset, offset)
	}
	disableEnterprise()
	_, err := evalFollowerReadOffset(uuid.MakeV4(), st)
	if !testutils.IsError(err, "requires an enterprise license") {
		t.Fatalf("failed to get error when evaluating follower read offset without " +
			"an enterprise license")
	}
}

func TestZeroDurationDisablesFollowerReadOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()
	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	closedts.TargetDuration.Override(ctx, &st.SV, 0)
	if offset, err := evalFollowerReadOffset(uuid.MakeV4(), st); err != nil {
		t.Fatal(err)
	} else if offset != math.MinInt64 {
		t.Fatalf("expected %v, got %v", math.MinInt64, offset)
	}
}

func TestCanSendToFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t, "test is flaky under deadlock+stress")

	ctx := context.Background()
	clock := hlc.NewClockWithSystemTimeSource(base.DefaultMaxClockOffset, base.DefaultMaxClockOffset)
	stale := clock.Now().Add(2*expectedFollowerReadOffset.Nanoseconds(), 0)
	current := clock.Now()
	future := clock.Now().Add(2*clock.MaxOffset().Nanoseconds(), 0)

	txn := func(ts hlc.Timestamp) *roachpb.Transaction {
		txn := roachpb.MakeTransaction("txn", nil, 0, 0, ts, 0, 1)
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
		disabledEnterprise    bool
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
			ba:   batch(txn(stale), &kvpb.GetRequest{KeyLocking: lock.Exclusive}),
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
			ba:       batch(txn(stale), &kvpb.GetRequest{KeyLocking: lock.Exclusive}),
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
			name:               "non-enterprise",
			ba:                 withBatchTimestamp(batch(nil, &kvpb.GetRequest{}), stale),
			disabledEnterprise: true,
			exp:                false,
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
			if !c.disabledEnterprise {
				defer utilccl.TestingEnableEnterprise()()
			}
			st := cluster.MakeTestingClusterSettings()
			kvserver.FollowerReadsEnabled.Override(ctx, &st.SV, !c.disabledFollowerReads)
			if c.zeroTargetDuration {
				closedts.TargetDuration.Override(ctx, &st.SV, 0)
			}

			can := canSendToFollower(uuid.MakeV4(), st, clock, c.ctPolicy, c.ba)
			require.Equal(t, c.exp, can)
		})
	}
}

// mockNodeStore implements the kvcoord.NodeDescStore interface.
type mockNodeStore []roachpb.NodeDescriptor

func (s mockNodeStore) GetNodeDescriptor(id roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	for i := range s {
		desc := &s[i]
		if desc.NodeID == id {
			return desc, nil
		}
	}
	return nil, errorutil.NewNodeNotFoundError(id)
}

func (s mockNodeStore) GetNodeDescriptorCount() int {
	return len(s)
}

func (s mockNodeStore) GetStoreDescriptor(id roachpb.StoreID) (*roachpb.StoreDescriptor, error) {
	return nil, errorutil.NewStoreNotFoundError(id)
}

// TestOracle tests the Oracle exposed by this package.
func TestOracle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	clock := hlc.NewClockWithSystemTimeSource(base.DefaultMaxClockOffset, base.DefaultMaxClockOffset)
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

	nodes := mockNodeStore{
		{NodeID: 1, Address: util.MakeUnresolvedAddr("tcp", "1")},
		{NodeID: 2, Address: util.MakeUnresolvedAddr("tcp", "2")},
		{NodeID: 3, Address: util.MakeUnresolvedAddr("tcp", "3")},
	}
	replicas := []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1},
		{NodeID: 2, StoreID: 2},
		{NodeID: 3, StoreID: 3},
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
		disabledEnterprise    bool
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
			name:               "stale txn, non-enterprise",
			txn:                staleTxn,
			lh:                 &leaseholder,
			disabledEnterprise: true,
			exp:                leaseholder,
		},
		{
			name:                  "stale txn, follower reads disabled",
			txn:                   staleTxn,
			lh:                    &leaseholder,
			disabledFollowerReads: true,
			exp:                   leaseholder,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			if !c.disabledEnterprise {
				defer utilccl.TestingEnableEnterprise()()
			}
			st := cluster.MakeTestingClusterSettings()
			kvserver.FollowerReadsEnabled.Override(ctx, &st.SV, !c.disabledFollowerReads)

			o := replicaoracle.NewOracle(followerReadOraclePolicy, replicaoracle.Config{
				NodeDescs:  nodes,
				Settings:   st,
				RPCContext: rpcContext,
				Clock:      clock,
			})

			res, _, err := o.ChoosePreferredReplica(ctx, c.txn, desc, c.lh, c.ctPolicy, replicaoracle.QueryState{})
			require.NoError(t, err)
			require.Equal(t, c.exp, res)
		})
	}
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

	historicalQuery := `SELECT * FROM test AS OF SYSTEM TIME follower_read_timestamp() WHERE k=2`
	recCh := make(chan tracingpb.Recording, 1)

	tc := testcluster.StartTestCluster(t, 4,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TODOTestTenantDisabled,
				UseDatabase:       "t",
			},
			// n4 pretends to have low latency to n2 and n3, so that it tries to use
			// them for follower reads.
			// Also, we're going to collect a trace of the test's final query.
			ServerArgsPerNode: map[int]base.TestServerArgs{
				3: {
					DefaultTestTenant: base.TODOTestTenantDisabled,
					UseDatabase:       "t",
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
								if stmt == historicalQuery {
									recCh <- trace
								}
							},
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)

	n1 := sqlutils.MakeSQLRunner(tc.Conns[0])
	n1.Exec(t, `CREATE DATABASE t`)
	n1.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY)`)
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE VOTERS VALUES (ARRAY[1,2], 1)`)
	// Speed up closing of timestamps, in order to sleep less below before we can
	// use follower_read_timestamp(). follower_read_timestamp() uses the sum of
	// the following settings.
	n1.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '0.1s'`)
	n1.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '0.1s'`)
	n1.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.propagation_slack = '0.1s'`)

	// Sleep so that we can perform follower reads. The read timestamp needs to be
	// above the timestamp when the table was created.
	log.Infof(ctx, "test sleeping for the follower read timestamps to pass the table creation timestamp...")
	time.Sleep(300 * time.Millisecond)
	log.Infof(ctx, "test sleeping... done")

	// Run a query on n4 to populate its cache.
	n4 := sqlutils.MakeSQLRunner(tc.Conns[3])
	n4.Exec(t, "SELECT * from test WHERE k=1")
	// Check that the cache was indeed populated.
	var tableID uint32
	n1.QueryRow(t, `SELECT id from system.namespace WHERE name='test'`).Scan(&tableID)
	tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
	n4Cache := tc.Server(3).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
	entry := n4Cache.GetCached(ctx, tablePrefix, false /* inverted */)
	require.NotNil(t, entry)
	require.False(t, entry.Lease().Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease().Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 2, StoreID: 2, ReplicaID: 2},
	}, entry.Desc().Replicas().Descriptors())

	// Remove the follower and add a new non-voter to n3. n2 will no longer have a
	// replica.
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE VOTERS VALUES (ARRAY[1], 1)`)
	n1.Exec(t, `ALTER TABLE test EXPERIMENTAL_RELOCATE NONVOTERS VALUES (ARRAY[3], 1)`)

	// Execute the query again and assert the cache is updated. This query will
	// not be executed as a follower read since it attempts to use n2 which
	// doesn't have a replica any more and then it tries n1 which returns an
	// updated descriptor.
	n4.Exec(t, historicalQuery)
	// As a sanity check, verify that this was not a follower read.
	rec := <-recCh
	require.False(t, kv.OnlyFollowerReads(rec), "query was served through follower reads: %s", rec)
	// Check that the cache was properly updated.
	entry = n4Cache.GetCached(ctx, tablePrefix, false /* inverted */)
	require.NotNil(t, entry)
	require.False(t, entry.Lease().Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease().Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 3, StoreID: 3, ReplicaID: 3, Type: roachpb.NON_VOTER},
	}, entry.Desc().Replicas().Descriptors())

	// Make a note of the follower reads metric on n3. We'll check that it was
	// incremented.
	var followerReadsCountBefore int64
	err := tc.Servers[2].Stores().VisitStores(func(s *kvserver.Store) error {
		followerReadsCountBefore = s.Metrics().FollowerReadsCount.Count()
		return nil
	})
	require.NoError(t, err)

	// Run a historical query and assert that it's served from the follower (n3).
	// n4 should attempt to route to n3 because we pretend n3 has a lower latency
	// (see testing knob).
	n4.Exec(t, historicalQuery)
	rec = <-recCh

	// Look at the trace and check that we've served a follower read.
	require.True(t, kv.OnlyFollowerReads(rec), "query was not served through follower reads: %s", rec)

	// Check that the follower read metric was incremented.
	var followerReadsCountAfter int64
	err = tc.Servers[2].Stores().VisitStores(func(s *kvserver.Store) error {
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
	entry = n3Cache.GetCached(ctx, tablePrefix, false /* inverted */)
	require.NotNil(t, entry)
	require.False(t, entry.Lease().Empty())
	require.Equal(t, roachpb.StoreID(1), entry.Lease().Replica.StoreID)
	require.Equal(t, []roachpb.ReplicaDescriptor{
		{NodeID: 1, StoreID: 1, ReplicaID: 1},
		{NodeID: 3, StoreID: 3, ReplicaID: 3, Type: roachpb.NON_VOTER},
	}, entry.Desc().Replicas().Descriptors())

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
	historicalQuery = `SELECT * FROM [SELECT * FROM test WHERE k=2 UNION ALL SELECT * FROM test WHERE k=3] AS OF SYSTEM TIME follower_read_timestamp()`
	n4.Exec(t, historicalQuery)
	rec = <-recCh

	// Sanity check that the plan was distributed.
	require.True(t, strings.Contains(rec.String(), "creating DistSQL plan with isLocal=false"))
	// Look at the trace and check that we've served a follower read.
	require.True(t, kv.OnlyFollowerReads(rec), "query was not served through follower reads: %s", rec)
	// Verify that we didn't produce the "misplanned ranges" metadata that would
	// purge the non-stale entries from the range cache on n4.
	require.False(t, strings.Contains(rec.String(), "clearing entries overlapping"))
	// Also confirm that we didn't even check for the "misplanned ranges"
	// metadata on n3.
	require.False(t, strings.Contains(rec.String(), "checking range cache to see if range info updates should be communicated to the gateway"))
}

// TestSecondaryTenantFollowerReadsRouting ensures that secondary tenants route
// their requests to the nearest replica. The test runs two versions -- one
// where accurate latency information between nodes is available and another
// where it needs to be estimated using node localities.
func TestSecondaryTenantFollowerReadsRouting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	skip.UnderStressRace(t, "times out")

	testutils.RunTrueAndFalse(t, "valid-latency-func", func(t *testing.T, validLatencyFunc bool) {
		const numNodes = 4

		serverArgs := make(map[int]base.TestServerArgs)
		localities := make(map[int]roachpb.Locality)
		for i := 0; i < numNodes; i++ {
			regionName := fmt.Sprintf("region_%d", i)
			if i == 3 {
				// Make it such that n4 and n2 are in the same region. Below, we'll
				// expect a follower read from n4 to be served by n2 because they're
				// in the same locality (when validLatencyFunc is false).
				regionName = fmt.Sprintf("region_%d", 1)
			}
			locality := roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: regionName}},
			}
			localities[i] = locality
			serverArgs[i] = base.TestServerArgs{
				Locality:          localities[i],
				DefaultTestTenant: base.TODOTestTenantDisabled, // we'll create one ourselves below.
			}
		}
		tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: serverArgs,
		})
		ctx := context.Background()
		defer tc.Stopper().Stop(ctx)

		historicalQuery := `SELECT * FROM t.test AS OF SYSTEM TIME follower_read_timestamp() WHERE k=2`
		recCh := make(chan tracingpb.Recording, 1)

		var tenants [numNodes]serverutils.TestTenantInterface
		for i := 0; i < numNodes; i++ {
			knobs := base.TestingKnobs{}
			if i == 3 { // n4
				knobs = base.TestingKnobs{
					KVClient: &kvcoord.ClientTestingKnobs{
						DontConsiderConnHealth: true,
						// For the validLatencyFunc=true version of the test, the client
						// pretends to have a low latency connection to n2. As a result, we
						// expect n2 to be used for follower reads originating from n4.
						//
						// For the variant where no latency information is available, we
						// expect n2 to serve follower reads as well, but because it
						// is in the same locality as the client.
						LatencyFunc: func(id roachpb.NodeID) (time.Duration, bool) {
							if !validLatencyFunc {
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
			tt, err := tc.Server(i).StartTenant(ctx, base.TestTenantArgs{
				TenantID:     serverutils.TestTenantID(),
				Locality:     localities[i],
				TestingKnobs: knobs,
			})
			require.NoError(t, err)
			tenants[i] = tt
		}

		// Speed up closing of timestamps in order to sleep less below before we can
		// use follower_read_timestamp(). Note that we need to override the setting
		// for the tenant as well, because the builtin is run in the tenant's sql pod.
		systemSQL := sqlutils.MakeSQLRunner(tc.Conns[0])
		systemSQL.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '0.1s'`)
		systemSQL.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '0.1s'`)
		systemSQL.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.propagation_slack = '0.1s'`)
		systemSQL.Exec(t, `ALTER TENANT ALL SET CLUSTER SETTING kv.closed_timestamp.target_duration = '0.1s'`)
		systemSQL.Exec(t, `ALTER TENANT ALL SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '0.1s'`)
		systemSQL.Exec(t, `ALTER TENANT ALL SET CLUSTER SETTING kv.closed_timestamp.propagation_slack = '0.1s'`)
		// We're making assertions on traces collected by the tenant using log lines
		// in KV so we must ensure they're not redacted.
		systemSQL.Exec(t, `SET CLUSTER SETTING server.secondary_tenants.redact_trace.enabled = 'false'`)

		// Wait until all tenant servers are aware of the setting override.
		testutils.SucceedsSoon(t, func() error {
			settingNames := []string{
				"kv.closed_timestamp.target_duration", "kv.closed_timestamp.side_transport_interval", "kv.closed_timestamp.propagation_slack",
			}
			for _, settingName := range settingNames {
				for i := 0; i < numNodes; i++ {
					pgURL, cleanup := sqlutils.PGUrl(t, tenants[i].SQLAddr(), "Tenant", url.User(username.RootUser))
					defer cleanup()
					db, err := gosql.Open("postgres", pgURL.String())
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()

					var val string
					err = db.QueryRow(
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

		pgURL, cleanupPGUrl := sqlutils.PGUrl(
			t, tenants[3].SQLAddr(), "Tenant", url.User(username.RootUser),
		)
		defer cleanupPGUrl()
		tenantSQLDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		defer tenantSQLDB.Close()
		tenantSQL := sqlutils.MakeSQLRunner(tenantSQLDB)

		tenantSQL.Exec(t, `CREATE DATABASE t`)
		tenantSQL.Exec(t, `CREATE TABLE t.test (k INT PRIMARY KEY)`)

		startKey := keys.MakeSQLCodec(serverutils.TestTenantID()).TenantPrefix()
		tc.AddVotersOrFatal(t, startKey, tc.Target(1), tc.Target(2))
		tc.WaitForVotersOrFatal(t, startKey, tc.Target(1), tc.Target(2))
		desc := tc.LookupRangeOrFatal(t, startKey)
		require.Equal(t, []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1, ReplicaID: 1},
			{NodeID: 2, StoreID: 2, ReplicaID: 2},
			{NodeID: 3, StoreID: 3, ReplicaID: 3},
		}, desc.Replicas().Descriptors())

		// Sleep so that we can perform follower reads. The read timestamp needs to be
		// above the timestamp when the table was created.
		log.Infof(ctx, "test sleeping for the follower read timestamps to pass the table creation timestamp...")
		time.Sleep(500 * time.Millisecond)
		log.Infof(ctx, "test sleeping... done")

		getFollowerReadCounts := func() [numNodes]int64 {
			var counts [numNodes]int64
			for i := range tc.Servers {
				err := tc.Servers[i].Stores().VisitStores(func(s *kvserver.Store) error {
					counts[i] = s.Metrics().FollowerReadsCount.Count()
					return nil
				})
				require.NoError(t, err)
			}
			return counts
		}

		// Check that the cache was indeed populated.
		tenantSQL.Exec(t, `SELECT * FROM t.test WHERE k = 1`)
		tablePrefix := keys.MustAddr(keys.MakeSQLCodec(serverutils.TestTenantID()).TenantPrefix())
		cache := tenants[3].DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
		entry := cache.GetCached(ctx, tablePrefix, false /* inverted */)
		require.NotNil(t, entry)
		require.False(t, entry.Lease().Empty())
		require.Equal(t, roachpb.StoreID(1), entry.Lease().Replica.StoreID)
		require.Equal(t, []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1, ReplicaID: 1},
			{NodeID: 2, StoreID: 2, ReplicaID: 2},
			{NodeID: 3, StoreID: 3, ReplicaID: 3},
		}, entry.Desc().Replicas().Descriptors())

		followerReadCountsBefore := getFollowerReadCounts()
		tenantSQL.Exec(t, historicalQuery)
		followerReadsCountsAfter := getFollowerReadCounts()

		rec := <-recCh
		// Look at the trace and check that we've served a follower read.
		require.True(t, kv.OnlyFollowerReads(rec), "query was served through follower reads: %s", rec)

		for i := 0; i < numNodes; i++ {
			if i == 1 { // n2
				require.Greater(t, followerReadsCountsAfter[i], followerReadCountsBefore[i])
				continue
			}
			require.Equal(t, followerReadsCountsAfter[i], followerReadCountsBefore[i])
		}
	})
}
