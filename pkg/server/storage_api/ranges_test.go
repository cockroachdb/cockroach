// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestRangesResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.EnableLeaseHistoryForTesting(100)()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	// Create few ranges
	r := sqlutils.MakeSQLRunner(ts.SQLConn(t))
	r.Exec(t, "CREATE TABLE t (x INT PRIMARY KEY, xsquared INT)")
	for i := 0; i < 5; i++ {
		r.Exec(t, fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES (%d)", 100*i/5))
	}

	t.Run("test ranges response", func(t *testing.T) {
		// Perform a scan to ensure that all the raft groups are initialized.
		if _, err := srv.SystemLayer().DB().Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
			t.Fatal(err)
		}

		var response serverpb.RangesResponse
		if err := srvtestutils.GetStatusJSONProto(ts, "ranges/local", &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Ranges) == 0 {
			t.Errorf("didn't get any ranges")
		}
		for _, ri := range response.Ranges {
			// Do some simple validation based on the fact that this is a
			// single-node cluster.
			if ri.RaftState.State != "StateLeader" && ri.RaftState.State != server.RaftStateDormant {
				t.Errorf("expected to be Raft leader or dormant, but was '%s'", ri.RaftState.State)
			}
			expReplica := roachpb.ReplicaDescriptor{
				NodeID:    1,
				StoreID:   1,
				ReplicaID: 1,
			}
			if len(ri.State.Desc.InternalReplicas) != 1 || ri.State.Desc.InternalReplicas[0] != expReplica {
				t.Errorf("unexpected replica list %+v", ri.State.Desc.InternalReplicas)
			}
			if ri.State.Lease == nil || ri.State.Lease.Empty() {
				t.Error("expected a nontrivial Lease")
			}
			if ri.State.LastIndex == 0 {
				t.Error("expected positive LastIndex")
			}
			if len(ri.LeaseHistory) == 0 {
				t.Error("expected at least one lease history entry")
			}
		}
	})

	t.Run("test ranges pagination", func(t *testing.T) {
		ctx := context.Background()
		rpcStopper := stop.NewStopper()
		defer rpcStopper.Stop(ctx)

		client := ts.GetStatusClient(t)
		resp1, err := client.Ranges(ctx, &serverpb.RangesRequest{
			Limit: 1,
		})
		require.NoError(t, err)
		require.Len(t, resp1.Ranges, 1)
		require.Equal(t, int(resp1.Next), 1)

		resp2, err := client.Ranges(ctx, &serverpb.RangesRequest{
			Limit:  1,
			Offset: resp1.Next,
		})
		require.NoError(t, err)
		require.Len(t, resp2.Ranges, 1)
		require.Equal(t, int(resp2.Next), 2)

		// Verify pagination functions based on ascending RangeID order.
		require.True(t, resp1.Ranges[0].State.Desc.RangeID < resp2.Ranges[0].State.Desc.RangeID)
	})
}

// TestTenantRangesSecurity checks that the storage layer properly zeroes out
// if it's missing a tenant ID in the TenantRanges back-end RPC.
func TestTenantRangesSecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.SystemLayer()

	t.Run("returns error when TenantID not set in ctx", func(t *testing.T) {
		rpcStopper := stop.NewStopper()
		defer rpcStopper.Stop(ctx)

		client := ts.GetStatusClient(t)
		_, err := client.TenantRanges(ctx, &serverpb.TenantRangesRequest{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no tenant ID found in context")
	})
}

func TestRangeResponse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer kvserver.EnableLeaseHistoryForTesting(100)()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	// Perform a scan to ensure that all the raft groups are initialized.
	if _, err := srv.SystemLayer().DB().Scan(context.Background(), keys.LocalMax, roachpb.KeyMax, 0); err != nil {
		t.Fatal(err)
	}

	t.Run("access range endpoint within current tenant's scope", func(t *testing.T) {
		var currentTenantRangeId int64
		// Create a new table in the current tenant.
		r := sqlutils.MakeSQLRunner(ts.SQLConn(t))
		r.Exec(t, "CREATE TABLE t (x INT PRIMARY KEY, xsquared INT)")
		for i := 0; i < 5; i++ {
			r.Exec(t, fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES (%d)", 100*i/5))
		}
		// Get some ranges from the given table
		r.QueryRow(t, "SELECT range_id FROM [SHOW RANGES FROM TABLE t WITH DETAILS] LIMIT 1;").Scan(&currentTenantRangeId)
		t.Logf("Table 't' range: %d", currentTenantRangeId)
		require.NoError(t, validateRangeBelongsToTenant(ts, currentTenantRangeId))
	})

	t.Run("access range endpoint within system tenant's scope", func(t *testing.T) {
		if !srv.StartedDefaultTestTenant() {
			require.NoError(t, validateRangeBelongsToTenant(ts, 1))
		} else {
			expectedError := fmt.Errorf("got the wrong number of ranges in the response, expected %d, actual %d", 1, 0)
			actualError := validateRangeBelongsToTenant(ts, 1)
			require.Error(t, actualError)
			require.Equal(t, expectedError, actualError)
		}

	})
}

func validateRangeBelongsToTenant(ts serverutils.ApplicationLayerInterface, rangeId int64) error {
	var response serverpb.RangeResponse
	if err := srvtestutils.GetStatusJSONProto(ts, fmt.Sprintf("range/%d", rangeId), &response); err != nil {
		return err
	}

	// This is a single node cluster, so only expect a single response.
	if e, a := 1, len(response.ResponsesByNodeID); e != a {
		return fmt.Errorf("got the wrong number of responses, expected %d, actual %d", e, a)
	}

	node1Response := response.ResponsesByNodeID[response.NodeID]

	// The response should come back as valid.
	if !node1Response.Response {
		return fmt.Errorf("node1's response returned as false, expected true")
	}

	// The response should include just the one range.
	if e, a := 1, len(node1Response.Infos); e != a {
		return fmt.Errorf("got the wrong number of ranges in the response, expected %d, actual %d", e, a)
	}

	info := node1Response.Infos[0]
	expReplica := roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: 1,
	}

	// Check some other values.
	if len(info.State.Desc.InternalReplicas) != 1 || info.State.Desc.InternalReplicas[0] != expReplica {
		return fmt.Errorf("unexpected replica list %+v", info.State.Desc.InternalReplicas)
	}

	if info.State.Lease == nil || info.State.Lease.Empty() {
		return fmt.Errorf("expected a nontrivial Lease")
	}

	if info.State.LastIndex == 0 {
		return fmt.Errorf("expected positive LastIndex")
	}

	if len(info.LeaseHistory) == 0 {
		return fmt.Errorf("expected at least one lease history entry")
	}
	return nil
}
