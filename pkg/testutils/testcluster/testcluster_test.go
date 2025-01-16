// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestClusterStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())
}

func TestClusterSqlDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var testTypes = []base.DefaultTestTenantOptions{base.TestTenantAlwaysEnabled, base.TestIsSpecificToStorageLayerAndNeedsASystemTenant}
	testutils.RunTrueAndFalse(t, "disable-sql", func(t *testing.T, disableSQL bool) {
		testutils.RunValues(t, "test-tenant", testTypes, func(t *testing.T, testTenant base.DefaultTestTenantOptions) {
			if disableSQL && testTenant == base.TestTenantAlwaysEnabled {
				// In this combination, it will both fatal and set t.Fail which
				// is not recoverable. Ideally we could validate this error
				// occurs, but there isn't an easy way to handle this.
				skip.IgnoreLint(t)
				return
			}
			tc := StartTestCluster(t, 3,
				base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						DefaultTestTenant: testTenant,
						DisableSQLServer:  disableSQL,
					},
				})
			defer tc.Stopper().Stop(context.Background())
		})
	})
}

func TestManualReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "t",
			},
		})
	defer tc.Stopper().Stop(context.Background())

	s0 := sqlutils.MakeSQLRunner(tc.Conns[0])
	s1 := sqlutils.MakeSQLRunner(tc.Conns[1])
	s2 := sqlutils.MakeSQLRunner(tc.Conns[2])

	s0.Exec(t, `CREATE DATABASE t`)
	s0.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY, v INT)`)
	s0.Exec(t, `INSERT INTO test VALUES (5, 1), (4, 2), (1, 2)`)

	if r := s1.Query(t, `SELECT * FROM test WHERE k = 5`); !r.Next() {
		t.Fatal("no rows")
	} else {
		r.Close()
	}

	s2.ExecRowsAffected(t, 3, `DELETE FROM test`)

	// Split the table to a new range.
	ts := tc.Server(0).ApplicationLayer()
	kvDB := ts.DB()
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, ts.Codec(), "t", "test")

	tableStartKey := ts.Codec().TablePrefix(uint32(tableDesc.GetID()))
	leftRangeDesc, tableRangeDesc, err := tc.SplitRange(tableStartKey)
	if err != nil {
		t.Fatal(err)
	}
	log.Infof(context.Background(), "After split got ranges: %+v and %+v.", leftRangeDesc, tableRangeDesc)
	if len(tableRangeDesc.InternalReplicas) == 0 {
		t.Fatalf(
			"expected replica on node 1, got no replicas: %+v", tableRangeDesc.InternalReplicas)
	}
	if tableRangeDesc.InternalReplicas[0].NodeID != 1 {
		t.Fatalf(
			"expected replica on node 1, got replicas: %+v", tableRangeDesc.InternalReplicas)
	}

	// Replicate the table's range to all the nodes.
	tableRangeDesc, err = tc.AddVoters(
		tableRangeDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tableRangeDesc.InternalReplicas) != 3 {
		t.Fatalf("expected 3 replicas, got %+v", tableRangeDesc.InternalReplicas)
	}
	for i := 0; i < 3; i++ {
		sl := tc.Servers[i].StorageLayer()
		if _, ok := tableRangeDesc.GetReplicaDescriptor(
			sl.GetFirstStoreID()); !ok {
			t.Fatalf("expected replica on store %d, got %+v",
				sl.GetFirstStoreID(), tableRangeDesc.InternalReplicas)
		}
	}

	// Transfer the lease to node 2.
	target := tc.Target(0)
	leaseHolder, err := tc.FindRangeLeaseHolder(tableRangeDesc, &target)
	if err != nil {
		t.Fatal(err)
	}
	if leaseHolder.StoreID != tc.Servers[0].StorageLayer().GetFirstStoreID() {
		t.Fatalf("expected initial lease on server idx 0, but is on node: %+v",
			leaseHolder)
	}

	err = tc.TransferRangeLease(tableRangeDesc, tc.Target(1))
	if err != nil {
		t.Fatal(err)
	}

	// Check that the lease holder has changed. We'll use the old lease holder as
	// the hint, since it's guaranteed that the old lease holder has applied the
	// new lease.
	// We wrap this in a SucceedsSoon because N2 might have not applied the lease
	// yet, it might think that the N1 is the current leaseholder, and the lease
	// might be INVALID if the time is close to the lease expiration time, or in
	// the case of leader leases, N2 might not be able to determine the validity
	// of the lease all together.
	testutils.SucceedsSoon(t, func() error {
		leaseHolder, err = tc.FindRangeLeaseHolder(tableRangeDesc, &target)
		return err
	})

	if leaseHolder.StoreID != tc.Servers[1].StorageLayer().GetFirstStoreID() {
		t.Fatalf("expected lease on server idx 1 (node: %d store: %d), but is on node: %+v",
			tc.Server(1).NodeID(),
			tc.Server(1).GetFirstStoreID(),
			leaseHolder)
	}
}

// A basic test of manual replication that used to fail because we weren't
// waiting for all of the stores to initialize.
func TestBasicManualReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			DisableSQLServer:  true,
		},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(context.Background())

	desc, err := tc.AddVoters(keys.MinKey, tc.Target(1), tc.Target(2))
	if err != nil {
		t.Fatal(err)
	}
	if expected := 3; expected != len(desc.InternalReplicas) {
		t.Fatalf("expected %d replicas, got %+v", expected, desc.InternalReplicas)
	}

	if err := tc.TransferRangeLease(desc, tc.Target(1)); err != nil {
		t.Fatal(err)
	}

	// NB: Removing the leaseholder (tc.Target(1)) causes the test to take ~11s
	// vs ~1.5s for removing a non-leaseholder. This is due to needing to wait
	// for the lease to timeout which takes ~9s. Testing leaseholder removal is
	// not necessary because internal rebalancing avoids ever removing the
	// leaseholder for the exact reason that it causes performance hiccups.
	desc, err = tc.RemoveVoters(desc.StartKey.AsRawKey(), tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}
	if expected := 2; expected != len(desc.InternalReplicas) {
		t.Fatalf("expected %d replicas, got %+v", expected, desc.InternalReplicas)
	}
}

func TestBasicAutoReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			DisableSQLServer:  true,
		},
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(context.Background())
	// NB: StartTestCluster will wait for full replication.
}

func TestStopServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,

			// We use Insecure: true because the .GetAdminHTTPClient() API
			// does not currently work when called from two different servers
			// in the same TestCluster.
			Insecure:         true,
			DisableSQLServer: true,
		},
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(context.Background())

	// Connect to server 1, ensure it is answering requests over HTTP and GRPC.
	server1 := tc.Server(1).SystemLayer()
	var response serverpb.JSONResponse

	httpClient1, err := server1.GetUnauthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	url := server1.AdminURL().WithPath("/_status/metrics/local").String()
	if err := httputil.GetJSON(httpClient1, url, &response); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	statusClient1 := server1.GetStatusClient(t)
	var cancel func()
	ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := statusClient1.Metrics(ctx, &serverpb.MetricsRequest{NodeId: "local"}); err != nil {
		t.Fatal(err)
	}

	// Stop server 1.
	tc.StopServer(1)

	// Verify HTTP and GRPC requests to server now fail.
	//
	// On *nix, this error is:
	//
	// dial tcp 127.0.0.1:65054: getsockopt: connection refused
	//
	// On Windows, this error is:
	//
	// dial tcp 127.0.0.1:59951: connectex: No connection could be made because the target machine actively refused it.
	//
	// So we look for the common bit.
	httpErrorText := `dial tcp .*: .* refused`
	if err := httputil.GetJSON(httpClient1, url, &response); err == nil {
		t.Fatal("Expected HTTP Request to fail after server stopped")
	} else if !testutils.IsError(err, httpErrorText) {
		t.Fatalf("Expected error from server with text %q, got error with text %q", httpErrorText, err.Error())
	}

	grpcErrorText := "rpc error"
	if _, err := statusClient1.Metrics(ctx, &serverpb.MetricsRequest{NodeId: "local"}); err == nil {
		t.Fatal("Expected GRPC Request to fail after server stopped")
	} else if !testutils.IsError(err, grpcErrorText) {
		t.Fatalf("Expected error from GRPC with text %q, got error with text %q", grpcErrorText, err.Error())
	}

	// Verify that request to Server 0 still works.
	srv0 := tc.Server(0).SystemLayer()
	httpClient1, err = srv0.GetUnauthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	url = srv0.AdminURL().WithPath("/_status/metrics/local").String()
	if err := httputil.GetJSON(httpClient1, url, &response); err != nil {
		t.Fatal(err)
	}
}

func TestRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: "TestRestart" + strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
				DisableSQLServer:  true,
			},
			ReplicationMode:     base.ReplicationAuto,
			ReusableListenerReg: lisReg,
			ServerArgsPerNode:   stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	ids := make([]roachpb.ReplicationTarget, numServers)
	for i := range tc.Servers {
		ids[i] = tc.Target(i)
	}

	incArgs := &kvpb.IncrementRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: roachpb.Key("b"),
		},
		Increment: 9,
	}
	if _, pErr := kv.SendWrapped(ctx, tc.GetFirstStoreFromServer(t, 0).DB().NonTransactionalSender(), incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	tc.WaitForValues(t, roachpb.Key("b"), []int64{9, 9, 9})

	// First try to restart a single server.
	tc.StopServer(1)
	require.NoError(t, tc.RestartServer(1))
	require.Equal(t, ids[1], tc.Target(1))
	tc.WaitForValues(t, roachpb.Key("b"), []int64{9, 9, 9})

	// Now restart the whole cluster.
	require.NoError(t, tc.Restart())

	// Validates that the NodeID and StoreID remain the same after a restart.
	for i := range tc.Servers {
		require.Equal(t, ids[i], tc.Target(i))
	}

	// Verify we can still read data.
	tc.WaitForValues(t, roachpb.Key("b"), []int64{9, 9, 9})
}

func TestExpirationBasedLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
				DisableSQLServer:  true,
			},
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRangeWithExpirationLease(t)
	repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(key))
	lease, _ := repl.GetLease()
	require.NotNil(t, lease.Expiration)

	// Verify idempotence of ScratchRangeWithExpirationLease
	keyAgain := tc.ScratchRangeWithExpirationLease(t)
	replAgain := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(keyAgain))
	require.Equal(t, repl, replAgain)
}
