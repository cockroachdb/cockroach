// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestManualReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	kvDB := tc.Servers[0].DB()
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	tableStartKey := keys.SystemSQLCodec.TablePrefix(uint32(tableDesc.GetID()))
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
		if _, ok := tableRangeDesc.GetReplicaDescriptor(
			tc.Servers[i].GetFirstStoreID()); !ok {
			t.Fatalf("expected replica on store %d, got %+v",
				tc.Servers[i].GetFirstStoreID(), tableRangeDesc.InternalReplicas)
		}
	}

	// Transfer the lease to node 1.
	leaseHolder, err := tc.FindRangeLeaseHolder(
		tableRangeDesc,
		&roachpb.ReplicationTarget{
			NodeID:  tc.Servers[0].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[0].GetFirstStoreID(),
		})
	if err != nil {
		t.Fatal(err)
	}
	if leaseHolder.StoreID != tc.Servers[0].GetFirstStoreID() {
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
	leaseHolder, err = tc.FindRangeLeaseHolder(
		tableRangeDesc,
		&roachpb.ReplicationTarget{
			NodeID:  tc.Servers[0].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[0].GetFirstStoreID(),
		})
	if err != nil {
		t.Fatal(err)
	}
	if leaseHolder.StoreID != tc.Servers[1].GetFirstStoreID() {
		t.Fatalf("expected lease on server idx 1 (node: %d store: %d), but is on node: %+v",
			tc.Servers[1].GetNode().Descriptor.NodeID,
			tc.Servers[1].GetFirstStoreID(),
			leaseHolder)
	}
}

// A basic test of manual replication that used to fail because we weren't
// waiting for all of the stores to initialize.
func TestBasicManualReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := StartTestCluster(t, 3, base.TestClusterArgs{ReplicationMode: base.ReplicationManual})
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

	tc := StartTestCluster(t, 3, base.TestClusterArgs{ReplicationMode: base.ReplicationAuto})
	defer tc.Stopper().Stop(context.Background())
	// NB: StartTestCluster will wait for full replication.
}

func TestStopServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use insecure mode so our servers listen on util.IsolatedTestAddr
	// and they fail cleanly instead of interfering with other tests.
	// See https://github.com/cockroachdb/cockroach/issues/9256
	tc := StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Insecure: true,
		},
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(context.Background())

	// Connect to server 1, ensure it is answering requests over HTTP and GRPC.
	server1 := tc.Server(1)
	var response serverpb.JSONResponse

	httpClient1, err := server1.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	url := server1.AdminURL() + "/_status/metrics/local"
	if err := httputil.GetJSON(httpClient1, url, &response); err != nil {
		t.Fatal(err)
	}

	rpcContext := rpc.NewContext(rpc.ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: tc.Server(0).ClusterSettings().Tracer},
		Config:     tc.Server(1).RPCContext().Config,
		Clock:      tc.Server(1).Clock(),
		Stopper:    tc.Stopper(),
		Settings:   tc.Server(1).ClusterSettings(),
	})
	conn, err := rpcContext.GRPCDialNode(server1.ServingRPCAddr(), server1.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	statusClient1 := serverpb.NewStatusClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
	httpClient1, err = tc.Server(0).GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	url = tc.Server(0).AdminURL() + "/_status/metrics/local"
	if err := httputil.GetJSON(httpClient1, url, &response); err != nil {
		t.Fatal(err)
	}
}

func TestRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: "TestRestart" + strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
		}
	}

	ctx := context.Background()
	tc := StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationAuto,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	ids := make([]roachpb.ReplicationTarget, numServers)
	for i := range tc.Servers {
		ids[i] = tc.Target(i)
	}

	incArgs := &roachpb.IncrementRequest{
		RequestHeader: roachpb.RequestHeader{
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
