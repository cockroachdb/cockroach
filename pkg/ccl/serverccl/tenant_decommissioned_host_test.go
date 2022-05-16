// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTenantWithDecommissionedID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is a regression test for a multi-tenant bug. Each tenant sql server
	// is assigned an InstanceID. The InstanceID corresponds to the id column in
	// the system.sql_instances table. The sql process sets rpcContext.NodeID =
	// InstanceID and PingRequest.NodeID = rpcContext.NodeID.
	//
	// When a KV node receives a ping, it checks the NodeID against a
	// decommissioned node tombstone list. Until PR #75766, this caused the KV
	// node to reject pings from sql servers. The rejected pings would manifest
	// as sql connection timeouts.

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	server := tc.Server(0)
	hostID := server.NodeID()
	decommissionID := roachpb.NodeID(int(hostID) + 1)

	liveness := server.NodeLiveness().(*liveness.NodeLiveness)
	require.NoError(t, liveness.CreateLivenessRecord(ctx, decommissionID))
	require.NoError(t, server.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{decommissionID}))
	require.NoError(t, server.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONED, []roachpb.NodeID{decommissionID}))

	tenantID := serverutils.TestTenantID()

	var tenantSQLServer serverutils.TestTenantInterface
	var tenantDB *gosql.DB
	for instanceID := 1; instanceID <= int(decommissionID); instanceID++ {
		sqlServer, tenant := serverutils.StartTenant(t, server, base.TestTenantArgs{
			TenantID: tenantID,
			// Set a low heartbeat interval. The first heartbeat succeeds
			// because the tenant needs to communicate with the kv node to
			// determine its instance id.
			RPCHeartbeatInterval: time.Millisecond * 5,
		})
		if sqlServer.RPCContext().NodeID.Get() == decommissionID {
			tenantSQLServer = sqlServer
			tenantDB = tenant
		} else {
			tenant.Close()
		}
	}
	require.NotNil(t, tenantSQLServer)
	defer tenantDB.Close()

	_, err := tenantDB.Exec("CREATE ROLE test_user WITH PASSWORD 'password'")
	require.NoError(t, err)
}
