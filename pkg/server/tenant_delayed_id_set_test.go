// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func TestStartTenantWithDelayedID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	clusterSettings := cluster.MakeTestingClusterSettings()
	args := base.TestServerArgs{
		Settings:          clusterSettings,
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	}
	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(ctx)

	func() {
		// Create the tenant.
		tenantStopper := stop.NewStopper()
		defer tenantStopper.Stop(ctx)
		_, db := serverutils.StartTenant(t, s, base.TestTenantArgs{
			Stopper:  tenantStopper,
			TenantID: serverutils.TestTenantID()})
		defer db.Close()
	}()

	st := cluster.MakeTestingClusterSettings()
	baseCfg := makeTestBaseConfig(st, s.Stopper().Tracer())
	sqlCfg := makeTestSQLConfig(st, roachpb.TenantID{})
	sqlCfg.TenantLoopbackAddr = s.AdvRPCAddr()

	var tenantIDSet, listenerReady sync.WaitGroup
	tenantIDSet.Add(1)
	listenerReady.Add(1)

	var timeTenantIDSet time.Time
	sqlCfg.DelayedSetTenantID = func(ctx context.Context) (roachpb.TenantID, error) {
		// Unblock the connect code bellow, so it can try to connect.
		listenerReady.Done()
		// Wait until getting a go ahead with setting the tenant id.
		tenantIDSet.Wait()
		return serverutils.TestTenantID(), nil
	}

	go func() {
		sw, err := NewSeparateProcessTenantServer(
			ctx,
			s.Stopper(),
			baseCfg,
			sqlCfg,
			roachpb.NewTenantNameContainer("delayed-tenant-set"),
		)
		require.NoError(t, err)
		require.NoError(t, sw.Start(ctx))
	}()

	listenerReady.Wait()
	// Try a connection.
	pgURL, cleanupFn, err := sqlutils.PGUrlE(
		baseCfg.SQLAdvertiseAddr, "testConn", url.User(username.RootUser))
	require.NoError(t, err)
	defer cleanupFn()
	go func() {
		time.Sleep(200 * time.Millisecond)
		timeTenantIDSet = timeutil.Now()
		tenantIDSet.Done()
	}()
	c, err := pgx.Connect(ctx, pgURL.String())
	durationFromTenantIDSetToConnect := timeutil.Since(timeTenantIDSet)
	require.NoError(t, err)
	defer func() { _ = c.Close(ctx) }()
	t.Logf("cold connect duration (from tenant id set to connect) %s", durationFromTenantIDSetToConnect)

	connectStart := timeutil.Now()
	c, err = pgx.Connect(ctx, pgURL.String())
	warmConnectDuration := timeutil.Since(connectStart)
	require.NoError(t, err)
	defer func() { _ = c.Close(ctx) }()
	t.Logf("warm connect duration %s", warmConnectDuration)
}
