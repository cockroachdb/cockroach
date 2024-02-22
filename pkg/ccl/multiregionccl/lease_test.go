// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestLeaseQueriesWithMR(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Enable settings required for configuring a tenant's system database as multi-region.
	makeSettings := func() *cluster.Settings {
		cs := cluster.MakeTestingClusterSettings()
		instancestorage.ReclaimLoopInterval.Override(ctx, &cs.SV, 150*time.Millisecond)
		return cs
	}

	cluster, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3,
		base.TestingKnobs{},
		multiregionccltestutils.WithSettings(makeSettings()))
	defer cleanup()

	id, err := roachpb.MakeTenantID(11)
	require.NoError(t, err)

	var tenantServers []serverutils.ApplicationLayerInterface
	var tenantSQL []*gosql.DB
	for idx := range cluster.Servers {
		tenantArgs := base.TestTenantArgs{
			Settings: makeSettings(),
			TenantID: id,
			Locality: cluster.Servers[idx].Locality(),
		}
		s, sql := serverutils.StartTenant(t, cluster.Servers[0], tenantArgs)
		tenantServers = append(tenantServers, s)
		tenantSQL = append(tenantSQL, sql)
	}

	tDB := sqlutils.MakeSQLRunner(tenantSQL[0])

	tDB.Exec(t, `ALTER DATABASE system SET PRIMARY REGION "us-east1"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east2"`)
	tDB.Exec(t, `ALTER DATABASE system ADD REGION "us-east3"`)

	tDB.Exec(t, "CREATE TABLE t1(n int)")

	// Intentionally use a short lease duration.
	expectedWaitTime := time.Second * 15
	for _, ts := range tenantServers {
		lease.LeaseDuration.Override(ctx, &ts.ClusterSettings().SV, expectedWaitTime)
	}
	descIDRow := tDB.QueryRow(t, "SELECT 't1'::REGCLASS::INT")
	var descID int
	descIDRow.Scan(&descID)
	grp := ctxgroup.WithContext(ctx)

	startWaiters := make(chan struct{})
	schemaChangeDone := make(chan struct{})

	// Start a transaction and ensure later lease operations below hit
	// some type of wait.
	grp.GoCtx(func(ctx context.Context) error {
		tx, err := tenantSQL[1].Begin()
		if err != nil {
			return err
		}
		// Start txn to hold the lease.
		startTime := timeutil.Now()
		_, err = tx.Exec("SELECT * FROM t1")
		if err != nil {
			return err
		}
		close(startWaiters)
		<-schemaChangeDone
		if timeutil.Since(startTime) < expectedWaitTime {
			return errors.AssertionFailedf("no lease wait was detected")
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		_, err = tenantSQL[1].Exec("DROP TABLE t1")
		return err
	})

	// Waits for the two version invariant inside the job.
	grp.GoCtx(func(ctx context.Context) error {
		<-startWaiters
		_, err = tenantSQL[2].Exec("ALTER TABLE t1 ADD COLUMN j INT")
		if err != nil {
			return err
		}
		close(schemaChangeDone)
		return nil
	})

	// Waits for one version of the descriptor to exist.
	grp.GoCtx(func(ctx context.Context) error {
		<-startWaiters
		// Wait for there to be a single version.
		lm := tenantServers[0].LeaseManager().(*lease.Manager)
		_, err := lm.WaitForOneVersion(ctx, descpb.ID(descID), retry.Options{})
		return err
	})

	// Waits for no version of the descriptor to exist.
	grp.GoCtx(func(ctx context.Context) error {
		<-startWaiters
		lm := tenantServers[2].LeaseManager().(*lease.Manager)
		return lm.WaitForNoVersion(ctx, descpb.ID(descID), retry.Options{})
	})

	require.NoError(t, grp.Wait())
}
