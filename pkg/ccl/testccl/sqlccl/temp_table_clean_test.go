// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func TestTenantTempTableCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	t.Helper()
	settings := cluster.MakeTestingClusterSettings()
	sql.TempObjectCleanupInterval.Override(ctx, &settings.SV, time.Second)
	sql.TempObjectWaitInterval.Override(ctx, &settings.SV, time.Second*0)
	// Set up sessions to expire within 5 seconds of a
	// nodes death.
	slinstance.DefaultTTL.Override(ctx, &settings.SV, 5*time.Second)
	slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, time.Second)
	// Channel that gets sent on each time a clean-up occurs.
	tempCleanupMutex := syncutil.Mutex{}
	tempCleanupPauseEnabled := true
	tempCleanUpFinishedCh := make(chan struct{})
	tenantTempKnobSettings := base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			OnTempObjectsCleanupDone: func() {
				tempCleanupMutex.Lock()
				if !tempCleanupPauseEnabled {
					tempCleanupMutex.Unlock()
					return
				}
				tempCleanupMutex.Unlock()
				// Inform that a cleanup has occurred
				<-tempCleanUpFinishedCh
			},
		},
	}
	// Waits for temporary object cleanup to occur, we
	// intentionally wait two cycles. Just in case the object
	// clean up hasn't occurred.
	waitForTempObjectCleanup := func() {
		tempCleanUpFinishedCh <- struct{}{}
		tempCleanUpFinishedCh <- struct{}{}
	}
	tc := serverutils.StartNewTestCluster(
		t, 3 /* numNodes */, base.TestClusterArgs{ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: settings,
			},
		},
	)
	log.TestingClearServerIdentifiers()
	tenantStoppers := []*stop.Stopper{stop.NewStopper(), stop.NewStopper()}
	_, tenantPrimaryDB := serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			TenantID:     serverutils.TestTenantID(),
			Settings:     settings,
			TestingKnobs: tenantTempKnobSettings,
			Stopper:      tenantStoppers[0],
		})
	tenantSQL := sqlutils.MakeSQLRunner(tenantPrimaryDB)

	// Sanity: Temporary table clean up works fine, we are going
	// to start two nodes and close the kill one of
	// them.
	tenantSQL.Exec(t, "SET experimental_enable_temp_tables = 'on'")
	tenantSQL.Exec(t, "set cluster setting sql.temp_object_cleaner.cleanup_interval='1 seconds'")
	tenantSQL.Exec(t, "CREATE TEMP TABLE temp_table (x INT PRIMARY KEY, y INT);")
	// Prevent a logging assertion that the server ID is initialized multiple times.
	log.TestingClearServerIdentifiers()
	_, tenantSecondDB := serverutils.StartTenant(t, tc.Server(1),
		base.TestTenantArgs{
			Existing: true,
			TenantID: serverutils.TestTenantID(),
			Settings: settings,
			Stopper:  tenantStoppers[1],
		})
	log.TestingClearServerIdentifiers()
	tenantSecondSQL := sqlutils.MakeSQLRunner(tenantSecondDB)
	tenantSecondSQL.CheckQueryResults(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
		})
	tenantSecondSQL.Exec(t, "SET experimental_enable_temp_tables = 'on'")
	tenantSecondSQL.Exec(t, "CREATE TEMP TABLE temp_table2 (x INT PRIMARY KEY, y INT);")
	waitForTempObjectCleanup()
	tenantSecondSQL.CheckQueryResults(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
			{"temp_table2"},
		})
	// Stop the second node, we should one be left with a single temp table.
	tenantStoppers[1].Stop(ctx)
	// Session should expire in 5 seconds, wait for
	// two clean up cycles just in case, so that we have
	// stable timing.
	waitForTempObjectCleanup()
	tenantSQL.CheckQueryResults(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
		})
	// Disable our hook to allow the database to be
	// brought down.
	tempCleanupMutex.Lock()
	tempCleanupPauseEnabled = false
	close(tempCleanUpFinishedCh)
	tempCleanupMutex.Unlock()
	// Close the primary DB, there should be no temporary
	// tables left.
	tenantStoppers[0].Stop(ctx)
	// Enable our hook to allow the database to be
	// brought up.
	tempCleanupMutex.Lock()
	tempCleanupPauseEnabled = true
	tempCleanUpFinishedCh = make(chan struct{})
	tempCleanupMutex.Unlock()
	// Prevent a logging assertion that the server ID is initialized multiple times.
	log.TestingClearServerIdentifiers()
	// Once we restart the tenant, no sessions should exist
	// so all temporary tables should be cleaned up.
	tenantStoppers[0] = stop.NewStopper()
	_, tenantPrimaryDB = serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			Existing:     true,
			TenantID:     serverutils.TestTenantID(),
			Settings:     settings,
			TestingKnobs: tenantTempKnobSettings,
			Stopper:      tenantStoppers[0]})
	tenantSQL = sqlutils.MakeSQLRunner(tenantPrimaryDB)
	waitForTempObjectCleanup()
	tenantSQL.CheckQueryResultsRetry(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{})
	// Disable our hook to allow the database to be
	// brought stopped.
	tempCleanupMutex.Lock()
	tempCleanupPauseEnabled = false
	close(tempCleanUpFinishedCh)
	<-tempCleanUpFinishedCh
	tempCleanupMutex.Unlock()
	// Bring down the tenants.
	tenantStoppers[0].Stop(ctx)
	tc.Stopper().Stop(ctx)
}
