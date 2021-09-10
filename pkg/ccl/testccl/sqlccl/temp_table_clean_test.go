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
	"reflect"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

	tc := serverutils.StartNewTestCluster(
		t, 3 /* numNodes */, base.TestClusterArgs{ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: settings,
			}},
	)
	log.TestingClearServerIdentifiers()
	tenantStoppers := []*stop.Stopper{stop.NewStopper(), stop.NewStopper()}
	_, tenantPrimaryDB := serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
			Settings: settings,
			Stopper:  tenantStoppers[0],
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
	tenantSecondSQL.CheckQueryResults(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
			{"temp_table2"},
		})
	// Stop the second node, we should one be left with a single temp table.
	tenantStoppers[1].Stop(ctx)
	// Session should expire in 5 seconds, but
	// we will wait for up to 15 seconds.
	{
		tEnd := timeutil.Now().Add(time.Second * 15)
		found := false
		lastResult := [][]string{}
		expectedResult := [][]string{
			{"temp_table"},
		}
		for tEnd.After(timeutil.Now()) {
			lastResult = tenantSQL.QueryStr(t, "SELECT table_name FROM [SHOW TABLES]")
			if reflect.DeepEqual(lastResult, expectedResult) {
				found = true
				break
			}
		}
		if !found {
			log.Fatalf(ctx, "temporary table was not correctly dropped, expected %v and got %v", expectedResult, lastResult)
		}
	}

	// Close the primary DB, there should be no temporary
	// tables left.
	tenantPrimaryDB.Close()
	// Prevent a logging assertion that the server ID is initialized multiple times.
	log.TestingClearServerIdentifiers()
	// Once we restart the tenant, no sessions should exist
	// so all temporary tables should be cleaned up.
	_, tenantPrimaryDB = serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			Existing: true,
			TenantID: serverutils.TestTenantID(),
			Settings: settings,
			Stopper:  tenantStoppers[0]})
	tenantSQL = sqlutils.MakeSQLRunner(tenantPrimaryDB)
	tenantSQL.CheckQueryResultsRetry(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{})
	tenantStoppers[0].Stop(ctx)
	tc.Stopper().Stop(ctx)
}
