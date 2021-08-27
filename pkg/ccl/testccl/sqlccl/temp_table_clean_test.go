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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestTenantTempTableCleanup(t *testing.T) {
	ctx := context.Background()
	t.Helper()
	settings := cluster.MakeTestingClusterSettings()
	sql.TempObjectCleanupInterval.Override(ctx, &settings.SV, time.Second*1)
	tc := serverutils.StartNewTestCluster(
		t, 3 /* numNodes */, base.TestClusterArgs{ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Settings: settings,
			}},
	)
	_, tenantPrimaryDB := serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
			Settings: settings})
	tenantSQL := sqlutils.MakeSQLRunner(tenantPrimaryDB)

	// Sanity: Temporary table clean up works fine, we are going
	// to start two nodes and close the connection on one of
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
			Settings: settings})
	log.TestingClearServerIdentifiers()
	serverutils.StartTenant(t, tc.Server(2),
		base.TestTenantArgs{
			Existing: true,
			TenantID: serverutils.TestTenantID(),
			Settings: settings})
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
	// Stop the connection the second node, we should
	// one be left with a single temp table.
	tenantSecondDB.Close()
	tenantSQL.CheckQueryResultsRetry(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
		})
	// Close the primary DB, there should be no databases
	// left.
	tenantPrimaryDB.Close()
	// Prevent a logging assertion that the server ID is initialized multiple times.
	log.TestingClearServerIdentifiers()
	// Once we restart the tenant, no sessions should exist
	// so all temporary tables should be cleaned up.
	_, tenantPrimaryDB = serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			Existing: true,
			TenantID: serverutils.TestTenantID(),
			Settings: settings})
	tenantSQL = sqlutils.MakeSQLRunner(tenantPrimaryDB)
	tenantSQL.CheckQueryResultsRetry(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{})
}
