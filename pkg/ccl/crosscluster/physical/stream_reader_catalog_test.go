// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package physical

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestSetupReaderCatalog validates creating a reader catalog
// using crdb_internal.setup_read_from_standby.
func TestSetupReaderCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Insecure:          true,
	},
	)
	defer ts.Stopper().Stop(ctx)

	// Create the src tenant and insert data into it.
	srcID, err := roachpb.MakeTenantID(4)
	require.NoError(t, err)
	srcStopper := stop.NewStopper()
	srcTenant, err := ts.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantName:    "src",
		TenantID:      srcID,
		Stopper:       srcStopper,
		ForceInsecure: true,
	})
	require.NoError(t, err)
	srcConn := srcTenant.SQLConn(t)
	srcRunner := sqlutils.MakeSQLRunner(srcConn)

	stmts := []string{
		"CREATE USER roacher WITH CREATEROLE",
		"GRANT ADMIN TO roacher",
		"ALTER USER roacher SET timezone='America/New_York'",
		"CREATE DATABASE db1",
		"CREATE SCHEMA db1.sc1",
		"CREATE SEQUENCE sq1",
		"CREATE TYPE IF NOT EXISTS status AS ENUM ('open', 'closed', 'inactive')",
		"CREATE TABLE t1(n int default nextval('sq1'), val status)",
		"INSERT INTO t1(val) VALUES('open')",
		"INSERT INTO t1(val) VALUES('closed')",
		"INSERT INTO t1(val) VALUES('inactive')",
		"CREATE VIEW v1 AS (SELECT n from t1)",
	}

	for _, stmt := range stmts {
		srcRunner.Exec(t, stmt)
	}
	defer srcTenant.AppStopper().Stop(ctx)

	// Create the tenant to replicate into.
	destName := roachpb.TenantName("dest")
	createDest := func() (serverutils.ApplicationLayerInterface, *stop.Stopper) {
		destID, err := roachpb.MakeTenantID(5)
		require.NoError(t, err)
		destStopper := stop.NewStopper()
		destStopperTenant, err := ts.TenantController().StartTenant(ctx, base.TestTenantArgs{
			TenantName:    destName,
			TenantID:      destID,
			Stopper:       destStopper,
			ForceInsecure: true,
		})
		require.NoError(t, err)
		return destStopperTenant, destStopper
	}
	_, destStopper := createDest()

	systemConn := ts.SQLConn(t)
	systemRunner := sqlutils.MakeSQLRunner(systemConn)

	// Run multiple iterations as well to ensure descriptors can
	// be updated, with the virtual cluster offline.
	for i := 0; i < 2; i++ {
		destStopper.Stop(ctx)
		systemRunner.Exec(t, "ALTER VIRTUAL CLUSTER dest STOP SERVICE ")
		destName = ""
		// Setup the reader catalog.
		systemTenant := ts.SQLConn(t)
		now := ts.Clock().Now()
		_, err = systemTenant.Exec("SELECT * FROM crdb_internal.setup_read_from_standby('src', 'dest', $1);", now.WallTime)
		require.NoError(t, err)

		// Confirm that data is readable.
		dest, destStopper := createDest()
		defer destStopper.Stop(ctx)

		destConn := dest.SQLConn(t)
		destRunner := sqlutils.MakeSQLRunner(destConn)

		// compareQueries executes the same query on both catalogs
		// and expects the same results.
		compareQueries := func(query string) {
			expectedResults := srcRunner.QueryStr(t, fmt.Sprintf("SELECT * FROM (%s) AS OF SYSTEM TIME %s", query, now.AsOfSystemTime()))
			destRunner.CheckQueryResults(t, query, expectedResults)
		}

		// Validate basic queries execute correctly, and we can
		// read data within tables.
		compareQueries("SELECT * FROM t1 ORDER BY n")
		compareQueries("SELECT * FROM v1 ORDER BY 1")

		// Validate reading from sequences works.
		compareQueries("SELECT * FROM sq1")

		// Confirm that sequence operations are blocked.
		destRunner.ExpectErr(t, "cannot execute nextval\\(\\) in a read-only transaction", "SELECT nextval('sq1')")
		destRunner.ExpectErr(t, "cannot execute setval\\(\\) in a read-only transaction", "SELECT setval('sq1', 32)")

		// Confirm that users and roles tables are replicated.
		compareQueries("SELECT * FROM system.users")
		compareQueries("SELECT * FROM system.role_members")
		compareQueries("SELECT * FROM system.role_options")
		compareQueries("SELECT * FROM system.database_role_settings")

		// Confirm that table_statistics are replicated.
		compareQueries("SELECT * FROM system.table_statistics")

		// Next modify the destination again.
		newSQLForSrc := []string{
			"INSERT INTO t1(val) VALUES('open')",
			"INSERT INTO t1(val) VALUES('closed')",
			"INSERT INTO t1(val) VALUES('inactive')",
			fmt.Sprintf("CREATE TABLE t_new_%d(n int PRIMARY KEY)", i),
			fmt.Sprintf("CREATE USER roacher%d WITH CREATEROLE", i),
			fmt.Sprintf("GRANT ADMIN TO roacher%d", i),
			fmt.Sprintf("ALTER USER roacher%d SET timezone='America/New_York'", i),
			"SELECT nextval('sq1')",
		}
		for _, dml := range newSQLForSrc {
			srcRunner.Exec(t, dml)
		}
	}
}
