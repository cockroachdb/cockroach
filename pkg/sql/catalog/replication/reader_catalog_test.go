// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package replication_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/replication"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestReaderCatalog(t *testing.T) {
	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer ts.Stop(ctx)
	srcTenant, _, err := ts.StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantID:   serverutils.TestTenantID(),
		TenantName: "src",
	})
	require.NoError(t, err)
	destTenant, _, err := ts.StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantID:   serverutils.TestTenantID2(),
		TenantName: "dest",
	})
	require.NoError(t, err)
	srcConn := srcTenant.SQLConn(t)
	srcRunner := sqlutils.MakeSQLRunner(srcConn)
	destConn := destTenant.SQLConn(t)

	ddlToExec := []string{
		"CREATE USER roacher WITH CREATEROLE;",
		"GRANT ADMIN TO roacher;",
		"ALTER USER roacher SET timezone='America/New_York';",
		"CREATE DATABASE db1;",
		"CREATE SCHEMA db1.sc1;",
		"CREATE SEQUENCE sq1;",
		"CREATE TYPE IF NOT EXISTS status AS ENUM ('open', 'closed', 'inactive');",
		"CREATE TABLE t1(n int default nextval('sq1'), val status);",
		"INSERT INTO t1(val) VALUES('open');",
		"INSERT INTO t1(val) VALUES('closed');",
		"INSERT INTO t1(val) VALUES('inactive');",
		"CREATE VIEW v1 AS (SELECT n from t1);",
		"CREATE TABLE t2(n int);",
	}
	for _, ddl := range ddlToExec {
		srcRunner.Exec(t, ddl)
	}

	now := ts.Clock().Now()
	idb := destTenant.InternalDB().(*sql.InternalDB)
	require.NoError(t, replication.SetupOrAdvanceStandbyReaderCatalog(ctx, serverutils.TestTenantID(), now, idb, destTenant.ClusterSettings()))

	destRunner := sqlutils.MakeSQLRunner(destConn)

	compareConnAt := func(query string, now hlc.Timestamp, isEqual bool) {
		srcRes := srcRunner.QueryStr(t, fmt.Sprintf("SELECT * FROM (%s) AS OF SYSTEM TIME %s", query, now.AsOfSystemTime()))
		destRes := destRunner.QueryStr(t, query)
		if isEqual {
			require.Equal(t, srcRes, destRes)
		} else {
			require.NotEqualValues(t, srcRes, destRes)
		}
	}

	compareConn := func(query string) {
		compareConnAt(query, now, true)
	}

	// Validate tables and views match in the catalog reader
	compareConn("SELECT * FROM t1 ORDER BY n")
	compareConn("SELECT * FROM v1 ORDER BY 1")
	compareConn("SELECT * FROM t2 ORDER BY n")

	// Validate that system tables are synced
	compareConn("SELECT * FROM system.users")
	compareConn("SELECT * FROM system.table_statistics")
	compareConn("SELECT * FROM system.role_options")
	compareConn("SELECT * FROM system.database_role_settings")

	// Validate that sequences can be selected.
	compareConn("SELECT * FROM sq1")

	// Modify the schema next in the src tenant.
	ddlToExec = []string{
		"INSERT INTO t1(val) VALUES('open');",
		"INSERT INTO t1(val) VALUES('closed');",
		"INSERT INTO t1(val) VALUES('inactive');",
		"CREATE USER roacher2 WITH CREATEROLE;",
		"GRANT ADMIN TO roacher2;",
		"ALTER USER roacher2 SET timezone='America/New_York';",
		"CREATE TABLE t4(n int)",
		"INSERT INTO t4 VALUES (32)",
	}
	for _, ddl := range ddlToExec {
		srcRunner.Exec(t, ddl)
	}

	// Validate that system tables are synced at the old timestamp.
	compareConn("SELECT * FROM t1 ORDER BY n")
	compareConn("SELECT * FROM v1 ORDER BY 1")
	compareConn("SELECT * FROM system.users")
	compareConn("SELECT * FROM system.table_statistics")
	compareConn("SELECT * FROM system.role_options")
	compareConn("SELECT * FROM system.database_role_settings")

	newTS := ts.Clock().Now()
	// Validate that system tables are not matching with new timestamps.
	compareConnAt("SELECT * FROM t1 ORDER BY n", newTS, false)
	compareConnAt("SELECT * FROM v1 ORDER BY 1", newTS, false)
	compareConnAt("SELECT * FROM system.users", newTS, false)
	compareConnAt("SELECT * FROM system.role_options", newTS, false)
	compareConnAt("SELECT * FROM system.database_role_settings", newTS, false)

	// Move the timestamp up on the reader catalog, and confirm that everything matches
	now = newTS
	require.NoError(t, replication.SetupOrAdvanceStandbyReaderCatalog(ctx, serverutils.TestTenantID(), now, idb, destTenant.ClusterSettings()))

	// Validate that system tables are synced and the new object shows.
	compareConn("SELECT * FROM t1 ORDER BY n")
	compareConn("SELECT * FROM v1 ORDER BY 1")
	compareConn("SELECT * FROM system.users")
	compareConn("SELECT * FROM system.table_statistics")
	compareConn("SELECT * FROM system.role_options")
	compareConn("SELECT * FROM system.database_role_settings")
	compareConn("SELECT * FROM t4 ORDER BY n")

	// Validate that sequence operations are blocked.
	destRunner.ExpectErr(t, "cannot execute nextval\\(\\) in a read-only transaction", "SELECT nextval('sq1')")
	destRunner.ExpectErr(t, "cannot execute setval\\(\\) in a read-only transaction", "SELECT setval('sq1', 32)")
	// Manipulate the schema first.
	ddlToExec = []string{
		"ALTER TABLE t1 ADD COLUMN j int default 32",
		"INSERT INTO t1(val, j) VALUES('open', 1);",
		"INSERT INTO t1(val, j) VALUES('closed', 2);",
		"INSERT INTO t1(val, j) VALUES('inactive', 3);",
		"DROP TABLE t2;",
		"CREATE TABLE t2(j int, i int);",
	}
	for _, ddl := range ddlToExec {
		_, err = srcConn.Exec(ddl)
		require.NoError(t, err)
	}
	// Confirm that everything matches at the old timestamp.
	compareConn("SELECT * FROM t1 ORDER BY n")
	compareConn("SELECT * FROM v1 ORDER BY 1")
	compareConn("SELECT * FROM t2 ORDER BY n")

	// Advance the timestamp.
	now = ts.Clock().Now()
	require.NoError(t, replication.SetupOrAdvanceStandbyReaderCatalog(ctx, serverutils.TestTenantID(), now, idb, destTenant.ClusterSettings()))

	// Confirm everything matches again.
	compareConn("SELECT * FROM t1 ORDER BY n")
	compareConn("SELECT * FROM v1 ORDER BY 1")
	compareConn("SELECT * FROM t2 ORDER BY j")
}

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}
