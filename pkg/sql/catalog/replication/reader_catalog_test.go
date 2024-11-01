// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replication_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/replication"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestReaderCatalog sets up a reader catalog and confirms
// the contents of the catalog and system tables which are
// replicated.
func TestReaderCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuressWithIssue(t, 130901)

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
	var setupCompleteTS atomic.Value
	setupCompleteTS.Store(hlc.Timestamp{})

	advanceTS := func(now hlc.Timestamp) error {
		err = replication.SetupOrAdvanceStandbyReaderCatalog(ctx, serverutils.TestTenantID(), now, idb, destTenant.ClusterSettings())
		if err != nil {
			return err
		}
		setupCompleteTS.Store(ts.Clock().Now())
		return nil
	}

	require.NoError(t, advanceTS(now))
	// Connect only after the reader catalog is setup, so the connection
	// executor is aware.
	destConn := destTenant.SQLConn(t)
	destRunner := sqlutils.MakeSQLRunner(destConn)

	check := func(query string, isEqual bool) {
		lm := destTenant.LeaseManager().(*lease.Manager)
		testutils.SucceedsSoon(t, func() error {
			// Waiting for leases to catch up to when the setup was done.
			if lm.GetSafeReplicationTS().Less(setupCompleteTS.Load().(hlc.Timestamp)) {
				return errors.AssertionFailedf("waiting for descriptor close timestamp to catch up")
			}
			return nil
		})

		tx := srcRunner.Begin(t)
		_, err := tx.Exec(fmt.Sprintf("SET TRANSACTION AS OF SYSTEM TIME %s", now.AsOfSystemTime()))
		require.NoError(t, err)
		srcRows, err := tx.Query(query)
		require.NoError(t, err)
		srcRes, err := sqlutils.RowsToStrMatrix(srcRows)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		destRes := destRunner.QueryStr(t, query)
		if isEqual {
			require.Equal(t, srcRes, destRes)
		} else {
			require.NotEqualValues(t, srcRes, destRes)
		}
	}

	compareEqual := func(query string) {
		check(query, true)
	}

	// Validate tables and views match in the catalog reader
	compareEqual("SELECT * FROM t1 ORDER BY n")
	compareEqual("SELECT * FROM v1 ORDER BY 1")
	compareEqual("SELECT * FROM t2 ORDER BY n")

	// Validate that system tables are synced
	compareEqual("SELECT * FROM system.users")
	compareEqual("SELECT * FROM system.table_statistics")
	compareEqual("SELECT * FROM system.role_options")
	compareEqual("SELECT * FROM system.database_role_settings")

	// Validate that sequences can be selected.
	compareEqual("SELECT * FROM sq1")

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
	compareEqual("SELECT * FROM t1 ORDER BY n")
	compareEqual("SELECT * FROM v1 ORDER BY 1")
	compareEqual("SELECT * FROM system.users")
	compareEqual("SELECT * FROM system.table_statistics")
	compareEqual("SELECT * FROM system.role_options")
	compareEqual("SELECT * FROM system.database_role_settings")

	now = ts.Clock().Now()
	// Validate that system tables are not matching with new timestamps.
	check("SELECT * FROM t1 ORDER BY n", false)
	check("SELECT * FROM v1 ORDER BY 1", false)
	check("SELECT * FROM system.users", false)
	check("SELECT * FROM system.role_options", false)
	check("SELECT * FROM system.database_role_settings", false)

	// Move the timestamp up on the reader catalog, and confirm that everything matches.
	require.NoError(t, advanceTS(now))

	// Validate that system tables are synced and the new object shows.
	compareEqual("SELECT * FROM t1 ORDER BY n")
	compareEqual("SELECT * FROM v1 ORDER BY 1")
	compareEqual("SELECT * FROM system.users")
	compareEqual("SELECT * FROM system.table_statistics")
	compareEqual("SELECT * FROM system.role_options")
	compareEqual("SELECT * FROM system.database_role_settings")
	compareEqual("SELECT * FROM t4 ORDER BY n")

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
	now = ts.Clock().Now()
	require.NoError(t, advanceTS(now))
	compareEqual("SELECT * FROM t1 ORDER BY n")
	compareEqual("SELECT * FROM v1 ORDER BY 1")
	compareEqual("SELECT * FROM t2 ORDER BY j")

	// Validate that schema changes are blocked.
	destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "CREATE SCHEMA sc1")
	destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "CREATE DATABASE db2")
	destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "CREATE SEQUENCE sq4")
	destRunner.ExpectErr(t, "cannot execute CREATE VIEW in a read-only transaction", "CREATE VIEW v3 AS (SELECT n FROM t1)")
	destRunner.ExpectErr(t, "cannot execute CREATE TABLE in a read-only transaction", "CREATE TABLE t4 AS (SELECT n FROM t1)")
	destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "ALTER TABLE t1 ADD COLUMN abc int")
	destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "ALTER SEQUENCE sq1 RENAME TO sq4")
	destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "ALTER TYPE status ADD VALUE 'newval' ")

}

// TestReaderCatalogTSAdvance tests repeated advances of timestamp
// within a tight loop, which could lead to the lease manager
// mixing timestamps if we didn't use AOST queries.
func TestReaderCatalogTSAdvance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuressWithIssue(t, 130901)

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

	waitForRefresh := make(chan struct{})
	descriptorRefreshHookEnabled := atomic.Bool{}
	closeWaitForRefresh := func() {
		if descriptorRefreshHookEnabled.Load() {
			descriptorRefreshHookEnabled.Store(false)
			close(waitForRefresh)
		}
	}
	defer closeWaitForRefresh()
	destTestingKnobs := base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			TestingDescriptorRefreshedEvent: func(descriptor *descpb.Descriptor) {
				if !descriptorRefreshHookEnabled.Load() {
					return
				}
				<-waitForRefresh
			},
		},
	}
	destTenant, _, err := ts.StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantID:   serverutils.TestTenantID2(),
		TenantName: "dest",
		Knobs:      destTestingKnobs,
	})
	require.NoError(t, err)
	srcConn := srcTenant.SQLConn(t)
	srcRunner := sqlutils.MakeSQLRunner(srcConn)

	ddlToExec := []string{
		"CREATE SEQUENCE sq1;",
		"CREATE TYPE IF NOT EXISTS status AS ENUM ('open', 'closed', 'inactive');",
		"CREATE TABLE t1(j int default nextval('sq1'), val status);",
		"CREATE VIEW v1 AS (SELECT j from t1);",
		"CREATE TABLE t2(i int,  j int);",
	}
	for _, ddl := range ddlToExec {
		srcRunner.Exec(t, ddl)
	}

	now := ts.Clock().Now()
	idb := destTenant.InternalDB().(*sql.InternalDB)
	var setupCompleteTS atomic.Value
	setupCompleteTS.Store(hlc.Timestamp{})

	advanceTS := func(now hlc.Timestamp) error {
		err = replication.SetupOrAdvanceStandbyReaderCatalog(ctx, serverutils.TestTenantID(), now, idb, destTenant.ClusterSettings())
		if err != nil {
			return err
		}
		setupCompleteTS.Store(ts.Clock().Now())
		return nil
	}

	// Connect only after the reader catalog is setup, so the connection
	// executor is aware.
	destConn := destTenant.SQLConn(t)
	destRunner := sqlutils.MakeSQLRunner(destConn)

	check := func(query string, isEqual bool) {
		lm := destTenant.LeaseManager().(*lease.Manager)
		testutils.SucceedsSoon(t, func() error {
			// Waiting for leases to catch up to when the setup was done.
			if lm.GetSafeReplicationTS().Less(setupCompleteTS.Load().(hlc.Timestamp)) {
				return errors.AssertionFailedf("waiting for descriptor close timestamp to catch up")
			}
			return nil
		})
		tx := srcRunner.Begin(t)
		_, err := tx.Exec(fmt.Sprintf("SET TRANSACTION AS OF SYSTEM TIME %s", now.AsOfSystemTime()))
		require.NoError(t, err)
		srcRows, err := tx.Query(query)
		require.NoError(t, err)
		srcRes, err := sqlutils.RowsToStrMatrix(srcRows)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		destRes := destRunner.QueryStr(t, query)
		if isEqual {
			require.Equal(t, srcRes, destRes)
		} else {
			require.NotEqualValues(t, srcRes, destRes)
		}
	}

	compareEqual := func(query string) {
		check(query, true)
	}

	require.NoError(t, advanceTS(now))
	compareEqual("SELECT * FROM t1")

	var newTS hlc.Timestamp
	descriptorRefreshHookEnabled.Store(true)
	for _, useAOST := range []bool{false, true} {
		if useAOST {
			closeWaitForRefresh()
		}
		// When AOST is enabled then a fixed number of iterations sufficient. When
		// the AOST is disabled, then we will iterate until a reader timestamp error
		// is generated.
		errorDetected := false
		checkAOSTError := func(err error) {
			if useAOST || err == nil {
				require.NoError(t, err)
				return
			}
			errorDetected = errorDetected ||
				strings.Contains(err.Error(), "PCR reader timestamp has moved forward, existing descriptor")
		}
		// Validate multiple advances of the timestamp work concurrently with queries.
		// The tight loop below should relatively easily hit errors if all the timestamps
		// are not line up on the reader catalog.
		grp := ctxgroup.WithContext(ctx)
		require.NoError(t, err)
		iterationsDoneCh := make(chan struct{})
		grp.GoCtx(func(ctx context.Context) error {
			defer func() {
				close(iterationsDoneCh)
			}()
			const NumIterations = 16
			// Ensure the minimum iterations are met, and any expected errors
			// are observed before stopping TS advances.
			for iter := 0; iter < NumIterations; iter++ {
				if _, err := srcRunner.DB.ExecContext(ctx,
					"INSERT INTO t1(val, j) VALUES('open', $1);",
					iter); err != nil {
					return err
				}
				// Signal the next timestamp value.
				newTS = ts.Clock().Now()
				// Advanced the timestamp next.
				if err := advanceTS(newTS); err != nil {
					return err
				}
			}
			return nil
		})
		// Validates that the implicit txn and explicit txn's
		// can safely use fixed timestamps.
		if useAOST {
			destRunner.Exec(t, "SET bypass_pcr_reader_catalog_aost='off'")
		} else {
			destRunner.Exec(t, "SET bypass_pcr_reader_catalog_aost='on'")
		}
		iterationsDone := false
		for !iterationsDone {
			if !useAOST {
				select {
				case waitForRefresh <- struct{}{}:
				case <-iterationsDoneCh:
					iterationsDone = true
				}
			}
			select {
			case <-iterationsDoneCh:
				iterationsDone = true
			default:
				tx := destRunner.Begin(t)
				_, err := tx.Exec("SELECT * FROM t1")
				checkAOSTError(err)
				_, err = tx.Exec("SELECT * FROM v1")
				checkAOSTError(err)
				_, err = tx.Exec("SELECT * FROM t2")
				checkAOSTError(err)
				checkAOSTError(tx.Commit())

				_, err = destRunner.DB.ExecContext(ctx, "SELECT * FROM t1,v1, t2")
				checkAOSTError(err)
				_, err = destRunner.DB.ExecContext(ctx, "SELECT * FROM v1 ORDER BY 1")
				checkAOSTError(err)
				_, err = destRunner.DB.ExecContext(ctx, "SELECT * FROM t2 ORDER BY 1")
				checkAOSTError(err)
			}
		}

		// Finally ensure the queries actually match.
		require.NoError(t, grp.Wait())
		// Check if the error was detected.
		require.Equalf(t, !useAOST, errorDetected,
			"error was detected unexpectedly (AOST = %t on connection)", useAOST)
	}
	now = newTS
	compareEqual("SELECT * FROM t1 ORDER BY j")
	compareEqual("SELECT * FROM v1 ORDER BY 1")
	compareEqual("SELECT * FROM t2 ORDER BY j, i")

}

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}
