// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replication_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type readerCatalogTest struct {
	ts         serverutils.TestServerInterface
	srcTenant  serverutils.ApplicationLayerInterface
	destTenant serverutils.ApplicationLayerInterface

	srcRunner  *sqlutils.SQLRunner
	destRunner *sqlutils.SQLRunner
	destURL    url.URL

	// setupCompleteTS tracks the time when the last timestamp advance
	// was completed.
	setupCompleteTS atomic.Value
	// srcAOST timestmap used for reading from the src tenant for
	// the comparison operatons below.
	srcAOST hlc.Timestamp
}

func newReaderCatalogTest(
	t *testing.T,
	ctx context.Context,
	destTestingKnobs base.TestingKnobs,
	destSettings *cluster.Settings,
) (r *readerCatalogTest, cleanup func()) {
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	srcTenant, _, err := ts.StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantID:   serverutils.TestTenantID(),
		TenantName: "src",
	})
	require.NoError(t, err)
	destTenant, _, err := ts.StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantID:   serverutils.TestTenantID2(),
		TenantName: "dest",
		Knobs:      destTestingKnobs,
		Settings:   destSettings,
	})
	require.NoError(t, err)
	srcRunner := sqlutils.MakeSQLRunner(srcTenant.SQLConn(t))
	destRunner := sqlutils.MakeSQLRunner(destTenant.SQLConn(t))

	r = &readerCatalogTest{
		ts:         ts,
		srcTenant:  srcTenant,
		destTenant: destTenant,
		srcRunner:  srcRunner,
		destRunner: destRunner,
	}
	r.setupCompleteTS.Store(hlc.Timestamp{})

	// Create a reader user.
	srcRunner.Exec(t, `
CREATE USER reader password 'reader';
GRANT ADMIN TO reader;
 `)

	destURL, destURLCleanup := r.destTenant.PGUrl(t, serverutils.UserPassword("reader", "reader"), serverutils.ClientCerts(false))
	r.destURL = destURL

	return r, func() {
		destURLCleanup()
		r.ts.Stop(ctx)
	}
}

// updateSrcAOST advances the AOST used by the comparison function, when reading
// from the source tenant.
func (r *readerCatalogTest) updateSrcAOST(now hlc.Timestamp) {
	r.srcAOST = now
}

// advanceTS advances the timestamp on the destination tenant, and can optionally
// advance the AOST for reading from the source tenant for comparisons.
func (r *readerCatalogTest) advanceTS(
	ctx context.Context, now hlc.Timestamp, refreshAOST bool,
) error {
	idb := r.destTenant.InternalDB().(*sql.InternalDB)
	err := replication.SetupOrAdvanceStandbyReaderCatalog(ctx, serverutils.TestTenantID(), now, idb, r.destTenant.ClusterSettings())
	if err != nil {
		return err
	}
	r.setupCompleteTS.Store(r.ts.Clock().Now())
	if refreshAOST {
		r.srcAOST = now
	}
	return nil
}

// compareEqual compares the source and destination tenant for equality. Reading
// the destination tenant at now and the source tenant srcAOST.
func (r *readerCatalogTest) compareEqual(t *testing.T, query string) {
	r.check(t, query, true)
}

// check the source and destination tenant for equality.
func (r *readerCatalogTest) check(t *testing.T, query string, isEqual bool) {
	lm := r.destTenant.LeaseManager().(*lease.Manager)
	testutils.SucceedsSoon(t, func() error {
		// Waiting for leases to catch up to when the setup was done.
		if lm.GetSafeReplicationTS().Less(r.setupCompleteTS.Load().(hlc.Timestamp)) {
			return errors.AssertionFailedf("waiting for descriptor close timestamp to catch up")
		}
		return nil
	})

	tx := r.srcRunner.Begin(t)
	_, err := tx.Exec(fmt.Sprintf("SET TRANSACTION AS OF SYSTEM TIME %s", r.srcAOST.AsOfSystemTime()))
	require.NoError(t, err)
	srcRows, err := tx.Query(query)
	require.NoError(t, err)
	srcRes, err := sqlutils.RowsToStrMatrix(srcRows)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	destRes := r.destRunner.QueryStr(t, query)
	if isEqual {
		require.Equal(t, srcRes, destRes)
	} else {
		require.NotEqualValues(t, srcRes, destRes)
	}

	// Sanity: Execute the same query as prepared statement inside the reader
	// catalog .
	ctx := context.Background()
	destPgxConn, err := pgx.Connect(ctx, r.destURL.String())
	require.NoError(t, err)
	_, err = destPgxConn.Prepare(ctx, query, query)
	require.NoError(t, err)
	rows, err := destPgxConn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	require.NoError(t, destPgxConn.Close(ctx))
}

// TestReaderCatalog sets up a reader catalog and confirms
// the contents of the catalog and system tables which are
// replicated.
func TestReaderCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuressWithIssue(t, 130901)

	ctx := context.Background()
	r, cleanup := newReaderCatalogTest(t, ctx, base.TestingKnobs{}, nil)
	defer cleanup()

	r.srcRunner.Exec(t, `
CREATE USER roacher WITH CREATEROLE;
GRANT ADMIN TO roacher;
ALTER USER roacher SET timezone='America/New_York';
CREATE DATABASE db1;
CREATE SCHEMA db1.sc1;
CREATE SEQUENCE sq1;
CREATE TYPE IF NOT EXISTS status AS ENUM ('open', 'closed', 'inactive');
CREATE TABLE t1(n int default nextval('sq1'), val status);
INSERT INTO t1(val) VALUES('open');
INSERT INTO t1(val) VALUES('closed');
INSERT INTO t1(val) VALUES('inactive');
CREATE VIEW v1 AS (SELECT n from t1);
CREATE TABLE t2(n int);
`)
	require.NoError(t, r.advanceTS(ctx, r.ts.Clock().Now(), true))

	// Validate tables and views match in the catalog reader
	r.compareEqual(t, "SELECT * FROM t1 ORDER BY n")
	r.compareEqual(t, "SELECT * FROM v1 ORDER BY 1")
	r.compareEqual(t, "SELECT * FROM t2 ORDER BY n")

	// Validate that system tables are synced
	r.compareEqual(t, "SELECT * FROM system.users")
	r.compareEqual(t, "SELECT * FROM system.table_statistics")
	r.compareEqual(t, "SELECT * FROM system.role_options")
	r.compareEqual(t, "SELECT * FROM system.database_role_settings")

	// Validate that sequences can be selected.
	r.compareEqual(t, "SELECT * FROM sq1")

	// Modify the schema next in the src tenant.
	ddlToExec := []string{
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
		r.srcRunner.Exec(t, ddl)
	}

	// Validate that system tables are synced at the old timestamp.
	r.compareEqual(t, "SELECT * FROM t1 ORDER BY n")
	r.compareEqual(t, "SELECT * FROM v1 ORDER BY 1")
	r.compareEqual(t, "SELECT * FROM system.users")
	r.compareEqual(t, "SELECT * FROM system.table_statistics")
	r.compareEqual(t, "SELECT * FROM system.role_options")
	r.compareEqual(t, "SELECT * FROM system.database_role_settings")

	// Move the src timestamp into the future when comparing
	// values.
	r.updateSrcAOST(r.ts.Clock().Now())
	// Validate that system tables are not matching with new timestamps.
	r.check(t, "SELECT * FROM t1 ORDER BY n", false)
	r.check(t, "SELECT * FROM v1 ORDER BY 1", false)
	r.check(t, "SELECT * FROM system.users", false)
	r.check(t, "SELECT * FROM system.role_options", false)
	r.check(t, "SELECT * FROM system.database_role_settings", false)

	// Move the timestamp up on the reader catalog, and confirm that everything matches.
	require.NoError(t, r.advanceTS(ctx, r.srcAOST, true))

	// Validate that system tables are synced and the new object shows.
	r.compareEqual(t, "SELECT * FROM t1 ORDER BY n")
	r.compareEqual(t, "SELECT * FROM v1 ORDER BY 1")
	r.compareEqual(t, "SELECT * FROM system.users")
	r.compareEqual(t, "SELECT * FROM system.table_statistics")
	r.compareEqual(t, "SELECT * FROM system.role_options")
	r.compareEqual(t, "SELECT * FROM system.database_role_settings")
	r.compareEqual(t, "SELECT * FROM t4 ORDER BY n")

	// Validate that sequence operations are blocked.
	r.destRunner.ExpectErr(t, "cannot execute nextval\\(\\) in a read-only transaction", "SELECT nextval('sq1')")
	r.destRunner.ExpectErr(t, "cannot execute setval\\(\\) in a read-only transaction", "SELECT setval('sq1', 32)")
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
		r.srcRunner.Exec(t, ddl)
	}
	// Confirm that everything matches at the old timestamp.
	require.NoError(t, r.advanceTS(ctx, r.ts.Clock().Now(), true))
	r.compareEqual(t, "SELECT * FROM t1 ORDER BY n")
	r.compareEqual(t, "SELECT * FROM v1 ORDER BY 1")
	r.compareEqual(t, "SELECT * FROM t2 ORDER BY j")

	// Validate that schema changes are blocked.
	r.destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "CREATE SCHEMA sc1")
	r.destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "CREATE DATABASE db2")
	r.destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "CREATE SEQUENCE sq4")
	r.destRunner.ExpectErr(t, "cannot execute CREATE VIEW in a read-only transaction", "CREATE VIEW v3 AS (SELECT n FROM t1)")
	r.destRunner.ExpectErr(t, "cannot execute CREATE TABLE in a read-only transaction", "CREATE TABLE t4 AS (SELECT n FROM t1)")
	r.destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "ALTER TABLE t1 ADD COLUMN abc int")
	r.destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "ALTER SEQUENCE sq1 RENAME TO sq4")
	r.destRunner.ExpectErr(t, "schema changes are not allowed on a reader catalog", "ALTER TYPE status ADD VALUE 'newval' ")

}

// TestReaderCatalogTSAdvance tests repeated advances of timestamp
// within a tight loop, which could lead to the lease manager
// mixing timestamps if we didn't use AOST queries.
func TestReaderCatalogTSAdvance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuressWithIssue(t, 130901)

	ctx := context.Background()

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
				if !descriptorRefreshHookEnabled.Swap(false) {
					return
				}
				<-waitForRefresh
			},
		},
	}

	r, cleanup := newReaderCatalogTest(t, ctx, destTestingKnobs, nil)
	defer cleanup()

	ddlToExec := []string{
		"CREATE USER bob password 'bob'",
		"GRANT ADMIN TO bob;",
		"CREATE SEQUENCE sq1;",
		"CREATE TYPE IF NOT EXISTS status AS ENUM ('open', 'closed', 'inactive');",
		"CREATE TABLE t1(j int default nextval('sq1'), val status);",
		"CREATE VIEW v1 AS (SELECT j from t1);",
		"CREATE TABLE t2(i int,  j int);",
	}
	for _, ddl := range ddlToExec {
		r.srcRunner.Exec(t, ddl)
	}

	require.NoError(t, r.advanceTS(ctx, r.ts.Clock().Now(), true))
	r.compareEqual(t, "SELECT * FROM t1")

	var newTS hlc.Timestamp
	descriptorRefreshHookEnabled.Store(true)
	existingPgxConn, err := pgx.Connect(ctx, r.destURL.String())
	require.NoError(t, err)
	_, err = existingPgxConn.Prepare(ctx, "basic select", "SELECT * FROM t1, v1, t2")
	require.NoError(t, err)
	for _, useAOST := range []bool{false, true} {
		if useAOST {
			closeWaitForRefresh()
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
				if _, err := r.srcRunner.DB.ExecContext(ctx,
					"INSERT INTO t1(val, j) VALUES('open', $1);",
					iter); err != nil {
					return err
				}
				// Signal the next timestamp value.
				newTS = r.ts.Clock().Now()
				// Advanced the timestamp next.
				if err := r.advanceTS(ctx, newTS, false); err != nil {
					return err
				}
			}
			return nil
		})
		// Validates that the implicit txn and explicit txn's
		// can safely use fixed timestamps.
		if useAOST {
			r.destRunner.Exec(t, "SET bypass_pcr_reader_catalog_aost='off'")
		} else {
			r.destRunner.Exec(t, "SET bypass_pcr_reader_catalog_aost='on'")
		}
		iterationsDone := false
		uniqueIdx := 0
		for !iterationsDone {
			uniqueIdx++
			if !useAOST {
				// Toggle the block on each iteration, so there is some risk
				// of not all descriptor updates being updated.
				descriptorRefreshHookEnabled.Swap(true)
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
				// Validate both prepares and opening connections while the timestamp
				// advances.
				validateNewConnsAndPrepare := func() {
					rows, err := existingPgxConn.Query(ctx, "SELECT * FROM t1, v1, t2")
					require.NoError(t, err)
					rows.Close()
					uniqueQuery := fmt.Sprintf("SELECT a.j + %d FROM t1 as a, v1 as b, t2 as c ", uniqueIdx)
					_, err = existingPgxConn.Prepare(ctx, fmt.Sprintf("q%d", uniqueIdx), uniqueQuery)
					require.NoError(t, err)
					rows, err = existingPgxConn.Query(ctx, uniqueQuery)
					require.NoError(t, err)
					rows.Close()
					// Open new connections.
					newPgxConn, err := pgx.Connect(ctx, r.destURL.String())
					require.NoError(t, err)
					defer func() {
						require.NoError(t, newPgxConn.Close(ctx))
					}()
					_, err = newPgxConn.Prepare(ctx, "basic select", "SELECT * FROM t1, v1, t2")
					require.NoError(t, err)
					rows, err = newPgxConn.Query(ctx, "SELECT * FROM t1, v1, t2")
					require.NoError(t, err)
					rows.Close()
				}
				validateNewConnsAndPrepare()

				// Only use txn's with AOST, which should never hit retryable errors.
				// Automatic retries are enabled for PCR errors, but they can still
				// happen in a txn. When AOST is off don't try the txn.
				if useAOST {
					tx := r.destRunner.Begin(t)
					_, err = tx.Exec("SELECT * FROM t1")
					require.NoError(t, err)
					_, err = tx.Exec("SELECT * FROM v1")
					require.NoError(t, err)
					_, err = tx.Exec("SELECT * FROM t2")
					require.NoError(t, err)
					require.NoError(t, tx.Commit())
				}

				// Validate implicit txn's never hit any error.
				_, err = r.destRunner.DB.ExecContext(ctx, "SELECT * FROM t1,v1,t2")
				require.NoError(t, err)
			}
		}
		// Finally ensure the queries actually match.
		require.NoError(t, grp.Wait())
	}
	require.NoError(t, existingPgxConn.Close(ctx))
	r.updateSrcAOST(newTS)
	r.compareEqual(t, "SELECT * FROM t1 ORDER BY j")
	r.compareEqual(t, "SELECT * FROM v1 ORDER BY 1")
	r.compareEqual(t, "SELECT * FROM t2 ORDER BY j, i")

}

// TestReaderCatalogTSAdvanceWithLongTxn confirms that timestamps can advance
// with PCR even if a long running txn is running.
func TestReaderCatalogTSAdvanceWithLongTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuress(t)

	ctx := context.Background()

	destSettings := cluster.MakeClusterSettings()
	const LeaseExpirationTime = time.Second * 15
	lease.LeaseDuration.Override(ctx, &destSettings.SV, LeaseExpirationTime)
	lease.LeaseJitterFraction.Override(ctx, &destSettings.SV, 0)
	r, cleanup := newReaderCatalogTest(t, ctx, base.TestingKnobs{}, destSettings)
	defer cleanup()

	ddlToExec := []string{
		"CREATE USER roacher WITH CREATEROLE;",
		"ALTER USER roacher SET timezone='America/New_York';",
		"CREATE SEQUENCE sq1;",
		"CREATE TABLE t1(n int default nextval('sq1'), val TEXT);",
		"INSERT INTO t1(val) VALUES('open');",
		"INSERT INTO t1(val) VALUES('closed');",
		"INSERT INTO t1(val) VALUES('inactive');",
		"CREATE TABLE t2(n int);",
	}
	for _, ddl := range ddlToExec {
		r.srcRunner.Exec(t, ddl)
	}

	require.NoError(t, r.advanceTS(ctx, r.ts.Clock().Now(), true))

	// Validate all tables match,
	r.compareEqual(t, "SELECT * FROM t1 ORDER BY n")
	r.compareEqual(t, "SELECT * FROM t2 ORDER BY n")
	r.compareEqual(t, "SELECT * FROM sq1")

	// Next attempt to advance the TS with a long-running
	// txn.
	tx := r.destRunner.Begin(t)
	_, err := tx.Exec("SELECT * FROM t1")
	require.NoError(t, err)

	// Attempt to advance the TS, this will wait for
	// the lease on t1 to expire.
	advanceStartTime := timeutil.Now()
	require.NoError(t, r.advanceTS(ctx, r.ts.Clock().Now(), true))
	// Confirm we waited for the lease to expire.
	require.LessOrEqual(t, LeaseExpirationTime, timeutil.Since(advanceStartTime))

	// Validate the long-running txn is fine and can
	// still commit after.
	_, err = tx.Exec("SELECT * FROM t1")
	require.NoError(t, err)
	_, err = tx.Exec("SELECT * FROM t2")
	require.NoError(t, err)
	_, err = tx.Exec("SELECT * FROM sq1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}
